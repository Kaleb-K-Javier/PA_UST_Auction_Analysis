# R/etl/03_merge_master_dataset.R
# ============================================================================
# Pennsylvania UST Auction Analysis - ETL Step 3: Construct Master Dataset
# ============================================================================
# Purpose: Single consolidated script producing the Master Analysis Dataset.
#          Implemented in data.table for performance.
#
# Logic:
#   1. Claims + Contracts (Many-to-Many Join -> Aggregation)
#   2. Facility Linkage (Left Join)
#   3. Tank History (Temporal Join: Active/Closed status relative to Claim Date)
#   4. Instrumental Variable (Leave-One-Out Adjuster Leniency)
#
# Output: data/processed/master_analysis_dataset.rds
# ============================================================================

cat("\n========================================\n")
cat("ETL Step 3: Master Merge (data.table Edition)\n")
cat("========================================\n\n")

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(stringr)
  library(janitor)
})

# Paths
paths <- list(
  processed = "data/processed",
  padep     = "data/external/padep"
)

# ============================================================================
# SECTION 1: LOAD SOURCE DATA
# ============================================================================
cat("Loading source datasets...\n")

# 1. Claims & Contracts
claims    <- readRDS(file.path(paths$processed, "claims_clean.rds"))
contracts <- readRDS(file.path(paths$processed, "contracts_clean.rds"))

setDT(claims)
setDT(contracts)

# 2. Facility Linkage
linkage_path <- file.path(paths$padep, "facility_linkage_table.csv")
if (file.exists(linkage_path)) {
  facility_linkage <- fread(linkage_path)
  # Deduplicate by permit_number to prevent explosion
  facility_linkage <- unique(facility_linkage, by = "permit_number")
} else {
  stop("CRITICAL: Facility linkage table missing. Run 02a first.")
}

# 3. Tank Data (SSRS Wide Format)
tank_path <- "data/processed/ssrs_tank_master_wide.csv"
if (file.exists(tank_path)) {
  tanks_raw <- fread(tank_path)
  
  # Clean names and types
  setnames(tanks_raw, old = names(tanks_raw), new = make_clean_names(names(tanks_raw)))
  
  tanks_raw[, date_installed := as.IDate(date_installed, format = "%Y-%m-%d")]
  
  # Handle missing date_closed
  if (!"date_closed" %in% names(tanks_raw)) {
    warning("SSRS data missing 'date_closed'. Closure metrics will be empty.")
    tanks_raw[, date_closed := as.IDate(NA)]
  } else {
    tanks_raw[, date_closed := as.IDate(date_closed, format = "%Y-%m-%d")]
  }
} else {
  warning("SSRS Tank data missing. Run 02d first.")
  tanks_raw <- NULL
}

# ============================================================================
# SECTION 2: JOIN CLAIMS & CONTRACTS
# ============================================================================
cat("Joining claims with contracts...\n")

# Select relevant contract columns
cols_contract <- c("claim_number", "contract_id", "auction_type", "adjuster", 
                   "total_contract_value", "contract_start", "brings_to_closure_flag")

# Merge Claims <- Contracts (Left Join, Allow Many-to-Many)
# NOTE: Claims without contracts will have NA adjuster.
master_joined <- merge(
  claims, 
  contracts[, ..cols_contract], 
  by = "claim_number", 
  all.x = TRUE, 
  allow.cartesian = TRUE
)

# ============================================================================
# SECTION 3: AGGREGATE TO CLAIM LEVEL
# ============================================================================
cat("Aggregating to claim level...\n")

# Helper function for first non-NA value
first_val <- function(x) first(na.omit(x))

master_agg <- master_joined[, .(
  # Identifiers
  department = first(department),
  claimant_name = first(claimant_name),
  claim_date = as.IDate(first(claim_date)),
  
  # Financials
  total_cost = first(fcase(
    is_closed == TRUE, fcoalesce(total_paid, 0),
    default = fcoalesce(incurred_loss, total_paid, 0)
  )),
  
  # Contract Flags
  has_contract = any(!is.na(contract_id)),
  is_pfp = any(auction_type == "Bid-to-Result", na.rm = TRUE),
  adjuster = first(na.omit(adjuster)), # First valid adjuster
  
  # Status
  is_closed = first(is_closed),
  claim_duration_years = first(claim_duration_years),
  
  # Metrics
  total_contract_value = sum(total_contract_value, na.rm = TRUE),
  first_contract_date = min(contract_start, na.rm = TRUE)
  
), by = claim_number]

# Cleanup Infinite dates
master_agg[is.infinite(first_contract_date), first_contract_date := NA]
master_agg[is.na(is_pfp), is_pfp := FALSE]

# Join Facility Linkage (Left Join)
cat("Joining facility characteristics...\n")
cols_linkage <- c("permit_number", "efacts_facility_id", "facility_name", "county")

master_agg <- merge(
  master_agg,
  facility_linkage[, ..cols_linkage],
  by.x = "department",
  by.y = "permit_number",
  all.x = TRUE
)

# ============================================================================
# SECTION 4: TEMPORAL TANK AGGREGATION
# ============================================================================
# TODO: Handle temporal tank status relative to claim date.
# We compare claim_date vs date_installed/date_closed to count active tanks at time of loss.

cat("Calculating temporal tank statistics (Active vs Closed History)...\n")

if (!is.null(tanks_raw)) {
  
  # 1. Isolate valid claims for join
  valid_claims <- master_agg[!is.na(efacts_facility_id), .(claim_number, efacts_facility_id, claim_date)]
  
  # 2. Cartesian Join (Claims * Tanks for that Facility)
  joined <- merge(
    valid_claims,
    tanks_raw,
    by.x = "efacts_facility_id",
    by.y = "facility_id",
    all.x = TRUE,
    allow.cartesian = TRUE
  )
  
  # 3. Define Status at Time of Loss
  joined[, tank_status_at_loss := fcase(
    # Active: Installed before claim AND (Still Open OR Closed after claim)
    !is.na(date_installed) & date_installed <= claim_date & (is.na(date_closed) | date_closed >= claim_date), "ACTIVE",
    # Prior Closed: Closed before claim
    !is.na(date_closed) & date_closed < claim_date, "PRIOR_CLOSED",
    default = "OTHER"
  )]
  
  # 4. Calculate Days Since Closure (for Prior Closed)
  joined[tank_status_at_loss == "PRIOR_CLOSED", days_since_closure := as.integer(claim_date - date_closed)]
  
  # 5. Aggregate Stats
  tank_stats <- joined[, .(
    # --- Active Tanks ---
    n_active_tanks = sum(tank_status_at_loss == "ACTIVE", na.rm = TRUE),
    active_has_double_wall = any(tank_status_at_loss == "ACTIVE" & str_detect(tank_construction, "(?i)DOUBLE|JACKETED"), na.rm=TRUE),
    active_has_steel       = any(tank_status_at_loss == "ACTIVE" & str_detect(tank_construction, "(?i)STEEL"), na.rm=TRUE),
    active_avg_age         = mean(ifelse(tank_status_at_loss == "ACTIVE", year(claim_date) - year(date_installed), NA), na.rm=TRUE),
    
    # --- Prior Closed Tanks ---
    n_closed_total = sum(tank_status_at_loss == "PRIOR_CLOSED", na.rm = TRUE),
    closed_has_double_wall = any(tank_status_at_loss == "PRIOR_CLOSED" & str_detect(tank_construction, "(?i)DOUBLE|JACKETED"), na.rm=TRUE),
    closed_has_steel       = any(tank_status_at_loss == "PRIOR_CLOSED" & str_detect(tank_construction, "(?i)STEEL"), na.rm=TRUE),
    
    # --- Closure Timing Buckets ---
    n_closed_0_30d   = sum(tank_status_at_loss == "PRIOR_CLOSED" & days_since_closure <= 30, na.rm=TRUE),
    n_closed_31_60d  = sum(tank_status_at_loss == "PRIOR_CLOSED" & days_since_closure > 30 & days_since_closure <= 60, na.rm=TRUE),
    n_closed_61_90d  = sum(tank_status_at_loss == "PRIOR_CLOSED" & days_since_closure > 60 & days_since_closure <= 90, na.rm=TRUE),
    n_closed_91_365d = sum(tank_status_at_loss == "PRIOR_CLOSED" & days_since_closure > 90 & days_since_closure <= 365, na.rm=TRUE),
    n_closed_gt_1yr  = sum(tank_status_at_loss == "PRIOR_CLOSED" & days_since_closure > 365, na.rm=TRUE)
    
  ), by = claim_number]
  
  # 6. Merge Back
  master_agg <- merge(master_agg, tank_stats, by = "claim_number", all.x = TRUE)
  
  # Fill NAs with 0 for counts where facility matched but no valid tanks found
  fill_cols <- c("n_active_tanks", "n_closed_total", "n_closed_0_30d", "n_closed_31_60d", "n_closed_61_90d", "n_closed_91_365d", "n_closed_gt_1yr")
  
  # Only fill if facility_id is present (if facility missing, we genuinely don't know, so leave NA)
  for (col in fill_cols) {
    master_agg[!is.na(efacts_facility_id) & is.na(get(col)), (col) := 0]
  }
  
} else {
  cat("  ! Skipping temporal tank logic (Data missing)\n")
}

# ============================================================================
# SECTION 5: IV & FINAL CLEANUP
# ============================================================================
cat("Constructing IV and finalizing...\n")

# IV: Adjuster Leniency
# Calculate counts per adjuster
master_agg[, adjuster_n_claims := .N, by = adjuster]

# Identify valid adjusters (>10 claims)
valid_adjs <- master_agg[!is.na(adjuster) & adjuster_n_claims > 10, unique(adjuster)]

# Calculate IV (Leave-One-Out Mean)
master_agg[, has_valid_iv := FALSE]
master_agg[adjuster %in% valid_adjs, `:=`(
  adjuster_leniency = (sum(is_pfp) - is_pfp) / (.N - 1),
  has_valid_iv = TRUE
), by = adjuster]

# Analysis Sample Flag
master_agg[, analysis_sample := as.integer(
  is_closed == TRUE & 
  total_cost > 1000 & 
  has_valid_iv == TRUE &
  !is.na(is_pfp) # Ensure PFP is defined
)]

# Derived Vars
master_agg[, cost_category := cut(
  total_cost,
  breaks = c(0, 10000, 50000, 100000, 250000, 500000, Inf),
  labels = c("$0-10K", "$10-50K", "$50-100K", "$100-250K", "$250-500K", "$500K+"),
  include.lowest = TRUE
)]

master_agg[, time_to_intervention_years := as.numeric(
  difftime(first_contract_date, claim_date, units = "days")
) / 365.25]

# Save
saveRDS(master_agg, file.path(paths$processed, "master_analysis_dataset.rds"))
fwrite(master_agg, file.path(paths$processed, "master_analysis_dataset.csv"))

cat("\n✓ Master Dataset Created Successfully.\n")
cat(sprintf("  Total Claims: %d\n", nrow(master_agg)))
cat(sprintf("  Analysis Sample: %d\n", sum(master_agg$analysis_sample, na.rm=TRUE)))