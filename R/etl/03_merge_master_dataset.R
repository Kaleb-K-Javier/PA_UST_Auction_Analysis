# R/etl/03_merge_master_dataset.R
# ============================================================================
# Pennsylvania UST Auction Analysis - ETL Step 3: Construct Master Dataset
# ============================================================================
# Purpose: Single consolidated script producing:
#   1. Full claim-level dataset for descriptive analysis
#   2. Facility characteristics (left-joined, NAs preserved)
#   3. IV construction (adjuster leniency)
#   4. analysis_sample flag for causal inference subset
#
# Output: data/processed/master_analysis_dataset.rds
# ============================================================================
# UNIT OF ANALYSIS: One row per claim_number
# ============================================================================

# Potential Error Sources:

# department format may not match permit_number if formatting differs (e.g., leading zeros). 
# Verify with head(claims$department) vs head(facility_linkage$permit_number).
# The adjuster field comes from contracts; claims without contracts have adjuster = NA and
#  thus has_valid_iv = FALSE. This is correct behavior but means non-auction claims are excluded from IV analysis by design.
# Script 04 is now obsolete — delete or rename to 04_DEPRECATED.R to avoid confusion.

cat("\n========================================\n")
cat("ETL Step 3: Constructing Master Dataset\n")
cat("========================================\n\n")

# Load dependencies
library(tidyverse)
library(lubridate)

# Load paths
if (!exists("paths")) {
  paths <- list(
    processed = "data/processed",
    external = "data/external",
    padep = "data/external/padep"
  )
}

# ============================================================================
# SECTION 1: LOAD SOURCE DATA
# ============================================================================

cat("Loading source datasets...\n")

# --- USTIF Data (Required) ---
claims <- readRDS(file.path(paths$processed, "claims_clean.rds"))
contracts <- readRDS(file.path(paths$processed, "contracts_clean.rds"))

cat(sprintf("  • Claims: %d records\n", nrow(claims)))
cat(sprintf("  • Contracts: %d records\n", nrow(contracts)))

n_claims_input <- nrow(claims)
n_unique_claims_input <- n_distinct(claims$claim_number)

# --- PA DEP Facility Data (Required for facility covariates) ---
facility_linkage_path <- file.path(paths$padep, "facility_linkage_table.csv")

if (!file.exists(facility_linkage_path)) {
  warning(sprintf(
    "Facility linkage table not found at: %s\n
     Facility covariates will be unavailable. Run R/etl/02a_padep_download.R first.",
    facility_linkage_path
  ))
  facility_linkage <- NULL
} else {
  facility_linkage <- read_csv(facility_linkage_path, show_col_types = FALSE)
  cat(sprintf("  • Facility linkage: %d facilities\n", nrow(facility_linkage)))
}

cat("\n")

# ============================================================================
# SECTION 2: JOIN CLAIMS ← CONTRACTS
# ============================================================================
# Join type: LEFT JOIN (preserve all claims)
# Cardinality: One claim may have multiple contracts (M:M)
# Strategy: After join, aggregate to claim-level

cat("Joining claims with contracts...\n")

master <- claims %>%
  left_join(
    contracts %>%
      select(
        claim_number,
        contract_id,
        auction_type,
        bid_type,
        contract_type,
        base_price,
        amendments_total,
        total_contract_value,
        contract_start,
        contract_end,
        brings_to_closure_flag,
        adjuster,
        site_name
      ),
    by = "claim_number",
    relationship = "many-to-many"
  )

# --- Cartesian Explosion Check ---
expansion_ratio <- nrow(master) / n_claims_input
if (expansion_ratio > 2.0) {
  warning(sprintf("CARTESIAN EXPLOSION: %.1fx expansion. Review contracts for duplicates.", expansion_ratio))
}
cat(sprintf("  • Post-join rows: %d (%.2fx expansion)\n", nrow(master), expansion_ratio))

# ============================================================================
# SECTION 3: CREATE CLAIM-LEVEL VARIABLES (Pre-Aggregation)
# ============================================================================

cat("\nCreating claim-level variables...\n")

master <- master %>%
  mutate(
    # --- Cost Definition (Status-Dependent) ---
    # Closed claims: actual expenditure (total_paid)
    # Open claims: incurred loss (paid + reserves)
    total_cost = case_when(
      is_closed ~ coalesce(total_paid, 0),
      !is_closed ~ coalesce(incurred_loss, total_paid, 0),
      TRUE ~ coalesce(total_paid, 0)
    ),
    log_total_cost = log(total_cost + 1),
    
    # --- Contract/Auction Indicators ---
    has_contract = !is.na(contract_id),
    is_auction = has_contract & auction_type %in% c("Scope of Work", "Bid-to-Result"),
    is_sow = auction_type == "Scope of Work",
    is_bid_to_result = auction_type == "Bid-to-Result",
    
    # --- Treatment Variable for Causal Inference ---
    # PFP (Pay-for-Performance) = Bid-to-Result contracts
    # Control = Everything else (T&M, no contract)
    # NOTE: SOW excluded from analysis_sample (confounds incentive comparison)
    is_PFP = (auction_type == "Bid-to-Result"),
    
    # --- Temporal ---
    fiscal_year = ifelse(month(claim_date) >= 7, year(claim_date) + 1, year(claim_date)),
    era = case_when(
      claim_year < 2000 ~ "Pre-2000",
      claim_year >= 2000 & claim_year < 2005 ~ "2000-2004",
      claim_year >= 2005 & claim_year < 2010 ~ "2005-2009",
      claim_year >= 2010 & claim_year < 2015 ~ "2010-2014",
      claim_year >= 2015 ~ "2015+",
      TRUE ~ "Unknown"
    ),
    
    # --- Geographic ---
    county_clean = str_to_title(str_trim(county))
  )

# ============================================================================
# SECTION 4: AGGREGATE TO CLAIM LEVEL
# ============================================================================
# Output: Exactly one row per claim_number

cat("Aggregating to claim level...\n")

master_agg <- master %>%
  group_by(claim_number) %>%
  summarise(
    # --- Identifiers ---
    department = first(department),
    claimant_name = first(claimant_name),
    
    # --- Geography ---
    county = first(county_clean),
    dep_region = first(dep_region),
    
    # --- Dates ---
    claim_date = first(claim_date),
    loss_reported_date = first(loss_reported_date),
    closed_date = first(closed_date),
    claim_year = first(claim_year),
    fiscal_year = first(fiscal_year),
    era = first(era),
    
    # --- Status ---
    claim_status = first(claim_status),
    is_closed = first(is_closed),
    is_open = first(is_open),
    claim_duration_days = first(claim_duration_days),
    claim_duration_years = first(claim_duration_years),
    
    # --- Costs ---
    paid_loss = first(paid_loss),
    paid_alae = first(paid_alae),
    incurred_loss = first(incurred_loss),
    total_paid = first(total_paid),
    total_cost = first(total_cost),
    log_total_cost = first(log_total_cost),
    
    # --- Contract Info (Aggregated) ---
    n_contracts = sum(!is.na(contract_id)),
    has_contract = any(!is.na(contract_id)),
    is_auction = any(is_auction, na.rm = TRUE),
    is_sow = any(is_sow, na.rm = TRUE),
    is_bid_to_result = any(is_bid_to_result, na.rm = TRUE),
    is_PFP = any(is_PFP, na.rm = TRUE),  # Treatment indicator
    brings_to_closure = any(brings_to_closure_flag, na.rm = TRUE),
    
    # --- Contract Costs ---
    total_contract_value = sum(total_contract_value, na.rm = TRUE),
    first_contract_date = min(contract_start, na.rm = TRUE),
    
    # --- Adjuster (for IV) ---
    # If multiple contracts, take first non-NA adjuster
    adjuster = first(na.omit(adjuster)),
    
    .groups = "drop"
  ) %>%
  mutate(
    # Clean Inf values
    first_contract_date = if_else(is.infinite(first_contract_date), NA_Date_, first_contract_date),
    total_contract_value = if_else(total_contract_value == 0 & !has_contract, NA_real_, total_contract_value),
    # Ensure is_PFP is FALSE (not NA) for claims without contracts
    is_PFP = coalesce(is_PFP, FALSE)
  )

# --- Validation: 1:1 mapping ---
stopifnot(nrow(master_agg) == n_distinct(master_agg$claim_number))
cat(sprintf("  • Aggregated to %d unique claims ✓\n", nrow(master_agg)))

# ============================================================================
# SECTION 5: LEFT JOIN FACILITY CHARACTERISTICS
# ============================================================================
# Join: claims.department = facility_linkage.permit_number
# Type: LEFT JOIN (preserve all claims; NAs for unmatched facilities)

cat("\nJoining facility characteristics...\n")

if (!is.null(facility_linkage)) {
  
  # Select relevant facility columns
  facility_covariates <- facility_linkage %>%
    select(
      permit_number,
      efacts_facility_id,
      site_id,
      facility_name,
      latitude,
      longitude,
      owner_id,
      owner_name,
      registration_status
    ) %>%
    # Deduplicate if needed (take first occurrence per permit)
    distinct(permit_number, .keep_all = TRUE)
  
  # Join
  master_agg <- master_agg %>%
    left_join(
      facility_covariates,
      by = c("department" = "permit_number")
    )
  
  # --- Match Rate Diagnostic ---
  n_matched <- sum(!is.na(master_agg$efacts_facility_id))
  match_rate <- n_matched / nrow(master_agg) * 100
  
  cat(sprintf("  • Facility match rate: %d / %d (%.1f%%)\n", 
              n_matched, nrow(master_agg), match_rate))
  
  if (match_rate < 50) {
    warning(sprintf("LOW FACILITY MATCH RATE: %.1f%%. Check department format.", match_rate))
  }
  
  # --- Create Missing Facility Flag ---
  master_agg <- master_agg %>%
    mutate(facility_data_missing = is.na(efacts_facility_id))
  
} else {
  # No facility data available
  master_agg <- master_agg %>%
    mutate(
      efacts_facility_id = NA_integer_,
      site_id = NA_integer_,
      facility_name = NA_character_,
      latitude = NA_real_,
      longitude = NA_real_,
      owner_id = NA_integer_,
      owner_name = NA_character_,
      registration_status = NA_character_,
      facility_data_missing = TRUE
    )
  cat("  • Facility data unavailable; columns added as NA\n")
}

# ============================================================================
# SECTION 6: CONSTRUCT INSTRUMENTAL VARIABLE (Adjuster Leniency)
# ============================================================================
# Instrument: Leave-one-out mean of adjuster's PFP assignment rate
# Validity requires: adjuster has >10 claims (stable estimate)

cat("\nConstructing adjuster leniency instrument...\n")

# --- Step 1: Count claims per adjuster ---
adjuster_counts <- master_agg %>%
  filter(!is.na(adjuster)) %>%
  count(adjuster, name = "adjuster_n_claims")

# --- Step 2: Identify valid adjusters (>10 claims) ---
valid_adjusters <- adjuster_counts %>%
  filter(adjuster_n_claims > 10) %>%
  pull(adjuster)

cat(sprintf("  • Total adjusters: %d\n", n_distinct(master_agg$adjuster, na.rm = TRUE)))
cat(sprintf("  • Valid adjusters (>10 claims): %d\n", length(valid_adjusters)))

# --- Step 3: Calculate Leave-One-Out Leniency ---
# For each observation, calculate adjuster's PFP rate EXCLUDING that observation
master_agg <- master_agg %>%
  left_join(adjuster_counts, by = "adjuster") %>%
  group_by(adjuster) %>%
  mutate(
    # Total PFP assignments by this adjuster
    adjuster_pfp_total = sum(is_PFP, na.rm = TRUE),
    # Leave-one-out: exclude current observation
    adjuster_pfp_loo = adjuster_pfp_total - as.numeric(is_PFP),
    # LOO mean (adjuster's leniency excluding this case)
    adjuster_leniency = if_else(
      adjuster_n_claims > 1,
      adjuster_pfp_loo / (adjuster_n_claims - 1),
      NA_real_
    )
  ) %>%
  ungroup() %>%
  # Clean up intermediate columns
  select(-adjuster_pfp_total, -adjuster_pfp_loo) %>%
  # Flag valid IV observations
  mutate(
    has_valid_iv = !is.na(adjuster) & adjuster %in% valid_adjusters & !is.na(adjuster_leniency)
  )

n_valid_iv <- sum(master_agg$has_valid_iv)
cat(sprintf("  • Observations with valid IV: %d (%.1f%%)\n", 
            n_valid_iv, 100 * n_valid_iv / nrow(master_agg)))

# ============================================================================
# SECTION 7: CREATE ANALYSIS SAMPLE FLAG
# ============================================================================
# analysis_sample = 1 for observations valid for causal inference
# Criteria:
#   1. Closed claims only (open claims have censored costs)
#   2. total_paid > $1,000 (exclude administrative noise)
#   3. NOT Scope of Work (SOW confounds PFP vs T&M comparison)
#   4. Has valid IV (adjuster with >10 claims)

cat("\nCreating analysis sample flag...\n")

master_agg <- master_agg %>%
  mutate(
    analysis_sample = as.integer(
      is_closed == TRUE &
      total_paid > 1000 &
      !is_sow &
      has_valid_iv
    )
  )

n_analysis <- sum(master_agg$analysis_sample, na.rm = TRUE)
cat(sprintf("  • Analysis sample: %d / %d claims (%.1f%%)\n",
            n_analysis, nrow(master_agg), 100 * n_analysis / nrow(master_agg)))

# Breakdown of exclusions
cat("\n  Exclusion breakdown:\n")
cat(sprintf("    - Open claims: %d\n", sum(!master_agg$is_closed, na.rm = TRUE)))
cat(sprintf("    - total_paid ≤ $1,000: %d\n", sum(master_agg$total_paid <= 1000, na.rm = TRUE)))
cat(sprintf("    - Scope of Work: %d\n", sum(master_agg$is_sow, na.rm = TRUE)))
cat(sprintf("    - Invalid IV (adjuster <10 claims): %d\n", sum(!master_agg$has_valid_iv)))

# ============================================================================
# SECTION 8: CREATE FINAL DERIVED VARIABLES
# ============================================================================

cat("\nCreating final derived variables...\n")

master_agg <- master_agg %>%
  mutate(
    # --- Cost Categories ---
    cost_category = cut(
      total_cost,
      breaks = c(0, 10000, 50000, 100000, 250000, 500000, Inf),
      labels = c("$0-10K", "$10-50K", "$50-100K", "$100-250K", "$250-500K", "$500K+"),
      include.lowest = TRUE
    ),
    
    # --- Auction Type Factor (for descriptive analysis) ---
    auction_type_factor = case_when(
      is_bid_to_result ~ "Bid-to-Result",
      is_sow ~ "Scope of Work",
      has_contract ~ "Other Contract",
      TRUE ~ "No Contract"
    ) %>% factor(levels = c("No Contract", "Scope of Work", "Bid-to-Result", "Other Contract")),
    
    # --- Time to Intervention (for policy analysis) ---
    # Years from claim filing to first contract
    time_to_intervention_years = as.numeric(
      difftime(first_contract_date, claim_date, units = "days")
    ) / 365.25,
    
    # --- Region Factor ---
    region_factor = factor(dep_region),
    
    # --- Year Factor (for FE) ---
    claim_year_factor = factor(claim_year)
  )

# ============================================================================
# SECTION 9: FINAL VALIDATION
# ============================================================================

cat("\nRunning final validation...\n")

# Check 1: One row per claim
stopifnot(nrow(master_agg) == n_distinct(master_agg$claim_number))

# Check 2: No negative costs
stopifnot(all(master_agg$total_cost >= 0, na.rm = TRUE))

# Check 3: analysis_sample is 0/1
stopifnot(all(master_agg$analysis_sample %in% c(0, 1, NA)))

# Check 4: IV only defined for valid adjusters
stopifnot(all(is.na(master_agg$adjuster_leniency[!master_agg$has_valid_iv]) | 
              master_agg$adjuster_leniency[!master_agg$has_valid_iv] == 0, na.rm = TRUE))

cat("  ✓ All validation checks passed\n")

# ============================================================================
# SECTION 10: SAVE OUTPUT
# ============================================================================

cat("\nSaving master dataset...\n")

saveRDS(master_agg, file.path(paths$processed, "master_analysis_dataset.rds"))
write_csv(master_agg, file.path(paths$processed, "master_analysis_dataset.csv"))

cat("✓ Saved: data/processed/master_analysis_dataset.rds\n")
cat("✓ Saved: data/processed/master_analysis_dataset.csv\n")

# ============================================================================
# SUMMARY
# ============================================================================

cat("\n")
cat("════════════════════════════════════════════════════════════════════════\n")
cat("                     MASTER DATASET COMPLETE                            \n")
cat("════════════════════════════════════════════════════════════════════════\n")
cat("\n")
cat("DATASET STRUCTURE:\n")
cat(sprintf("  • Total observations: %d claims\n", nrow(master_agg)))
cat(sprintf("  • Unit of analysis: claim_number (1 row per claim)\n"))
cat("\n")
cat("SAMPLE DEFINITIONS:\n")
cat(sprintf("  • Full sample (descriptive): %d claims\n", nrow(master_agg)))
cat(sprintf("  • Analysis sample (causal):  %d claims (filter: analysis_sample == 1)\n", n_analysis))
cat("\n")
cat("KEY VARIABLES:\n")
cat("  • Outcome:    total_cost, log_total_cost\n")
cat("  • Treatment:  is_PFP (1 = Bid-to-Result auction)\n")
cat("  • Instrument: adjuster_leniency (leave-one-out mean)\n")
cat("  • Sample:     analysis_sample (1 = valid for causal inference)\n")
cat("\n")
cat("FACILITY LINKAGE:\n")
cat(sprintf("  • Match rate: %.1f%%\n", 100 * sum(!master_agg$facility_data_missing) / nrow(master_agg)))
cat("  • Missing flag: facility_data_missing\n")
cat("\n")
cat("USAGE:\n")
cat("  # Descriptive analysis (full sample)\n")
cat("  master <- readRDS('data/processed/master_analysis_dataset.rds')\n")
cat("\n")
cat("  # Causal inference (restricted sample)\n")
cat("  analysis <- master %>% filter(analysis_sample == 1)\n")
cat("\n")
cat("════════════════════════════════════════════════════════════════════════\n\n")