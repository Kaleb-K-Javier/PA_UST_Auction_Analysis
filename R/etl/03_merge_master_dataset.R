# R/etl/03_merge_master_dataset.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 3: Construct Master Analysis Dataset
# ============================================================================
# Purpose: Consolidate Claims, Contracts, Engineering (Tanks), and Spatial data.
#          Performs feature engineering for Cost, Causal, and Policy analysis.
#
# Proven Logic (Validated in Audit):
#   1. Clean Keys: trimws() + clean_names() on all IDs.
#   2. Fix Schema: Rename linkage 'facility_id' -> 'permit_number'.
#   3. Primary Join: Claims -> Tanks (93.8% Match).
#   4. Spatial Join: Tanks -> Linkage (100% Match).
#
# Output: data/processed/master_analysis_dataset.rds
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(stringr)
  library(janitor)
  library(here)
})

cat("\n========================================\n")
cat("ETL Step 3: Master Dataset Construction\n")
cat("========================================\n\n")

# 1. LOAD & CLEAN DATA
# ----------------------------------------------------------------------------
cat("--- Loading Source Data ---\n")

# A. Claims (The Backbone)
claims_path <- here("data/processed/claims_clean.rds")
if (!file.exists(claims_path)) stop("Claims data missing. Run 01_load_ustif_data.R")
claims <- readRDS(claims_path)
setDT(claims)
claims <- janitor::clean_names(claims)

# FIX: Remove the "Sum" row artifact from the raw Excel file
# The raw file has a footer row where claim_number starts with "Sum:"
initial_rows <- nrow(claims)
claims <- claims[!str_detect(claim_number, "^Sum") & !is.na(department)]
if (nrow(claims) < initial_rows) cat(sprintf("✓ Removed %d summary/garbage rows from claims.\n", initial_rows - nrow(claims)))

# Standardize Key (XX-XXXXX)
claims[, department := trimws(as.character(department))]

# B. Contracts (The Intervention)
contracts_path <- here("data/processed/contracts_clean.rds")
if (file.exists(contracts_path)) {
  contracts <- readRDS(contracts_path)
  setDT(contracts)
  contracts <- janitor::clean_names(contracts)
} else {
  contracts <- NULL
  cat("! Warning: Contracts data not found. Analysis variables will be NA.\n")
}

# C. Master Tank Database (Engineering + Spatial from 02a)
tanks_path <- here("data/processed/master_tank_list.rds")
if (!file.exists(tanks_path)) stop("CRITICAL: master_tank_list.rds (02a) missing.")
tanks <- readRDS(tanks_path)
setDT(tanks)
tanks <- janitor::clean_names(tanks)

# Standardize Tank Keys
# The 02a output key is 'facility_id' (Excel) or 'permit_number' (GIS)
# We ensure we have a standard 'fac_id' for joining
tanks[, fac_id := trimws(as.character(ifelse(!is.na(facility_id), facility_id, permit_number)))]

cat(sprintf("Loaded: %d Claims, %d Tank Records\n", nrow(claims), nrow(tanks)))


# 2. FEATURE ENGINEERING: TANK FLEET PROFILE
# ----------------------------------------------------------------------------
cat("--- Constructing Tank Fleet Profiles (Time-Machine Logic) ---\n")

# Objective: Describe the facility *at the time of the claim*
# We join Claims to Tanks to calculate age, count, and risk profile.

# A. Prepare Dates
claims_mini <- claims[, .(claim_number, department, claim_date = as.IDate(claim_date))]
tanks[, install_date := as.IDate(install_date)]

# B. Join (Many Tanks per Claim)
tank_history <- merge(
  claims_mini,
  tanks,
  by.x = "department",
  by.y = "fac_id",
  all.x = TRUE,
  allow.cartesian = TRUE
)

# C. Calculate Tank Age & Status at Loss
tank_history[, tank_age_at_loss := as.numeric(claim_date - install_date) / 365.25]

# Define "Active at Loss": Installed before claim, not permanently closed before claim
# (Note: Data might lack closure dates, assuming active if no status says otherwise)
tank_history[, is_active_at_loss := !is.na(install_date) & install_date <= claim_date & 
               (is.na(status_code) | status_code != "P")] # P = Permanently Closed

# D. Aggregate to Facility Level
facility_profile <- tank_history[, .(
  # Scale
  n_tanks_total    = .N,
  n_tanks_active   = sum(is_active_at_loss, na.rm = TRUE),
  
  # Age Risk
  avg_tank_age     = mean(tank_age_at_loss[is_active_at_loss == TRUE], na.rm = TRUE),
  oldest_tank_age  = max(tank_age_at_loss[is_active_at_loss == TRUE], na.rm = TRUE),
  
  # Construction Risk (Keywords in description if available, or infer from date)
  has_pre_1990_tank = any(year(install_date) < 1990, na.rm = TRUE),
  
  # Substance Risk (Gasoline is more volatile than Heating Oil)
  has_gasoline     = any(str_detect(substance, "(?i)Gasoline|Unleaded"), na.rm = TRUE),
  has_diesel       = any(str_detect(substance, "(?i)Diesel"), na.rm = TRUE),
  
  # Spatial (Take first valid from linkage)
  county_eng       = first(na.omit(county)),
  municipality     = first(na.omit(municipality)),
  latitude         = first(na.omit(latitude)),
  longitude        = first(na.omit(longitude)),
  owner_name       = first(na.omit(owner_name))
  
), by = claim_number]

# 3. FEATURE ENGINEERING: CONTRACTS & INTERVENTIONS
# ----------------------------------------------------------------------------
cat("--- Aggregating Contracts (Interventions) ---\n")

contract_stats <- NULL
if (!is.null(contracts)) {
  contract_stats <- contracts[, .(
    # Financials
    total_contract_value = sum(total_contract_value, na.rm = TRUE),
    n_contracts          = .N,
    
    # Intervention Types
    is_pfp               = any(auction_type == "Bid-to-Result", na.rm = TRUE),
    is_sow               = any(auction_type == "Scope of Work", na.rm = TRUE),
    
    # Timing (Dynamic Causal Inference)
    date_first_contract  = min(contract_start, na.rm = TRUE),
    
    # Adjuster (Instrument)
    adjuster_id          = first(na.omit(adjuster))
  ), by = claim_number]
}

# 4. MASTER MERGE
# ----------------------------------------------------------------------------
cat("--- Merging All Components ---\n")

master <- merge(claims, facility_profile, by = "claim_number", all.x = TRUE)

if (!is.null(contract_stats)) {
  master <- merge(master, contract_stats, by = "claim_number", all.x = TRUE)
}

# 5. FINAL VARIABLE CREATION (For Analysis Scripts)
# ----------------------------------------------------------------------------
cat("--- Finalizing Analysis Variables ---\n")

# A. Financials & Cost Efficiency
# Ensure total_cost is robust (fallback to incurred if paid is 0/NA for open claims)
master[, total_cost := fcoalesce(total_paid, incurred_loss, 0)]
master[, log_cost := log(total_cost + 1)]
master[, burn_rate := total_cost / fmax(claim_duration_years, 0.1)] # $/Year

# B. Contract Types (Categorical for Regression 02_cost_correlates)
# Fix NAs for boolean logic
master[is.na(is_pfp), is_pfp := FALSE]
master[is.na(is_sow), is_sow := FALSE]
master[is.na(n_contracts), n_contracts := 0]

master[, contract_type := fcase(
  is_pfp == TRUE, "Bid-to-Result",
  is_sow == TRUE, "Scope of Work",
  n_contracts > 0, "Other Contract",
  default = "No Contract"
)]
# Set reference level for regressions
master[, contract_type := factor(contract_type, levels = c("No Contract", "Scope of Work", "Bid-to-Result", "Other Contract"))]

# Aliases for different scripts
master[, is_bid_to_result := is_pfp] 
master[, is_auction := (is_pfp | is_sow)] # For 01_descriptive_stats

# C. Time-to-Intervention (Crucial for Causal Dynamic Selection)
master[, time_to_auction := as.numeric(date_first_contract - claim_date) / 365.25]
master[is.na(time_to_auction), time_to_auction := 0] 

# D. Adjuster Instrument (Leave-One-Out Mean for 03_causal)
if ("adjuster_id" %in% names(master)) {
  # Calculate global stats per adjuster
  adj_stats <- master[!is.na(adjuster_id), .(
    total_claims_adj = .N,
    total_pfp_adj = sum(is_pfp, na.rm=TRUE)
  ), by = adjuster_id]
  
  # Join back and calculate LOO
  master <- merge(master, adj_stats, by = "adjuster_id", all.x = TRUE)
  master[, adjuster_leniency := (total_pfp_adj - fifelse(is_pfp, 1, 0)) / (total_claims_adj - 1)]
  
  # Clean up small sample adjusters
  master[total_claims_adj < 5, adjuster_leniency := NA]
  master[, c("total_pfp_adj", "total_claims_adj") := NULL]
}

# E. Geography & Fixed Effects
# Fill spatial gaps from tank engineering data if claim location is missing
master[is.na(county) & !is.na(county_eng), county := county_eng]
master[, county := str_to_title(trimws(county))] # Standardization

# Define Regions (Used in Policy Brief)
master[, region_cluster := fcase(
  county %in% c("Philadelphia", "Delaware", "Montgomery", "Bucks", "Chester"), "Southeast",
  county %in% c("Allegheny", "Washington", "Westmoreland"), "Southwest",
  default = "Other"
)]

# 6. SAVE
# ----------------------------------------------------------------------------
# Primary Output
saveRDS(master, here("data/processed/master_analysis_dataset.rds"))
fwrite(master, here("data/processed/master_analysis_dataset.csv"))

# Analysis Panel (Specific Alias for 03_causal)
saveRDS(master, here("data/processed/analysis_panel.rds"))

cat("\n========================================\n")
cat("SUCCESS: Master Dataset Created\n")
cat("========================================\n")
cat(sprintf("Total Claims:      %d\n", nrow(master)))
cat(sprintf("Matched w/ Tanks:  %d (%.1f%%)\n", sum(!is.na(master$n_tanks_total)), 100*mean(!is.na(master$n_tanks_total))))
cat(sprintf("Matched w/ Loc:    %d (%.1f%%)\n", sum(!is.na(master$latitude)), 100*mean(!is.na(master$latitude))))
cat(sprintf("Contract Data:     %d (%.1f%%)\n", sum(master$n_contracts > 0), 100*mean(master$n_contracts > 0)))
cat("----------------------------------------\n")
cat("Variables Created for Analysis:\n")
cat(" [01_descriptive] total_cost, is_auction, claim_duration_years\n")
cat(" [02_cost_corr]   log_cost, contract_type (Factor), region_cluster\n")
cat(" [03_causal]      is_pfp, adjuster_leniency, time_to_auction\n")
cat("========================================\n")