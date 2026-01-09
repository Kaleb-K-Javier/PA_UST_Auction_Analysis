# R/etl/03_merge_master_dataset.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 3: Construct Master Analysis Dataset
# ============================================================================
# Purpose: Merge Claims + Contracts + Tanks (02d) + Linkage + Compliance
# 
# Inputs:
#   - data/processed/claims_clean.rds
#   - data/processed/contracts_clean.rds
#   - data/processed/pa_ust_master_facility_tank_database.rds
#   - data/processed/facility_linkage_table.csv
#   - data/processed/efacts_violations.csv (if LOAD_EFACTS = TRUE)
#   - data/processed/efacts_inspections.csv (if LOAD_EFACTS = TRUE)
#
# Outputs:
#   - data/processed/master_analysis_dataset.rds
#   - data/processed/analysis_panel.rds
#   - data/processed/iv_analysis_panel.rds
# ============================================================================

# ============================================================================
# CONTROL FLAGS
# ============================================================================

LOAD_EFACTS <- FALSE  # Set TRUE when eFACTS scrape is complete

# ============================================================================

cat("\n========================================\n")
cat("ETL Step 3: Master Dataset Construction\n")
cat("========================================\n\n")

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(stringr)
  library(janitor)
  library(here)
})

paths <- list(processed = here("data/processed"),
              padep = here('data', 'external', 'padep'))
# ============================================================================
# 1. LOAD DATA
# ============================================================================

cat("--- Loading Data ---\n")

# Claims
claims <- readRDS(file.path(paths$processed, "claims_clean.rds"))
setDT(claims)
claims[, department := trimws(as.character(department))]
claims[, claim_number := trimws(as.character(claim_number))]
claims <- claims[!str_detect(claim_number, "^Sum") & !is.na(department)]
cat(sprintf("  Claims: %d\n", nrow(claims)))

# Contracts
contracts <- readRDS(file.path(paths$processed, "contracts_clean.rds"))
setDT(contracts)
contracts[, claim_number := trimws(as.character(claim_number))]
cat(sprintf("  Contracts: %d\n", nrow(contracts)))

# Tanks (02d output)
tanks <- readRDS(file.path(paths$processed, "pa_ust_master_facility_tank_database.rds"))
setDT(tanks)
tanks[, FAC_ID := trimws(as.character(FAC_ID))]
tanks[, DATE_INSTALLED := as.IDate(DATE_INSTALLED)]
cat(sprintf("  Tanks: %d\n", nrow(tanks)))

# Linkage
# Linkage
linkage <- fread(file.path(paths$padep, "facility_linkage_table.csv"))
linkage[, permit_number := trimws(as.character(facility_id))]
cat(sprintf("  Linkage: %d\n", nrow(linkage)))

# Compliance (conditional)
if (LOAD_EFACTS) {
  violations <- fread(file.path(paths$processed, "efacts_violations.csv"))
  inspections <- fread(file.path(paths$processed, "efacts_inspections.csv"))
  cat(sprintf("  Violations: %d\n", nrow(violations)))
  cat(sprintf("  Inspections: %d\n", nrow(inspections)))
} else {
  cat("  eFACTS: SKIPPED (LOAD_EFACTS = FALSE)\n")
}

# ============================================================================
# 2. TANK FLEET PROFILES
# ============================================================================

cat("\n--- Building Tank Profiles ---\n")

claims_mini <- claims[, .(claim_number, department, claim_date = as.IDate(claim_date))]

# Join claims to tanks (many tanks per claim)
tank_history <- merge(
  claims_mini, tanks,
  by.x = "department", by.y = "FAC_ID",
  all.x = TRUE, allow.cartesian = TRUE
)

# Tank age at claim date
tank_history[, tank_age_at_claim := as.numeric(claim_date - DATE_INSTALLED) / 365.25]

# Active at claim: installed before claim, not closed
tank_history[, is_active := 
  !is.na(DATE_INSTALLED) & 
  DATE_INSTALLED <= claim_date & 
  (is_closed == 0 | is.na(is_closed))]

# Aggregate to claim level
facility_profile <- tank_history[, .(
  # Tank counts
  n_tanks_total = .N,
  n_tanks_active = sum(is_active, na.rm = TRUE),
  
  # Age metrics
  avg_tank_age = mean(tank_age_at_claim[is_active == TRUE], na.rm = TRUE),
  oldest_tank_age = max(tank_age_at_claim[is_active == TRUE], na.rm = TRUE),
  
  # Construction era risk
  has_pre_1990_tank = any(year(DATE_INSTALLED) < 1990, na.rm = TRUE),
  has_pre_1998_tank = any(year(DATE_INSTALLED) < 1998, na.rm = TRUE),
  
  # Construction type
  has_single_wall = any(str_detect(TANK_CONSTRUCTION, "SINGLE|UNPROTECTED"), na.rm = TRUE),
  has_double_wall = any(str_detect(TANK_CONSTRUCTION, "DOUBLE"), na.rm = TRUE),
  has_fiberglass = any(str_detect(TANK_CONSTRUCTION, "FIBERGLASS|FRP"), na.rm = TRUE),
  has_cathodic = any(str_detect(TANK_CONSTRUCTION, "CATHODIC"), na.rm = TRUE),
  
 # Substance risk (full PADEP code list)
  has_gasoline = any(SUBSTANCE_CODE %in% c("GAS", "GASOL"), na.rm = TRUE),
  has_diesel = any(SUBSTANCE_CODE == "DIESL", na.rm = TRUE),
  has_heating_oil = any(SUBSTANCE_CODE == "HO", na.rm = TRUE),
  has_fuel_oil = any(SUBSTANCE_CODE == "FO", na.rm = TRUE),
  has_kerosene = any(SUBSTANCE_CODE == "KERO", na.rm = TRUE),
  has_jet_fuel = any(SUBSTANCE_CODE == "JET", na.rm = TRUE),
  has_aviation_gas = any(SUBSTANCE_CODE == "AVGAS", na.rm = TRUE),
  has_used_oil = any(SUBSTANCE_CODE %in% c("UMO", "USDOL", "WO"), na.rm = TRUE),
  has_new_motor_oil = any(SUBSTANCE_CODE == "NMO", na.rm = TRUE),
  has_biodiesel = any(SUBSTANCE_CODE == "BIDSL", na.rm = TRUE),
  has_ethanol = any(SUBSTANCE_CODE == "ETHNL", na.rm = TRUE),
  has_hazardous = any(SUBSTANCE_CODE %in% c("HZPRL", "HZSUB"), na.rm = TRUE),
  has_unknown_substance = any(SUBSTANCE_CODE %in% c("UNK", "OTHER", "NPOIL"), na.rm = TRUE),
  
  # Capacity
  total_capacity = sum(CAPACITY[is_active == TRUE], na.rm = TRUE),
  max_capacity = max(CAPACITY[is_active == TRUE], na.rm = TRUE)
  
), by = claim_number]

# Fix infinites
facility_profile[is.infinite(oldest_tank_age), oldest_tank_age := NA]
facility_profile[is.infinite(max_capacity), max_capacity := NA]

cat(sprintf("  Profiles: %d claims\n", nrow(facility_profile)))

# ============================================================================
# 3. SPATIAL FEATURES
# ============================================================================
## no zip code data in the dataset atm
# cat("\n--- Spatial Features ---\n")

# spatial <- linkage[, .(
#   permit_number, latitude, longitude, municipality, zip,
#   owner_name, efacts_facility_id
# )]

# # Density per zip
# zip_density <- linkage[, .(facility_density_zip = .N), by = zip]
# spatial <- merge(spatial, zip_density, by = "zip", all.x = TRUE)

# ============================================================================
# 4. COMPLIANCE FEATURES
# ============================================================================

cat("\n--- Compliance Features ---\n")

if (LOAD_EFACTS) {
  compliance <- violations[, .(
    n_violations = .N,
    total_penalties = sum(penalty_assessed, na.rm = TRUE)
  ), by = efacts_facility_id]
  
  insp_summary <- inspections[, .(
    n_inspections = .N,
    n_with_violations = sum(str_detect(tolower(result), "violation"), na.rm = TRUE)
  ), by = efacts_facility_id]
  
  compliance <- merge(compliance, insp_summary, by = "efacts_facility_id", all = TRUE)
  cat(sprintf("  Compliance records: %d\n", nrow(compliance)))
} else {
  compliance <- NULL
  cat("  Compliance: SKIPPED (will create NA columns)\n")
}

# ============================================================================
# 5. CONTRACT AGGREGATION
# ============================================================================

cat("\n--- Contract Stats ---\n")

contract_stats <- contracts[, .(
  total_contract_value = sum(total_contract_value, na.rm = TRUE),
  n_contracts = .N,
  is_pfp = any(auction_type == "Bid-to-Result", na.rm = TRUE),
  is_sow = any(auction_type == "Scope of Work", na.rm = TRUE),
  date_first_contract = min(contract_start, na.rm = TRUE),
  adjuster_id = first(na.omit(adjuster)),
  brings_to_closure = any(brings_to_closure_flag == TRUE, na.rm = TRUE)
), by = claim_number]

contract_stats[is.infinite(as.numeric(date_first_contract)), date_first_contract := NA]

# ============================================================================
# 6. MASTER MERGE
# ============================================================================

cat("\n--- Merging ---\n")

master <- copy(claims)
master <- merge(master, facility_profile, by = "claim_number", all.x = TRUE)
# master <- merge(master, spatial, by.x = "department", by.y = "permit_number", all.x = TRUE)

if (!is.null(compliance) && "efacts_facility_id" %in% names(master)) {
  master <- merge(master, compliance, by = "efacts_facility_id", all.x = TRUE)
}

master <- merge(master, contract_stats, by = "claim_number", all.x = TRUE)

cat(sprintf("  Final: %d rows\n", nrow(master)))

# ============================================================================
# 7. ANALYSIS VARIABLES
# ============================================================================

cat("\n--- Creating Analysis Variables ---\n")

# --- COSTS ---
master[, total_cost := fcoalesce(total_paid, incurred_loss, 0)]
master[, log_cost := log(total_cost + 1)]
master[, log_total_paid := log_cost]

# --- CONTRACT FLAGS ---
master[is.na(is_pfp), is_pfp := FALSE]
master[is.na(is_sow), is_sow := FALSE]
master[is.na(n_contracts), n_contracts := 0]

master[, contract_type := fcase(
  is_pfp, "Bid-to-Result",
  is_sow, "Scope of Work",
  n_contracts > 0, "Other Contract",
  default = "No Contract"
)]
master[, contract_type := factor(contract_type, 
  levels = c("No Contract", "Scope of Work", "Bid-to-Result", "Other Contract"))]

master[, has_contract := n_contracts > 0]
master[, is_auction := is_pfp | is_sow]
master[, is_PFP := is_pfp]
master[, is_bid_to_result := is_pfp]

master[, auction_type_factor := fcase(
  is_pfp, "Bid-to-Result (PFP)",
  is_sow, "Scope of Work",
  n_contracts > 0, "Other Contract",
  default = "No Contract"
)]

# --- DURATION ---
master[, duration_years := claim_duration_years]

# --- TANK DATA (No imputation - flag missing instead) ---
master[, tank_age := avg_tank_age]
master[, tank_age_missing := as.integer(is.na(avg_tank_age))]

master[, capacity := total_capacity]
master[, capacity_missing := as.integer(is.na(total_capacity) | total_capacity == 0)]

master[, n_tanks := n_tanks_total]
master[, tank_data_missing := as.integer(is.na(n_tanks_total))]

# --- TIME TO INTERVENTION ---
master[, time_to_auction := as.numeric(date_first_contract - as.IDate(claim_date)) / 365.25]
master[is.na(time_to_auction), time_to_auction := 0]

# --- ADJUSTER IV (Leave-One-Out) ---
adj_stats <- master[!is.na(adjuster_id) & adjuster_id != "", .(
  total_claims = .N,
  total_pfp = sum(is_pfp)
), by = adjuster_id]

master <- merge(master, adj_stats, by = "adjuster_id", all.x = TRUE)
master[, adjuster_leniency := (total_pfp - fifelse(is_pfp, 1, 0)) / (total_claims - 1)]
master[total_claims < 5, adjuster_leniency := NA]
master[, c("total_pfp", "total_claims") := NULL]
master[, adjuster := adjuster_id]
master[, has_valid_iv := !is.na(adjuster_leniency)]

# --- GEOGRAPHY ---
master[, county := str_to_title(trimws(county))]
master[, region := str_extract(dep_region, "(?<=PADEP )\\w+")]
master[is.na(region), region := "Unknown"]

master[, region_cluster := fcase(
  county %in% c("Philadelphia", "Delaware", "Montgomery", "Bucks", "Chester"), "Southeast",
  county %in% c("Allegheny", "Washington", "Westmoreland"), "Southwest",
  default = "Other"
)]

# --- ERA ---
master[, era := fcase(
  claim_year < 2000, "Pre-2000",
  claim_year < 2005, "2000-2004",
  claim_year < 2010, "2005-2009",
  claim_year < 2015, "2010-2014",
  default = "2015+"
)]
master[, era_factor := factor(era, levels = c("Pre-2000", "2000-2004", "2005-2009", "2010-2014", "2015+"))]

# --- COMPLIANCE (create columns regardless of LOAD_EFACTS) ---
if (!"n_violations" %in% names(master)) {
  master[, n_violations := NA_integer_]
  master[, total_penalties := NA_real_]
  master[, n_inspections := NA_integer_]
  master[, n_with_violations := NA_integer_]
}
master[, has_prior_violations := fifelse(!is.na(n_violations) & n_violations > 0, TRUE, NA)]
master[, compliance_data_missing := as.integer(is.na(n_violations))]

# ============================================================================
# 8. SAVE
# ============================================================================

cat("\n--- Saving ---\n")

saveRDS(master, file.path(paths$processed, "master_analysis_dataset.rds"))
fwrite(master, file.path(paths$processed, "master_analysis_dataset.csv"))
saveRDS(master, file.path(paths$processed, "analysis_panel.rds"))

iv_panel <- master[has_valid_iv == TRUE]
saveRDS(iv_panel, file.path(paths$processed, "iv_analysis_panel.rds"))

# ============================================================================
# SUMMARY
# ============================================================================

cat("\n========================================\n")
cat("COMPLETE\n")
cat("========================================\n")
cat(sprintf("Claims:          %d\n", nrow(master)))
cat(sprintf("Tank match:      %.1f%% (missing flag: tank_data_missing)\n", 
            100 * mean(!is.na(master$n_tanks_total))))
cat(sprintf("Spatial:         %.1f%%\n", 100 * mean(!is.na(master$latitude))))
cat(sprintf("Contracts:       %.1f%%\n", 100 * mean(master$n_contracts > 0)))
cat(sprintf("IV valid:        %.1f%%\n", 100 * mean(master$has_valid_iv, na.rm = TRUE)))
cat(sprintf("Compliance:      %s\n", ifelse(LOAD_EFACTS, "LOADED", "NA (set LOAD_EFACTS=TRUE)")))
cat("\nMissing data flags:\n")
cat(sprintf("  tank_data_missing:     %d (%.1f%%)\n", 
            sum(master$tank_data_missing), 100 * mean(master$tank_data_missing)))
cat(sprintf("  tank_age_missing:      %d (%.1f%%)\n", 
            sum(master$tank_age_missing), 100 * mean(master$tank_age_missing)))
cat(sprintf("  capacity_missing:      %d (%.1f%%)\n", 
            sum(master$capacity_missing), 100 * mean(master$capacity_missing)))
cat(sprintf("  compliance_data_missing: %d (%.1f%%)\n", 
            sum(master$compliance_data_missing), 100 * mean(master$compliance_data_missing)))
cat("========================================\n\n")