# R/etl/03_merge_master_dataset.R
# ==============================================================================
# Pennsylvania UST Analysis - ETL Step 3: Construct Master Analysis Dataset
# ==============================================================================
# Version: 6.1 (Final Production - Full Analysis Support)
#
# Purpose: 
#   1. Create "Facility Census" (Population Baseline) using pre-calculated flags.
#   2. Create "Master Analysis Dataset" (Claims) by linking:
#      - Claims Career Variables (timelines, delays, prior history)
#      - Historical Site Churn (closure activity prior to claim)
#      - Facility Risk Profile (Aggregated from 02d flags/indicators)
#      - Contract/Auction Data (Detailed classification)
#   3. Perform Inflation Adjustment (CPI)
#   4. Generate Derived Analysis Variables (Regional clusters, outcome flags)
#
# Inputs:
#   - data/processed/pa_ust_master_facility_tank_database.rds (v7.0+)
#   - data/processed/claims_clean.rds
#   - data/processed/contracts_clean.rds
#   - data/external/padep/facility_linkage_table.csv
# ==============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(stringr)
  library(janitor)
  library(here)
  library(quantmod)
})

# ==============================================================================
# 0. CONFIGURATION & PATHS
# ==============================================================================
paths <- list(
  processed   = here("data/processed"),
  padep       = here("data/external/padep")
)

if(!dir.exists(paths$processed)) dir.create(paths$processed, recursive = TRUE)

# ==============================================================================
# 1. HELPER FUNCTIONS
# ==============================================================================

get_cpi_factor <- function() {
  message("Fetching CPI data from FRED...")
  tryCatch({
    cpi_xts <- getSymbols("CPIAUCNS", src = "FRED", auto.assign = FALSE)
    cpi_dt <- as.data.table(cpi_xts)
    setnames(cpi_dt, c("date", "cpi"))
    cpi_dt[, date := as.Date(date)]
    base_cpi <- cpi_dt[which.max(date), cpi]
    cpi_dt[, Infl_Factor := base_cpi / cpi]
    cpi_dt[, `:=`(Year = year(date), Month = month(date))]
    message(sprintf("CPI Base: %.1f (as of %s)", base_cpi, max(cpi_dt$date)))
    return(cpi_dt[, .(Year, Month, cpi, Infl_Factor)])
  }, error = function(e) {
    warning("FRED fetch failed. Using fallback CPI table.")
    return(data.table(Year = 2024, Month = 6, cpi = 314.5, Infl_Factor = 1.0))
  })
}

classify_pa_sector <- function(name) {
  fcase(
    grepl("SHEETZ", name), "Major Chain (Sheetz)",
    grepl("WAWA", name), "Major Chain (Wawa)",
    grepl("RUTTER|CHR CORP", name), "Major Chain (Rutters)",
    grepl("GETGO|GIANT EAGLE|GIANT FOOD|GIANT CO LLC", name), "Major Chain (GetGo/Giant)",
    grepl("TURKEY HILL|KROGER|EG GROUP", name), "Major Chain (Turkey Hill/EG)",
    grepl("SUNOCO|SUN OIL", name), "Major Chain (Sunoco)",
    grepl("7 ELEVEN|7-11|7 11", name), "Major Chain (7-Eleven)",
    grepl("SPEEDWAY", name), "Major Chain (Speedway)",
    grepl("PILOT TRAVEL|FLYING J|LOVES TRAVEL", name), "Major Chain (Travel Center)",
    grepl("ROYAL FARMS", name), "Major Chain (Royal Farms)",
    grepl("COUNTRY FAIR", name), "Major Chain (Country Fair)",
    grepl("UNITED REF|KWIK FILL|RED APPLE", name), "Major Chain (United Refining)",
    grepl("WEIS MKT|WEIS MARKETS", name), "Major Chain (Weis)",
    grepl("EXXON|MOBIL|SHELL|BP |GULF |TEXACO|CITGO|VALERO|LUKOIL", name), "Major Chain (Other Brand)",
    grepl("WAL-MART|WALMART|SAMS CLUB", name), "Major Retail (Walmart)",
    grepl("HOME DEPOT|LOWES|TARGET|COSTCO", name), "Major Retail (Big Box)",
    grepl("COMMONWEALTH OF PA|PA DEPT|PENNDOT", name), "State Govt/Agency",
    grepl("SCHOOL|SCH DIST|ACADEMY|COLLEGE|UNIV", name), "Education/School",
    grepl("BORO|TWP|CITY OF|COUNTY|MUNICIPAL", name), "Local Govt/Muni",
    grepl("FIRE|VOLUNTEER|EMS|AMBULANCE", name), "Emergency Services",
    grepl("FEDERAL|US GOVT|POSTAL", name), "Federal Govt",
    grepl("VERIZON|BELL ATLANTIC|COMCAST", name), "Utility/Telecom",
    grepl("PP&L|PPL|PECO|DUQUESNE|FIRSTENERGY", name), "Utility/Energy",
    grepl("HOSPITAL|HEALTH|UPMC|GEISINGER", name), "Healthcare",
    grepl("AUTO|MOTORS|FORD|CHEVY|TOYOTA|HONDA", name), "Auto Dealership/Repair",
    grepl("TRUCK|HAULING|TRANSPORT|LOGISTICS|PENSKE", name), "Trucking/Logistics",
    grepl("CONST|EXCAVATING|PAVING|BUILDERS", name), "Construction/Development",
    grepl("REALTY|PROPERTIES|MANAGEMENT|APARTMENTS", name), "Real Estate/Property Mgmt",
    grepl("FARM|DAIRY|NURSERY", name), "Agriculture",
    grepl("MFG|MANUFACTURING|STEEL|IRON|WORKS", name), "Manufacturing/Industrial",
    default = "Private Commercial/Other"
  )
}

# ==============================================================================
# 2. LOAD DATA
# ==============================================================================
message("\n--- Loading Data ---")

# Claims
claims <- readRDS(file.path(paths$processed, "claims_clean.rds"))
setDT(claims)
claims[, department := trimws(as.character(department))]
claims[, claim_number := trimws(as.character(claim_number))]
claims <- claims[!str_detect(claim_number, "^Sum") & !is.na(department)]

# Contracts
contracts <- readRDS(file.path(paths$processed, "contracts_clean.rds"))
setDT(contracts)
contracts[, claim_number := trimws(as.character(claim_number))]

# Tanks (LOAD v7.0 DATABASE FROM 02d)
tanks <- readRDS(file.path(paths$processed, "pa_ust_master_facility_tank_database.rds"))
setDT(tanks)
tanks[, `:=`(FAC_ID = trimws(as.character(FAC_ID)), TANK_ID = trimws(as.character(TANK_ID)))]
tanks[, DATE_INSTALLED := as.IDate(DATE_INSTALLED)]
if("Tank_Closed_Date" %in% names(tanks)) tanks[, Tank_Closed_Date := as.IDate(Tank_Closed_Date)]

# Linkage Table
linkage <- fread(file.path(paths$padep, "facility_linkage_table.csv"))
if ("permit_number" %in% names(linkage)) setnames(linkage, "permit_number", "facility_id")
if ("owner_id" %in% names(linkage)) setnames(linkage, "owner_id", "client_id")

linkage[, `:=`(
  FAC_ID = trimws(as.character(facility_id)),
  client_id = as.numeric(client_id),
  owner_name = toupper(trimws(as.character(owner_name)))
)]

message(sprintf("Loaded: %d claims, %d contracts, %d tank records, %d facilities",
                nrow(claims), nrow(contracts), nrow(tanks), uniqueN(linkage$FAC_ID)))

# ==============================================================================
# 3. OWNER CLASSIFICATION
# ==============================================================================
message("\n--- Running Owner Classification ---")

linkage[, owner_sector := classify_pa_sector(owner_name)]
linkage[is.na(owner_name) | owner_name == "", owner_sector := "Unknown/Missing"]

owner_stats <- linkage[!is.na(client_id), .(
  owner_sector_mode = first(owner_sector),
  facility_count = uniqueN(FAC_ID)
), by = client_id]

owner_stats[, business_category := fcase(
  owner_sector_mode %in% c("State Govt/Agency", "Local Govt/Muni", "Federal Govt", 
                           "Education/School", "Emergency Services"), "Publicly Owned",
  grepl("Major Chain|Major Retail", owner_sector_mode), "Retail Gas (Branded Commercial)",
  owner_sector_mode %in% c("Utility/Telecom", "Utility/Energy", "Trucking/Logistics", 
                           "Construction/Development", "Auto Dealership/Repair"), "Non-Retail: Fleet Fuel",
  owner_sector_mode %in% c("Manufacturing/Industrial", "Agriculture", "Healthcare", 
                           "Real Estate/Property Mgmt"), "Private Firm (Non-Motor Fuel)",
  owner_sector_mode == "Private Commercial/Other" & facility_count == 1, "Retail Gas - Single Site",
  owner_sector_mode == "Private Commercial/Other" & facility_count > 1, "Retail Gas - Multi-Site Unbranded",
  default = "Unknown/Unclassified"
)]

owner_stats[, Owner_Size_Class := fcase(
  facility_count == 1, "Single-Site Owner (Mom & Pop)",
  facility_count >= 2 & facility_count <= 9, "Small Fleet (2-9)",
  facility_count >= 10 & facility_count <= 49, "Medium Fleet (10-49)",
  facility_count >= 50, "Large Fleet/Corporate (50+)",
  default = "Unknown"
)]

linkage <- merge(linkage, owner_stats[, .(client_id, business_category, Owner_Size_Class)], 
                 by = "client_id", all.x = TRUE)
linkage[, final_owner_sector := owner_sector]

# ==============================================================================
# 4. FACILITY CENSUS (Population Baseline)
# ==============================================================================
message("\n--- Building Facility Census ---")

# Aggregating 02d flags for population baseline
census_risk_profile <- tanks[, .(
  share_pressure_piping = mean(flag_pressure_piping, na.rm = TRUE),
  share_bare_steel      = mean(flag_bare_steel, na.rm = TRUE),
  share_single_wall     = mean(flag_single_wall, na.rm = TRUE),
  share_noncompliant    = mean(flag_noncompliant, na.rm = TRUE),
  share_data_unknown    = mean(flag_data_unknown, na.rm = TRUE)
), by = FAC_ID]

for(j in names(census_risk_profile)[-1]) {
  set(census_risk_profile, which(is.na(census_risk_profile[[j]])), j, 0)
}

census_master <- tanks[, .(
  n_tanks_history = uniqueN(TANK_ID), # Total historical tanks (Active + Closed)
  facility_age = as.numeric(Sys.Date() - min(DATE_INSTALLED, na.rm = TRUE)) / 365.25
), by = FAC_ID]

census_master <- merge(census_master, census_risk_profile, by = "FAC_ID", all.x = TRUE)
census_master <- merge(census_master, 
                       unique(linkage[, .(FAC_ID, final_owner_sector, business_category, Owner_Size_Class)]), 
                       by = "FAC_ID", all.x = TRUE)

message(sprintf("Census: %d facilities profiled", nrow(census_master)))
saveRDS(census_master, file.path(paths$processed, "facility_census_profile.rds"))

# ==============================================================================
# 5. CLAIMS CAREER VARIABLES
# ==============================================================================
message("\n--- Computing Claims Career Variables ---")

claims[, `:=`(
  claim_date = as.IDate(claim_date),
  loss_reported_date = as.IDate(loss_reported_date),
  closed_date = as.IDate(closed_date)
)]

# Lags and Durations
claims[, `:=`(
  reporting_lag_days = as.numeric(claim_date - loss_reported_date),
  claim_open_days = as.numeric(closed_date - claim_date),
  current_duration_days = fifelse(is.na(closed_date), as.numeric(Sys.Date() - claim_date), NA_real_)
)]

# Development/Maturity Flags
claims[, `:=`(
  development_months = as.integer(fifelse(is.na(closed_date), 
                                          interval(claim_date, Sys.Date()) %/% months(1), 
                                          interval(claim_date, closed_date) %/% months(1))),
  is_mature_2yr = (!is.na(closed_date) & closed_date < (Sys.Date() - 730)),
  is_mature_3yr = (!is.na(closed_date) & closed_date < (Sys.Date() - 1095)),
  is_stuck = (is.na(closed_date) & (Sys.Date() - claim_date) > 1095)
)]

# Prior History
# FIX: Explicitly use data.table::shift to avoid namespace conflicts
setorder(claims, department, loss_reported_date)
claims[, `:=`(
  prior_claims_n = seq_len(.N) - 1L,
  prior_total_paid = cumsum(data.table::shift(total_paid, fill = 0)),
  days_since_prior_claim = as.numeric(loss_reported_date - data.table::shift(loss_reported_date))
), by = department]

claims[prior_claims_n > 0, avg_prior_claim_cost := prior_total_paid / prior_claims_n]
claims[prior_claims_n == 0, avg_prior_claim_cost := NA_real_]
claims[, is_repeat_filer := (prior_claims_n > 0)]

message(sprintf("Career variables computed. Repeat filers: %d (%.1f%%)",
                sum(claims$is_repeat_filer), 100 * mean(claims$is_repeat_filer)))
                

# ==============================================================================
# 6. HISTORICAL SITE CHURN
# ==============================================================================
message("\n--- Calculating Historical Closure Windows ---")

history_base <- merge(
  claims[, .(claim_number, FAC_ID = department, claim_date)],
  tanks[, .(FAC_ID, TANK_ID, Tank_Closed_Date)], 
  by = "FAC_ID", allow.cartesian = TRUE
)

history_base[, days_since_closure := as.numeric(claim_date - Tank_Closed_Date)]

churn_stats <- history_base[!is.na(days_since_closure) & days_since_closure > 0, .(
  n_closed_0_30d   = uniqueN(TANK_ID[days_since_closure <= 30]),
  n_closed_31_90d  = uniqueN(TANK_ID[days_since_closure > 30 & days_since_closure <= 90]),
  n_closed_1yr     = uniqueN(TANK_ID[days_since_closure <= 365]),
  n_closed_pre_1yr = uniqueN(TANK_ID[days_since_closure > 365]),
  n_closed_total   = uniqueN(TANK_ID)
), by = claim_number]

# Zero-fill
all_claims <- unique(claims$claim_number)
churn_stats <- merge(data.table(claim_number = all_claims), churn_stats, by = "claim_number", all.x = TRUE)
for (col in names(churn_stats)[-1]) set(churn_stats, which(is.na(churn_stats[[col]])), col, 0L)

churn_stats[, has_recent_closure := (n_closed_0_30d > 0 | n_closed_31_90d > 0)]
message(sprintf("Churn stats built: %.1f%% with recent closures", 100 * mean(churn_stats$has_recent_closure)))

# ==============================================================================
# 7. EXPOSURE WINDOWS & RISK PROFILE (REFACTORED)
# ==============================================================================
message("\n--- Constructing Claim Exposure Windows ---")

claim_windows <- claims[, .(claim_number, FAC_ID = department, claim_date, loss_reported_date)]
exposure_tanks <- merge(claim_windows, tanks, by = "FAC_ID", all.x = TRUE, allow.cartesian = TRUE)

# Filter: Tanks Active at claim_date
exposure_tanks <- exposure_tanks[
  DATE_INSTALLED <= claim_date & 
  (is.na(Tank_Closed_Date) | Tank_Closed_Date >= (claim_date - 365))
]

message(sprintf("Identified %d active tank exposures.", nrow(exposure_tanks)))

# ---------------------------------------------------------------------------
# 7.1 REGULATORY RISK (Layer 1: Aggregated Flags from 02d)
# ---------------------------------------------------------------------------
message("   Layer 1: Aggregating Regulatory Flags...")

# Use max() to create "has_*" flags (Did any active tank have the risk?)
reg_profile <- exposure_tanks[, .(
  # Construction
  has_bare_steel        = max(flag_bare_steel, na.rm = TRUE),
  has_single_walled     = max(flag_single_wall, na.rm = TRUE),
  has_secondary_containment = max(flag_secondary_containment, na.rm = TRUE),
  
  # Piping
  has_pressure_piping   = max(flag_pressure_piping, na.rm = TRUE),
  has_suction_piping    = max(flag_suction_piping, na.rm = TRUE),
  has_galvanized_piping = max(flag_galvanized_piping, na.rm = TRUE),
  
  # Detection
  has_electronic_atg    = max(flag_electronic_atg, na.rm = TRUE),
  has_manual_detection  = max(flag_manual_detection, na.rm = TRUE),
  has_overfill_alarm    = max(flag_overfill_alarm, na.rm = TRUE),
  
  # Regulatory & Data Quality
  has_noncompliant      = max(flag_noncompliant, na.rm = TRUE),
  has_legacy_grandfathered = max(flag_legacy, na.rm = TRUE),
  has_unknown_data      = max(flag_data_unknown, na.rm = TRUE), 
  
  n_tanks_active        = .N,
  avg_tank_age          = mean(as.numeric(claim_date - DATE_INSTALLED)/365.25, na.rm = TRUE)
), by = claim_number]

# Clean Inf/-Inf
for (col in names(reg_profile)) set(reg_profile, which(is.infinite(reg_profile[[col]])), col, 0)

# ---------------------------------------------------------------------------
# 7.2 ML INDICATORS (Layer 2: Aggregated Indicators from 02d)
# ---------------------------------------------------------------------------
message("   Layer 2: Aggregating ML Indicators (Bin/Qty)...")

ind_cols <- grep("^ind_", names(exposure_tanks), value = TRUE)

if (length(ind_cols) > 0) {
  # A. BINARY (bin_): Did ANY active tank have this feature?
  bin_agg <- exposure_tanks[, lapply(.SD, function(x) as.integer(max(x, na.rm = TRUE) > 0)), 
                            by = claim_number, .SDcols = ind_cols]
  setnames(bin_agg, ind_cols, paste0("bin_", ind_cols))
  
  # B. QUANTITY (qty_): HOW MANY active tanks had this feature?
  qty_agg <- exposure_tanks[, lapply(.SD, function(x) sum(x, na.rm = TRUE)), 
                            by = claim_number, .SDcols = ind_cols]
  setnames(qty_agg, ind_cols, paste0("qty_", ind_cols))
  
  # Merge
  ml_profile <- merge(bin_agg, qty_agg, by = "claim_number")
} else {
  warning("No 'ind_' columns found in tank database.")
  ml_profile <- data.table(claim_number = unique(claims$claim_number))
}

# ---------------------------------------------------------------------------
# 7.3 RISK SHARES (Layer 3: Risk Proportions)
# ---------------------------------------------------------------------------
message("   Layer 3: Aggregating Risk Shares...")

risk_shares <- exposure_tanks[, .(
  share_pressure_piping = mean(flag_pressure_piping, na.rm = TRUE),
  share_bare_steel      = mean(flag_bare_steel, na.rm = TRUE),
  share_single_wall     = mean(flag_single_wall, na.rm = TRUE),
  share_noncompliant    = mean(flag_noncompliant, na.rm = TRUE),
  share_data_unknown    = mean(flag_data_unknown, na.rm = TRUE)
), by = claim_number]

# Combine Layers
risk_profile <- merge(reg_profile, ml_profile, by = "claim_number", all = TRUE)
risk_profile <- merge(risk_profile, risk_shares, by = "claim_number", all.x = TRUE)
risk_profile <- merge(risk_profile, churn_stats, by = "claim_number", all.x = TRUE)

# Facility Stats (Substance)
substance_stats <- exposure_tanks[, .(
  has_gasoline = as.integer(any(SUBSTANCE_CODE %in% c("GAS", "GASOL"), na.rm = TRUE)),
  has_diesel = as.integer(any(SUBSTANCE_CODE == "DIESL", na.rm = TRUE))
), by = claim_number]

risk_profile <- merge(risk_profile, substance_stats, by = "claim_number", all.x = TRUE)

message(sprintf("\n   RISK PROFILE COMPLETE: %d features", ncol(risk_profile)-1))

# ==============================================================================
# 8. CONTRACT AGGREGATION
# ==============================================================================
message("\n--- Aggregating Contracts ---")

contract_stats <- contracts[, .(
  total_contract_value = sum(total_contract_value, na.rm = TRUE),
  n_contracts = .N,
  # Flags
  is_pfp = any(bid_type == "Bid to Result", na.rm = TRUE),
  is_sow = any(bid_type == "Defined Scope of Work", na.rm = TRUE),
  is_competitive = any(contract_category == "Competitively Bid", na.rm = TRUE),
  is_sole_source = any(contract_category == "Sole Source", na.rm = TRUE),
  # Types
  has_fixed_price = any(contract_type_raw == "Fixed Price", na.rm = TRUE),
  has_tm = any(contract_type_raw == "Time and Material", na.rm = TRUE),
  has_pfp_contract = any(contract_type_raw == "Pay for Performance", na.rm = TRUE),
  # Timing
  date_first_contract = min(contract_start, na.rm = TRUE),
  date_last_contract = max(contract_end, na.rm = TRUE),
  # Entities
  adjuster_id = first(na.omit(adjuster)),
  n_consultants = uniqueN(consultant[!is.na(consultant)])
), by = claim_number]

contract_stats[is.infinite(date_first_contract), date_first_contract := NA]
contract_stats[is.infinite(date_last_contract), date_last_contract := NA]

# ==============================================================================
# 9. FINAL MERGE & CPI
# ==============================================================================
message("\n--- Final Master Merge ---")

master <- copy(claims)
master <- merge(master, risk_profile, by = "claim_number", all.x = TRUE)
master <- merge(master, contract_stats, by = "claim_number", all.x = TRUE)
master <- merge(master, unique(linkage[, .(FAC_ID, final_owner_sector, business_category, Owner_Size_Class)]), 
                by.x = "department", by.y = "FAC_ID", all.x = TRUE)

# -- CRITICAL FIX: Merge total historical tank count (needed for 02_cost_correlates) --
master <- merge(master, census_master[, .(FAC_ID, n_tanks_total = n_tanks_history)], 
                by.x = "department", by.y = "FAC_ID", all.x = TRUE)

master[, has_facility_match := !is.na(business_category)]

# CPI Adjustment
cpi_table <- get_cpi_factor()
master[, `:=`(Year = year(claim_date), Month = month(claim_date))]
master <- merge(master, cpi_table[, .(Year, Month, Infl_Factor)], by = c("Year", "Month"), all.x = TRUE)
master[is.na(Infl_Factor), Infl_Factor := 1.0]

# Real Dollars
monetary_cols <- c("paid_loss", "paid_alae", "total_paid", "incurred_loss")
for(col in monetary_cols) {
  if(col %in% names(master)) master[, paste0(col, "_real") := get(col) * Infl_Factor]
}
master[, total_contract_value_real := total_contract_value * Infl_Factor]

# Contracts Output
contracts[, Year := year(contract_start)]
contracts[, Month := month(contract_start)]
contracts <- merge(contracts, cpi_table[, .(Year, Month, Infl_Factor)], by = c("Year", "Month"), all.x = TRUE)
contracts[is.na(Infl_Factor), Infl_Factor := 1.0]
contracts[, total_contract_value_real := total_contract_value * Infl_Factor]

# ==============================================================================
# 10. DERIVED ANALYSIS VARIABLES
# ==============================================================================
message("\n--- Creating Derived Variables ---")

# Fill NAs
master[is.na(n_contracts), n_contracts := 0L]
for (col in c("is_pfp", "is_sow", "is_competitive", "is_sole_source")) {
  set(master, which(is.na(master[[col]])), col, FALSE)
}

# Alias variable for 02_cost_correlates
master[, claim_duration_days := claim_open_days]

# Detailed Contract Type
master[, contract_type_detailed := fcase(
  is_pfp & is_competitive, "Bid-to-Result (Competitive Fixed Price)",
  is_sow & is_competitive, "Scope of Work (Competitive Fixed Price)",
  is_competitive & has_tm, "Competitive (Time & Material)",
  is_sole_source & has_fixed_price, "Sole Source (Fixed Price)",
  is_sole_source & has_tm, "Sole Source (Time & Material)",
  is_sole_source & has_pfp_contract, "Sole Source (Pay for Performance)",
  n_contracts > 0, "Other/Legacy Contract",
  default = "No Contract"
)]

# Simplified Contract Type
master[, contract_type := fcase(
  is_pfp, "Bid-to-Result",
  is_sow, "Scope of Work", 
  n_contracts > 0, "Other Contract",
  default = "No Contract"
)]

# Intervention Timing
master[, intervention_lag_days := as.numeric(date_first_contract - claim_date)]
master[intervention_lag_days < 0, intervention_lag_days := NA]
master[claim_open_days > 0, intervention_share := intervention_lag_days / claim_open_days]

# Log Costs
master[, log_cost := log(total_paid_real + 1)]
master[, log_paid_loss := log(paid_loss_real + 1)]
master[, log_paid_alae := log(paid_alae_real + 1)]

# Regional Clusters
master[, county := str_to_title(trimws(county))]
master[, region_cluster := fcase(
  county %in% c("Philadelphia", "Delaware", "Montgomery", "Bucks", "Chester"), "Southeast",
  county %in% c("Allegheny", "Washington", "Westmoreland"), "Southwest",
  county %in% c("Lancaster", "York", "Berks", "Dauphin"), "South Central",
  county %in% c("Erie", "Crawford", "Warren"), "Northwest",
  county %in% c("Luzerne", "Lackawanna", "Schuylkill"), "Northeast",
  default = "Other"
)]

# Status Groups
master[, status_group := fcase(
  claim_status == "Closed Denied", "Denied",
  claim_status == "Closed Withdrawn", "Withdrawn",
  grepl("Eligible", claim_status), "Eligible",
  grepl("Post Remedial", claim_status), "Post Remedial",
  grepl("Open", claim_status), "Open",
  default = "Other"
)]

# Outcomes
master[, went_to_auction := as.integer(n_contracts > 0 & is_competitive)]
master[, is_high_cost := as.integer(total_paid_real > quantile(total_paid_real, 0.90, na.rm = TRUE))]
master[, is_long_duration := as.integer(claim_open_days > quantile(claim_open_days, 0.90, na.rm = TRUE))]

# ==============================================================================
# 11. SAVE OUTPUTS
# ==============================================================================
message("\n--- Saving Outputs ---")

saveRDS(master, file.path(paths$processed, "master_analysis_dataset.rds"))
message(sprintf("Saved: master_analysis_dataset.rds (%d rows, %d columns)", nrow(master), ncol(master)))

# Analysis Panel
analysis_panel <- master[total_paid_real > 1000 & has_facility_match == TRUE]
saveRDS(analysis_panel, file.path(paths$processed, "analysis_panel.rds"))
message(sprintf("Saved: analysis_panel.rds (%d rows)", nrow(analysis_panel)))

# Real Value Contracts
saveRDS(contracts, file.path(paths$processed, "contracts_with_real_values.rds"))

# ==============================================================================
# 12. SUMMARY DIAGNOSTICS
# ==============================================================================
message("\n========================================")
message("ETL 03 COMPLETE: Master Dataset Built")
message("========================================")
message(sprintf("Total Claims: %s", format(nrow(master), big.mark = ",")))
message(sprintf("Features: %d (Regulatory Flags: %d, ML Binary: %d)", 
                ncol(master), sum(grepl("^has_", names(master))), sum(grepl("^bin_", names(master)))))
message(sprintf("Unknown Data Risk Found: %.1f%% of claims", 100 * mean(master$has_unknown_data, na.rm=TRUE)))

message("\nContract Type Distribution:")
print(master[, .N, by = contract_type_detailed][order(-N)])