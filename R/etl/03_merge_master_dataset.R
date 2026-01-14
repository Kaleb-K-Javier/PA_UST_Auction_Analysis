# R/etl/03_merge_master_dataset.R
# ==============================================================================
# Pennsylvania UST Analysis - ETL Step 3: Construct Master Analysis Dataset
# ==============================================================================
# Purpose: 
#   1. Create "Facility Census" (Population Baseline) with Risk Shares
#   2. Create "Master Analysis Dataset" (Claims) with:
#      - Real Dollar Adjustment (FRED CPI)
#      - Exposure Windows (tanks active at claim_date)
#      - ML Features (One-Hot + Risk Shares)
#      - Contract/Auction Linkage
#   3. Support both Descriptive (01) and Auction (02) analyses
#
# Key Linkage (from README):
#   claims.department = facility_linkage.permit_number (93.7% match)
#   claims.claim_number = contracts.claim_number (100% match)
#
# Inputs:
#   - data/processed/claims_clean.rds
#   - data/processed/contracts_clean.rds  
#   - data/processed/pa_ust_master_facility_tank_database.rds
#   - data/external/padep/allattributes(in).csv
#   - data/external/padep/facility_linkage_table.csv
#
# Outputs:
#   - data/processed/master_analysis_dataset.rds (Claims with Features)
#   - data/processed/facility_census_profile.rds (Population Baseline)
#   - data/processed/analysis_panel.rds (Filtered for Regressions)
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
  processed = here("data/processed"),
  padep     = here("data/external/padep"),
  raw_comps = here("data/external/padep/allattributes(in).csv")
)

if(!dir.exists(paths$processed)) dir.create(paths$processed, recursive = TRUE)

# ==============================================================================
# 1. HELPER FUNCTIONS
# ==============================================================================

#' Fetch CPI from FRED and compute inflation factors
#' Base: Most recent month in series
get_cpi_factor <- function() {
  message("Fetching CPI data from FRED...")
  tryCatch({
    cpi_xts <- getSymbols("CPIAUCNS", src = "FRED", auto.assign = FALSE)
    
    cpi_dt <- as.data.table(cpi_xts)
    setnames(cpi_dt, c("date", "cpi"))
    cpi_dt[, date := as.Date(date)]
    cpi_dt[, Year := year(date)]
    cpi_dt[, Month := month(date)]
    
    # Use most recent CPI as base
    base_cpi <- cpi_dt[which.max(date), cpi]
    cpi_dt[, Infl_Factor := base_cpi / cpi]
    
    message(sprintf("CPI Base: %.1f (as of %s)", base_cpi, max(cpi_dt$date)))
    return(cpi_dt[, .(Year, Month, cpi, Infl_Factor)])
    
  }, error = function(e) {
    warning("FRED fetch failed. Using fallback CPI table.")
    # Fallback: Manual CPI table (BLS annual averages)
    cpi_fallback <- data.table(
      Year = 1985:2024,
      cpi = c(107.6, 109.6, 113.6, 118.3, 124.0, 130.7, 136.2, 140.3, 144.5, 148.2,
              152.4, 156.9, 160.5, 163.0, 166.6, 172.2, 177.1, 179.9, 184.0, 188.9,
              195.3, 201.6, 207.3, 215.3, 214.5, 218.1, 224.9, 229.6, 233.0, 236.7,
              237.0, 240.0, 245.1, 251.1, 255.7, 258.8, 270.9, 292.6, 304.7, 314.5)
    )
    base_cpi <- max(cpi_fallback$cpi)
    cpi_fallback[, Infl_Factor := base_cpi / cpi]
    cpi_fallback[, Month := 6L]  # Use midyear
    return(cpi_fallback)
  })
}

#' Classify PA Owner/Operator Sectors
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

#' Categorize Business Model from Sector + Portfolio Size
categorize_business_model <- function(sector, count) {
  fcase(
    sector %in% c("State Govt/Agency", "Local Govt/Muni", "Federal Govt", 
                  "Education/School", "Emergency Services"), "Publicly Owned",
    grepl("Major Chain|Major Retail", sector), "Retail Gas (Branded Commercial)",
    sector %in% c("Utility/Telecom", "Utility/Energy", "Trucking/Logistics", 
                  "Construction/Development", "Auto Dealership/Repair"), "Non-Retail: Fleet Fuel",
    sector %in% c("Manufacturing/Industrial", "Agriculture", "Healthcare", 
                  "Real Estate/Property Mgmt"), "Private Firm (Non-Motor Fuel)",
    sector == "Private Commercial/Other" & count == 1, "Retail Gas - Single Site",
    sector == "Private Commercial/Other" & count > 1, "Retail Gas - Multi-Site Unbranded",
    default = "Unknown/Unclassified"
  )
}

getmode <- function(v) {
  uniqv <- unique(na.omit(v))
  if(length(uniqv) == 0) return(NA_character_)
  uniqv[which.max(tabulate(match(v, uniqv)))]
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

# Tanks (Master Database from 02d)
tanks <- readRDS(file.path(paths$processed, "pa_ust_master_facility_tank_database.rds"))
setDT(tanks)
tanks[, `:=`(FAC_ID = trimws(as.character(FAC_ID)), TANK_ID = trimws(as.character(TANK_ID)))]
tanks[, DATE_INSTALLED := as.IDate(DATE_INSTALLED)]
if("Tank_Closed_Date" %in% names(tanks)) tanks[, Tank_Closed_Date := as.IDate(Tank_Closed_Date)]

# Components (Raw for Granular Features)
if (file.exists(paths$raw_comps)) {
  components <- fread(paths$raw_comps, na.strings = c("", "NA", "NULL"), colClasses = "character")
  components <- clean_names(components)
  setnames(components, old = c("fac_id", "tank_name", "attribute"), 
           new = c("FAC_ID", "TANK_ID", "CODE"), skip_absent = TRUE)
  components[, `:=`(
    FAC_ID = trimws(as.character(FAC_ID)),
    TANK_ID = trimws(as.character(TANK_ID)),
    CODE = trimws(as.character(CODE)),
    description = as.character(description)
  )]
} else {
  stop("CRITICAL: Component data not found at ", paths$raw_comps)
}

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
  owner_sector_mode = getmode(owner_sector),
  facility_count = uniqueN(FAC_ID)
), by = client_id]

owner_stats[, business_category := categorize_business_model(owner_sector_mode, facility_count)]
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

# Calculate facility-level risk shares from components
census_comps <- merge(tanks[, .(FAC_ID, TANK_ID)], 
                      components[, .(FAC_ID, TANK_ID, CODE)],
                      by = c("FAC_ID", "TANK_ID"), all.x = TRUE)

census_risk_profile <- census_comps[, .(
  share_pressure_piping = mean(CODE == "4C", na.rm = TRUE),
  share_bare_steel = mean(CODE %in% c("1A", "2A"), na.rm = TRUE),
  share_suction_safe = mean(CODE == "4A", na.rm = TRUE),
  share_no_electronic_detection = mean(CODE %in% c("12N", "5G"), na.rm = TRUE)
), by = FAC_ID]

for(j in names(census_risk_profile)[-1]) {
  set(census_risk_profile, which(is.na(census_risk_profile[[j]])), j, 0)
}

census_master <- tanks[, .(
  n_tanks = uniqueN(TANK_ID),
  facility_age = as.numeric(Sys.Date() - min(DATE_INSTALLED, na.rm = TRUE)) / 365.25
), by = FAC_ID]

census_master <- merge(census_master, census_risk_profile, by = "FAC_ID", all.x = TRUE)
census_master <- merge(census_master, 
                       unique(linkage[, .(FAC_ID, final_owner_sector, business_category, Owner_Size_Class)]), 
                       by = "FAC_ID", all.x = TRUE)

message(sprintf("Census: %d facilities profiled", nrow(census_master)))
saveRDS(census_master, file.path(paths$processed, "facility_census_profile.rds"))

# ==============================================================================
# 5. CONSTRUCT EXPOSURE WINDOWS (Claims → Tanks at t=0)
# ==============================================================================
message("\n--- Constructing Claim Exposure Windows ---")

# department = FAC_ID (permit_number) per README linkage
claim_windows <- claims[, .(claim_number, FAC_ID = department, claim_date = as.IDate(claim_date))]

# Merge claims to tanks (Cartesian: each claim × all tanks at facility)
exposure_tanks <- merge(claim_windows, tanks, by = "FAC_ID", all.x = TRUE, allow.cartesian = TRUE)

# Filter: Only tanks ACTIVE at claim_date
# Logic: Installed before claim AND (not closed OR closed after claim - 1 year grace)
exposure_tanks <- exposure_tanks[
  DATE_INSTALLED <= claim_date & 
    (is.na(Tank_Closed_Date) | Tank_Closed_Date >= (claim_date - 365))
]

message(sprintf("Identified %s active tank exposures across %d claims",
                format(nrow(exposure_tanks), big.mark = ","),
                uniqueN(exposure_tanks$claim_number)))

# ==============================================================================
# 6. DYNAMIC ML FEATURE GENERATION
# ==============================================================================
message("\n--- Generating Granular Features ---")

exposure_tanks[, TANK_ID := as.character(TANK_ID)]
exposure_tanks[, FAC_ID := as.character(FAC_ID)]

tank_features_raw <- merge(
  exposure_tanks[, .(claim_number, FAC_ID, TANK_ID)],
  components[, .(FAC_ID, TANK_ID, CODE, description)],
  by = c("FAC_ID", "TANK_ID"),
  all.x = TRUE, 
  allow.cartesian = TRUE
)

# Track missing data
tanks_with_comps <- unique(tank_features_raw[!is.na(CODE), .(claim_number, TANK_ID)])
tanks_with_comps[, has_data := TRUE]

# One-Hot Encoding
tank_features_clean <- tank_features_raw[!is.na(CODE) & CODE != ""]
tank_features_clean[, clean_desc := str_replace_all(toupper(description), "[^A-Z0-9]+", "_")]
tank_features_clean[, clean_desc := str_trunc(clean_desc, 30, ellipsis = "")]
tank_features_clean[, feature_name := paste0("feat_", CODE, "_", clean_desc)]

claim_features_long <- unique(tank_features_clean[, .(claim_number, feature_name)])
claim_features_long[, present := 1]
claim_features_wide <- dcast(claim_features_long, claim_number ~ feature_name, 
                              value.var = "present", fill = 0)

message(sprintf("Generated %d one-hot features", ncol(claim_features_wide) - 1))

# ==============================================================================
# 7. FACILITY RISK PROFILE (Shares & Counts)
# ==============================================================================
message("\n--- Calculating Risk Shares ---")

risk_shares <- tank_features_raw[, .(
  share_pressure_piping = mean(CODE == "4C", na.rm = TRUE),
  share_suction_safe = mean(CODE == "4A", na.rm = TRUE),
  share_bare_steel = mean(CODE %in% c("1A", "2A"), na.rm = TRUE),
  share_fiberglass = mean(CODE %in% c("1E", "1F", "2D"), na.rm = TRUE),
  share_no_electronic_detection = mean(CODE %in% c("12N", "5G"), na.rm = TRUE),
  share_pre_1989_permit = mean(CODE == "9A", na.rm = TRUE)
), by = claim_number]

# Facility-level stats
exposure_tanks <- merge(exposure_tanks, tanks_with_comps, by = c("claim_number", "TANK_ID"), all.x = TRUE)
exposure_tanks[is.na(has_data), has_data := FALSE]

facility_stats <- exposure_tanks[, .(
  n_tanks_total = uniqueN(TANK_ID),
  n_tanks_missing_install_date = sum(is.na(DATE_INSTALLED)),
  share_tanks_missing_install_date = mean(is.na(DATE_INSTALLED)),
  n_tanks_missing_components = sum(has_data == FALSE),
  share_tanks_missing_components = mean(has_data == FALSE),
  avg_tank_age = mean(as.numeric(claim_date - DATE_INSTALLED) / 365.25, na.rm = TRUE),
  max_tank_age = max(as.numeric(claim_date - DATE_INSTALLED) / 365.25, na.rm = TRUE),
  has_gasoline = any(SUBSTANCE_CODE %in% c("GAS", "GASOL"), na.rm = TRUE),
  has_diesel = any(SUBSTANCE_CODE == "DIESL", na.rm = TRUE)
), by = claim_number]

facility_stats[!is.finite(max_tank_age), max_tank_age := NA]
facility_stats[!is.finite(avg_tank_age), avg_tank_age := NA]
facility_stats[, is_over_30_years := (!is.na(max_tank_age) & max_tank_age >= 30)]

# ==============================================================================
# 8. CONTRACT AGGREGATION
# ==============================================================================
message("\n--- Aggregating Contract Data ---")

contract_stats <- contracts[, .(
  total_contract_value = sum(total_contract_value, na.rm = TRUE),
  n_contracts = .N,
  is_pfp = any(auction_type == "Bid-to-Result", na.rm = TRUE),
  is_sow = any(auction_type == "Scope of Work", na.rm = TRUE),
  date_first_contract = min(contract_start, na.rm = TRUE),
  adjuster_id = first(na.omit(adjuster)),
  consultant_primary = first(na.omit(consultant))
), by = claim_number]

contract_stats[is.infinite(date_first_contract), date_first_contract := NA]

# ==============================================================================
# 9. CPI ADJUSTMENT (FRED)
# ==============================================================================
message("\n--- Applying Inflation Adjustment ---")

cpi_table <- get_cpi_factor()

claims[, `:=`(Year = year(claim_date), Month = month(claim_date))]
claims <- merge(claims, cpi_table[, .(Year, Month, Infl_Factor)], by = c("Year", "Month"), all.x = TRUE)

# Fallback for missing months: use annual average
claims[is.na(Infl_Factor), Infl_Factor := {
  annual_avg <- cpi_table[, .(Infl_Factor = mean(Infl_Factor)), by = Year]
  merge(.SD, annual_avg, by = "Year", all.x = TRUE)$Infl_Factor.y
}, by = .I]

claims[is.na(Infl_Factor), Infl_Factor := 1.0]

# Apply inflation to monetary columns
monetary_cols <- c("paid_loss", "paid_alae", "total_paid", "incurred_loss")
for(col in monetary_cols) {
  if(col %in% names(claims)) {
    new_col <- paste0(col, "_real")
    claims[, (new_col) := get(col) * Infl_Factor]
  }
}

# Also adjust contracts
contracts[, Year := year(contract_start)]
contracts[, Month := month(contract_start)]
contracts <- merge(contracts, cpi_table[, .(Year, Month, Infl_Factor)], 
                   by = c("Year", "Month"), all.x = TRUE)
contracts[is.na(Infl_Factor), Infl_Factor := 1.0]
contracts[, total_contract_value_real := total_contract_value * Infl_Factor]

# Re-aggregate with real values
contract_stats_real <- contracts[, .(
  total_contract_value_real = sum(total_contract_value_real, na.rm = TRUE)
), by = claim_number]

message("Inflation adjustment applied (base: most recent FRED CPI)")

# ==============================================================================
# 10. MASTER MERGE
# ==============================================================================
message("\n--- Final Master Merge ---")

master <- copy(claims)

# Merge one-hot features
master <- merge(master, claim_features_wide, by = "claim_number", all.x = TRUE)
feat_cols <- names(claim_features_wide)[names(claim_features_wide) != "claim_number"]
for(col in feat_cols) set(master, which(is.na(master[[col]])), col, 0)

# Merge risk shares
master <- merge(master, risk_shares, by = "claim_number", all.x = TRUE)
share_cols <- grep("^share_", names(risk_shares), value = TRUE)
for(col in share_cols) set(master, which(is.na(master[[col]])), col, 0)

# Merge facility stats
master <- merge(master, facility_stats, by = "claim_number", all.x = TRUE)

# Merge owner classification (department = FAC_ID)
master <- merge(master, 
                unique(linkage[, .(FAC_ID, final_owner_sector, business_category, Owner_Size_Class)]),
                by.x = "department", by.y = "FAC_ID", all.x = TRUE)

# Merge contract stats
master <- merge(master, contract_stats, by = "claim_number", all.x = TRUE)
master <- merge(master, contract_stats_real, by = "claim_number", all.x = TRUE)

# ==============================================================================
# 11. DERIVED ANALYSIS VARIABLES
# ==============================================================================
message("\n--- Creating Derived Variables ---")

# Contract type classification
master[is.na(is_pfp), is_pfp := FALSE]
master[is.na(is_sow), is_sow := FALSE]
master[is.na(n_contracts), n_contracts := 0]

master[, contract_type := fcase(
  is_pfp, "Bid-to-Result",
  is_sow, "Scope of Work",
  n_contracts > 0, "Other Contract",
  default = "No Contract"
)]

# Intervention timing (days from claim to first contract)
master[, intervention_lag_days := as.numeric(date_first_contract - claim_date)]
master[intervention_lag_days < 0, intervention_lag_days := NA]

# Log cost
master[, log_cost := log(total_paid_real + 1)]

# Regional clusters
master[, county := str_to_title(trimws(county))]
master[, region_cluster := fcase(
  county %in% c("Philadelphia", "Delaware", "Montgomery", "Bucks", "Chester"), "Southeast",
  county %in% c("Allegheny", "Washington", "Westmoreland"), "Southwest",
  county %in% c("Lancaster", "York", "Berks", "Dauphin"), "South Central",
  county %in% c("Erie", "Crawford", "Warren"), "Northwest",
  county %in% c("Luzerne", "Lackawanna", "Schuylkill"), "Northeast",
  default = "Other"
)]

# Status grouping for eligibility analysis
master[, status_group := fcase(
  claim_status == "Closed Denied", "Denied",
  claim_status == "Closed Withdrawn", "Withdrawn",
  grepl("Eligible", claim_status), "Eligible",
  grepl("Post Remedial", claim_status), "Post Remedial",
  grepl("Open", claim_status), "Open",
  default = "Other"
)]

# ==============================================================================
# 12. SAVE OUTPUTS
# ==============================================================================
message("\n--- Saving Outputs ---")

saveRDS(master, file.path(paths$processed, "master_analysis_dataset.rds"))
message(sprintf("Saved: master_analysis_dataset.rds (%d rows, %d columns)", 
                nrow(master), ncol(master)))

# Filtered analysis panel (for regressions)
analysis_panel <- master[total_paid_real > 1000 & !is.na(business_category)]
saveRDS(analysis_panel, file.path(paths$processed, "analysis_panel.rds"))
message(sprintf("Saved: analysis_panel.rds (%d rows)", nrow(analysis_panel)))

# Also save updated contracts with real values
saveRDS(contracts, file.path(paths$processed, "contracts_with_real_values.rds"))

message("\n========================================")
message("ETL 03 COMPLETE: Master Dataset Built")
message("========================================")
message(sprintf("Total Claims: %s", format(nrow(master), big.mark = ",")))
message(sprintf("Claims with Contracts: %s (%.1f%%)", 
                format(sum(master$n_contracts > 0), big.mark = ","),
                100 * mean(master$n_contracts > 0)))
message(sprintf("Facility Match Rate: %.1f%%", 
                100 * mean(!is.na(master$business_category))))