# R/etl/03_merge_master_dataset.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 3: Construct Master Analysis Dataset
# ============================================================================
# Purpose: 
#   1. Create "Facility Census" (Population Baseline) with Risk Shares.
#   2. Create "Master Analysis Dataset" (Claims) with Exposure Windows & ML Features.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(stringr)
  library(janitor)
  library(here)
  library(quantmod)
})

# ============================================================================
# 0. CONFIGURATION & PATHS
# ============================================================================
paths <- list(
  processed = here("data/processed"),
  padep = here("data", "external", "padep"),
  raw_comps = here("data", "external", "padep", "allattributes(in).csv") 
)

# ============================================================================
# 1. HELPER FUNCTIONS
# ============================================================================
get_cpi_factor <- function() {
  tryCatch({
    getSymbols("CPIAUCNS", src = "FRED", auto.assign = FALSE) %>%
      as.data.table() %>%
      setnames(c("index", "cpi")) %>%
      .[, .(Year = year(index), Month = month(index), cpi)] %>%
      .[, Infl_Factor := tail(cpi, 1) / cpi] 
  }, error = function(e) {
    return(data.table(Year = 1980:2030, Month = 1, Infl_Factor = 1.0))
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
    grepl("EXXON|MOBIL|SHELL|BP |GULF |TEXACO|CITGO|VALERO|LUKOIL", name), "Major Chain (Other Fuel Brand)",
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

categorize_business_model <- function(sector, count) {
  fcase(
    sector %in% c("State Govt/Agency", "Local Govt/Muni", "Federal Govt", 
                  "Education/School", "Emergency Services", "Utility/Water & Sewer"), "Publicly Owned",
    grepl("Major Chain|Major Retail", sector), "Retail Gas (Branded Commercial)",
    sector %in% c("Utility/Telecom", "Utility/Energy", "Trucking/Logistics", 
                  "Construction/Development", "Auto Dealership/Repair"), "Non-Retail: Fleet Fuel Facility",
    sector %in% c("Manufacturing/Industrial", "Agriculture", "Healthcare", 
                  "Real Estate/Property Mgmt", "Financial Services"), "Private Firm - Non-motor fuel seller",
    sector == "Private Commercial/Other" & count == 1, "Retail Gas - Single Proprietor",
    sector == "Private Commercial/Other" & count > 1, "Retail Gas - Multi-property Not Branded",
    default = "Unknown/Unclassified"
  )
}

getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

# ============================================================================
# 2. LOAD DATA
# ============================================================================
cat("--- Loading Data ---\n")
LOAD_EFACTS = FALSE

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

# Tanks (Base dates only)
tanks <- readRDS(file.path(paths$processed, "pa_ust_master_facility_tank_database.rds"))
setDT(tanks)
tanks[, `:=`(FAC_ID = trimws(as.character(FAC_ID)), TANK_ID = trimws(as.character(TANK_ID)))]
tanks[, DATE_INSTALLED := as.IDate(DATE_INSTALLED)]
if("Tank_Closed_Date" %in% names(tanks)) tanks[, Tank_Closed_Date := as.IDate(Tank_Closed_Date)]

# Components
if (file.exists(paths$raw_comps)) {
  components <- fread(paths$raw_comps, na.strings = c("", "NA", "NULL"), colClasses = "character") 
  components <- clean_names(components)
  if("fac_id" %in% names(components)) setnames(components, "fac_id", "FAC_ID")
  if("tank_name" %in% names(components)) setnames(components, "tank_name", "TANK_ID")
  if("attribute" %in% names(components)) setnames(components, "attribute", "CODE")
  
  components[, `:=`(
    FAC_ID = trimws(as.character(FAC_ID)),
    TANK_ID = trimws(as.character(TANK_ID)),
    CODE = trimws(as.character(CODE)),
    description = as.character(description)
  )]
} else {
  stop("CRITICAL: Component data not found.")
}

# Linkage
linkage <- fread(file.path(paths$padep, "facility_linkage_table.csv"))
if ("permit_number" %in% names(linkage) && !"facility_id" %in% names(linkage)) setnames(linkage, "permit_number", "facility_id")
if ("owner_id" %in% names(linkage) && !"client_id" %in% names(linkage)) setnames(linkage, "owner_id", "client_id")
linkage[, `:=`(
  FAC_ID = trimws(as.character(facility_id)),
  client_id = as.numeric(client_id),
  owner_name = toupper(trimws(as.character(owner_name)))
)]

# ============================================================================
# 3. OWNER CLASSIFICATION
# ============================================================================
cat("--- Running Owner Classification ---\n")

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

linkage[owner_stats, on = "client_id", `:=`(
  final_owner_sector = i.owner_sector_mode,
  business_category = i.business_category,
  Owner_Size_Class = i.Owner_Size_Class
)]
linkage[is.na(business_category), business_category := "Unknown/Unclassified"]

# ============================================================================
# 4. CONSTRUCT CENSUS (POPULATION BASELINE)
# ============================================================================
cat("--- Generating Census (Whole Population Risk Shares) ---\n")
# This creates the dataset for 01_predictive_risk_profiling (Claims vs Population)

# Merge Components to ALL tanks
census_comps <- merge(tanks[, .(FAC_ID, TANK_ID)], components[, .(FAC_ID, TANK_ID, CODE)], 
                      by = c("FAC_ID", "TANK_ID"), all.x = TRUE)

# Calculate Facility-Level Risk Shares for the Census
census_risk_profile <- census_comps[, .(
  share_pressure_piping = mean(CODE == "4C", na.rm = TRUE),
  share_bare_steel = mean(CODE %in% c("1A", "2A"), na.rm = TRUE),
  share_no_electronic_detection = mean(CODE %in% c("12N", "5G"), na.rm = TRUE)
), by = FAC_ID]

# Handle NAs (no components = 0 share of specific risks, but high uncertainty)
for(j in names(census_risk_profile)) set(census_risk_profile, which(is.na(census_risk_profile[[j]])), j, 0)

# Merge back to Facility List
census_master <- tanks[, .(
  n_tanks = uniqueN(TANK_ID),
  facility_age = 2025 - min(year(DATE_INSTALLED), na.rm=TRUE)
), by = FAC_ID]

census_master <- merge(census_master, census_risk_profile, by = "FAC_ID", all.x = TRUE)
census_master <- merge(census_master, linkage[, .(FAC_ID, final_owner_sector, business_category)], by = "FAC_ID", all.x = TRUE)

cat("Saving Census Profile...\n")
saveRDS(census_master, file.path(paths$processed, "facility_census_profile.rds"))

# ============================================================================
# 5. CONSTRUCT EXPOSURE WINDOWS (CLAIMS ANALYSIS)
# ============================================================================
cat("--- Constructing Claim Exposure Windows ---\n")

claim_windows <- claims[, .(claim_number, FAC_ID = department, claim_date = as.IDate(claim_date))]
exposure_tanks <- merge(claim_windows, tanks, by = "FAC_ID", all.x = TRUE, allow.cartesian = TRUE)

exposure_tanks <- exposure_tanks[
  DATE_INSTALLED <= claim_date & 
    (is.na(Tank_Closed_Date) | Tank_Closed_Date >= (claim_date - 365))
]
cat(sprintf("Identified %d active tank exposures.\n", nrow(exposure_tanks)))

# ============================================================================
# 6. DYNAMIC ML FEATURE GENERATION (CLAIMS ONLY)
# ============================================================================
cat("--- Generating Granular Features (Claims) ---\n")

exposure_tanks[, TANK_ID := as.character(TANK_ID)]
exposure_tanks[, FAC_ID := as.character(FAC_ID)]

tank_features_raw <- merge(
  exposure_tanks[, .(claim_number, FAC_ID, TANK_ID)],
  components[, .(FAC_ID, TANK_ID, CODE, description)],
  by = c("FAC_ID", "TANK_ID"),
  all.x = TRUE, 
  allow.cartesian = TRUE
)

# Missing Data Tracking
tanks_with_comps <- unique(tank_features_raw[!is.na(CODE), .(claim_number, TANK_ID)])
tanks_with_comps[, has_data := TRUE]

# Dynamic One-Hot
tank_features_clean <- tank_features_raw[!is.na(CODE) & CODE != ""]
tank_features_clean[, clean_desc := str_replace_all(toupper(description), "[^A-Z0-9]+", "_")]
tank_features_clean[, clean_desc := str_trunc(clean_desc, 30, ellipsis = "")] 
tank_features_clean[, feature_name := paste0("feat_", CODE, "_", clean_desc)]

claim_features_long <- unique(tank_features_clean[, .(claim_number, feature_name)])
claim_features_long[, present := 1]
claim_features_wide <- dcast(claim_features_long, claim_number ~ feature_name, value.var = "present", fill = 0)

# ============================================================================
# 7. FACILITY PROFILE (SHARES & COUNTS)
# ============================================================================
cat("--- Calculating Risk Shares (Claims) ---\n")

risk_shares <- tank_features_raw[, .(
  share_pressure_piping = mean(CODE == "4C", na.rm = TRUE),
  share_suction_safe = mean(CODE == "4A", na.rm = TRUE),
  share_bare_steel = mean(CODE %in% c("1A", "2A"), na.rm = TRUE),
  share_fiberglass = mean(CODE %in% c("1E", "1F", "2D"), na.rm = TRUE),
  share_no_electronic_detection = mean(CODE %in% c("12N", "5G"), na.rm = TRUE),
  share_pre_1989_permit = mean(CODE == "9A", na.rm = TRUE)
), by = claim_number]

# Merge missingness info
exposure_tanks <- merge(exposure_tanks, tanks_with_comps, by=c("claim_number", "TANK_ID"), all.x=TRUE)
exposure_tanks[is.na(has_data), has_data := FALSE]

facility_stats <- exposure_tanks[, .(
  n_tanks_total = uniqueN(TANK_ID),
  n_tanks_missing_install_date = sum(is.na(DATE_INSTALLED)),
  share_tanks_missing_install_date = mean(is.na(DATE_INSTALLED)),
  n_tanks_missing_components = sum(has_data == FALSE),
  share_tanks_missing_components = mean(has_data == FALSE),
  avg_tank_age = mean(as.numeric(claim_date - DATE_INSTALLED)/365.25, na.rm = TRUE),
  max_tank_age = max(as.numeric(claim_date - DATE_INSTALLED)/365.25, na.rm = TRUE),
  share_single_wall = mean(str_detect(TANK_CONSTRUCTION, "SINGLE|UNPROTECTED"), na.rm = TRUE),
  share_double_wall = mean(str_detect(TANK_CONSTRUCTION, "DOUBLE"), na.rm = TRUE),
  has_gasoline = any(SUBSTANCE_CODE %in% c("GAS", "GASOL"), na.rm = TRUE),
  has_diesel = any(SUBSTANCE_CODE == "DIESL", na.rm = TRUE)
), by = claim_number]

facility_stats[!is.finite(max_tank_age), max_tank_age := NA]
facility_stats[!is.finite(avg_tank_age), avg_tank_age := NA]
facility_stats[, is_over_30_years := (!is.na(max_tank_age) & max_tank_age >= 30)]

# ============================================================================
# 8. MASTER MERGE
# ============================================================================
cat("--- Final Merge ---\n")

# Contract Stats
contract_stats <- contracts[, .(
  total_contract_value = sum(total_contract_value, na.rm = TRUE),
  n_contracts = .N,
  is_pfp = any(auction_type == "Bid-to-Result", na.rm = TRUE),
  is_sow = any(auction_type == "Scope of Work", na.rm = TRUE),
  date_first_contract = min(contract_start, na.rm = TRUE),
  adjuster_id = first(na.omit(adjuster))
), by = claim_number]

# CPI
cpi_table <- get_cpi_factor()
claims[, `:=`(Year = year(claim_date), Month = month(claim_date))]
claims <- merge(claims, cpi_table[, .(Year, Month, Infl_Factor)], by = c("Year", "Month"), all.x = TRUE)
claims[is.na(Infl_Factor), Infl_Factor := 1.0]
claims[, total_paid_real := total_paid * Infl_Factor]

# Master Join
master <- copy(claims)
master <- merge(master, claim_features_wide, by = "claim_number", all.x = TRUE)
feat_cols <- names(claim_features_wide)[names(claim_features_wide) != "claim_number"]
for(col in feat_cols) set(master, which(is.na(master[[col]])), col, 0)

master <- merge(master, risk_shares, by = "claim_number", all.x = TRUE)
share_cols <- c("share_pressure_piping", "share_bare_steel", "share_no_electronic_detection")
for(col in share_cols) set(master, which(is.na(master[[col]])), col, 0) 

master <- merge(master, facility_stats, by = "claim_number", all.x = TRUE)
master <- merge(master, linkage[, .(FAC_ID, final_owner_sector, business_category, Owner_Size_Class)], 
                by.x = "department", by.y = "FAC_ID", all.x = TRUE)
master <- merge(master, contract_stats, by = "claim_number", all.x = TRUE)

# Analysis Vars
master[, log_cost := log(total_paid_real + 1)]
master[is.na(is_pfp), is_pfp := FALSE]
master[is.na(is_sow), is_sow := FALSE]
master[is.na(n_contracts), n_contracts := 0]
master[, contract_type := fcase(
  is_pfp, "Bid-to-Result",
  is_sow, "Scope of Work",
  n_contracts > 0, "Other Contract",
  default = "No Contract"
)]
master[, county := str_to_title(trimws(county))]
master[, region_cluster := fcase(
  county %in% c("Philadelphia", "Delaware", "Montgomery", "Bucks", "Chester"), "Southeast",
  county %in% c("Allegheny", "Washington", "Westmoreland"), "Southwest",
  default = "Other"
)]

# ============================================================================
# 9. SAVE OUTPUTS
# ============================================================================
cat("--- Saving ---\n")
if(!dir.exists(paths$processed)) dir.create(paths$processed, recursive=TRUE)

saveRDS(master, file.path(paths$processed, "master_analysis_dataset.rds"))
analysis_panel <- master[total_paid_real > 1000 & !is.na(business_category)]
saveRDS(analysis_panel, file.path(paths$processed, "analysis_panel.rds"))

cat("COMPLETE: master_analysis_dataset.rds & facility_census_profile.rds created.\n")