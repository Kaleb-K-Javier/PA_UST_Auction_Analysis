# R/etl/02a_padep_download.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 2a: PA DEP Bulk Data Downloads
# ============================================================================
# Purpose: 
#   1. Download Active/Inactive Facility Locations via GIS API (Spatial Truth)
#   2. Download Detailed Tank Inventory via Regional Excel Archives (Engineering Truth)
#   3. Build Facility Linkage Table (Required for Step 02b Scraper)
#   4. Build Master Tank Dataset (Engineering + Spatial + IDs)
# ============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════════╗\n")
cat("║  ETL Step 2a: PA DEP Bulk Data Downloads (Refactored v4)             ║\n")
cat("║  Outputs:                                                            ║\n")
cat("║    1. facility_linkage_table.csv (Input for 02b Scraper)             ║\n")
cat("║    2. master_tank_list.rds (Merged Engineering + Spatial Data)       ║\n")
cat("╚══════════════════════════════════════════════════════════════════════╝\n\n")

# ============================================================================
# SETUP
# ============================================================================

if (!file.exists("R/etl") && !file.exists("data/processed")) {
  stop("Wrong directory. Please setwd() to project root (PA_UST_Auction_Analysis/).")
}

suppressPackageStartupMessages({
  library(tidyverse)
  library(httr)
  library(jsonlite)
  library(janitor)
  library(readxl)
  library(lubridate)
})

paths <- list(
  external = "data/external",
  padep = "data/external/padep",
  processed = "data/processed",
  raw_csv = "data/external/padep/raw_csv_backups"
)

for (p in paths) if (!dir.exists(p)) dir.create(p, recursive = TRUE)

log_msg <- function(msg, level = "INFO") {
  cat(sprintf("[%s] %s: %s\n", format(Sys.time(), "%H:%M:%S"), level, msg))
}

# ============================================================================
# SECTION 1: DOWNLOAD GIS FACILITY LAYERS (SPATIAL + EFACTS ID)
# ============================================================================

log_msg("Starting GIS Facility Downloads...")

GIS_ENDPOINTS <- list(
  active = list(
    url = "https://mapservices.pasda.psu.edu/server/rest/services/pasda/DEP/MapServer/27/query",
    output = "pasda_facilities_active.rds"
  ),
  inactive = list(
    url = "https://mapservices.pasda.psu.edu/server/rest/services/pasda/DEP2/MapServer/20/query",
    output = "pasda_facilities_inactive.rds"
  )
)

query_arcgis_rest <- function(base_url) {
  all_records <- list()
  offset <- 0
  has_more <- TRUE
  
  while (has_more) {
    tryCatch({
      resp <- GET(
        base_url, 
        query = list(where="1=1", outFields="*", f="json", resultOffset=offset, resultRecordCount=1000),
        timeout(60)
      )
      
      if (http_error(resp)) stop("HTTP Error")
      
      content_json <- fromJSON(content(resp, "text", encoding="UTF-8"), flatten = TRUE)
      features <- if("attributes" %in% names(content_json$features)) content_json$features$attributes else content_json$features
      
      if (!is.null(features) && nrow(features) > 0) {
        all_records[[length(all_records) + 1]] <- features
        cat(sprintf("\r  Downloaded %d records...", offset + nrow(features)))
        if (nrow(features) < 1000) has_more <- FALSE else offset <- offset + 1000
      } else { has_more <- FALSE }
      
    }, error = function(e) { has_more <<- FALSE })
    Sys.sleep(0.2)
  }
  cat("\n")
  if(length(all_records)==0) return(NULL)
  bind_rows(all_records) %>% clean_names() %>% as_tibble()
}

active_gis <- query_arcgis_rest(GIS_ENDPOINTS$active$url)
inactive_gis <- query_arcgis_rest(GIS_ENDPOINTS$inactive$url)

# Save Backups
if(!is.null(active_gis)) {
  saveRDS(active_gis, file.path(paths$padep, GIS_ENDPOINTS$active$output))
  write_csv(active_gis, file.path(paths$raw_csv, "raw_gis_active.csv"))
}
if(!is.null(inactive_gis)) {
  saveRDS(inactive_gis, file.path(paths$padep, GIS_ENDPOINTS$inactive$output))
  write_csv(inactive_gis, file.path(paths$raw_csv, "raw_gis_inactive.csv"))
}

log_msg("✓ GIS Facility downloads complete (RDS + Raw CSV).")

# ============================================================================
# SECTION 2: DOWNLOAD REGIONAL TANK ARCHIVES (ENGINEERING TRUTH)
# ============================================================================

log_msg("Starting Regional Excel Archive Downloads...")

base_url <- "http://files.dep.state.pa.us/EnvironmentalCleanupBrownfields/StorageTanks/StorageTanksPortalFiles/Registration/"

regions <- list(
  "4100" = "Southeast",
  "4200" = "Northeast",
  "4300" = "Southcentral",
  "4400" = "Northcentral",
  "4500" = "Southwest",
  "4600" = "Northwest"
)

downloaded_excels <- c()

for (code in names(regions)) {
  region_name <- regions[[code]]
  filename <- sprintf("Storage_Tank_Listing_External_public_region%s.xls", code)
  target_url <- paste0(base_url, filename)
  local_path <- file.path(paths$padep, sprintf("tank_inventory_%s.xls", tolower(region_name)))
  
  if (!file.exists(local_path)) {
    tryCatch({
      cat(sprintf("  Fetching %s Region (%s)...\n", region_name, code))
      download.file(url = target_url, destfile = local_path, mode = "wb", quiet = TRUE)
      
      if (file.exists(local_path) && file.size(local_path) > 10000) {
        log_msg(sprintf("✓ Saved: %s", basename(local_path)))
        downloaded_excels <- c(downloaded_excels, local_path)
      } else {
        log_msg(sprintf("⚠ Download failed or empty: %s", region_name), "WARN")
      }
    }, error = function(e) log_msg(sprintf("✗ Error: %s", e$message), "ERROR"))
    Sys.sleep(1)
  } else {
    log_msg(sprintf("  Skipping %s (Exists)", region_name))
    downloaded_excels <- c(downloaded_excels, local_path)
  }
}

# ============================================================================
# SECTION 3: PROCESS & NORMALIZE EXCEL DATA
# ============================================================================

log_msg("Processing Excel Archives into Unified Tank Dataset...")

process_region_excel <- function(file_path) {
  tryCatch({
    # Read Excel (Auto-detect types)
    df <- suppressMessages(read_excel(file_path, .name_repair = "universal"))
    
    # 1. SAVE RAW CSV BACKUP
    raw_csv_name <- paste0("raw_", str_replace(basename(file_path), "\\.xls$", ".csv"))
    write_csv(df, file.path(paths$raw_csv, raw_csv_name))
    
    # 2. Map columns using ACTUAL names
    df_clean <- df %>%
      select(
        site_id = any_of(c("PF_SITE_ID", "SITE_ID")), 
        facility_id = any_of(c("PF_OTHER_ID", "OTHER_ID")),
        facility_name = any_of(c("PF_NAME", "NAME")),
        address = any_of(c("LOCAD_PF_ADDRESS_1", "ADDRESS")),
        county = any_of(c("PF_COUNTY_NAME", "COUNTY")),
        municipality = any_of(c("PF_MUNICIPALITY_NAME", "MUNICIPALITY")),
        region_code = any_of(c("REGION_ICS_CODE", "ORGANIZATION")),
        client_id = any_of("CLIENT_ID"),
        owner_name = any_of(c("MAILING_NAME", "CLIENT_NAME")),
        tank_seq = any_of(c("SEQUENCE_NUMBER", "SEQUENCE.NUMBER")),
        tank_type = any_of(c("TANK_CODE", "TANK.CODE")),
        install_date = any_of(c("DATE_INSTALLED", "DATE.INSTALLED")),
        capacity_gallons = any_of("CAPACITY"),
        substance = any_of(c("SUBSTANCE_CODE", "SUBSTANCE")),
        status_code = any_of(c("STATUS_CODE", "STATUS.CODE")),
        permit_status = any_of(c("STATUS", "PERMIT.STATUS")),
        last_inspection_date = any_of(c("TANK_LAST_DATE_INSPECTED", "DATE.LAST.INSPECTION")),
        inspection_type = any_of(c("INSPECTION_CODE", "INSPECTION.CODE"))
      ) %>%
      mutate(
        source_file = basename(file_path),
        is_underground = (tank_type == "UST"),
        is_aboveground = (tank_type == "AST"),
        capacity_gallons = as.numeric(capacity_gallons),
        install_date = as.Date(install_date),
        last_inspection_date = as.Date(last_inspection_date)
      )
    
    return(df_clean)
  }, error = function(e) {
    log_msg(sprintf("Error parsing %s: %s", basename(file_path), e$message), "ERROR")
    return(NULL)
  })
}

tank_list <- map(downloaded_excels, process_region_excel)
all_tanks_df <- bind_rows(tank_list)

if (nrow(all_tanks_df) > 0) {
  log_msg(sprintf("✓ Successfully parsed %d tank records from Excel.", nrow(all_tanks_df)))
} else {
  stop("CRITICAL: No tank records parsed. Cannot proceed.")
}

# ============================================================================
# SECTION 4: BUILD LINKAGE & MASTER DATASETS
# ============================================================================

log_msg("Building Linkage Table and Master Tank Dataset...")

if (!is.null(active_gis) || !is.null(inactive_gis)) {
  
  # 1. Normalize GIS Data
  # We select the permit_number (for joining) and efacts_facility_id (for scraping)
  gis_combined <- bind_rows(
    if(!is.null(active_gis)) active_gis %>% mutate(gis_status = "Active"),
    if(!is.null(inactive_gis)) inactive_gis %>% mutate(gis_status = "Inactive")
  ) %>%
    clean_names() %>%
    select(
      permit_number = any_of(c("facility_id", "attributes_facility_id", "facility_i", "attributes_facility_i")),
      efacts_facility_id = any_of(c("primary_facility_id", "attributes_primary_facility_id", "primary_fa", "attributes_primary_fa")),
      latitude = any_of(c("latitude", "attributes_latitude")),
      longitude = any_of(c("longitude", "attributes_longitude")),
      gis_address = any_of(c("facility_address1", "attributes_facility_address1", "attributes_facility_a"))
    ) %>%
    filter(!is.na(permit_number)) %>%
    # Ensure IDs are character to match Excel
    mutate(
      permit_number = as.character(permit_number),
      efacts_facility_id = as.character(efacts_facility_id)
    ) %>%
    # Deduplicate: prefer records with eFACTS IDs
    arrange(permit_number, desc(!is.na(efacts_facility_id))) %>%
    distinct(permit_number, .keep_all = TRUE)
  
  # 2. CREATE FACILITY LINKAGE TABLE (For Scraper 02b)
  # This is the master list of unique facilities
  excel_facilities <- all_tanks_df %>%
    distinct(facility_id, .keep_all = TRUE) %>%
    select(facility_id, facility_name, address, county, municipality, region_code, site_id, client_id, owner_name)
  
  linkage_table <- excel_facilities %>%
    left_join(gis_combined, by = c("facility_id" = "permit_number"))
  
  saveRDS(linkage_table, file.path(paths$padep, "facility_linkage_table.rds"))
  write_csv(linkage_table, file.path(paths$padep, "facility_linkage_table.csv"))
  
  log_msg(sprintf("✓ Saved Linkage Table: %d facilities. Ready for 02b Scraper.", nrow(linkage_table)))
  log_msg(sprintf("  - Records with eFACTS ID: %d", sum(!is.na(linkage_table$efacts_facility_id))))
  
  # 3. CREATE MASTER TANK DATASET (For Analysis)
  # This attaches the GIS location (lat/long) and eFACTS ID to every single tank row
  master_tank_list <- all_tanks_df %>%
    left_join(gis_combined, by = c("facility_id" = "permit_number"))
  
  saveRDS(master_tank_list, file.path(paths$processed, "master_tank_list.rds"))
  write_csv(master_tank_list, file.path(paths$processed, "master_tank_list.csv"))
  
  log_msg(sprintf("✓ Saved Master Tank List: %d tanks with geospatial data.", nrow(master_tank_list)))
  
} else {
  log_msg("⚠ GIS data missing - skipping Linkage/Master merge.", "WARN")
}

cat("\nETL 02a Complete.\n")