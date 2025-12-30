# R/etl/02_padep_acquisition.R
# ============================================================================
# Pennsylvania UST Auction Analysis - ETL Step 2: PA DEP External Data
# ============================================================================
# Purpose: Download PA DEP administrative datasets to enrich USTIF data
# Status: OPTIONAL - Analysis can proceed without this step
# ============================================================================

cat("\n========================================\n")
cat("ETL Step 2: PA DEP Data Acquisition\n")
cat("========================================\n\n")

# Load dependencies
library(tidyverse)
library(httr)
library(jsonlite)
library(janitor)

# Load paths
if (!exists("paths")) {
  paths <- list(
    external = "data/external",
    processed = "data/processed"
  )
}

# Ensure external data directory exists
if (!dir.exists(paths$external)) {
  dir.create(paths$external, recursive = TRUE)
}

# ============================================================================
# FUNCTION: Download from PA DEP ArcGIS REST API
# ============================================================================

download_padep_arcgis <- function(service_name, layer_id = 0, output_name, max_records = 10000) {
  
  base_url <- "https://services1.arcgis.com/HVMpV1fZrjkpsQRi/arcgis/rest/services"
  query_url <- paste0(base_url, "/", service_name, "/FeatureServer/", layer_id, "/query")
  
  # Query parameters
  query_params <- list(
    where = "1=1",
    outFields = "*",
    returnGeometry = "true",
    f = "json",
    resultRecordCount = max_records
  )
  
  cat(paste("Attempting to download:", service_name, "\n"))
  cat(paste("URL:", query_url, "\n"))
  
  # Make API request with timeout and retry logic
  max_attempts <- 3
  attempt <- 1
  success <- FALSE
  
  while (attempt <= max_attempts && !success) {
    tryCatch({
      response <- GET(
        url = query_url,
        query = query_params,
        timeout(60),
        add_headers("User-Agent" = "R-USTIF-Research/1.0")
      )
      
      if (http_status(response)$category == "Success") {
        success <- TRUE
      } else {
        cat(paste("  Attempt", attempt, "failed:", http_status(response)$message, "\n"))
        attempt <- attempt + 1
        Sys.sleep(2)
      }
    }, error = function(e) {
      cat(paste("  Attempt", attempt, "error:", e$message, "\n"))
      attempt <<- attempt + 1
      Sys.sleep(2)
    })
  }
  
  if (!success) {
    cat(paste("✗ Failed to download:", service_name, "\n\n"))
    return(NULL)
  }
  
  # Parse JSON response
  content_text <- content(response, as = "text", encoding = "UTF-8")
  data_list <- fromJSON(content_text, flatten = TRUE)
  
  # Check for features
  if (is.null(data_list$features) || length(data_list$features) == 0) {
    cat(paste("✗ No features returned for:", service_name, "\n\n"))
    return(NULL)
  }
  
  # Extract to data frame
  df <- as_tibble(data_list$features$attributes) %>%
    clean_names()
  
  # Extract geometry if present
  if ("geometry" %in% names(data_list$features)) {
    geom <- data_list$features$geometry
    if (!is.null(geom) && "x" %in% names(geom)) {
      df <- df %>%
        mutate(
          longitude = geom$x,
          latitude = geom$y
        )
    }
  }
  
  # Save raw data
  saveRDS(df, file.path(paths$external, paste0(output_name, "_raw.rds")))
  write_csv(df, file.path(paths$external, paste0(output_name, "_raw.csv")))
  
  cat(paste("✓ Downloaded:", nrow(df), "records\n"))
  cat(paste("✓ Saved:", file.path(paths$external, paste0(output_name, "_raw.rds")), "\n\n"))
  
  return(df)
}

# ============================================================================
# ATTEMPT DATA DOWNLOADS
# ============================================================================

# Note: These service names are educated guesses based on PA DEP portal structure
# Manual verification may be required at: https://data-padep-1.opendata.arcgis.com/

cat("Attempting PA DEP data downloads...\n")
cat("(Note: Service names may need manual verification)\n\n")

# Dataset 1: Storage Tanks Registry
tanks_padep <- NULL
tank_service_names <- c(
  "Storage_Tanks",
  "StorageTanks", 
  "UST_Tanks",
  "Underground_Storage_Tanks"
)

for (service in tank_service_names) {
  tanks_padep <- download_padep_arcgis(
    service_name = service,
    layer_id = 0,
    output_name = "padep_tanks"
  )
  if (!is.null(tanks_padep)) break
}

# Dataset 2: Storage Tank Cleanup Sites
cleanup_padep <- NULL
cleanup_service_names <- c(
  "Storage_Tank_Cleanup_Program",
  "StorageTankCleanup",
  "LUST_Sites",
  "Tank_Cleanup"
)

for (service in cleanup_service_names) {
  cleanup_padep <- download_padep_arcgis(
    service_name = service,
    layer_id = 0,
    output_name = "padep_cleanup"
  )
  if (!is.null(cleanup_padep)) break
}

# ============================================================================
# MANUAL DOWNLOAD INSTRUCTIONS
# ============================================================================

if (is.null(tanks_padep) || is.null(cleanup_padep)) {
  cat("========================================\n")
  cat("MANUAL DOWNLOAD REQUIRED\n")
  cat("========================================\n\n")
  cat("Automated download failed. Please manually download from PA DEP:\n\n")
  cat("1. Navigate to: https://data-padep-1.opendata.arcgis.com/\n\n")
  cat("2. Search for relevant datasets:\n")
  cat("   • 'Underground Storage Tanks' or 'Storage Tanks'\n")
  cat("   • 'Storage Tank Cleanup' or 'LUST Sites'\n\n")
  cat("3. Download as CSV and save to:\n")
  cat(paste("   •", file.path(paths$external, "padep_tanks_manual.csv"), "\n"))
  cat(paste("   •", file.path(paths$external, "padep_cleanup_manual.csv"), "\n\n"))
  cat("4. Re-run this script to load manual downloads.\n\n")
  
  # Check for manual downloads
  manual_tanks <- file.path(paths$external, "padep_tanks_manual.csv")
  manual_cleanup <- file.path(paths$external, "padep_cleanup_manual.csv")
  
  if (file.exists(manual_tanks)) {
    cat("Found manual tanks file, loading...\n")
    tanks_padep <- read_csv(manual_tanks, show_col_types = FALSE) %>% clean_names()
    saveRDS(tanks_padep, file.path(paths$external, "padep_tanks_raw.rds"))
    cat(paste("✓ Loaded:", nrow(tanks_padep), "records from manual download\n\n"))
  }
  
  if (file.exists(manual_cleanup)) {
    cat("Found manual cleanup file, loading...\n")
    cleanup_padep <- read_csv(manual_cleanup, show_col_types = FALSE) %>% clean_names()
    saveRDS(cleanup_padep, file.path(paths$external, "padep_cleanup_raw.rds"))
    cat(paste("✓ Loaded:", nrow(cleanup_padep), "records from manual download\n\n"))
  }
}

# ============================================================================
# SUMMARY
# ============================================================================

cat("========================================\n")
cat("ETL STEP 2 COMPLETE\n")
cat("========================================\n")
cat("\nStatus:\n")
cat(paste("  • PA DEP Tanks:", ifelse(!is.null(tanks_padep), 
                                      paste(nrow(tanks_padep), "records"), 
                                      "NOT LOADED"), "\n"))
cat(paste("  • PA DEP Cleanup:", ifelse(!is.null(cleanup_padep), 
                                        paste(nrow(cleanup_padep), "records"), 
                                        "NOT LOADED"), "\n"))
cat("\n")
cat("NOTE: This step is optional. Analysis can proceed with USTIF data alone.\n")
cat("\n")
cat("NEXT STEP:\n")
cat("  Run: source('R/etl/03_merge_master_dataset.R')\n")
cat("========================================\n\n")
