# R/etl/02a_padep_download.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 2a: PA DEP Bulk Data Downloads
# ============================================================================
# Location: PA_UST_Auction_Analysis/R/etl/02a_padep_download.R
# Purpose: Download all programmatically accessible PA UST datasets
# Runtime: ~5-10 minutes (API rate limits, pagination)
# Output: Normalized raw tables in data/external/padep/
# ============================================================================
# WORKING DIRECTORY: Run from project root (PA_UST_Auction_Analysis/)
#   Rscript R/etl/02a_padep_download.R
#   OR in RStudio with project open
# ============================================================================
# Data Sources:
#   1. PASDA ArcGIS REST (Active tanks, Inactive tanks, Cleanup sites)
#   2. eMapPA External Extraction (Alternative schema, Land Recycling)
#   3. Tank Cleanup Incidents (LUST release data - if SSRS accessible)
# ============================================================================
# NOTE: This replaces/enhances the original 02_padep_acquisition.R
# ============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════════╗\n")
cat("║  ETL Step 2a: PA DEP Bulk Data Downloads                            ║\n")
cat("║  Downloads all non-eFACTS PA UST datasets                           ║\n")
cat("╚══════════════════════════════════════════════════════════════════════╝\n\n")

# ============================================================================
# SETUP
# ============================================================================

# Validate working directory (should be project root)
if (!file.exists("R/etl") && !file.exists("data/processed")) {
  stop(paste0(
    "Working directory does not appear to be the project root.\n",
    "Current directory: ", getwd(), "\n",
    "Expected: PA_UST_Auction_Analysis/\n",
    "Please setwd() to project root or open the .Rproj file in RStudio."
  ))
}

# Load dependencies
suppressPackageStartupMessages({
  library(tidyverse)
  library(httr)
  library(jsonlite)
  library(janitor)
})

# Paths configuration (relative to project root: PA_UST_Auction_Analysis/)
if (!exists("paths")) {
  paths <- list(
    external = "data/external",
    padep = "data/external/padep",    # Created by this script
    processed = "data/processed"
  )
}

# Create directory structure
for (p in paths) {
  if (!dir.exists(p)) {
    dir.create(p, recursive = TRUE)
    cat(sprintf("Created directory: %s\n", p))
  }
}

# Logging function
log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(sprintf("[%s] %s: %s\n", timestamp, level, msg))
}

# ============================================================================
# SECTION 1: ArcGIS REST API ENDPOINTS
# ============================================================================

# PASDA MapServer endpoints (monthly updates, authoritative)
ENDPOINTS <- list(
  
  # PASDA DEP MapServer - Primary source
  pasda_active = list(
    name = "PASDA Active Storage Tanks",
    url = "https://mapservices.pasda.psu.edu/server/rest/services/pasda/DEP/MapServer/27/query",
    output = "pasda_tanks_active.rds",
    description = "Active registered storage tank facilities"
  ),
  
  pasda_inactive = list(
    name = "PASDA Inactive Storage Tanks", 
    url = "https://mapservices.pasda.psu.edu/server/rest/services/pasda/DEP2/MapServer/20/query",
    output = "pasda_tanks_inactive.rds",
    description = "Closed/removed storage tank facilities"
  ),
  
  pasda_cleanup = list(
    name = "PASDA Land Recycling Cleanup",
    url = "https://mapservices.pasda.psu.edu/server/rest/services/pasda/DEP/MapServer/18/query",
    output = "pasda_cleanup_sites.rds",
    description = "Land recycling and cleanup locations (includes tank sites)"
  ),
  
  # eMapPA External Extraction - Alternative/supplementary
  emappa_active = list(
    name = "eMapPA Active Storage Tanks",
    url = "https://gis.dep.pa.gov/depgisprd/rest/services/emappa/eMapPA_External_Extraction/MapServer/118/query",
    output = "emappa_tanks_active.rds",
    description = "DEP eMapPA active tanks (may have additional fields)"
  ),
  
  emappa_inactive = list(
    name = "eMapPA Inactive Storage Tanks",
    url = "https://gis.dep.pa.gov/depgisprd/rest/services/emappa/eMapPA_External_Extraction/MapServer/119/query",
    output = "emappa_tanks_inactive.rds",
    description = "DEP eMapPA inactive tanks"
  ),
  
  emappa_land_recycling = list(
    name = "eMapPA Land Recycling Cleanup",
    url = "https://gis.dep.pa.gov/depgisprd/rest/services/emappa/eMapPA_External_Extraction/MapServer/26/query",
    output = "emappa_land_recycling.rds",
    description = "Act 2 Land Recycling Program sites"
  )
)

# ============================================================================
# SECTION 2: GENERIC ArcGIS REST QUERY FUNCTION
# ============================================================================

#' Query ArcGIS REST endpoint with automatic pagination
#' 
#' @param base_url Full query URL for the layer
#' @param where_clause SQL WHERE clause (default "1=1" for all records)
#' @param out_fields Fields to return ("*" for all)
#' @param batch_size Records per request (max typically 1000)
#' @param max_retries Number of retry attempts on failure
#' @param timeout_seconds Request timeout
#' @return tibble of all records, or NULL on complete failure
query_arcgis_rest <- function(base_url,
                               where_clause = "1=1",
                               out_fields = "*",
                               batch_size = 1000,
                               max_retries = 3,
                               timeout_seconds = 120) {
  
  all_records <- list()
  offset <- 0
  has_more <- TRUE
  total_retrieved <- 0
  
  while (has_more) {
    
    # Build query with current offset
    query_params <- list(
      where = where_clause,
      outFields = out_fields,
      returnGeometry = "false",
      f = "json",
      resultOffset = offset,
      resultRecordCount = batch_size
    )
    
    # Retry logic
    success <- FALSE
    attempts <- 0
    
    while (!success && attempts < max_retries) {
      attempts <- attempts + 1
      
      tryCatch({
        response <- GET(
          url = base_url,
          query = query_params,
          timeout(timeout_seconds),
          add_headers("User-Agent" = "R-USTIF-Research/2.0")
        )
        
        if (http_status(response)$category == "Success") {
          success <- TRUE
        } else {
          log_msg(sprintf("HTTP %d at offset %d (attempt %d/%d)", 
                          status_code(response), offset, attempts, max_retries), "WARN")
          Sys.sleep(2^attempts)  # Exponential backoff
        }
        
      }, error = function(e) {
        log_msg(sprintf("Request error at offset %d (attempt %d/%d): %s",
                        offset, attempts, max_retries, e$message), "WARN")
        Sys.sleep(2^attempts)
      })
    }
    
    if (!success) {
      log_msg(sprintf("Failed after %d attempts at offset %d", max_retries, offset), "ERROR")
      break
    }
    
    # Parse response
    content_text <- content(response, as = "text", encoding = "UTF-8")
    result <- tryCatch(
      fromJSON(content_text, flatten = TRUE),
      error = function(e) {
        log_msg(sprintf("JSON parse error: %s", e$message), "ERROR")
        return(NULL)
      }
    )
    
    if (is.null(result)) break
    
    # Check for API error
    if (!is.null(result$error)) {
      log_msg(sprintf("API error: %s", result$error$message), "ERROR")
      break
    }
    
    # Extract features
    if (!is.null(result$features) && length(result$features) > 0) {
      
      # Handle nested attributes structure
      if ("attributes" %in% names(result$features)) {
        attrs <- result$features$attributes
      } else {
        attrs <- result$features
      }
      
      if (is.data.frame(attrs) && nrow(attrs) > 0) {
        all_records[[length(all_records) + 1]] <- attrs
        batch_count <- nrow(attrs)
        total_retrieved <- total_retrieved + batch_count
        
        cat(sprintf("\r  Retrieved %d records (batch: %d)...", total_retrieved, batch_count))
        
        # Check if more records exist
        if (batch_count < batch_size) {
          has_more <- FALSE
        } else {
          offset <- offset + batch_size
        }
      } else {
        has_more <- FALSE
      }
    } else {
      has_more <- FALSE
    }
    
    # Rate limiting - be respectful
    Sys.sleep(0.5)
  }
  
  cat("\n")
  
  if (length(all_records) == 0) {
    return(NULL)
  }
  
  # Combine and clean
  combined <- bind_rows(all_records) %>%
    clean_names() %>%
    as_tibble()
  
  return(combined)
}

# ============================================================================
# SECTION 3: DOWNLOAD ALL ENDPOINTS
# ============================================================================

log_msg("Starting PA DEP data downloads...")
cat("\n")

download_results <- list()

for (endpoint_name in names(ENDPOINTS)) {
  
  endpoint <- ENDPOINTS[[endpoint_name]]
  output_path <- file.path(paths$padep, endpoint$output)
  
  cat(sprintf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"))
  cat(sprintf("Downloading: %s\n", endpoint$name))
  cat(sprintf("Source: %s\n", endpoint$url))
  cat(sprintf("Output: %s\n", output_path))
  cat(sprintf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"))
  
  start_time <- Sys.time()
  
  data <- query_arcgis_rest(endpoint$url)
  
  elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 1)
  
  if (!is.null(data) && nrow(data) > 0) {
    
    # Add metadata columns
    data <- data %>%
      mutate(
        .source = endpoint_name,
        .downloaded_at = Sys.time()
      )
    
    # Save RDS (preserves types)
    saveRDS(data, output_path)
    
    # Also save CSV for inspection/portability
    csv_path <- str_replace(output_path, "\\.rds$", ".csv")
    write_csv(data, csv_path, na = "")
    
    log_msg(sprintf("✓ Saved %d records in %.1fs (RDS + CSV)", nrow(data), elapsed))
    
    download_results[[endpoint_name]] <- list(
      success = TRUE,
      records = nrow(data),
      fields = ncol(data),
      elapsed_seconds = as.numeric(elapsed)
    )
    
  } else {
    log_msg(sprintf("✗ No data retrieved (%.1fs)", elapsed), "WARN")
    
    download_results[[endpoint_name]] <- list(
      success = FALSE,
      records = 0,
      fields = 0,
      elapsed_seconds = as.numeric(elapsed)
    )
  }
  
  cat("\n")
}

# ============================================================================
# SECTION 4: CREATE UNIFIED FACILITY LINKAGE TABLE
# ============================================================================

log_msg("Building unified facility linkage table...")

# Load downloaded data
pasda_active <- tryCatch(

  readRDS(file.path(paths$padep, "pasda_tanks_active.rds")),
  error = function(e) NULL
)

pasda_inactive <- tryCatch(
  readRDS(file.path(paths$padep, "pasda_tanks_inactive.rds")),
  error = function(e) NULL
)

# Combine and deduplicate
if (!is.null(pasda_active) || !is.null(pasda_inactive)) {
  
  # Standardize column names across sources
  # Uses any_of() to handle missing columns gracefully
  standardize_facility_cols <- function(df, status_label) {
    if (is.null(df)) return(NULL)
    
    # Check which key columns exist
    available_cols <- names(df)
    has_permit <- any(c("facility_i", "facility_id") %in% available_cols)
    has_efacts <- any(c("primary_fa", "primary_facility_id") %in% available_cols)
    
    if (!has_permit) {
      log_msg(sprintf("WARNING: No permit number column found in %s data", status_label), "WARN")
    }
    if (!has_efacts) {
      log_msg(sprintf("WARNING: No eFACTS facility ID column (PRIMARY_FA) found in %s data", status_label), "WARN")
    }
    
    result <- df %>%
      select(
        permit_number = any_of(c("facility_i", "facility_id")),
        efacts_facility_id = any_of(c("primary_fa", "primary_facility_id")),
        site_id = any_of(c("site_id")),
        facility_name = any_of(c("facility_n", "facility_name")),
        address = any_of(c("facility_a", "facility_address")),
        city = any_of(c("facility_2", "facility_city")),
        county = any_of(c("facility_c", "county")),
        municipality = any_of(c("facility_m", "municipality")),
        zip = any_of(c("facility_z", "zip")),
        latitude = any_of(c("latitude")),
        longitude = any_of(c("longitude")),
        owner_id = any_of(c("tank_owner")),
        owner_name = any_of(c("tank_own_1", "owner_name"))
      ) %>%
      mutate(registration_status = status_label)
    
    # Ensure efacts_facility_id is character for consistent handling
    if ("efacts_facility_id" %in% names(result)) {
      result <- result %>%
        mutate(efacts_facility_id = as.character(efacts_facility_id))
    }
    
    return(result)
  }
  
  linkage_active <- standardize_facility_cols(pasda_active, "active")
  linkage_inactive <- standardize_facility_cols(pasda_inactive, "inactive")
  
  # Combine, keeping active status if facility appears in both
  linkage_table <- bind_rows(linkage_active, linkage_inactive) %>%
    group_by(permit_number) %>%
    arrange(desc(registration_status == "active")) %>%
    slice(1) %>%
    ungroup() %>%
    filter(!is.na(permit_number))
  
  # Save linkage table (RDS + CSV)
  saveRDS(linkage_table, file.path(paths$padep, "facility_linkage_table.rds"))
  write_csv(linkage_table, file.path(paths$padep, "facility_linkage_table.csv"), na = "")
  
  log_msg(sprintf("✓ Created linkage table: %d unique facilities (RDS + CSV)", nrow(linkage_table)))
  
  # Validate linkage table for 02b compatibility
  n_with_efacts <- sum(!is.na(linkage_table$efacts_facility_id))
  if (n_with_efacts == 0) {
    log_msg("CRITICAL: No facilities have eFACTS IDs - 02b scraping will fail!", "ERROR")
    log_msg("Check that PASDA data contains PRIMARY_FA column", "ERROR")
  } else if (n_with_efacts < nrow(linkage_table) * 0.5) {
    log_msg(sprintf("WARNING: Only %d/%d (%.1f%%) facilities have eFACTS IDs", 
                    n_with_efacts, nrow(linkage_table), 
                    100 * n_with_efacts / nrow(linkage_table)), "WARN")
  }
  
  # Summary statistics
  cat("\n")
  cat("Linkage Table Summary:\n")
  cat(sprintf("  Total facilities: %d\n", nrow(linkage_table)))
  cat(sprintf("  Active: %d\n", sum(linkage_table$registration_status == "active")))
  cat(sprintf("  Inactive: %d\n", sum(linkage_table$registration_status == "inactive")))
  cat(sprintf("  With eFACTS ID: %d (required for 02b scraping)\n", n_with_efacts))
  cat(sprintf("  With coordinates: %d\n", sum(!is.na(linkage_table$latitude))))
  
} else {
  log_msg("Could not create linkage table - no facility data downloaded", "WARN")
}

# ============================================================================
# SECTION 5: DOWNLOAD SUMMARY
# ============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════════╗\n")
cat("║  DOWNLOAD SUMMARY                                                    ║\n")
cat("╚══════════════════════════════════════════════════════════════════════╝\n\n")

summary_df <- tibble(
  Source = names(download_results),
  Success = map_lgl(download_results, ~ .x$success),
  Records = map_int(download_results, ~ .x$records),
  Fields = map_int(download_results, ~ .x$fields),
  Seconds = map_dbl(download_results, ~ .x$elapsed_seconds)
)

print(summary_df, n = Inf)

cat("\n")
cat(sprintf("Total records downloaded: %d\n", sum(summary_df$Records)))
cat(sprintf("Total time: %.1f seconds\n", sum(summary_df$Seconds)))

# Save download manifest
manifest <- list(
  download_time = Sys.time(),
  r_version = R.version.string,
  results = download_results,
  paths = paths
)
saveRDS(manifest, file.path(paths$padep, "download_manifest.rds"))

cat("\n")
cat("Files created in", paths$padep, ":\n")
list.files(paths$padep, pattern = "\\.(rds|csv)$") %>%
  paste0("  • ", .) %>%
  cat(sep = "\n")

cat("\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
cat("NEXT STEP: Run 02b_efacts_scrape.R to collect inspection/violation data\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
