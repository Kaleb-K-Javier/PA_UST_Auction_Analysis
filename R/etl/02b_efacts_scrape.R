# R/etl/02b_efacts_scrape.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 2b: eFACTS Compliance Data Scraping
# ============================================================================
# Location: PA_UST_Auction_Analysis/R/etl/02b_efacts_scrape.R
# Purpose: Scrape inspection history, violations, and penalties from eFACTS
# Runtime: ~8-12 hours for full PA universe (~20K facilities)
# Features: Checkpoint/resume capability for long-running server execution
# ============================================================================
# WORKING DIRECTORY: Run from project root (PA_UST_Auction_Analysis/)
#   Rscript R/etl/02b_efacts_scrape.R
#   OR: nohup Rscript R/etl/02b_efacts_scrape.R > scrape.log 2>&1 &
# ============================================================================
# PREREQUISITE: Run 02a_padep_download.R first to create linkage table
# ============================================================================
# Data Collected:
#   - Facility inspection history (FOI inspections, compliance evaluations)
#   - Violation records and details
#   - Tank remediation/release incidents
#   - Permit/authorization history
# ============================================================================
# IMPORTANT: This script scrapes ALL PA facilities to avoid selection bias
# in downstream causal analysis. Do NOT filter to USTIF claims only.
# ============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════════╗\n")
cat("║  ETL Step 2b: eFACTS Compliance Data Scraping                       ║\n")
cat("║  Checkpoint-enabled for long-running server execution               ║\n")
cat("╚══════════════════════════════════════════════════════════════════════╝\n\n")

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG <- list(
  # Scraping behavior
  delay_seconds = 1.5,           # Delay between facility requests (be respectful)
  delay_violation = 0.75,        # Delay for violation detail pages
  batch_size = 100,              # Save checkpoint every N facilities
  max_retries = 3,               # Retries per failed request
  timeout_seconds = 30,          # HTTP timeout
  
  # Paths (relative to project root: PA_UST_Auction_Analysis/)
  padep_dir = "data/external/padep",
  efacts_dir = "data/external/efacts",
  checkpoint_file = "data/external/efacts/scrape_checkpoint.rds",
  
  # Output files (saved to efacts_dir)
  output_inspections = "efacts_inspections.rds",
  output_violations = "efacts_violations.rds",
  output_remediation = "efacts_remediation.rds",
  output_permits = "efacts_permits.rds",
  output_errors = "efacts_scrape_errors.rds",
  output_coverage = "efacts_facility_coverage.rds"  # Missingness tracking
)

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

suppressPackageStartupMessages({
  library(tidyverse)
  library(httr)
  library(rvest)
  library(janitor)
})

# Create directories
if (!dir.exists(CONFIG$efacts_dir)) {
  dir.create(CONFIG$efacts_dir, recursive = TRUE)
}

# Logging with timestamps
log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  message <- sprintf("[%s] %s: %s", timestamp, level, msg)
  cat(message, "\n")
  
  # Also append to log file

  log_file <- file.path(CONFIG$efacts_dir, "scrape_log.txt")
  cat(message, "\n", file = log_file, append = TRUE)
}

# ============================================================================
# SECTION 1: LOAD FACILITY UNIVERSE
# ============================================================================

log_msg("Loading facility universe from 02a downloads...")

linkage_path <- file.path(CONFIG$padep_dir, "facility_linkage_table.rds")

if (!file.exists(linkage_path)) {
  stop(paste0(
    "Facility linkage table not found at: ", linkage_path, "\n",
    "Run 02a_padep_download.R first to create the linkage table."
  ))
}

linkage <- readRDS(linkage_path)

# ============================================================================
# VALIDATE LINKAGE TABLE STRUCTURE
# ============================================================================

# Check required columns exist
required_cols <- c("efacts_facility_id", "permit_number", "registration_status")
missing_cols <- setdiff(required_cols, names(linkage))

if (length(missing_cols) > 0) {

  stop(paste0(
    "Linkage table missing required columns: ", paste(missing_cols, collapse = ", "), "\n",
    "Available columns: ", paste(names(linkage), collapse = ", "), "\n",
    "Re-run 02a_padep_download.R to regenerate the linkage table."
  ))
}

# Check efacts_facility_id is not all NA
if (all(is.na(linkage$efacts_facility_id))) {
  stop(paste0(
    "All efacts_facility_id values are NA in linkage table.\n",
    "This suggests the PASDA data didn't contain PRIMARY_FA field.\n",
    "Check 02a download logs and verify ArcGIS endpoints."
  ))
}

# Extract unique eFACTS facility IDs (PRIMARY_FA)
# Convert to character for consistent handling (checkpoint stores as character)
facility_ids <- linkage %>%
  filter(!is.na(efacts_facility_id)) %>%
  pull(efacts_facility_id) %>%
  as.character() %>%  # Ensure character type for checkpoint compatibility
  unique() %>%
  sort()

log_msg(sprintf("Found %d unique eFACTS facility IDs to scrape", length(facility_ids)))
log_msg(sprintf("  Active facilities: %d", sum(linkage$registration_status == "active", na.rm = TRUE)))
log_msg(sprintf("  Inactive facilities: %d", sum(linkage$registration_status == "inactive", na.rm = TRUE)))

# ============================================================================
# SECTION 2: CHECKPOINT MANAGEMENT
# ============================================================================

#' Load or initialize checkpoint state
load_checkpoint <- function() {
  if (file.exists(CONFIG$checkpoint_file)) {
    checkpoint <- readRDS(CONFIG$checkpoint_file)
    log_msg(sprintf("Resuming from checkpoint: %d/%d facilities completed",
                    checkpoint$completed_count, checkpoint$total_count))
    return(checkpoint)
  } else {
    log_msg("Starting fresh scrape (no checkpoint found)")
    return(list(
      completed_ids = character(0),
      completed_count = 0,
      total_count = length(facility_ids),
      started_at = Sys.time(),
      last_save = Sys.time()
    ))
  }
}

#' Save checkpoint state
save_checkpoint <- function(checkpoint) {
  checkpoint$last_save <- Sys.time()
  saveRDS(checkpoint, CONFIG$checkpoint_file)
}

#' Save accumulated data to disk (RDS + CSV)
save_accumulated_data <- function(data_list, checkpoint) {
  

  # Helper to append or create (saves both RDS and CSV)
  save_or_append <- function(new_data, filename) {
    if (length(new_data) == 0) return(0)
    
    filepath_rds <- file.path(CONFIG$efacts_dir, filename)
    filepath_csv <- str_replace(filepath_rds, "\\.rds$", ".csv")
    new_df <- bind_rows(new_data)
    
    if (nrow(new_df) == 0) return(0)
    
    if (file.exists(filepath_rds)) {
      existing <- readRDS(filepath_rds)
      combined <- bind_rows(existing, new_df)
      saveRDS(combined, filepath_rds)
      write_csv(combined, filepath_csv, na = "")
      return(nrow(new_df))
    } else {
      saveRDS(new_df, filepath_rds)
      write_csv(new_df, filepath_csv, na = "")
      return(nrow(new_df))
    }
  }
  
  n_insp <- save_or_append(data_list$inspections, CONFIG$output_inspections)
  n_viol <- save_or_append(data_list$violations, CONFIG$output_violations)
  n_remed <- save_or_append(data_list$remediation, CONFIG$output_remediation)
  n_perm <- save_or_append(data_list$permits, CONFIG$output_permits)
  n_err <- save_or_append(data_list$errors, CONFIG$output_errors)
  n_cov <- save_or_append(data_list$coverage, CONFIG$output_coverage)
  
  log_msg(sprintf("Saved batch: %d inspections, %d violations, %d remediation, %d permits, %d coverage, %d errors",
                  n_insp, n_viol, n_remed, n_perm, n_cov, n_err))
  
  save_checkpoint(checkpoint)
}

# ============================================================================
# SECTION 3: eFACTS SCRAPING FUNCTIONS
# ============================================================================

#' Scrape a single eFACTS facility page
#' 
#' @param facility_id eFACTS FacilityID (numeric, e.g., 575215)
#' @return List with inspections, permits, remediation, violation_ids, or error
scrape_facility_page <- function(facility_id) {
  
  url <- sprintf(
    "https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleFacility.aspx?FacilityID=%s",
    facility_id
  )
  
  result <- list(
    facility_id = as.character(facility_id),
    url = url,
    scraped_at = Sys.time(),
    inspections = NULL,
    permits = NULL,
    remediation = NULL,
    violation_inspection_ids = character(0),
    error = NULL
  )
  
  # Attempt request with retries
  page <- NULL
  for (attempt in 1:CONFIG$max_retries) {
    tryCatch({
      response <- GET(
        url,
        timeout(CONFIG$timeout_seconds),
        add_headers("User-Agent" = "R-USTIF-Research/2.0 (Academic)")
      )
      
      if (status_code(response) == 200) {
        page <- read_html(response)
        break
      } else {
        Sys.sleep(2^attempt)
      }
    }, error = function(e) {
      if (attempt == CONFIG$max_retries) {
        result$error <- e$message
      }
      Sys.sleep(2^attempt)
    })
  }
  
  if (is.null(page)) {
    if (is.null(result$error)) result$error <- "Failed to retrieve page"
    return(result)
  }
  
  # Check if page contains valid facility data (not an error page)
  # eFACTS error pages typically have specific text patterns
  page_text <- tryCatch(
    page %>% html_text(),
    error = function(e) ""
  )
  
  if (grepl("No records found|Invalid Facility|Error occurred", page_text, ignore.case = TRUE)) {
    result$error <- "No facility data found (possibly invalid FacilityID)"
    return(result)
  }
  
  # Extract all tables safely
  tables <- tryCatch(
    page %>% html_elements("table"),
    error = function(e) {
      result$error <- paste("Failed to extract tables:", e$message)
      return(list())
    }
  )
  
  # If no tables found, return early (not an error - facility may just have no data)
  if (length(tables) == 0) {
    return(result)
  }
  
  # Helper function to safely parse a table
  safe_parse_table <- function(tbl_node, facility_id) {
    tryCatch({
      tbl <- html_table(tbl_node, fill = TRUE)
      
      # Check for empty or malformed table
      if (is.null(tbl) || !is.data.frame(tbl)) return(NULL)
      if (nrow(tbl) == 0) return(NULL)
      if (ncol(tbl) < 2) return(NULL)
      
      # Check if table has actual data (not just headers)
      # Some empty tables render with header row only
      if (nrow(tbl) == 1 && all(is.na(tbl[1,]) | tbl[1,] == "")) return(NULL)
      
      # Clean names and add facility ID
      tbl <- tbl %>%
        clean_names() %>%
        mutate(
          efacts_facility_id = as.character(facility_id),
          .row_id = row_number()
        )
      
      return(tbl)
      
    }, error = function(e) {
      return(NULL)
    })
  }
  
  # Parse each table by identifying its type from column names
  for (tbl_node in tables) {
    
    tbl <- safe_parse_table(tbl_node, facility_id)
    
    # Skip if table parsing failed or returned empty
    if (is.null(tbl)) next
    if (nrow(tbl) == 0) next
    
    col_names <- tolower(names(tbl))
    col_str <- paste(col_names, collapse = " ")
    
    # Inspection table: has "inspection" in columns
    if (grepl("inspection.*type|inspection.*date", col_str) && is.null(result$inspections)) {
      # Additional validation: should have date-like column
      if (any(grepl("date", col_names))) {
        result$inspections <- tbl
      }
    }
    
    # Permits table: has "authorization" or "permit"
    if (grepl("authorization|permit.*number|date.*received", col_str) && is.null(result$permits)) {
      result$permits <- tbl
    }
    
    # Remediation/Tank Remediation table: has "incident" or "release" or "cleanup"
    if (grepl("incident.*name|release.*date|cleanup.*status|confirmed.*release", col_str) && is.null(result$remediation)) {
      result$remediation <- tbl
    }
  }
  
  # Extract inspection IDs that have violation links
  # These are links to the violation detail page
  violation_links <- tryCatch({
    links <- page %>%
      html_elements("a[href*='searchResults_singleViol']") %>%
      html_attr("href")
    
    # Return empty if NULL or not character
    if (is.null(links) || !is.character(links)) return(character(0))
    return(links)
    
  }, error = function(e) character(0))
  
  if (length(violation_links) > 0) {
    # Extract InspectionID from URLs like "...?InspectionID=2908157"
    extracted_ids <- str_extract(violation_links, "\\d+$")
    result$violation_inspection_ids <- extracted_ids[!is.na(extracted_ids)] %>%
      unique() %>%
      as.character()
  }
  
  return(result)
}

#' Scrape violation detail page
#' 
#' @param inspection_id InspectionID from violation link
#' @param facility_id Parent facility ID for reference
#' @return tibble with violation details
scrape_violation_page <- function(inspection_id, facility_id) {
  
  url <- sprintf(
    "https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleViol.aspx?InspectionID=%s",
    inspection_id
  )
  
  for (attempt in 1:CONFIG$max_retries) {
    tryCatch({
      response <- GET(
        url,
        timeout(CONFIG$timeout_seconds),
        add_headers("User-Agent" = "R-USTIF-Research/2.0 (Academic)")
      )
      
      if (status_code(response) == 200) {
        page <- read_html(response)
        
        # Extract all tables from violation page
        table_nodes <- tryCatch(
          page %>% html_elements("table"),
          error = function(e) list()
        )
        
        if (length(table_nodes) == 0) {
          # No tables found - return info row (not an error)
          return(tibble(
            inspection_id = as.character(inspection_id),
            efacts_facility_id = as.character(facility_id),
            violation_url = url,
            note = "No violation tables found on page",
            scraped_at = Sys.time()
          ))
        }
        
        # Parse each table safely
        parsed_tables <- list()
        for (tbl_node in table_nodes) {
          tbl <- tryCatch({
            parsed <- html_table(tbl_node, fill = TRUE)
            
            # Skip empty or malformed tables
            if (is.null(parsed) || !is.data.frame(parsed)) return(NULL)
            if (nrow(parsed) == 0) return(NULL)
            if (ncol(parsed) < 1) return(NULL)
            
            # Clean and return
            parsed %>% clean_names()
            
          }, error = function(e) NULL)
          
          if (!is.null(tbl) && nrow(tbl) > 0) {
            parsed_tables[[length(parsed_tables) + 1]] <- tbl
          }
        }
        
        if (length(parsed_tables) > 0) {
          # Combine all valid tables
          combined <- bind_rows(parsed_tables) %>%
            mutate(
              inspection_id = as.character(inspection_id),
              efacts_facility_id = as.character(facility_id),
              violation_url = url,
              scraped_at = Sys.time()
            )
          return(combined)
        } else {
          # Tables existed but all were empty/unparseable
          return(tibble(
            inspection_id = as.character(inspection_id),
            efacts_facility_id = as.character(facility_id),
            violation_url = url,
            note = "Tables found but none contained parseable data",
            scraped_at = Sys.time()
          ))
        }
      }
      
      Sys.sleep(2^attempt)
      
    }, error = function(e) {
      Sys.sleep(2^attempt)
    })
  }
  
  # Return error row if all attempts failed
  return(tibble(
    inspection_id = as.character(inspection_id),
    efacts_facility_id = as.character(facility_id),
    violation_url = url,
    error = "Failed to retrieve violation details after retries",
    scraped_at = Sys.time()
  ))
}

# ============================================================================
# SECTION 4: MAIN SCRAPING LOOP
# ============================================================================

log_msg("Starting eFACTS scrape...")
cat("\n")

# Load checkpoint
checkpoint <- load_checkpoint()

# Determine which facilities still need scraping
remaining_ids <- setdiff(as.character(facility_ids), checkpoint$completed_ids)
log_msg(sprintf("%d facilities remaining to scrape", length(remaining_ids)))

if (length(remaining_ids) == 0) {
  log_msg("All facilities already scraped. Nothing to do.")
  quit(save = "no")
}

# Initialize batch accumulators
batch_data <- list(
  inspections = list(),
  violations = list(),
  remediation = list(),
  permits = list(),
  errors = list(),
  coverage = list()  # Facility-level missingness tracking
)

batch_count <- 0
total_processed <- checkpoint$completed_count

# Progress tracking
start_time <- Sys.time()

for (i in seq_along(remaining_ids)) {
  
  fid <- remaining_ids[i]
  
  # Progress indicator
  total_processed <- total_processed + 1
  pct <- round(100 * total_processed / checkpoint$total_count, 1)
  
  elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  rate <- i / elapsed
  eta_seconds <- (length(remaining_ids) - i) / rate
  eta_hours <- round(eta_seconds / 3600, 1)
  
  cat(sprintf("\r[%s] Facility %d/%d (%.1f%%) | Rate: %.1f/sec | ETA: %.1fh    ",
              format(Sys.time(), "%H:%M:%S"),
              total_processed, 
              checkpoint$total_count,
              pct,
              rate,
              eta_hours))
  
  # Scrape facility page
  facility_result <- scrape_facility_page(fid)
  
  # =========================================================================
  # CREATE COVERAGE/MISSINGNESS RECORD FOR THIS FACILITY
  # =========================================================================
  # This tracks what data exists (or is missing) for each facility
  # Old/closed facilities with no inspection history will have 0s - this is
  # expected and useful for analysis, not an error condition
  
  n_inspections <- if (!is.null(facility_result$inspections)) nrow(facility_result$inspections) else 0
  n_permits <- if (!is.null(facility_result$permits)) nrow(facility_result$permits) else 0
  n_remediation <- if (!is.null(facility_result$remediation)) nrow(facility_result$remediation) else 0
  n_violation_links <- length(facility_result$violation_inspection_ids)
  
  coverage_record <- tibble(
    efacts_facility_id = as.character(fid),
    
    # Binary missingness indicators (1 = missing/no data, 0 = has data)
    missing_inspections = as.integer(n_inspections == 0),
    missing_permits = as.integer(n_permits == 0),
    missing_remediation = as.integer(n_remediation == 0),
    missing_violations = as.integer(n_violation_links == 0),
    
    # Inverse: has data indicators (1 = has data, 0 = no data)
    has_inspections = as.integer(n_inspections > 0),
    has_permits = as.integer(n_permits > 0),
    has_remediation = as.integer(n_remediation > 0),
    has_violations = as.integer(n_violation_links > 0),
    
    # Counts for more granular analysis
    n_inspection_records = n_inspections,
    n_permit_records = n_permits,
    n_remediation_records = n_remediation,
    n_violation_links = n_violation_links,
    
    # Scrape metadata
    scrape_status = if (is.null(facility_result$error)) "success" else "error",
    scrape_error = if (is.null(facility_result$error)) NA_character_ else facility_result$error,
    scraped_at = Sys.time()
  )
  
  batch_data$coverage[[length(batch_data$coverage) + 1]] <- coverage_record
  
  # =========================================================================
  # ACCUMULATE DATA RESULTS (unchanged logic)
  # =========================================================================
  
  # Only log errors for actual HTTP/parsing failures, not missing data
  if (!is.null(facility_result$error)) {
    batch_data$errors[[length(batch_data$errors) + 1]] <- tibble(
      efacts_facility_id = fid,
      error = facility_result$error,
      url = facility_result$url,
      scraped_at = Sys.time()
    )
  }
  
  if (!is.null(facility_result$inspections) && nrow(facility_result$inspections) > 0) {
    batch_data$inspections[[length(batch_data$inspections) + 1]] <- facility_result$inspections
  }
  
  if (!is.null(facility_result$permits) && nrow(facility_result$permits) > 0) {
    batch_data$permits[[length(batch_data$permits) + 1]] <- facility_result$permits
  }
  
  if (!is.null(facility_result$remediation) && nrow(facility_result$remediation) > 0) {
    batch_data$remediation[[length(batch_data$remediation) + 1]] <- facility_result$remediation
  }
  
  # Scrape violation detail pages
  if (length(facility_result$violation_inspection_ids) > 0) {
    for (insp_id in facility_result$violation_inspection_ids) {
      Sys.sleep(CONFIG$delay_violation)
      viol_result <- scrape_violation_page(insp_id, fid)
      batch_data$violations[[length(batch_data$violations) + 1]] <- viol_result
    }
  }
  
  # Update checkpoint
  checkpoint$completed_ids <- c(checkpoint$completed_ids, fid)
  checkpoint$completed_count <- checkpoint$completed_count + 1
  batch_count <- batch_count + 1
  
  # Save batch checkpoint
  if (batch_count >= CONFIG$batch_size) {
    cat("\n")
    log_msg(sprintf("Saving checkpoint at %d facilities...", total_processed))
    save_accumulated_data(batch_data, checkpoint)
    
    # Reset batch accumulators
    batch_data <- list(
      inspections = list(),
      violations = list(),
      remediation = list(),
      permits = list(),
      errors = list(),
      coverage = list()
    )
    batch_count <- 0
    cat("\n")
  }
  
  # Rate limiting
  Sys.sleep(CONFIG$delay_seconds)
}

# Final save
cat("\n")
log_msg("Saving final batch...")
save_accumulated_data(batch_data, checkpoint)

# ============================================================================
# SECTION 5: POST-PROCESSING AND SUMMARY
# ============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════════╗\n")
cat("║  SCRAPING COMPLETE                                                   ║\n")
cat("╚══════════════════════════════════════════════════════════════════════╝\n\n")

# Load and summarize results
load_and_count <- function(filename) {
  filepath <- file.path(CONFIG$efacts_dir, filename)
  if (file.exists(filepath)) {
    df <- readRDS(filepath)
    return(list(records = nrow(df), file = filepath))
  }
  return(list(records = 0, file = NA))
}

results <- list(
  Inspections = load_and_count(CONFIG$output_inspections),
  Violations = load_and_count(CONFIG$output_violations),
  Remediation = load_and_count(CONFIG$output_remediation),
  Permits = load_and_count(CONFIG$output_permits),
  Coverage = load_and_count(CONFIG$output_coverage),
  Errors = load_and_count(CONFIG$output_errors)
)

cat("Scraped Data Summary:\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
for (name in names(results)) {
  cat(sprintf("  %-15s %8d records\n", paste0(name, ":"), results[[name]]$records))
}
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

# Coverage/Missingness Summary
coverage_path <- file.path(CONFIG$efacts_dir, CONFIG$output_coverage)
if (file.exists(coverage_path)) {
  coverage_df <- readRDS(coverage_path)
  
  cat("\nMissingness Summary (from coverage table):\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat(sprintf("  Total facilities scraped:     %d\n", nrow(coverage_df)))
  cat(sprintf("  Missing inspections:          %d (%.1f%%)\n", 
              sum(coverage_df$missing_inspections), 
              100 * mean(coverage_df$missing_inspections)))
  cat(sprintf("  Missing permits:              %d (%.1f%%)\n", 
              sum(coverage_df$missing_permits), 
              100 * mean(coverage_df$missing_permits)))
  cat(sprintf("  Missing remediation:          %d (%.1f%%)\n", 
              sum(coverage_df$missing_remediation), 
              100 * mean(coverage_df$missing_remediation)))
  cat(sprintf("  Missing violations:           %d (%.1f%%)\n", 
              sum(coverage_df$missing_violations), 
              100 * mean(coverage_df$missing_violations)))
  cat(sprintf("  Scrape errors:                %d (%.1f%%)\n",
              sum(coverage_df$scrape_status == "error"),
              100 * mean(coverage_df$scrape_status == "error")))
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("\nNote: Missing data for old/closed facilities is EXPECTED.\n")
  cat("Use coverage table to analyze missingness patterns vs facility characteristics.\n")
}

total_time <- difftime(Sys.time(), checkpoint$started_at, units = "hours")
cat(sprintf("\nTotal scraping time: %.1f hours\n", as.numeric(total_time)))
cat(sprintf("Facilities scraped: %d\n", checkpoint$completed_count))
cat(sprintf("Average rate: %.2f facilities/minute\n", 
            checkpoint$completed_count / (as.numeric(total_time) * 60)))

# Final CSV export (verification - CSVs already saved incrementally with checkpoints)
log_msg("Verifying final CSV exports...")
for (rds_file in c(CONFIG$output_inspections, CONFIG$output_violations, 
                   CONFIG$output_remediation, CONFIG$output_permits)) {
  filepath <- file.path(CONFIG$efacts_dir, rds_file)
  if (file.exists(filepath)) {
    csv_path <- str_replace(filepath, "\\.rds$", ".csv")
    df <- readRDS(filepath)
    write_csv(df, csv_path)
    log_msg(sprintf("  → %s", basename(csv_path)))
  }
}

cat("\n")
cat("Files created in", CONFIG$efacts_dir, ":\n")
list.files(CONFIG$efacts_dir, pattern = "\\.(rds|csv)$") %>%
  paste0("  • ", .) %>%
  cat(sep = "\n")

cat("\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
cat("NEXT STEP: Run 03_build_panel.R to construct facility-year panel\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

# Clean up checkpoint file on successful completion
if (checkpoint$completed_count >= checkpoint$total_count) {
  log_msg("Scrape completed successfully. Checkpoint file retained for reference.")
}
