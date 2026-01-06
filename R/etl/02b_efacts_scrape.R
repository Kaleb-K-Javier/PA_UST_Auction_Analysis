# R/etl/02b_efacts_scrape.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 2b: eFACTS Scraper v23 (Production)
# ============================================================================
# Purpose: Scrape PA DEP eFACTS compliance system for facility-level data
# 
# FIXES IMPLEMENTED (v23):
#   1. Schema enforcement in save_batch() with fill=TRUE and use.names=TRUE
#   2. Inspection table: Explicit ID extraction from "Type (ID)" format
#   3. Permit tasks: Ghost row filtering (empty task names)
#   4. Violations: Full enforcement data parsing with state machine
#   5. Checkpoint/resume with atomic saves
#   6. ERROR HANDLING FIX: Direct data.table construction instead of template[1, :=]
#
# Input:  facility_linkage_table.csv (from 02a) → efacts_facility_id column
# Output: 9 normalized CSVs in data/external/efacts/
# ============================================================================
# USAGE:
#   source("R/etl/02b_efacts_scrape.R")
#   linkage <- read.csv("data/external/padep/facility_linkage_table.csv")
#   ids <- unique(linkage$efacts_facility_id[!is.na(linkage$efacts_facility_id)])
#   run_scraper(ids)
# ============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════════╗\n")
cat("║  ETL Step 2b: eFACTS Scraper v23 (Production Ready)                 ║\n")
cat("║  Fixes: Schema enforcement, ID splitting, ghost row removal,        ║\n")
cat("║         error handling data.table construction                      ║\n")
cat("╚══════════════════════════════════════════════════════════════════════╝\n\n")

suppressPackageStartupMessages({
  library(data.table)
  library(httr)
  library(rvest)
  library(janitor)
  library(stringr)
})



# ============================================================================
# 1. CONFIGURATION (UPDATED FOR PARALLEL WORKERS)
# ============================================================================

# Define Worker ID (Default to "main" if not specified)
if (!exists("WORKER_ID")) WORKER_ID <- "main"

CONFIG <- list(
  # Timing
  delay_facility = 1.5,
  delay_detail = 0.75,
  
  # Batch settings
  batch_size = 100,
  timeout = 45,
  max_retries = 3,
  
  # Feature flags
  scrape_violations = TRUE,
  scrape_permit_details = TRUE,
  scrape_remediation_details = TRUE,
  
  # Dynamic Paths (Append WORKER_ID to prevent file conflicts)
  efacts_dir = "data/external/efacts",
  checkpoint_file = sprintf("data/external/efacts/scrape_checkpoint_v23_%s.rds", WORKER_ID),
  log_file = sprintf("data/external/efacts/scrape_log_%s.txt", WORKER_ID),
  
  # Function to generate unique output filenames
  get_outfile = function(base_name) {
    return(sprintf("efacts_%s_%s", base_name, WORKER_ID))
  }
)

# ============================================================================
# 2. CANONICAL SCHEMA DEFINITIONS
# ============================================================================
# These schemas are AUTHORITATIVE. All scraped data is coerced to match.

SCHEMAS <- list(
  
  # 1. Facility Metadata
  meta = data.table(
    efacts_facility_id = character(),
    facility_id = character(),
    facility_name = character(),
    address = character(),
    status = character(),
    program = character()
  ),
  
  # 2. Tanks (Sub-facilities)
  tanks = data.table(
    efacts_facility_id = character(),
    tank_internal_id = character(),
    sub_facility_name = character(),
    type = character(),
    other_id = character(),
    status = character(),
    e_map_pa_location = character()
  ),
  
  # 3. Inspections (FIX: explicit ID and clean name columns)
  inspections = data.table(
    efacts_facility_id = character(),
    inspection_id = character(),
    inspection_type_clean = character(),
    inspection_type_raw = character(),
    inspection_date = character(),
    result = character()
  ),
  
  # 4. Violations (Full enforcement schema)
  violations = data.table(
    efacts_facility_id = character(),
    inspection_id = character(),
    violation_id = character(),
    violation_date = character(),
    description = character(),
    resolution = character(),
    citation = character(),
    violation_type = character(),
    enforcement_id = character(),
    enf_type = character(),
    penalty_assessed = character(),
    penalty_collected = character(),
    date_executed = character(),
    penalty_final_date = character(),
    total_amount_due = character(),
    taken_against = character(),
    on_appeal = character(),
    penalty_status = character(),
    enforcement_status = character(),
    num_violations_addressed = character()
  ),
  
  # 5. Remediation Summary
  rem_summary = data.table(
    efacts_facility_id = character(),
    lrpact_id = character(),
    incident_name = character(),
    confirmed_release_date = character(),
    type = character(),
    cleanup_status = character(),
    cleanup_status_date = character()
  ),
  
  # 6. Remediation Substances
  rem_sub = data.table(
    efacts_facility_id = character(),
    lrpact_id = character(),
    substance_internal_id = character(),
    incident_name = character(),
    confirmed_release_date = character(),
    incident_id = character(),
    incident_type = character(),
    cleanup_status = character(),
    cleanup_status_date = character(),
    substance_released = character(),
    environmental_impact = character()
  ),
  
  # 7. Remediation Milestones
  rem_mil = data.table(
    efacts_facility_id = character(),
    lrpact_id = character(),
    milestone_internal_id = character(),
    milestone_name = character(),
    milestone_event_date = character(),
    milestone_due_date = character(),
    i_milestone_status = character(),
    milestone_response_date = character()
  ),
  
  # 8. Permits Detail
  perm_det = data.table(
    efacts_facility_id = character(),
    auth_id = character(),
    authorization_id = character(),
    permit_number = character(),
    site = character(),
    client = character(),
    authorization_type = character(),
    application_type = character(),
    authorization_is_for = character(),
    date_received = character(),
    status = character()
  ),
  
  # 9. Permit Tasks (FIX: removed artifact columns)
  perm_tasks = data.table(
    efacts_facility_id = character(),
    auth_id = character(),
    task_internal_id = character(),
    task = character(),
    start_date = character(),
    target_date = character(),
    completion_date = character()
  ),
  
  # 10. Coverage Tracking
  coverage = data.table(
    efacts_facility_id = character(),
    found_meta = logical(),
    n_tanks = integer(),
    n_inspections = integer(),
    n_violations = integer(),
    n_permits = integer(),
    n_remediation = integer(),
    status = character(),
    scraped_at = character()
  ),
  
  # 11. Errors
  errors = data.table(
    efacts_facility_id = character(),
    error_msg = character(),
    error_time = character()
  )
)

# ============================================================================
# 3. SCHEMA ENFORCEMENT HELPERS
# ============================================================================

#' Get empty template matching canonical schema
get_template <- function(schema_name) {
  if (!schema_name %in% names(SCHEMAS)) {
    stop(paste("Unknown schema:", schema_name))
  }
  copy(SCHEMAS[[schema_name]])
}

#' Coerce data.table to match canonical schema
#' FIX: Handles column mismatches gracefully
enforce_schema <- function(dt, schema_name) {
  if (is.null(dt) || nrow(dt) == 0) {
    return(get_template(schema_name))
  }
  
  target <- SCHEMAS[[schema_name]]
  target_cols <- names(target)
  

  # Add missing columns as NA
  missing_cols <- setdiff(target_cols, names(dt))
  if (length(missing_cols) > 0) {
    dt[, (missing_cols) := NA_character_]
  }
  
  # Select only canonical columns in correct order
  dt <- dt[, ..target_cols]
  
  # Force character type for all columns except coverage integers
  if (schema_name == "coverage") {
    char_cols <- c("efacts_facility_id", "status", "scraped_at")
    int_cols <- c("n_tanks", "n_inspections", "n_violations", "n_permits", "n_remediation")
    bool_cols <- c("found_meta")
    dt[, (char_cols) := lapply(.SD, as.character), .SDcols = char_cols]
    dt[, (int_cols) := lapply(.SD, as.integer), .SDcols = int_cols]
    dt[, (bool_cols) := lapply(.SD, as.logical), .SDcols = bool_cols]
  } else {
    dt[, (target_cols) := lapply(.SD, as.character)]
  }
  
  return(dt)
}

# ============================================================================
# 4. LOGGING
# ============================================================================

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  formatted <- sprintf("[%s] %s: %s", timestamp, level, msg)
  cat(sprintf("[%s] %s: %s\n", format(Sys.time(), "%H:%M:%S"), level, msg))
  write(formatted, file = CONFIG$log_file, append = TRUE)
}

# ============================================================================
# 5. HTTP REQUEST HELPER
# ============================================================================

fetch_page <- function(url, retries = CONFIG$max_retries) {
  attempt <- 0
  
  while (attempt < retries) {
    attempt <- attempt + 1
    
    result <- tryCatch({
      res <- GET(
        url,
        timeout(CONFIG$timeout),
        add_headers("User-Agent" = "R-USTIF-Research/2.3")
      )
      
      if (status_code(res) == 200) {
        return(list(success = TRUE, page = read_html(res), error = NULL))
      } else {
        list(success = FALSE, page = NULL, error = paste("HTTP", status_code(res)))
      }
    }, error = function(e) {
      list(success = FALSE, page = NULL, error = e$message)
    })
    
    if (result$success) return(result)
    
    if (attempt < retries) {
      Sys.sleep(2^attempt)  # Exponential backoff
    }
  }
  
  return(list(success = FALSE, page = NULL, error = result$error))
}

# ============================================================================
# 6. TABLE PARSING HELPER
# ============================================================================

parse_table_raw <- function(node) {
  if (is.na(node) || is.null(node)) return(NULL)
  
  tryCatch({
    tbl <- html_table(node, fill = TRUE)
    if (nrow(tbl) == 0) return(NULL)
    
    # Skip "No records" tables
    if (nrow(tbl) == 1 && grepl("No records", as.character(tbl[1,1]), ignore.case = TRUE)) {
      return(NULL)
    }
    
    # Handle tables where first row is actually header
    if (all(grepl("^(X|Var)\\d+$", names(tbl), ignore.case = TRUE)) && nrow(tbl) > 1) {
      names(tbl) <- as.character(tbl[1,])
      tbl <- tbl[-1,]
    }
    
    dt <- as.data.table(tbl)
    setnames(dt, janitor::make_clean_names(names(dt)))
    
    # Remove completely empty rows
    dt <- dt[rowSums(is.na(dt) | dt == "") < ncol(dt)]
    
    return(dt)
  }, error = function(e) NULL)
}

# ============================================================================
# 7. FACILITY PAGE SCRAPER
# ============================================================================

scrape_facility_page <- function(facility_id) {
  url <- sprintf(
    "https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleFacility.aspx?FacilityID=%s",
    facility_id
  )
  
  result <- list(
    id = facility_id,
    url = url,
    error = NULL,
    meta_dt = NULL,
    tanks_dt = NULL,
    insp_dt = NULL,
    perm_dt = NULL,
    rem_dt = NULL,
    viol_links = character(),
    auth_links = character(),
    remed_links = character()
  )
  
  # Fetch page
  fetch_result <- fetch_page(url)
  if (!fetch_result$success) {
    result$error <- fetch_result$error
    return(result)
  }
  page <- fetch_result$page
  
  # Check for invalid facility
  page_text <- html_text(page)
  if (grepl("Invalid Facility|No records found", page_text)) {
    result$error <- "Invalid/Empty Facility"
    return(result)
  }
  
  # ---- 1. METADATA ----
  meta_node <- page %>% html_element("#ContentPlaceHolder2_DetailsView1")
  if (!is.na(meta_node)) {
    rows <- meta_node %>% html_elements("tr")
    if (length(rows) > 0) {
      keys <- rows %>% 
        html_element("td:nth-child(1)") %>% 
        html_text(trim = TRUE) %>% 
        str_remove(":$") %>% 
        make_clean_names()
      vals <- rows %>% 
        html_element("td:nth-child(2)") %>% 
        html_text(trim = TRUE)
      
      valid_idx <- !is.na(keys) & !is.na(vals) & keys != ""
      if (any(valid_idx)) {
        dt <- as.data.table(t(vals[valid_idx]))
        setnames(dt, keys[valid_idx])
        dt[, efacts_facility_id := as.character(facility_id)]
        result$meta_dt <- enforce_schema(dt, "meta")
      }
    }
  }
  
  # ---- 2. TANKS ----
  tanks_node <- page %>% html_element("#ContentPlaceHolder2_GridView1")
  t_dt <- parse_table_raw(tanks_node)
  if (!is.null(t_dt)) {
    t_dt[, efacts_facility_id := as.character(facility_id)]
    if ("sub_facility_name" %in% names(t_dt)) {
      t_dt[, tank_internal_id := paste0(facility_id, "_", make_clean_names(sub_facility_name))]
    }
    result$tanks_dt <- enforce_schema(t_dt, "tanks")
  }
  
  # ---- 3. PERMITS (Summary + extract links) ----
  permits_node <- page %>% html_element("#ContentPlaceHolder2_GridView2")
  if (!is.na(permits_node)) {
    result$auth_links <- permits_node %>%
      html_elements("a[href*='AuthID']") %>%
      html_attr("href") %>%
      str_extract("AuthID=\\d+") %>%
      str_remove("AuthID=") %>%
      unique() %>%
      na.omit()
  }
  
  # ---- 4. INSPECTIONS (FIX: Extract ID from "Type (ID)" format) ----
  insp_node <- page %>% html_element("#ContentPlaceHolder2_GridView3")
  i_dt <- parse_table_raw(insp_node)
  if (!is.null(i_dt)) {
    i_dt[, efacts_facility_id := as.character(facility_id)]
    
    # FIX: Handle "Inspection Type (ID)" → split into clean name and ID
    type_col <- intersect(c("inspection_type", "type"), names(i_dt))
    if (length(type_col) > 0) {
      setnames(i_dt, type_col[1], "inspection_type_raw", skip_absent = TRUE)
      
      # Extract numeric ID from parentheses
      i_dt[, inspection_id := str_extract(inspection_type_raw, "(?<=\\()\\d+(?=\\))")]
      
      # Clean name (remove ID portion)
      i_dt[, inspection_type_clean := str_trim(str_remove(inspection_type_raw, "\\s*\\(\\d+\\)"))]
    }
    
    result$insp_dt <- enforce_schema(i_dt, "inspections")
    
    # Extract violation links
    result$viol_links <- insp_node %>%
      html_elements("a[href*='singleViol']") %>%
      html_attr("href") %>%
      str_extract("InspectionID=\\d+") %>%
      str_remove("InspectionID=") %>%
      unique() %>%
      na.omit()
  }
  
  # ---- 5. REMEDIATION (Summary + extract links) ----
  rem_node <- page %>% html_element("#ContentPlaceHolder2_GridView4")
  r_dt <- parse_table_raw(rem_node)
  if (!is.null(r_dt)) {
    r_dt[, efacts_facility_id := as.character(facility_id)]
    
    # Extract LRPACT_ID from incident_name if present
    if ("incident_name" %in% names(r_dt)) {
      r_dt[, lrpact_id := str_extract(incident_name, "(?<=\\()\\d+(?=\\))")]
    }
    
    result$rem_dt <- enforce_schema(r_dt, "rem_summary")
    
    # Extract remediation detail links
    result$remed_links <- rem_node %>%
      html_elements("a[href*='LRPACT_ID']") %>%
      html_attr("href") %>%
      str_extract("LRPACT_ID=\\d+") %>%
      str_remove("LRPACT_ID=") %>%
      unique() %>%
      na.omit()
  }
  
  return(result)
}

# ============================================================================
# 8. VIOLATION DETAIL SCRAPER (State Machine Parser)
# ============================================================================

scrape_violation_detail <- function(inspection_id, facility_id) {
  url <- sprintf(
    "https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleViol.aspx?InspectionID=%s",
    inspection_id
  )
  
  final_dt <- get_template("violations")
  
  fetch_result <- fetch_page(url)
  if (!fetch_result$success) return(final_dt)
  page <- fetch_result$page
  
  # Find violation tables (border="2" GridViewTable)
  viol_tables <- page %>% html_elements("table.GridViewTable[border='2']")
  if (length(viol_tables) == 0) return(final_dt)
  
  row_list <- list()
  
  for (tbl in viol_tables) {
    rows <- tbl %>% html_elements("tr")
    
    # State machine for parsing
    curr_v <- list(
      id = NA, date = NA, desc = NA, res = NA, cit = NA, type = NA
    )
    enforcements <- list()
    
    if (length(rows) > 1) {
      for (i in 2:length(rows)) {
        tr <- rows[[i]]
        tds <- tr %>% html_elements("td")
        tr_text <- html_text(tr, trim = TRUE)
        
        # Check if this is a NEW VIOLATION row (has rowspan)
        rowspan_attr <- html_attr(tds, "rowspan")
        has_rowspan <- length(rowspan_attr) > 0 && !is.na(rowspan_attr[1])
        
        if (has_rowspan) {
          # Flush previous violation
          if (!is.na(curr_v$id)) {
            if (length(enforcements) == 0) {
              row_list[[length(row_list) + 1]] <- list(
                efacts_facility_id = facility_id,
                inspection_id = inspection_id,
                violation_id = curr_v$id,
                violation_date = curr_v$date,
                description = curr_v$desc,
                resolution = curr_v$res,
                citation = curr_v$cit,
                violation_type = curr_v$type
              )
            } else {
              for (e in enforcements) {
                row_list[[length(row_list) + 1]] <- c(
                  list(
                    efacts_facility_id = facility_id,
                    inspection_id = inspection_id,
                    violation_id = curr_v$id,
                    violation_date = curr_v$date,
                    description = curr_v$desc,
                    resolution = curr_v$res,
                    citation = curr_v$cit,
                    violation_type = curr_v$type
                  ),
                  e
                )
              }
            }
          }
          
          # Start new violation
          curr_v <- list(
            id = html_text(tds[[1]], trim = TRUE),
            date = html_text(tds[[2]], trim = TRUE),
            desc = html_text(tds[[3]], trim = TRUE),
            res = NA, cit = NA, type = NA
          )
          enforcements <- list()
          
        } else {
          # Parse detail rows
          if (str_starts(tr_text, "Resolution:")) {
            curr_v$res <- str_remove(tr_text, "^Resolution:\\s*")
          }
          if (str_starts(tr_text, "PA Code Legal Citation:")) {
            curr_v$cit <- str_remove(tr_text, "^PA Code Legal Citation:\\s*") %>%
              str_remove(" : PA Code Website.*")
          }
          if (str_starts(tr_text, "Violation Type:")) {
            curr_v$type <- str_remove(tr_text, "^Violation Type:\\s*")
          }
          
          # Parse nested ENFORCEMENT tables
          nested_tables <- tr %>% html_elements("table")
          for (nt in nested_tables) {
            if (grepl("Enforcement ID:", html_text(nt))) {
              cells <- nt %>% html_elements("td") %>% html_text(trim = TRUE)
              
              e_rec <- list(
                enforcement_id = NA, enf_type = NA, penalty_assessed = NA,
                penalty_collected = NA, date_executed = NA, penalty_final_date = NA,
                total_amount_due = NA, taken_against = NA, on_appeal = NA,
                penalty_status = NA, enforcement_status = NA, num_violations_addressed = NA
              )
              
              for (cell in cells) {
                if (str_detect(cell, "^Enforcement ID:")) 
                  e_rec$enforcement_id <- str_remove(cell, "Enforcement ID:\\s*")
                if (str_detect(cell, "^Enforcement Type:")) 
                  e_rec$enf_type <- str_remove(cell, "Enforcement Type:\\s*")
                if (str_detect(cell, "^Penalty Amount Assessed:")) 
                  e_rec$penalty_assessed <- str_remove(cell, "Penalty Amount Assessed:\\s*")
                if (str_detect(cell, "^Total Amount Collected:")) 
                  e_rec$penalty_collected <- str_remove(cell, "Total Amount Collected:\\s*")
                if (str_detect(cell, "^Date Executed:")) 
                  e_rec$date_executed <- str_remove(cell, "Date Executed:\\s*")
                if (str_detect(cell, "^Penalty Final Date:")) 
                  e_rec$penalty_final_date <- str_remove(cell, "Penalty Final Date:\\s*")
                if (str_detect(cell, "^Total Amount Due:")) 
                  e_rec$total_amount_due <- str_remove(cell, "Total Amount Due:\\s*")
                if (str_detect(cell, "^Taken Against:")) 
                  e_rec$taken_against <- str_remove(cell, "Taken Against:\\s*")
                if (str_detect(cell, "^On Appeal\\?")) 
                  e_rec$on_appeal <- str_remove(cell, "On Appeal\\?\\s*")
                if (str_detect(cell, "^Penalty Status:")) 
                  e_rec$penalty_status <- str_remove(cell, "Penalty Status:\\s*")
                if (str_detect(cell, "^Enforcement Status:")) 
                  e_rec$enforcement_status <- str_remove(cell, "Enforcement Status:\\s*")
                if (str_detect(cell, "^# of Violations Addressed")) 
                  e_rec$num_violations_addressed <- str_extract(cell, "\\d+$")
              }
              
              enforcements[[length(enforcements) + 1]] <- e_rec
            }
          }
        }
      }
    }
    
    # Flush final violation
    if (!is.na(curr_v$id)) {
      if (length(enforcements) == 0) {
        row_list[[length(row_list) + 1]] <- list(
          efacts_facility_id = facility_id,
          inspection_id = inspection_id,
          violation_id = curr_v$id,
          violation_date = curr_v$date,
          description = curr_v$desc,
          resolution = curr_v$res,
          citation = curr_v$cit,
          violation_type = curr_v$type
        )
      } else {
        for (e in enforcements) {
          row_list[[length(row_list) + 1]] <- c(
            list(
              efacts_facility_id = facility_id,
              inspection_id = inspection_id,
              violation_id = curr_v$id,
              violation_date = curr_v$date,
              description = curr_v$desc,
              resolution = curr_v$res,
              citation = curr_v$cit,
              violation_type = curr_v$type
            ),
            e
          )
        }
      }
    }
  }
  
  if (length(row_list) > 0) {
    dt_raw <- rbindlist(row_list, fill = TRUE)
    return(enforce_schema(dt_raw, "violations"))
  }
  
  return(final_dt)
}

# ============================================================================
# 9. PERMIT DETAIL SCRAPER
# ============================================================================

scrape_permit_detail <- function(auth_id, facility_id) {
  url <- sprintf(
    "https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleAuth.aspx?AuthID=%s",
    auth_id
  )
  
  out <- list(
    details = get_template("perm_det"),
    tasks = get_template("perm_tasks")
  )
  
  fetch_result <- fetch_page(url)
  if (!fetch_result$success) return(out)
  page <- fetch_result$page
  
  # ---- MAIN DETAILS ----
  detail_rows <- page %>% html_elements("#ContentPlaceHolder2_DetailsView1 tr")
  if (length(detail_rows) > 0) {
    keys <- detail_rows %>%
      html_element("td:nth-child(1)") %>%
      html_text(trim = TRUE) %>%
      str_remove(":$") %>%
      make_clean_names()
    vals <- detail_rows %>%
      html_element("td:nth-child(2)") %>%
      html_text(trim = TRUE)
    
    valid_idx <- !is.na(keys) & !is.na(vals) & keys != ""
    if (any(valid_idx)) {
      dt <- as.data.table(t(vals[valid_idx]))
      setnames(dt, keys[valid_idx])
      dt[, `:=`(
        auth_id = as.character(auth_id),
        efacts_facility_id = as.character(facility_id)
      )]
      out$details <- enforce_schema(dt, "perm_det")
    }
  }
  
  # ---- TASKS (FIX: Filter ghost rows) ----
  static_tables <- page %>% html_elements("table.StaticTable")
  for (tbl in static_tables) {
    t <- html_table(tbl, fill = TRUE)
    if ("Task" %in% names(t)) {
      t_dt <- as.data.table(t)
      setnames(t_dt, janitor::make_clean_names(names(t_dt)))
      
      # FIX: Remove ghost rows (empty task names from nested table artifacts)
      t_dt <- t_dt[!is.na(task) & task != "" & str_length(str_trim(task)) > 1]
      
      if (nrow(t_dt) > 0) {
        t_dt[, `:=`(
          auth_id = as.character(auth_id),
          efacts_facility_id = as.character(facility_id),
          task_internal_id = paste0(auth_id, "_", make_clean_names(task), "_", .I)
        )]
        out$tasks <- enforce_schema(t_dt, "perm_tasks")
      }
      break
    }
  }
  
  return(out)
}

# ============================================================================
# 10. REMEDIATION DETAIL SCRAPER
# ============================================================================

scrape_remediation_detail <- function(lrpact_id, facility_id) {
  url <- sprintf(
    "https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleTankRemediation.aspx?LRPACT_ID=%s",
    lrpact_id
  )
  
  out <- list(
    substances = get_template("rem_sub"),
    milestones = get_template("rem_mil")
  )
  
  fetch_result <- fetch_page(url)
  if (!fetch_result$success) return(out)
  page <- fetch_result$page
  
  # ---- SUBSTANCES ----
  s_dt <- parse_table_raw(page %>% html_element("#ContentPlaceHolder2_GridView1"))
  if (!is.null(s_dt)) {
    s_dt[, `:=`(
      efacts_facility_id = as.character(facility_id),
      lrpact_id = as.character(lrpact_id),
      substance_internal_id = paste0(lrpact_id, "_sub_", .I)
    )]
    out$substances <- enforce_schema(s_dt, "rem_sub")
  }
  
  # ---- MILESTONES ----
  m_dt <- parse_table_raw(page %>% html_element("#ContentPlaceHolder2_GridView2"))
  if (!is.null(m_dt)) {
    m_dt[, `:=`(
      efacts_facility_id = as.character(facility_id),
      lrpact_id = as.character(lrpact_id),
      milestone_internal_id = paste0(lrpact_id, "_mil_", .I)
    )]
    out$milestones <- enforce_schema(m_dt, "rem_mil")
  }
  
  return(out)
}

# ============================================================================
# 11. BATCH SAVE (FIX: Schema enforcement on append + Parallel Support)
# ============================================================================

save_batch <- function(batch) {
  
  save_single <- function(data_list, schema_name, file_name) {
    if (length(data_list) == 0) return()
    
    # Combine all tables in batch, filling missing columns
    combined_dt <- rbindlist(data_list, fill = TRUE, use.names = TRUE)
    combined_dt[, batch_import_date := as.character(Sys.time())]
    
    # ------------------------------------------------------------------
    # PARALLELIZATION FIX: Append WORKER_ID if it exists
    # This prevents race conditions when running multiple scrapers.
    # Defaults to empty string (original behavior) if WORKER_ID is missing.
    # ------------------------------------------------------------------
    suffix <- if (exists("WORKER_ID")) paste0("_", WORKER_ID) else ""
    final_name <- paste0(file_name, suffix)
    
    rds_path <- file.path(CONFIG$efacts_dir, paste0(final_name, ".rds"))
    csv_path <- file.path(CONFIG$efacts_dir, paste0(final_name, ".csv"))
    
    # FIX: If file exists, load and merge with schema enforcement
    if (file.exists(rds_path)) {
      old_dt <- tryCatch(readRDS(rds_path), error = function(e) NULL)
      if (!is.null(old_dt)) {
        # Ensure both have same columns before binding
        combined_dt <- rbindlist(
          list(old_dt, combined_dt),
          fill = TRUE,
          use.names = TRUE
        )
      }
    }
    
    # Save atomically (write temp, then rename)
    temp_rds <- paste0(rds_path, ".tmp")
    temp_csv <- paste0(csv_path, ".tmp")
    
    saveRDS(combined_dt, temp_rds)
    fwrite(combined_dt, temp_csv)
    
    file.rename(temp_rds, rds_path)
    file.rename(temp_csv, csv_path)
  }
  
  # Save each table type
  save_single(batch$meta, "meta", "efacts_facility_meta")
  save_single(batch$tanks, "tanks", "efacts_tanks")
  save_single(batch$insp, "inspections", "efacts_inspections")
  save_single(batch$viol, "violations", "efacts_violations")
  save_single(batch$rem, "rem_summary", "efacts_remediation_summary")
  save_single(batch$rem_sub, "rem_sub", "efacts_remediation_substances")
  save_single(batch$rem_mil, "rem_mil", "efacts_remediation_milestones")
  save_single(batch$perm_det, "perm_det", "efacts_permits_detail")
  save_single(batch$perm_tasks, "perm_tasks", "efacts_permits_tasks")
  save_single(batch$cov, "coverage", "efacts_facility_coverage")
  save_single(batch$err, "errors", "efacts_scrape_errors")
}


# ============================================================================
# 12. MAIN SCRAPER LOOP
# ============================================================================

run_scraper <- function(facility_ids) {
  
  # Load or initialize checkpoint
  if (file.exists(CONFIG$checkpoint_file)) {
    cp <- readRDS(CONFIG$checkpoint_file)
    log_msg(sprintf("RESUMING: Skipping %d previously completed facilities", length(cp$completed_ids)))
  } else {
    cp <- list(completed_ids = character(0), started_at = Sys.time())
    log_msg("STARTING FRESH SCRAPE")
  }
  
  # Determine remaining work
  all_ids <- as.character(facility_ids)
  remaining <- setdiff(all_ids, cp$completed_ids)
  
  if (length(remaining) == 0) {
    log_msg("ALL FACILITIES ALREADY SCRAPED")
    return(invisible(NULL))
  }
  
  log_msg(sprintf("Scraping %d facilities (%.1f%% remaining)", 
                  length(remaining), 100 * length(remaining) / length(all_ids)))
  
  # Initialize batch containers
  batch <- list(
    meta = list(), tanks = list(), insp = list(), viol = list(),
    rem = list(), rem_sub = list(), rem_mil = list(),
    perm_det = list(), perm_tasks = list(), cov = list(), err = list()
  )
  
  batch_count <- 0
  
  # Emergency save on interrupt
  on.exit({
    if (batch_count > 0) {
      log_msg("INTERRUPT DETECTED: Emergency save...", "WARN")
      save_batch(batch)
      saveRDS(cp, CONFIG$checkpoint_file)
    }
  })
  
  # Main loop
  for (i in seq_along(remaining)) {
    fid <- remaining[i]
    batch_count <- batch_count + 1
    
    # ---- SCRAPE FACILITY PAGE ----
    fac_res <- scrape_facility_page(fid)
    
    # Initialize coverage record - build directly instead of template modification
    cov_rec <- data.table(
      efacts_facility_id = as.character(fid),
      found_meta = !is.null(fac_res$meta_dt),
      n_tanks = if (!is.null(fac_res$tanks_dt)) as.integer(nrow(fac_res$tanks_dt)) else 0L,
      n_inspections = if (!is.null(fac_res$insp_dt)) as.integer(nrow(fac_res$insp_dt)) else 0L,
      n_violations = 0L,
      n_permits = 0L,
      n_remediation = if (!is.null(fac_res$rem_dt)) as.integer(nrow(fac_res$rem_dt)) else 0L,
      status = as.character(if (is.null(fac_res$error)) "success" else "error"),
      scraped_at = as.character(Sys.time())
    )

    if (is.null(fac_res$error)) {
      # Store facility-level data
      if (!is.null(fac_res$meta_dt)) batch$meta[[length(batch$meta) + 1]] <- fac_res$meta_dt
      if (!is.null(fac_res$tanks_dt)) batch$tanks[[length(batch$tanks) + 1]] <- fac_res$tanks_dt
      if (!is.null(fac_res$insp_dt)) batch$insp[[length(batch$insp) + 1]] <- fac_res$insp_dt
      if (!is.null(fac_res$rem_dt)) batch$rem[[length(batch$rem) + 1]] <- fac_res$rem_dt
      
      # ---- DEEP SCRAPE: VIOLATIONS ----
      if (CONFIG$scrape_violations && length(fac_res$viol_links) > 0) {
        for (vid in fac_res$viol_links) {
          v_dt <- scrape_violation_detail(vid, fid)
          if (nrow(v_dt) > 0) {
            batch$viol[[length(batch$viol) + 1]] <- v_dt
            cov_rec[, n_violations := n_violations + nrow(v_dt)]
          }
          Sys.sleep(CONFIG$delay_detail)
        }
      }
      
      # ---- DEEP SCRAPE: PERMITS ----
      if (CONFIG$scrape_permit_details && length(fac_res$auth_links) > 0) {
        for (aid in fac_res$auth_links) {
          p_res <- scrape_permit_detail(aid, fid)
          if (nrow(p_res$details) > 0) {
            batch$perm_det[[length(batch$perm_det) + 1]] <- p_res$details
          }
          if (nrow(p_res$tasks) > 0) {
            batch$perm_tasks[[length(batch$perm_tasks) + 1]] <- p_res$tasks
            cov_rec[, n_permits := n_permits + nrow(p_res$tasks)]
          }
          Sys.sleep(CONFIG$delay_detail)
        }
      }
      
      # ---- DEEP SCRAPE: REMEDIATION ----
      if (CONFIG$scrape_remediation_details && length(fac_res$remed_links) > 0) {
        for (rid in fac_res$remed_links) {
          r_res <- scrape_remediation_detail(rid, fid)
          if (nrow(r_res$substances) > 0) {
            batch$rem_sub[[length(batch$rem_sub) + 1]] <- r_res$substances
          }
          if (nrow(r_res$milestones) > 0) {
            batch$rem_mil[[length(batch$rem_mil) + 1]] <- r_res$milestones
          }
          Sys.sleep(CONFIG$delay_detail)
        }
      }
      
    } else {
      # ============================================================
      # FIX v23: Direct data.table construction for error logging
      # Previously used get_template() + [1, :=] which fails on 0-row dt
      # ============================================================
      err_rec <- data.table(
        efacts_facility_id = as.character(fid),
        error_msg = as.character(fac_res$error),
        error_time = as.character(Sys.time())
      )
      batch$err[[length(batch$err) + 1]] <- err_rec
    }
    
    batch$cov[[length(batch$cov) + 1]] <- cov_rec
    
    # Progress output
    pct <- round(100 * i / length(remaining), 1)
    cat(sprintf("\r[%s] %d/%d (%.1f%%) | ID: %s | T:%d I:%d V:%d P:%d",
                format(Sys.time(), "%H:%M:%S"),
                i, length(remaining), pct, fid,
                cov_rec$n_tanks, cov_rec$n_inspections,
                cov_rec$n_violations, cov_rec$n_permits))
    
    # Mark complete
    cp$completed_ids <- c(cp$completed_ids, fid)
    
    # ---- CHECKPOINT SAVE ----
    if (batch_count >= CONFIG$batch_size) {
      cat("\n")
      log_msg(sprintf("Checkpoint: Saving batch of %d facilities", batch_count))
      save_batch(batch)
      saveRDS(cp, CONFIG$checkpoint_file)
      
      # Reset batch
      batch <- list(
        meta = list(), tanks = list(), insp = list(), viol = list(),
        rem = list(), rem_sub = list(), rem_mil = list(),
        perm_det = list(), perm_tasks = list(), cov = list(), err = list()
      )
      batch_count <- 0
    }
    
    Sys.sleep(CONFIG$delay_facility)
  }
  
  # Final save
  if (batch_count > 0) {
    cat("\n")
    log_msg(sprintf("Final save: %d facilities", batch_count))
    save_batch(batch)
    saveRDS(cp, CONFIG$checkpoint_file)
  }
  
  cat("\n")
  log_msg("SCRAPE COMPLETE")
  
  # Summary
  log_msg(sprintf("Total scraped: %d facilities", length(cp$completed_ids)))
  
  invisible(NULL)
}

# ============================================================================
# 13. UTILITY: RESET CHECKPOINT (for re-scrape)
# ============================================================================

reset_checkpoint <- function() {
  if (file.exists(CONFIG$checkpoint_file)) {
    file.remove(CONFIG$checkpoint_file)
    log_msg("Checkpoint reset. Next run will start fresh.")
  } else {
    log_msg("No checkpoint file to reset.")
  }
}

# ============================================================================
# 14. UTILITY: GET SCRAPE STATUS
# ============================================================================

get_scrape_status <- function() {
  if (!file.exists(CONFIG$checkpoint_file)) {
    cat("No scrape in progress.\n")
    return(invisible(NULL))
  }
  
  cp <- readRDS(CONFIG$checkpoint_file)
  
  # Try to get total from linkage table
  linkage_path <- "data/external/padep/facility_linkage_table.csv"
  if (file.exists(linkage_path)) {
    linkage <- fread(linkage_path, select = "efacts_facility_id")
    total <- linkage[!is.na(efacts_facility_id), .N]
  } else {
    total <- NA
  }
  
  completed <- length(cp$completed_ids)
  
  cat("\n=== SCRAPE STATUS ===\n")
  cat(sprintf("Completed:  %d facilities\n", completed))
  if (!is.na(total)) {
    remaining <- total - completed
    pct <- round(100 * completed / total, 1)
    cat(sprintf("Total:      %d facilities\n", total))
    cat(sprintf("Remaining:  %d facilities\n", remaining))
    cat(sprintf("Progress:   %.1f%%\n", pct))
    
    # ETA calculation
    if (!is.null(cp$started_at) && completed > 0) {
      elapsed_hours <- as.numeric(difftime(Sys.time(), cp$started_at, units = "hours"))
      rate <- completed / elapsed_hours
      eta_hours <- remaining / rate
      cat(sprintf("Rate:       %.0f facilities/hour\n", rate))
      cat(sprintf("ETA:        %.1f hours\n", eta_hours))
    }
  }
  cat("=====================\n\n")
  
  invisible(list(completed = completed, total = total))
}

# ============================================================================
# 15. UTILITY: MIGRATE FROM V22 CHECKPOINT (if needed)
# ============================================================================

migrate_checkpoint_v22_to_v23 <- function() {
  old_cp_file <- "data/external/efacts/scrape_checkpoint_v22.rds"
  new_cp_file <- CONFIG$checkpoint_file
  
  if (file.exists(old_cp_file) && !file.exists(new_cp_file)) {
    old_cp <- readRDS(old_cp_file)
    saveRDS(old_cp, new_cp_file)
    log_msg(sprintf("Migrated checkpoint: %d completed IDs from v22 to v23", 
                    length(old_cp$completed_ids)))
    return(TRUE)
  } else if (file.exists(old_cp_file) && file.exists(new_cp_file)) {
    log_msg("Both v22 and v23 checkpoints exist. Using v23.", "WARN")
    return(FALSE)
  } else {
    log_msg("No v22 checkpoint to migrate.")
    return(FALSE)
  }
}

cat("\n✓ eFACTS Scraper v23 loaded successfully.\n")
cat("\nUsage:\n")
cat("  linkage <- read.csv('data/external/padep/facility_linkage_table.csv')\n")
cat("  ids <- unique(linkage$efacts_facility_id[!is.na(linkage$efacts_facility_id)])\n")
cat("  run_scraper(ids)\n")
cat("\nUtilities:\n")
cat("  get_scrape_status()           - Check progress\n")
cat("  reset_checkpoint()            - Start over\n")
cat("  migrate_checkpoint_v22_to_v23() - Migrate from v22 checkpoint\n\n")