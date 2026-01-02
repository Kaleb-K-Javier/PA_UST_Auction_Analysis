# R/etl/02b_efacts_scrape_v21_cleaning_fix.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 2b: eFACTS Scraper v21 (Clean & Split)
# ============================================================================
# Purpose: Scrape Everything + FIX ID SPLITTING + REMOVE GHOST ROWS
# Fixes:
#   1. Inspections: Explicitly splits "Type (ID)" into two separate columns.
#   2. Permit Tasks: Filters out nested-table artifacts (ghost rows).
#   3. Schema: Updated to include _clean and _id columns for all tables.
# ============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════════╗\n")
cat("║  ETL Step 2b: eFACTS Scraper v21 (Cleaning & De-Duplication)        ║\n")
cat("║  Status: Fixing Inspection IDs and Permit Task Ghost Rows           ║\n")
cat("╚══════════════════════════════════════════════════════════════════════╝\n\n")

suppressPackageStartupMessages({
  library(data.table)
  library(httr)
  library(rvest)
  library(janitor)
  library(stringr)
})

# ============================================================================
# 1. CONFIGURATION & SCHEMA DEFINITIONS
# ============================================================================

CONFIG <- list(
  delay_facility = 1.0,
  delay_detail = 0.5,
  batch_size = 50,
  timeout = 30,
  scrape_violations = TRUE,
  scrape_permit_details = TRUE,
  scrape_remediation_details = TRUE,
  efacts_dir = "data/external/efacts",
  checkpoint_file = "data/external/efacts/scrape_checkpoint_v21.rds"
)

if (!dir.exists(CONFIG$efacts_dir)) dir.create(CONFIG$efacts_dir, recursive = TRUE)

# --- STRICT SCHEMA DEFINITIONS (UPDATED) ---
SCHEMAS <- list(
  # 1. Facility Metadata
  meta = data.table(
    efacts_facility_id = character(), facility_name = character(), 
    address = character(), status = character(), program = character(), 
    facility_id = character()
  ),
  
  # 2. Tanks
  tanks = data.table(
    efacts_facility_id = character(), tank_internal_id = character(),
    sub_facility_name = character(), type = character(), 
    other_id = character(), status = character(), e_map_pa_location = character()
  ),
  
  # 3. Inspections (UPDATED SCHEMA)
  inspections = data.table(
    efacts_facility_id = character(), 
    inspection_id = character(),          # Extracted ID (PK)
    inspection_type_clean = character(),  # Clean Name
    inspection_type_raw = character(),    # Original "Name (ID)"
    inspection_date = character(), result = character()
  ),
  
  # 4. Violations
  violations = data.table(
    efacts_facility_id = character(), inspection_id = character(),
    violation_id = character(), violation_date = character(),
    description = character(), resolution = character(),
    citation = character(), violation_type = character(),
    enforcement_id = character(), enf_type = character(),
    penalty_assessed = character(), penalty_collected = character(),
    date_executed = character(), penalty_final_date = character(),
    total_amount_due = character(), taken_against = character(),
    on_appeal = character(), penalty_status = character(),
    enforcement_status = character(), num_violations_addressed = character()
  ),
  
  # 5. Remediation (Summary)
  rem_summary = data.table(
    efacts_facility_id = character(), lrpact_id = character(),
    incident_name = character(), confirmed_release_date = character(),
    type = character(), cleanup_status = character(), cleanup_status_date = character()
  ),
  
  # 6. Remediation Substances
  rem_sub = data.table(
    efacts_facility_id = character(), lrpact_id = character(), substance_internal_id = character(),
    incident_name = character(), confirmed_release_date = character(),
    incident_id = character(), incident_type = character(),
    cleanup_status = character(), cleanup_status_date = character(),
    substance_released = character(), environmental_impact = character()
  ),
  
  # 7. Remediation Milestones
  rem_mil = data.table(
    efacts_facility_id = character(), lrpact_id = character(), milestone_internal_id = character(),
    milestone_name = character(), milestone_event_date = character(),
    milestone_due_date = character(), i_milestone_status = character(),
    milestone_response_date = character()
  ),
  
  # 8. Permits Detail
  perm_det = data.table(
    efacts_facility_id = character(), auth_id = character(),
    permit_number = character(), site = character(), client = character(),
    authorization_type = character(), application_type = character(),
    authorization_is_for = character(), date_received = character(), status = character()
  ),
  
  # 9. Permit Tasks (UPDATED SCHEMA - Removed junk columns)
  perm_tasks = data.table(
    efacts_facility_id = character(), auth_id = character(), task_internal_id = character(),
    task = character(), start_date = character(), target_date = character(), completion_date = character()
  ),
  
  # 10. Coverage & Errors
  cov = data.table(
    efacts_facility_id = character(), found_meta = logical(),
    n_tanks = integer(), n_inspections = integer(), 
    n_violations = integer(), n_permits = integer(),
    status = character(), scraped_at = character()
  ),
  
  err = data.table(
    efacts_facility_id = character(), error_msg = character(), time = character()
  )
)

# Helper: Create empty row
get_template <- function(schema_name) {
  copy(SCHEMAS[[schema_name]])
}

# Helper: Coerce scraped DT to match Schema
enforce_schema <- function(dt, schema_name) {
  if (is.null(dt) || nrow(dt) == 0) return(get_template(schema_name))
  
  target <- SCHEMAS[[schema_name]]
  target_cols <- names(target)
  
  # Add missing cols as NA
  missing_cols <- setdiff(target_cols, names(dt))
  if (length(missing_cols) > 0) dt[, (missing_cols) := NA_character_]
  
  # Select and order
  dt <- dt[, ..target_cols]
  
  # Force char types (except coverage ints)
  if (schema_name != "cov") dt[, (target_cols) := lapply(.SD, as.character)]
  
  return(dt)
}

# ============================================================================
# 2. LOGGING
# ============================================================================

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(sprintf("[%s] %s: %s\n", format(Sys.time(), "%H:%M:%S"), level, msg))
  write(sprintf("[%s] %s: %s", timestamp, level, msg),
        file = file.path(CONFIG$efacts_dir, "scrape_log.txt"), append = TRUE)
}

# ============================================================================
# 3. PARSING HELPERS (Raw)
# ============================================================================

parse_table_raw <- function(node) {
  if (is.na(node) || is.null(node)) return(NULL)
  tryCatch({
    tbl <- html_table(node, fill = TRUE)
    if (nrow(tbl) == 0) return(NULL)
    if (nrow(tbl) == 1 && grepl("No records matched", as.character(tbl[1,1]), ignore.case=TRUE)) return(NULL)
    if (all(grepl("^(X|Var)\\d+$", names(tbl), ignore.case=TRUE)) && nrow(tbl) > 1) {
      names(tbl) <- as.character(tbl[1,])
      tbl <- tbl[-1,]
    }
    dt <- as.data.table(tbl)
    setnames(dt, janitor::make_clean_names(names(dt)))
    dt <- dt[rowSums(is.na(dt) | dt == "") < ncol(dt)]
    return(dt)
  }, error = function(e) NULL)
}

# ============================================================================
# 4. SCRAPING FUNCTIONS
# ============================================================================

scrape_facility_page <- function(facility_id) {
  url <- sprintf("https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleFacility.aspx?FacilityID=%s", facility_id)
  
  raw_res <- list(id = facility_id, url = url, error = NULL,
                  meta_dt = NULL, tanks_dt = NULL, insp_dt = NULL, 
                  perm_dt = NULL, rem_dt = NULL,
                  viol_links = character(), auth_links = character(), remed_links = character())
  
  page <- tryCatch({
    res <- GET(url, timeout(CONFIG$timeout), add_headers("User-Agent" = "R-USTIF-Research/2.0"))
    if (status_code(res) != 200) stop(paste("HTTP", status_code(res)))
    read_html(res)
  }, error = function(e) { raw_res$error <<- e$message; return(NULL) })
  
  if (is.null(page)) return(raw_res)
  if (grepl("Invalid Facility|No records found", html_text(page))) { raw_res$error <- "Invalid/Empty"; return(raw_res) }

  # 1. Meta
  meta_node <- page %>% html_element("#ContentPlaceHolder2_DetailsView1")
  if (!is.na(meta_node)) {
    rows <- meta_node %>% html_elements("tr")
    if (length(rows) > 0) {
      keys <- rows %>% html_element("td:nth-child(1)") %>% html_text(trim=TRUE) %>% str_remove(":$") %>% make_clean_names()
      vals <- rows %>% html_element("td:nth-child(2)") %>% html_text(trim=TRUE)
      valid_idx <- !is.na(keys) & !is.na(vals)
      if(any(valid_idx)) {
        dt <- as.data.table(t(vals[valid_idx]))
        setnames(dt, keys[valid_idx])
        dt[, efacts_facility_id := as.character(facility_id)]
        raw_res$meta_dt <- enforce_schema(dt, "meta")
      }
    }
  }

  # 2. Tanks
  t_dt <- parse_table_raw(page %>% html_element("#ContentPlaceHolder2_GridView1"))
  if (!is.null(t_dt)) {
    t_dt[, efacts_facility_id := as.character(facility_id)]
    if("sub_facility_name" %in% names(t_dt)) {
      t_dt[, tank_internal_id := paste0(efacts_facility_id, "_", make_clean_names(sub_facility_name))]
    }
    raw_res$tanks_dt <- enforce_schema(t_dt, "tanks")
  }

  # 3. Permits (Summary)
  # Just used for links
  p_dt <- parse_table_raw(page %>% html_element("#ContentPlaceHolder2_GridView2"))
  if (!is.null(p_dt)) {
    raw_res$auth_links <- page %>% html_element("#ContentPlaceHolder2_GridView2") %>% 
      html_elements("a[href*='AuthID']") %>% html_attr("href") %>% 
      str_extract("AuthID=\\d+") %>% str_remove("AuthID=") %>% unique()
  }

  # 4. Inspections (CLEANING FIX)
  i_dt <- parse_table_raw(page %>% html_element("#ContentPlaceHolder2_GridView3"))
  if (!is.null(i_dt)) {
    i_dt[, efacts_facility_id := as.character(facility_id)]
    
    # RENAME to inspection_type_raw if valid
    if("inspection_type" %in% names(i_dt)) {
      setnames(i_dt, "inspection_type", "inspection_type_raw")
      
      # Extract ID and Clean Name
      i_dt[, `:=`(
        inspection_id = str_extract(inspection_type_raw, "(?<=\\()\\d+(?=\\))"),
        inspection_type_clean = str_trim(str_remove(inspection_type_raw, "\\s*\\(\\d+\\)"))
      )]
    }
    
    raw_res$insp_dt <- enforce_schema(i_dt, "inspections")
    
    raw_res$viol_links <- page %>% html_element("#ContentPlaceHolder2_GridView3") %>% 
      html_elements("a[href*='singleViol']") %>% html_attr("href") %>% 
      str_extract("InspectionID=\\d+") %>% str_remove("InspectionID=") %>% unique()
  }

  # 5. Remediation (Summary)
  r_dt <- parse_table_raw(page %>% html_element("#ContentPlaceHolder2_GridView4"))
  if (!is.null(r_dt)) {
    r_dt[, efacts_facility_id := as.character(facility_id)]
    if("incident_name" %in% names(r_dt)) {
      raw_res$remed_links <- page %>% html_element("#ContentPlaceHolder2_GridView4") %>% 
        html_elements("a[href*='LRPACT_ID']") %>% html_attr("href") %>% 
        str_extract("LRPACT_ID=\\d+") %>% str_remove("LRPACT_ID=") %>% unique()
      
      # Try to extract ID from name: "INCIDENT NAME (1234)"
      r_dt[, lrpact_id := str_extract(incident_name, "(?<=\\()\\d+(?=\\))")]
    }
    raw_res$rem_dt <- enforce_schema(r_dt, "rem_summary")
  }
  
  return(raw_res)
}

# --- VIOLATION DETAIL (State Machine) ---
scrape_violation_detail <- function(inspection_id, facility_id) {
  url <- sprintf("https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleViol.aspx?InspectionID=%s", inspection_id)
  final_dt <- get_template("violations")
  
  tryCatch({
    res <- GET(url, timeout(CONFIG$timeout))
    if (status_code(res) != 200) return(final_dt)
    page <- read_html(res)
    
    viol_tables <- page %>% html_elements("table.GridViewTable[border='2']")
    if (length(viol_tables) == 0) return(final_dt)
    
    row_list <- list()
    
    for (tbl in viol_tables) {
      rows <- tbl %>% html_elements("tr")
      
      curr_v <- list(id=NA, date=NA, desc=NA, res=NA, cit=NA, type=NA)
      enforcements <- list()
      
      if (length(rows) > 1) {
        for (i in 2:length(rows)) {
          tr <- rows[[i]]
          tds <- tr %>% html_elements("td")
          tr_text <- html_text(tr, trim = TRUE)
          
          # NEW VIOLATION
          if (length(html_attr(tds, "rowspan")) > 0 && !is.na(html_attr(tds, "rowspan")[1])) {
            if (!is.na(curr_v$id)) {
              if (length(enforcements) == 0) {
                row_list[[length(row_list)+1]] <- list(
                  efacts_facility_id=facility_id, inspection_id=inspection_id,
                  violation_id=curr_v$id, violation_date=curr_v$date, description=curr_v$desc,
                  resolution=curr_v$res, citation=curr_v$cit, violation_type=curr_v$type
                )
              } else {
                for (e in enforcements) {
                  row_list[[length(row_list)+1]] <- c(
                    list(efacts_facility_id=facility_id, inspection_id=inspection_id,
                         violation_id=curr_v$id, violation_date=curr_v$date, description=curr_v$desc,
                         resolution=curr_v$res, citation=curr_v$cit, violation_type=curr_v$type),
                    e
                  )
                }
              }
            }
            curr_v <- list(
              id = html_text(tds[[1]], trim=TRUE),
              date = html_text(tds[[2]], trim=TRUE),
              desc = html_text(tds[[3]], trim=TRUE),
              res=NA, cit=NA, type=NA
            )
            enforcements <- list()
          } else {
            # DETAILS
            if (str_starts(tr_text, "Resolution:")) curr_v$res <- str_remove(tr_text, "^Resolution:\\s*")
            if (str_starts(tr_text, "PA Code Legal Citation:")) curr_v$cit <- str_remove(tr_text, "^PA Code Legal Citation:\\s*") %>% str_remove(" : PA Code Website.*")
            if (str_starts(tr_text, "Violation Type:")) curr_v$type <- str_remove(tr_text, "^Violation Type:\\s*")
            
            # ENFORCEMENT TABLE (Cell Census)
            nested <- tr %>% html_elements("table")
            if (length(nested) > 0) {
              for (nt in nested) {
                if (grepl("Enforcement ID:", html_text(nt))) {
                  cells <- nt %>% html_elements("td") %>% html_text(trim = TRUE)
                  e_rec <- list(enforcement_id=NA, enf_type=NA, penalty_assessed=NA, penalty_collected=NA,
                                date_executed=NA, penalty_final_date=NA, total_amount_due=NA, taken_against=NA,
                                on_appeal=NA, penalty_status=NA, enforcement_status=NA, num_violations_addressed=NA)
                  for (c in cells) {
                    if (str_detect(c, "^Enforcement ID:")) e_rec$enforcement_id <- str_remove(c, "Enforcement ID:\\s*")
                    if (str_detect(c, "^Enforcement Type:")) e_rec$enf_type <- str_remove(c, "Enforcement Type:\\s*")
                    if (str_detect(c, "^Penalty Amount Assessed:")) e_rec$penalty_assessed <- str_remove(c, "Penalty Amount Assessed:\\s*")
                    if (str_detect(c, "^Total Amount Collected:")) e_rec$penalty_collected <- str_remove(c, "Total Amount Collected:\\s*")
                    if (str_detect(c, "^Date Executed:")) e_rec$date_executed <- str_remove(c, "Date Executed:\\s*")
                    if (str_detect(c, "^Penalty Final Date:")) e_rec$penalty_final_date <- str_remove(c, "Penalty Final Date:\\s*")
                    if (str_detect(c, "^Total Amount Due:")) e_rec$total_amount_due <- str_remove(c, "Total Amount Due:\\s*")
                    if (str_detect(c, "^Taken Against:")) e_rec$taken_against <- str_remove(c, "Taken Against:\\s*")
                    if (str_detect(c, "^On Appeal\\?")) e_rec$on_appeal <- str_remove(c, "On Appeal\\?\\s*")
                    if (str_detect(c, "^Penalty Status:")) e_rec$penalty_status <- str_remove(c, "Penalty Status:\\s*")
                    if (str_detect(c, "^Enforcement Status:")) e_rec$enforcement_status <- str_remove(c, "Enforcement Status:\\s*")
                    if (str_detect(c, "^# of Violations Addressed")) e_rec$num_violations_addressed <- str_extract(c, "\\d+$")
                  }
                  enforcements[[length(enforcements)+1]] <- e_rec
                }
              }
            }
          }
        }
      }
      
      # Flush Final
      if (!is.na(curr_v$id)) {
        if (length(enforcements) == 0) {
          row_list[[length(row_list)+1]] <- list(
            efacts_facility_id=facility_id, inspection_id=inspection_id,
            violation_id=curr_v$id, violation_date=curr_v$date, description=curr_v$desc,
            resolution=curr_v$res, citation=curr_v$cit, violation_type=curr_v$type
          )
        } else {
          for (e in enforcements) {
            row_list[[length(row_list)+1]] <- c(
              list(efacts_facility_id=facility_id, inspection_id=inspection_id,
                   violation_id=curr_v$id, violation_date=curr_v$date, description=curr_v$desc,
                   resolution=curr_v$res, citation=curr_v$cit, violation_type=curr_v$type),
              e
            )
          }
        }
      }
    }
    
    if (length(row_list) > 0) {
      dt_raw <- rbindlist(row_list, fill = TRUE)
      return(enforce_schema(dt_raw, "violations"))
    } else {
      return(final_dt)
    }
    
  }, error = function(e) final_dt)
}

# --- PERMIT DETAIL (CLEANING FIX) ---
scrape_permit_detail <- function(auth_id, facility_id) {
  url <- sprintf("https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleAuth.aspx?AuthID=%s", auth_id)
  
  out <- list(details = get_template("perm_det"), tasks = get_template("perm_tasks"))
  
  tryCatch({
    res <- GET(url, timeout(CONFIG$timeout))
    if (status_code(res) != 200) return(out)
    page <- read_html(res)
    
    # 1. Main
    rows <- page %>% html_elements("#ContentPlaceHolder2_DetailsView1 tr")
    keys <- rows %>% html_element("td:nth-child(1)") %>% html_text(trim=TRUE) %>% str_remove(":$") %>% make_clean_names()
    vals <- rows %>% html_element("td:nth-child(2)") %>% html_text(trim=TRUE)
    valid_idx <- !is.na(keys) & !is.na(vals)
    
    if(any(valid_idx)) {
      dt <- as.data.table(t(vals[valid_idx]))
      setnames(dt, keys[valid_idx])
      dt[, `:=`(auth_id = as.character(auth_id), efacts_facility_id = as.character(facility_id))]
      out$details <- enforce_schema(dt, "perm_det")
    }
    
    # 2. Tasks (GHOST ROW FIX)
    static_tables <- page %>% html_elements("table.StaticTable")
    for (tbl in static_tables) {
      t <- html_table(tbl, fill=TRUE)
      if ("Task" %in% names(t)) {
        t_dt <- as.data.table(t)
        setnames(t_dt, janitor::make_clean_names(names(t_dt)))
        
        # FILTER: Remove ghost rows (where Task is empty or just whitespace)
        t_dt <- t_dt[task != "" & !is.na(task) & str_length(task) > 1]
        
        if (nrow(t_dt) > 0) {
          t_dt[, `:=`(
            auth_id = as.character(auth_id), efacts_facility_id = as.character(facility_id), 
            task_internal_id = paste0(auth_id, "_", make_clean_names(task), "_", .I)
          )]
          out$tasks <- enforce_schema(t_dt, "perm_tasks")
        }
        break
      }
    }
    return(out)
  }, error = function(e) out)
}

scrape_remediation_detail <- function(lrpact_id, facility_id) {
  url <- sprintf("https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleTankRemediation.aspx?LRPACT_ID=%s", lrpact_id)
  out <- list(substances = get_template("rem_sub"), milestones = get_template("rem_mil"))
  
  tryCatch({
    res <- GET(url, timeout(CONFIG$timeout))
    if (status_code(res) != 200) return(out)
    page <- read_html(res)
    
    s_dt <- parse_table_raw(page %>% html_element("#ContentPlaceHolder2_GridView1"))
    if (!is.null(s_dt)) {
      s_dt[, `:=`(
        efacts_facility_id = as.character(facility_id),
        lrpact_id = as.character(lrpact_id),
        substance_internal_id = paste0(lrpact_id, "_", make_clean_names(substance_released), "_", .I)
      )]
      out$substances <- enforce_schema(s_dt, "rem_sub")
    }
    
    m_dt <- parse_table_raw(page %>% html_element("#ContentPlaceHolder2_GridView2"))
    if (!is.null(m_dt)) {
      m_dt[, `:=`(
        efacts_facility_id = as.character(facility_id),
        lrpact_id = as.character(lrpact_id),
        milestone_internal_id = paste0(lrpact_id, "_", make_clean_names(milestone_name), "_", .I)
      )]
      out$milestones <- enforce_schema(m_dt, "rem_mil")
    }
    return(out)
  }, error = function(e) out)
}

# ============================================================================
# 5. EXECUTION & SAVE
# ============================================================================

save_batch <- function(batch) {
  save_rds_csv <- function(data_list, name) {
    if (length(data_list) == 0) return()
    combined_dt <- rbindlist(data_list) # Schemas match perfectly now
    combined_dt[, batch_import_date := as.character(Sys.time())]
    
    path_rds <- file.path(CONFIG$efacts_dir, paste0(name, ".rds"))
    if (file.exists(path_rds)) {
      old_dt <- readRDS(path_rds)
      combined_dt <- rbindlist(list(old_dt, combined_dt), fill = TRUE) 
    } 
    saveRDS(combined_dt, path_rds)
    
    path_csv <- file.path(CONFIG$efacts_dir, paste0(name, ".csv"))
    fwrite(combined_dt, path_csv)
  }
  
  save_rds_csv(batch$meta, "efacts_facility_meta")
  save_rds_csv(batch$tanks, "efacts_tanks")
  save_rds_csv(batch$insp, "efacts_inspections")
  save_rds_csv(batch$viol, "efacts_violations")
  save_rds_csv(batch$rem, "efacts_remediation_summary")
  save_rds_csv(batch$rem_sub, "efacts_remediation_substances")
  save_rds_csv(batch$rem_mil, "efacts_remediation_milestones")
  save_rds_csv(batch$perm_det, "efacts_permits_detail")
  save_rds_csv(batch$perm_tasks, "efacts_permits_tasks")
  save_rds_csv(batch$cov, "efacts_facility_coverage")
  save_rds_csv(batch$err, "efacts_scrape_errors")
}

run_scraper <- function(facility_ids) {
  if (file.exists(CONFIG$checkpoint_file)) {
    cp <- readRDS(CONFIG$checkpoint_file)
    log_msg(sprintf("RESUMING: Skipping %d facilities.", length(cp$completed_ids)))
  } else {
    cp <- list(completed_ids = character(0))
    log_msg("STARTING FRESH.")
  }
  
  remaining <- setdiff(as.character(facility_ids), cp$completed_ids)
  if (length(remaining) == 0) return(invisible(NULL))
  
  batch <- list(meta=list(), tanks=list(), insp=list(), viol=list(), 
                rem=list(), rem_sub=list(), rem_mil=list(),
                perm_det=list(), perm_tasks=list(), cov=list(), err=list())
  count <- 0
  
  on.exit({
    if (count > 0) {
      log_msg("⚠️  INTERRUPT: Emergency Save...")
      save_batch(batch)
      cp$completed_ids <- c(cp$completed_ids, remaining[1:count])
      saveRDS(cp, CONFIG$checkpoint_file)
    }
  })
  
  for (fid in remaining) {
    count <- count + 1
    fac_res <- scrape_facility_page(fid)
    
    cov_rec <- get_template("cov")
    cov_rec[, `:=`(
      efacts_facility_id = fid,
      found_meta = !is.null(fac_res$meta_dt),
      n_tanks = if(!is.null(fac_res$tanks_dt)) nrow(fac_res$tanks_dt) else 0,
      n_inspections = if(!is.null(fac_res$insp_dt)) nrow(fac_res$insp_dt) else 0,
      n_violations = 0, n_permits = 0,
      status = if(is.null(fac_res$error)) "success" else "error",
      scraped_at = as.character(Sys.time())
    )]
    
    if (is.null(fac_res$error)) {
      if(!is.null(fac_res$meta_dt)) batch$meta[[length(batch$meta)+1]] <- fac_res$meta_dt
      if(!is.null(fac_res$tanks_dt)) batch$tanks[[length(batch$tanks)+1]] <- fac_res$tanks_dt
      if(!is.null(fac_res$insp_dt)) batch$insp[[length(batch$insp)+1]] <- fac_res$insp_dt
      if(!is.null(fac_res$rem_dt)) batch$rem[[length(batch$rem)+1]] <- fac_res$rem_dt
      
      if (CONFIG$scrape_violations) {
        for (vid in fac_res$viol_links) {
          v_dt <- scrape_violation_detail(vid, fid)
          if (nrow(v_dt) > 0) {
            batch$viol[[length(batch$viol)+1]] <- v_dt
            cov_rec[, n_violations := n_violations + nrow(v_dt)]
          }
          Sys.sleep(CONFIG$delay_detail)
        }
      }
      
      if (CONFIG$scrape_permit_details) {
        for (aid in fac_res$auth_links) {
          p_res <- scrape_permit_detail(aid, fid)
          if (nrow(p_res$details) > 0) batch$perm_det[[length(batch$perm_det)+1]] <- p_res$details
          if (nrow(p_res$tasks) > 0) {
            batch$perm_tasks[[length(batch$perm_tasks)+1]] <- p_res$tasks
            cov_rec[, n_permits := n_permits + nrow(p_res$tasks)]
          }
          Sys.sleep(CONFIG$delay_detail)
        }
      }
      
      if (CONFIG$scrape_remediation_details) {
        for (rid in fac_res$remed_links) {
          r_res <- scrape_remediation_detail(rid, fid)
          if (nrow(r_res$substances) > 0) batch$rem_sub[[length(batch$rem_sub)+1]] <- r_res$substances
          if (nrow(r_res$milestones) > 0) batch$rem_mil[[length(batch$rem_mil)+1]] <- r_res$milestones
          Sys.sleep(CONFIG$delay_detail)
        }
      }
    } else {
      err_rec <- get_template("err")
      err_rec[, `:=`(efacts_facility_id = fid, error_msg = fac_res$error, time = as.character(Sys.time()))]
      batch$err[[length(batch$err)+1]] <- err_rec
    }
    
    batch$cov[[length(batch$cov)+1]] <- cov_rec
    
    cat(sprintf("\r[%s] %d/%d (%s) | T:%d I:%d V:%d", 
                format(Sys.time(), "%H:%M"), count, length(remaining), fid,
                cov_rec$n_tanks, cov_rec$n_inspections, cov_rec$n_violations))
    
    if (count %% CONFIG$batch_size == 0) {
      save_batch(batch)
      batch <- list(meta=list(), tanks=list(), insp=list(), viol=list(), 
                    rem=list(), rem_sub=list(), rem_mil=list(),
                    perm_det=list(), perm_tasks=list(), cov=list(), err=list())
      cp$completed_ids <- c(cp$completed_ids, remaining[(count - CONFIG$batch_size + 1):count])
      saveRDS(cp, CONFIG$checkpoint_file)
    }
    Sys.sleep(CONFIG$delay_facility)
  }
  
  final_count <- count; count <- 0 
  if (final_count %% CONFIG$batch_size != 0) {
    save_batch(batch)
    cp$completed_ids <- c(cp$completed_ids, remaining[(final_count - (final_count %% CONFIG$batch_size) + 1):final_count])
    saveRDS(cp, CONFIG$checkpoint_file)
  }
  log_msg("JOB COMPLETE.")
}