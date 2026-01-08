# R/etl/02c_phase1_redline_timed.R
# ============================================================================
# PHASE 1: REDLINE PRODUCTION (14 Workers | Timed | Auto-Resume)
# ============================================================================
# Speed: ~115 reports/min (Est. ~5 Hours)
# Safety: Auto-aborts on IP Block (403/429) to protect your network.
# Resume: Automatically skips files marked 'success=1' in the log.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(httr)
  library(stringr)
  library(cli)
  library(fs)
  library(furrr)
  library(purrr)
})

# 1. REDLINE CONFIGURATION
CONFIG <- list(
  input_rds  = "data/external/padep/pasda_facilities_inactive.rds",
  output_dir = "data/external/padep/ssrs_inactive",
  log_file   = "data/external/padep/scraping_log.csv", 
  workers    = 14,         # <--- MAX SPEED
  min_delay  = 0.0,        # <--- ZERO DELAY
  max_delay  = 0.1
)

# Safety
options(future.rng.onMisuse = "ignore")
dir_create(CONFIG$output_dir)

# 2. INITIALIZE LOG & RESUME LOGIC
if (file_exists(CONFIG$log_file)) {
  log_dt <- fread(CONFIG$log_file)
  
  # Ensure columns exist if resuming from an older log version
  if (!"duration" %in% names(log_dt)) log_dt[, duration := NA_real_]
  
  # IDENTIFY COMPLETED DOWNLOADS
  # We only skip IDs that have at least one "success == 1" entry.
  done_ids <- unique(log_dt[facility_id == 1, V1])
  # length(done_ids)
  cli_alert_info("Resume Mode: Found {length(done_ids)} completed facilities.")
} else {
  log_dt <- data.table(
    facility_id = character(), 
    success = integer(), 
    status_msg = character(),
    duration = numeric()
  )
  done_ids <- character()
  fwrite(log_dt, CONFIG$log_file)
}

# 3. LOAD & FILTER TARGETS
dt <- readRDS(CONFIG$input_rds)
setDT(dt)

# EXCLUDE DONE IDS -> Queue only the missing/failed ones
targets <- dt[!attributes_facility_i %in% done_ids, .(
  facility_id = attributes_facility_i,
  base_url    = attributes_tank_infor
)]

cli_alert_info("Queueing {nrow(targets)} files. (Workers={CONFIG$workers})")

# 4. WORKER FUNCTION
scrape_worker <- function(id, url) {
  
  start_time <- Sys.time()
  
  safe_id <- str_replace_all(id, "[^a-zA-Z0-9]", "_")
  f_path  <- path(CONFIG$output_dir, sprintf("inactive_%s.csv", safe_id))
  full_url <- paste0(url, "&rs:Command=Render&rs:Format=CSV")
  
  status_msg <- "FAILED"
  success_bin <- 0
  
  for (try in 1:3) {
    # Minimal sleep for retries only
    if (try > 1) Sys.sleep(0.5 * try)
    
    result <- tryCatch({
      resp <- GET(full_url, add_headers("User-Agent" = "Mozilla/5.0"), timeout(90))
      
      stat <- status_code(resp)
      
      if (stat == 200) {
        content <- resp$content
        # CRITICAL FIX: 100 bytes allows valid small files
        if (length(content) > 100) { 
          if (!str_detect(rawToChar(head(content, 1000)), "(?i)<!DOCTYPE html")) {
            writeBin(content, f_path)
            "SUCCESS"
          } else { "HTML_ERR" }
        } else { "EMPTY" }
      } else if (stat %in% c(403, 429)) {
        "BLOCKED"
      } else { paste0("HTTP_", stat) }
    }, error = function(e) "TIMEOUT")
    
    if (result == "SUCCESS") {
      status_msg <- "OK"
      success_bin <- 1
      break
    }
    if (result == "BLOCKED") {
      status_msg <- "BLOCKED"
      break
    }
    status_msg <- result 
  }
  
  duration <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  return(data.table(facility_id = id, success = success_bin, status_msg = status_msg, duration = round(duration, 2)))
}

# 5. EXECUTION
plan(multisession, workers = CONFIG$workers)

# Chunk size 5000 for efficiency
chunk_size <- 5000 
chunks <- split(targets, ceiling(seq_len(nrow(targets))/chunk_size))

for (i in seq_along(chunks)) {
  chunk <- chunks[[i]]
  cli_h2(sprintf("Processing Chunk %d/%d", i, length(chunks)))
  
  results_list <- future_map2(chunk$facility_id, chunk$base_url, scrape_worker, .progress = TRUE)
  chunk_dt <- rbindlist(results_list)
  
  # APPEND TO LOG (Saves progress incrementally)
  fwrite(chunk_dt, CONFIG$log_file, append = TRUE)
  
  # SAFETY VALVE
  if ("BLOCKED" %in% chunk_dt$status_msg) {
    cli_abort("CRITICAL: IP Block detected (403/429). Script stopped to protect IP.")
  }
  
  successes <- sum(chunk_dt$success)
  fails     <- sum(chunk_dt$success == 0)
  avg_time  <- mean(chunk_dt$duration, na.rm=TRUE)
  
  cli_alert_info("Stats: {successes} OK | {fails} Failed | Avg Time: {round(avg_time, 2)}s")
  
  gc()
}

cli_alert_success("Redline Scrape Complete.")