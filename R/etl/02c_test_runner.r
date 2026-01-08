# R/etl/02c_test_turbo_logged_timed.R
# ============================================================================
# FINAL TURBO TEST (N=40) - WITH TIMING & ERROR TRACKING
# ============================================================================
# Purpose: Stress test 14 workers and log exact duration/errors.
# Logic: Aborts immediately on IP Block (403/429).
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(httr)
  library(stringr)
  library(cli)
  library(fs)
  library(furrr)
  library(purrr)
  library(tictoc)
})

# 1. REDLINE CONFIGURATION
CONFIG <- list(
  input_rds  = "data/external/padep/pasda_facilities_inactive.rds",
  output_dir = "data/external/padep/ssrs_inactive_TEST_TURBO",
  log_file   = "data/external/padep/test_turbo_log.csv", 
  workers    = 14,        # <--- MAX SPEED (REDLINE)
  min_delay  = 0.0,       # <--- ZERO DELAY
  max_delay  = 0.1
)

# Safety
options(future.rng.onMisuse = "ignore")

# Clean Slate
if (dir_exists(CONFIG$output_dir)) dir_delete(CONFIG$output_dir)
dir_create(CONFIG$output_dir)
if (file_exists(CONFIG$log_file)) file_delete(CONFIG$log_file)

# Initialize Log (Added 'duration' column)
log_dt <- data.table(
  facility_id = character(), 
  success = integer(), 
  status_msg = character(), 
  duration = numeric()
)
fwrite(log_dt, CONFIG$log_file)

# 2. TARGET SELECTION
dt <- readRDS(CONFIG$input_rds)
setDT(dt)

problem_ids <- c("08-70036", "09-45499", "22-63640")
pool_ids <- setdiff(dt$attributes_facility_i, problem_ids)
set.seed(555) 
random_ids <- sample(pool_ids, 37)

target_ids <- c(problem_ids, random_ids)
targets <- dt[attributes_facility_i %in% target_ids, .(
  facility_id = attributes_facility_i,
  base_url    = attributes_tank_infor
)]

cli_h1(sprintf("Starting Turbo Test (N=%d | Workers=%d)", nrow(targets), CONFIG$workers))

# 3. WORKER FUNCTION (Timed)
scrape_worker <- function(id, url) {
  
  start_time <- Sys.time() # <--- Start Timer
  
  safe_id <- str_replace_all(id, "[^a-zA-Z0-9]", "_")
  f_path  <- path(CONFIG$output_dir, sprintf("inactive_%s.csv", safe_id))
  full_url <- paste0(url, "&rs:Command=Render&rs:Format=CSV")
  
  status_msg <- "FAILED"
  success_bin <- 0
  
  for (try in 1:3) {
    if (try > 1) Sys.sleep(runif(1, CONFIG$min_delay, CONFIG$max_delay) * try)
    
    result <- tryCatch({
      resp <- GET(full_url, add_headers("User-Agent" = "Mozilla/5.0"), timeout(90))
      
      stat <- status_code(resp)
      
      if (stat == 200) {
        content <- resp$content
        if (length(content) > 100) { 
          if (!str_detect(rawToChar(head(content, 1000)), "(?i)<!DOCTYPE html")) {
            writeBin(content, f_path)
            "SUCCESS"
          } else { "HTML_ERR" }
        } else { "EMPTY" }
      } else if (stat %in% c(403, 429)) {
        "BLOCKED"
      } else { 
        paste0("HTTP_", stat) # <--- Record specific error code
      }
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
  
  # Calculate Duration
  duration <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  
  return(data.table(facility_id = id, success = success_bin, status_msg = status_msg, duration = round(duration, 2)))
}

# 4. EXECUTION
plan(multisession, workers = CONFIG$workers)

chunks <- split(targets, ceiling(seq_len(nrow(targets))/20))

tic("Turbo Test")
for (i in seq_along(chunks)) {
  chunk <- chunks[[i]]
  cli_h2(sprintf("Processing Chunk %d/%d", i, length(chunks)))
  
  results_list <- future_map2(chunk$facility_id, chunk$base_url, scrape_worker, .progress = TRUE)
  chunk_dt <- rbindlist(results_list)
  
  fwrite(chunk_dt, CONFIG$log_file, append = TRUE)
  
  # SAFETY VALVE: Stop immediately if blocked
  if ("BLOCKED" %in% chunk_dt$status_msg) {
    cli_abort("CRITICAL: IP Block (403/429) detected. Script stopped to protect IP.")
  }
  
  successes <- sum(chunk_dt$success)
  fails     <- sum(chunk_dt$success == 0)
  cli_alert_info("Chunk Result: {successes} OK | {fails} Failed")
}
total_time <- toc()

# 5. VERIFICATION REPORT
cli_h2("Final Verification")

final_log <- fread(CONFIG$log_file)
throughput <- round((sum(final_log$success) / (total_time$toc - total_time$tic)) * 60, 1)

cli_text("Total Rows:    {nrow(final_log)}")
cli_text("Success Rate:  {sum(final_log$success)}/{nrow(final_log)}")
cli_text("Throughput:    ~{throughput} reports/min")
cli_text("Avg Duration:  {round(mean(final_log$duration), 2)}s")

# Error Breakdown
if (any(final_log$success == 0)) {
  cli_h3("Error Codes Encountered")
  print(final_log[success == 0, .N, by = status_msg])
}

# Check Problem IDs
cli_h3("Status of Previously Failed IDs")
print(final_log[facility_id %in% problem_ids])