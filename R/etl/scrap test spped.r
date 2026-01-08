# R/etl/02c_speed_test_short.R
# ============================================================================
# PA DEP SSRS Quick Speed Check (N=20)
# ============================================================================

suppressPackageStartupMessages({
  library(httr)
  library(cli)
  library(data.table)
  library(tictoc)
  library(fs)
})

# 1. SETUP
CONFIG <- list(
  input_rds = "data/external/padep/facility_linkage_table.rds",
  base_url  = "http://cedatareporting.pa.gov/ReportServer/Pages/ReportViewer.aspx",
  test_size = 20,       # UPDATED: Reduced to 20 for quick check
  delay     = 0.5       # Testing at 0.5s interval
)

# 2. GET TEST SUBJECTS
if (file_exists(CONFIG$input_rds)) {
  linkage <- readRDS(CONFIG$input_rds)
  all_ids <- unique(na.omit(linkage$facility_id))
  
  if(length(all_ids) < CONFIG$test_size) {
    test_ids <- sample(all_ids, CONFIG$test_size, replace = TRUE)
  } else {
    test_ids <- sample(all_ids, CONFIG$test_size)
  }
} else {
  cli_alert_warning("RDS not found. Using synthetic IDs.")
  test_ids <- rep("51-09156", CONFIG$test_size)
}

# 3. SPEED TEST LOOP
cli_h1(sprintf("Starting Speed Check (N=%d)", CONFIG$test_size))

results <- data.table(
  iter = integer(),
  id = character(),
  status = integer(),
  duration_sec = numeric()
)

# Simplified Progress Bar
pb <- cli_progress_bar(total = length(test_ids), format = "{cli::pb_bar} {cli::pb_percent}")

for (i in seq_along(test_ids)) {
  fid <- test_ids[i]
  url <- paste0(CONFIG$base_url, "?/Public/DEP/Tanks/SSRS/TANKS",
                "&rs:Command=Render&rs:Format=CSV&P_ZIP=%&P_OTHER_ID=", fid)
  
  tic()
  tryCatch({
    resp <- GET(url, timeout(10)) 
    elapsed <- toc(quiet = TRUE)
    dur <- elapsed$toc - elapsed$tic
    stat <- status_code(resp)
    
    results <- rbind(results, data.table(
      iter = i, id = fid, status = stat, duration_sec = dur
    ))
    
    cli_progress_update(id = pb, set = i)
    
    if (stat == 403 || stat == 429) {
      cli_progress_done(id = pb)
      cli_abort("CRITICAL: IP BLOCK DETECTED.")
    }
    
  }, error = function(e) {
    results <- rbind(results, data.table(iter = i, id = fid, status = 0, duration_sec = NA))
    cli_progress_update(id = pb, set = i)
  })
  
  Sys.sleep(CONFIG$delay)
}

cli_progress_done(id = pb)

# 4. REPORT
success_count <- sum(results$status == 200, na.rm = TRUE)
avg_time      <- mean(results$duration_sec[results$status == 200], na.rm = TRUE)

cli_h2("Speed Results")
cli_text("Avg Latency: {round(avg_time, 3)} sec")
cli_text("Success:     {success_count}/{CONFIG$test_size}")

if (avg_time < 0.5) {
  cli_alert_success("FAST: Server is responding in {round(avg_time, 2)}s. You can safely scrape at 0.5s delay.")
} else {
  cli_alert_warning("SLOW: Server takes {round(avg_time, 2)}s. Do not go faster than 1.0s delay.")
}