# R/etl/02b_efacts_test_runner_v3.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 2b: eFACTS Test Runner (v19 Compat)
# ============================================================================
# Purpose: Validate scraper v19 (Full Enforcement Logic) on sampled IDs.
#          Saves artifacts to 'data/external/efacts_test_results'
# ============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════════╗\n")
cat("║  eFACTS Scraper Test Runner (v19 - Full Enforcement Check)          ║\n")
cat("║  Target: data/external/efacts_test_results                          ║\n")
cat("╚══════════════════════════════════════════════════════════════════════╝\n\n")

# ============================================================================
# 1. SETUP & SOURCE
# ============================================================================

suppressPackageStartupMessages({
  library(data.table) # v19 uses data.table
  library(httr)
  library(rvest)
  library(janitor)
  library(stringr)
  library(tidyverse) # For tribble/pipes in runner
})

# Source the main scraper script
scraper_path <- "R/etl/02b_efacts_scrape.R"
if (!file.exists(scraper_path)) {
  stop(paste0("CRITICAL ERROR: Main scraper file not found at: ", scraper_path, "\n",
              "Please ensure the 'v19' scraper code is saved there."))
}

cat(sprintf("Sourcing scraper logic from %s...\n", scraper_path))
source(scraper_path)

# --- OVERRIDE CONFIG FOR TEST ---
CONFIG$efacts_dir <- "data/external/efacts_test_results"
if (!dir.exists(CONFIG$efacts_dir)) dir.create(CONFIG$efacts_dir, recursive = TRUE)

cat(sprintf("Output Directory set to: %s\n\n", CONFIG$efacts_dir))

# ============================================================================
# 2. TEST MANIFEST
# ============================================================================

test_manifest <- tribble(
  ~efacts_facility_id, ~category,            ~expected_behavior,
  "579415",            "Active (Random)",    "Standard active facility data",
  "774673",            "Active (Random)",    "Institutional/non-retail active",
  "584173",            "Active (Random)",    "Standard retail active",
  "604812",            "Active (Random)",    "LLC Owner parsing",
  "821085",            "Active (Random)",    "Municipal/Government facility",
  "587953",            "Inactive (Random)",  "Standard inactive/closed",
  "576892",            "Inactive (Random)",  "Sparse data expected",
  "592069",            "Inactive (Random)",  "Automotive/Service station",
  "598483",            "Inactive (Random)",  "Legacy/Old School (potential empty)",
  "615585",            "Inactive (Random)",  "Legacy Service station",
  "604849",            "Chain (Wawa)",       "High permit volume",
  "696269",            "Chain (Wawa)",       "High inspection volume",
  "597035",            "Chain (Sheetz)",     "High volume inspections",
  "743955",            "Chain (Sheetz)",     "Complex history (Rebuild)",
  "576567",            "Chain (7-Eleven)",   "Inactive chain location",
  "575873",            "Chain (7-Eleven)",   "Active chain location",
  "575014",            "Edge Case (Min)",    "Lowest ID in dataset",
  "888548",            "Edge Case (Max)",    "Highest ID in dataset"
) %>%
  mutate(url = sprintf("https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleFacility.aspx?FacilityID=%s", efacts_facility_id))

# ============================================================================
# 3. VALIDATION LOOP (Accumulating Data)
# ============================================================================

# Initialize the batch container
batch <- list(
  meta = list(), tanks = list(), insp = list(), viol = list(), 
  rem = list(), rem_sub = list(), rem_mil = list(),
  perm_sum = list(), perm_det = list(), perm_tasks = list(), 
  cov = list(), err = list()
)

results_summary <- list()

cat(sprintf("Starting full extraction for %d facilities...\n", nrow(test_manifest)))
cat(paste(rep("=", 160), collapse = ""), "\n")
cat(sprintf("%-10s | %-18s | %-10s | %-6s | %-6s | %-6s | %-6s | %s\n", 
            "ID", "Category", "Status", "Tanks", "Viol", "P_Sum", "P_Tsk", "URL"))
cat(paste(rep("-", 160), collapse = ""), "\n")

for (i in seq_len(nrow(test_manifest))) {
  
  fid <- test_manifest$efacts_facility_id[i]
  cat_name <- test_manifest$category[i]
  page_url <- test_manifest$url[i]
  
  tryCatch({
    # --- A. Scrape Facility ---
    fac_res <- scrape_facility_page(fid)
    
    # Store Facility Level Data
    if (!is.null(fac_res$meta)) batch$meta[[length(batch$meta) + 1]] <- fac_res$meta
    if (!is.null(fac_res$tanks)) batch$tanks[[length(batch$tanks) + 1]] <- fac_res$tanks
    if (!is.null(fac_res$inspections)) batch$insp[[length(batch$insp) + 1]] <- fac_res$inspections
    if (!is.null(fac_res$permits)) batch$perm_sum[[length(batch$perm_sum) + 1]] <- fac_res$permits
    if (!is.null(fac_res$remediation)) batch$rem[[length(batch$rem) + 1]] <- fac_res$remediation
    
    # --- B. Deep Scrape Violations ---
    n_viol_rows <- 0
    if (length(fac_res$viol_links) > 0) {
      for (vid in fac_res$viol_links) {
        if (exists("scrape_violation_detail")) {
          v_data <- scrape_violation_detail(vid, fid)
          if (!is.null(v_data)) {
            batch$viol[[length(batch$viol) + 1]] <- v_data
            n_viol_rows <- n_viol_rows + nrow(v_data)
          }
        }
        Sys.sleep(0.1) 
      }
    }
    
    # --- C. Deep Scrape Permits (Tasks) ---
    n_perm_tasks <- 0
    if (length(fac_res$auth_links) > 0) {
      for (aid in fac_res$auth_links) {
        if (exists("scrape_permit_detail")) {
          p_res <- scrape_permit_detail(aid, fid)
          if (!is.null(p_res$details)) batch$perm_det[[length(batch$perm_det) + 1]] <- p_res$details
          if (!is.null(p_res$tasks)) {
            batch$perm_tasks[[length(batch$perm_tasks) + 1]] <- p_res$tasks
            n_perm_tasks <- n_perm_tasks + nrow(p_res$tasks)
          }
        }
        Sys.sleep(0.1)
      }
    }
    
    # --- D. Deep Scrape Remediation ---
    if (length(fac_res$remed_links) > 0) {
      for (rid in fac_res$remed_links) {
        if (exists("scrape_remediation_detail")) {
          r_res <- scrape_remediation_detail(rid, fid)
          if (!is.null(r_res$substances)) batch$rem_sub[[length(batch$rem_sub) + 1]] <- r_res$substances
          if (!is.null(r_res$milestones)) batch$rem_mil[[length(batch$rem_mil) + 1]] <- r_res$milestones
        }
        Sys.sleep(0.1)
      }
    }
    
    # --- Metrics & Logging ---
    n_tanks <- if (!is.null(fac_res$tanks)) nrow(fac_res$tanks) else 0
    status <- if (is.null(fac_res$error)) "SUCCESS" else "FAIL"
    
    cat(sprintf("%-10s | %-18s | %-10s | %6d | %6d | %6d | %6d | %s\n", 
                fid, str_trunc(cat_name, 18), status, n_tanks, n_viol_rows, 
                if(!is.null(fac_res$permits)) nrow(fac_res$permits) else 0,
                n_perm_tasks, page_url))
    
    results_summary[[i]] <- tibble(
      id = fid, status = status, url = page_url
    )
    
  }, error = function(e) {
    cat(sprintf("%-10s | %-18s | %-10s | %s | %s\n", fid, cat_name, "CRASH", e$message, page_url))
    batch$err[[length(batch$err) + 1]] <- data.table(id = fid, error = e$message, time = as.character(Sys.time()))
  })
  
  Sys.sleep(0.5) 
}

cat(paste(rep("=", 160), collapse = ""), "\n")

# ============================================================================
# 4. SAVE OUTPUTS (The Real Data)
# ============================================================================

cat("\nSaving scraped data to CSV/RDS...\n")

# Call the save_batch function from the sourced script
# This uses the OVERRIDDEN CONFIG$efacts_dir
save_batch(batch)

cat(sprintf("✅ Data saved to: %s\n", CONFIG$efacts_dir))
cat("Files generated:\n")
list.files(CONFIG$efacts_dir, pattern = "\\.csv$") %>% walk(~cat(sprintf("  - %s\n", .)))

cat("\nDone. You can now inspect these CSVs to verify the full Enforcement schema.\n")