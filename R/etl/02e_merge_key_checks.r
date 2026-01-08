# R/validation/02_pre_refactor_audit.R
# ============================================================================
# Pre-Refactor Audit: Linkage, Keys & Geometry (Master Version)
# ============================================================================
# Purpose: Comprehensive check of critical data paths before Refactoring.
#          1. Schema Validation (County + Lat/Long Geometry).
#          2. Key Format Inspection (Leading Zeros, Hyphens) across all DBs.
#          3. Merge Match Rates & Spatial Data Coverage.
# Output:  logs/pre_refactor_audit_log_master.txt
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(janitor)
  library(stringr)
  library(here)
})

# 1. Setup Logging
# ----------------------------------------------------------------------------
log_file <- here("logs", "pre_refactor_audit_log_master.txt")
dir.create(dirname(log_file), showWarnings = FALSE, recursive = TRUE)
sink(log_file, split = TRUE)

cat(paste0("================================================================\n"))
cat(paste0("PRE-REFACTOR AUDIT REPORT (MASTER)\n"))
cat(paste0("Run Date: ", Sys.time(), "\n"))
cat(paste0("================================================================\n\n"))

# 2. Load Datasets
# ----------------------------------------------------------------------------
cat("--- Loading Datasets ---\n")

path_tanks   <- here("data/processed/pa_ust_master_facility_tank_database.rds")
path_claims  <- here("data/processed/claims_clean.rds")
path_linkage <- here("data/external/padep/facility_linkage_table.csv")

# Load Tanks (New Master)
if (file.exists(path_tanks)) {
  tanks <- readRDS(path_tanks)
  setDT(tanks)
  tanks <- janitor::clean_names(tanks)
  cat(sprintf("✓ Loaded Master Tank DB: %d rows\n", nrow(tanks)))
} else {
  stop("CRITICAL: New tank database not found at ", path_tanks)
}

# Load Claims
if (file.exists(path_claims)) {
  claims <- readRDS(path_claims)
  setDT(claims)
  cat(sprintf("✓ Loaded Claims: %d rows\n", nrow(claims)))
} else {
  stop("CRITICAL: Claims data not found at ", path_claims)
}

# Load Linkage
if (file.exists(path_linkage)) {
  linkage <- fread(path_linkage)
  cat(sprintf("✓ Loaded Facility Linkage: %d rows\n", nrow(linkage)))
  cat("  Raw Linkage Columns (first 5): ", paste(head(names(linkage), 5), collapse=", "), "...\n")
  
  # Clean names to standardize (e.g., attributes_latitude -> latitude)
  linkage <- janitor::clean_names(linkage)
  linkage <- unique(linkage, by = "permit_number") 
} else {
  stop("CRITICAL: Facility Linkage table not found at ", path_linkage)
}
cat("\n")

# 3. Test 1: Linkage Table Schema Checks
# ----------------------------------------------------------------------------
cat("--- Test 1: Linkage Table Schema Checks ---\n")
cat("Reference: qmd/data_dictionary.qmd (Section: facility_linkage_table)\n")

# A. Permit Number Check
if ("permit_number" %in% names(linkage)) {
  cat("PASS: 'permit_number' column found.\n")
} else {
  cat("FAIL: 'permit_number' column NOT found.\n")
}

# B. County Check
if ("county" %in% names(linkage)) {
  cat("PASS: 'county' column found.\n")
} else {
  cat("FAIL: 'county' column NOT found. Available: ", paste(head(names(linkage), 5), collapse=", "), "...\n")
  grep_county <- grep("county", names(linkage), value=TRUE)
  if (length(grep_county) > 0) cat("      Did you mean: ", paste(grep_county, collapse=", "), "?\n")
}

# C. Geometry Check (Lat/Long)
geom_cols <- c("latitude", "longitude")
missing_geom <- setdiff(geom_cols, names(linkage))

if (length(missing_geom) == 0) {
  cat("PASS: 'latitude' and 'longitude' columns found.\n")
} else {
  # Fallback check for "attributes_" prefix if clean_names didn't strip it
  alt_geom <- c("attributes_latitude", "attributes_longitude")
  if (all(alt_geom %in% names(linkage))) {
    cat("PASS: Geometry found as 'attributes_latitude' / 'attributes_longitude'.\n")
    setnames(linkage, alt_geom, geom_cols) # Standardize
  } else {
    cat("FAIL: Geometry columns missing. Expected 'latitude'/'longitude'.\n")
    cat("      Available columns matching 'lat|long':\n")
    print(grep("lat|long", names(linkage), value=TRUE, ignore.case=TRUE))
  }
}
cat("\n")

# 4. Test 2: Key Format Inspection
# ----------------------------------------------------------------------------
cat("--- Test 2: Key Format Inspection (XX-XXXXX) ---\n")

regex_pat <- "^[0-9]{2}-[0-9]{5}$"

bad_claims  <- mean(!str_detect(claims$department, regex_pat), na.rm=TRUE)
bad_linkage <- mean(!str_detect(linkage$permit_number, regex_pat), na.rm=TRUE)
bad_tanks   <- mean(!str_detect(tanks$fac_id, regex_pat), na.rm=TRUE)

cat(sprintf("%% Non-Conforming Formats (Format Mismatch Risk):\n"))
cat(sprintf("  Claims (department):    %.2f%%\n", bad_claims * 100))
cat(sprintf("  Linkage (permit_number): %.2f%%\n", bad_linkage * 100))
cat(sprintf("  Tanks (fac_id):          %.2f%%\n", bad_tanks * 100))
cat("\n")

# 5. Test 3: Merge Coverage & Spatial Data Availability
# ----------------------------------------------------------------------------
cat("--- Test 3: Merge Coverage & Spatial Data Availability ---\n")

# We only select the columns strictly defined in the data dictionary
cols_to_pull <- c("permit_number", "county", "latitude", "longitude")

# Verify columns exist before merge
cols_present <- intersect(cols_to_pull, names(linkage))
if (length(cols_present) < length(cols_to_pull)) {
  cat("WARNING: Proceeding with available columns only: ", paste(cols_present, collapse=", "), "\n")
}

# Merge Claims -> Linkage
claims_link_merge <- merge(
  claims[, .(claim_number, department)],
  linkage[, ..cols_present],
  by.x = "department",
  by.y = "permit_number",
  all.x = TRUE
)

# Calculate Match Rates
match_rate <- mean(!is.na(claims_link_merge$county))
geom_rate  <- mean(!is.na(claims_link_merge$latitude) & !is.na(claims_link_merge$longitude))

cat(sprintf("Claims with linked County:    %.2f%%\n", match_rate * 100))
cat(sprintf("Claims with linked Geometry:  %.2f%%\n", geom_rate * 100))

if (geom_rate < match_rate) {
  cat("NOTE: Some facilities matched the Linkage table but are missing Lat/Long values in the source.\n")
}

# 6. Diagnostics for Mismatches
# ----------------------------------------------------------------------------
if (match_rate < 0.9) {
  cat("\n!!! DIAGNOSTIC: Mismatch Examples !!!\n")
  cat("Claims 'department' NOT found in Linkage 'permit_number' (First 10):\n")
  missed_ids <- setdiff(claims$department, linkage$permit_number)
  print(head(missed_ids, 10))
  
  cat("\n--- Hypothesis Check: Leading Zeros ---\n")
  cat("Checking if stripping leading zeros (e.g., '02-12345' -> '2-12345') improves match...\n")
  
  linkage[, permit_stripped := str_remove(permit_number, "^0+")]
  claims[, dept_stripped := str_remove(department, "^0+")]
  
  stripped_match <- length(intersect(claims$dept_stripped, linkage$permit_stripped)) / nrow(claims)
  cat(sprintf("Hypothetical match rate ignoring leading zeros: %.2f%%\n", stripped_match * 100))
}

cat("\n================================================================\n")
cat("END OF AUDIT MASTER\n")
sink()
cat(paste0("Audit Complete. Log saved to: ", log_file, "\n"))