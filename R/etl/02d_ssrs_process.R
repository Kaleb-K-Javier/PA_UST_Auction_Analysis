# R/etl/02d_ssrs_process.R
# ==============================================================================
# PA UST Facility-Tank Master Database Build Script
# Version: 8.0 (Strict Slug + Duplicate Diagnostics)
# ==============================================================================
# CHANGES FROM v7.1:
#   - CAPACITY tracking verified at checkpoints (Steps 1, 2, 3, 5)
#   - Duplicate diagnostic audit table before pivoting (Step 4.4)
#   - clean_label now includes (CODE) suffix for guaranteed uniqueness (Step 4.6)
# ==============================================================================

# 0. SETUP & PACKAGES
# ==============================================================================
suppressPackageStartupMessages({
  library(data.table)
  library(janitor)
  library(stringr)
  library(readxl)
  library(pbapply)
  library(future)
  library(furrr)
  library(tictoc)
  library(here)
})

# Configure data.table threads
setDTthreads(percent = 90)

# Initialize Validation Tracker
validation_log <- list()
log_check <- function(item, status) {
  validation_log[[length(validation_log) + 1]] <<- list(item = item, status = status)
  mark <- if(status) "[x]" else "[ ]"
  message(sprintf("%s %s", mark, item))
}

# Ensure Output Directory Exists
if(!dir.exists("data/processed")) dir.create("data/processed", recursive = TRUE)

# ==============================================================================
# STEP 1: PROCESS ACTIVE TANK INVENTORIES
# ==============================================================================
message("\n--- Step 1: Processing Active Tank Inventories ---")

# 1.1 Source Files
active_path_root <- "data/external/padep"
active_files <- list.files(active_path_root, pattern = "^tank_inventory_.*\\.xls$", full.names = TRUE)
log_check("All six regional active tank Excel files exist", length(active_files) >= 6)

# 1.2 Combine Regional Files
active_tanks_list <- lapply(active_files, function(file) {
  region_name <- gsub(".*tank_inventory_(.+)\\.xls$", "\\1", basename(file))
  dt <- as.data.table(read_excel(file))
  
  # Map headers to Master Standard
  selected_dt <- dt[, .(
    FAC_ID              = PF_OTHER_ID,
    TANK_ID             = SEQUENCE_NUMBER,
    TANK_TYPE           = TANK_CODE,
    DATE_INSTALLED      = DATE_INSTALLED,
    CAPACITY            = CAPACITY,
    SUBSTANCE_CODE      = SUBSTANCE_CODE,
    STATUS_CODE         = STATUS_CODE,
    Tank_Status_Meaning = STATUS,
    Tank_Status_Date    = STATUS_CODE_DATE_END,
    source_region       = region_name
  )]
  return(selected_dt)
})

active_tanks <- rbindlist(active_tanks_list, fill = TRUE)
log_check("All regional files loaded successfully", nrow(active_tanks) > 0)

# 1.3 Data Type Standardization
active_tanks[, FAC_ID := trimws(as.character(FAC_ID))]
active_tanks[, TANK_ID := trimws(as.character(TANK_ID))]
active_tanks[, TANK_TYPE := toupper(trimws(as.character(TANK_TYPE)))]
active_tanks[, SUBSTANCE_CODE := toupper(trimws(as.character(SUBSTANCE_CODE)))]
active_tanks[, STATUS_CODE := toupper(trimws(as.character(STATUS_CODE)))]
active_tanks[, Tank_Status_Meaning := trimws(as.character(Tank_Status_Meaning))]
active_tanks[, DATE_INSTALLED := as.Date(DATE_INSTALLED)]
active_tanks[, Tank_Status_Date := as.Date(Tank_Status_Date)]
active_tanks[, CAPACITY := as.numeric(CAPACITY)]

# 1.5 Tank Sequence ID Cleaning
active_tanks[, TANK_ID := gsub("A$", "", TANK_ID)]
active_tanks[, TANK_ID := as.integer(gsub("^0+", "", TANK_ID))]

# 1.6 Filter to USTs Only
n_before <- nrow(active_tanks)
active_tanks <- active_tanks[TANK_TYPE == "UST"]
message(sprintf("Filtered out %d AST records", n_before - nrow(active_tanks)))

# 1.7 Source Indicator & Checkpoint
active_tanks[, source_dataset := "active"]

# v8.0: CAPACITY checkpoint
log_check("Active tanks: CAPACITY column present", "CAPACITY" %in% names(active_tanks))

saveRDS(active_tanks, "data/processed/active_tanks_harmonized.rds")
fwrite(active_tanks, "data/processed/active_tanks_harmonized.csv", nThread = getDTthreads())

# ==============================================================================
# STEP 2: PROCESS INACTIVE TANK RECORDS (PARALLEL & MEMORY OPTIMIZED)
# ==============================================================================
message("\n--- Step 2: Processing Inactive Tank Records (Parallel) ---")

# 2.1 Source Files
inactive_path_root <- "data/external/padep/ssrs_inactive"
inactive_files <- list.files(inactive_path_root, pattern = "\\.csv$", full.names = TRUE)
log_check("Inactive tank directory contains files", length(inactive_files) > 100)

# Batching Configuration
batch_size <- 2000 
batches <- split(inactive_files, ceiling(seq_along(inactive_files) / batch_size))

# Parallel Workers
available_cores <- parallel::detectCores(logical = FALSE) - 1
plan(multisession, workers = available_cores)
message(sprintf("Parallel processing enabled on %d cores.", available_cores))

# 2.2 Worker Function
process_single_file_parallel <- function(file) {
  require(data.table)
  require(janitor)
  
  tryCatch({
    dt <- fread(file, showProgress = FALSE, na.strings = c("", "NA", "NULL"))
    setnames(dt, janitor::make_clean_names(names(dt)))
    
    target_map <- c(
      "other_id"           = "FAC_ID",
      "seq_number"         = "TANK_ID",
      "tank_code"          = "TANK_TYPE",
      "installed_date"     = "DATE_INSTALLED",
      "capacity"           = "CAPACITY",
      "substance_code"     = "SUBSTANCE_CODE",
      "status_code"        = "STATUS_CODE",
      "status_description" = "Tank_Status_Meaning",
      "status_date"        = "Tank_Status_Date"
    )
    
    found_cols <- intersect(names(dt), names(target_map))
    dt_subset <- dt[, ..found_cols]
    new_names <- target_map[match(names(dt_subset), names(target_map))]
    setnames(dt_subset, names(dt_subset), new_names)
    
    return(dt_subset)
  }, error = function(e) return(NULL))
}

# 2.3 Execute Batches
message(sprintf("Processing %d batches...", length(batches)))
tic("Batch Processing")

all_batches <- future_map(batches, function(current_batch_files) {
  batch_list <- lapply(current_batch_files, process_single_file_parallel)
  rbindlist(Filter(Negate(is.null), batch_list), fill = TRUE, use.names = TRUE)
}, .progress = TRUE)

toc()

inactive_tanks <- rbindlist(Filter(Negate(is.null), all_batches), fill = TRUE, use.names = TRUE)
log_check("Inactive parallel processing completed", nrow(inactive_tanks) > 0)

plan(sequential)
rm(all_batches); gc()

# 2.4 Final Type Standardization
inactive_tanks[, FAC_ID := trimws(as.character(FAC_ID))]
inactive_tanks[, TANK_ID := trimws(as.character(TANK_ID))]
inactive_tanks[, TANK_TYPE := toupper(trimws(as.character(TANK_TYPE)))]
inactive_tanks[, SUBSTANCE_CODE := toupper(trimws(as.character(SUBSTANCE_CODE)))]
inactive_tanks[, STATUS_CODE := toupper(trimws(as.character(STATUS_CODE)))]
inactive_tanks[, Tank_Status_Meaning := trimws(as.character(Tank_Status_Meaning))]

parse_date_flexible <- function(date_col) {
  parsed <- as.Date(date_col, format = "%Y-%m-%d")
  if (all(is.na(parsed))) parsed <- as.Date(date_col, format = "%m/%d/%Y")
  if (all(is.na(parsed))) parsed <- as.Date(date_col, format = "%d-%b-%Y")
  return(parsed)
}
inactive_tanks[, DATE_INSTALLED := parse_date_flexible(DATE_INSTALLED)]
inactive_tanks[, Tank_Status_Date := parse_date_flexible(Tank_Status_Date)]
inactive_tanks[, CAPACITY := as.numeric(CAPACITY)]

# 2.5 Tank Sequence ID Cleaning
inactive_tanks[, TANK_ID := sub("^.*-\\s*", "", TANK_ID)]
inactive_tanks[, TANK_ID := trimws(TANK_ID)]
inactive_tanks[, TANK_ID := gsub("A$", "", TANK_ID)]
inactive_tanks[, TANK_ID := as.integer(gsub("^0+", "", TANK_ID))]
inactive_tanks <- inactive_tanks[!is.na(TANK_ID) & TANK_ID > 0]

# 2.6 Filter to USTs Only & Source
inactive_tanks <- inactive_tanks[TANK_TYPE == "UST"]
inactive_tanks[, source_dataset := "inactive"]

# v8.0: CAPACITY checkpoint
log_check("Inactive tanks: CAPACITY column present", "CAPACITY" %in% names(inactive_tanks))

saveRDS(inactive_tanks, "data/processed/inactive_tanks_harmonized.rds")
fwrite(inactive_tanks, "data/processed/inactive_tanks_harmonized.csv", nThread = getDTthreads())

# ==============================================================================
# STEP 3: COMBINE ACTIVE AND INACTIVE TANKS
# ==============================================================================
message("\n--- Step 3: Combining Datasets ---")

combined_tanks <- rbindlist(list(active_tanks, inactive_tanks), use.names = TRUE, fill = TRUE)
log_check("Row count equals sum", nrow(combined_tanks) == nrow(active_tanks) + nrow(inactive_tanks))

combined_tanks[, Tank_Closed_Date := fifelse(source_dataset == "inactive", Tank_Status_Date, as.Date("9999-01-01"))]
combined_tanks[, is_closed := fifelse(source_dataset == "inactive", 1L, 0L)]

setkey(combined_tanks, FAC_ID, TANK_ID, DATE_INSTALLED)
setorder(combined_tanks, FAC_ID, TANK_ID, DATE_INSTALLED)

# v8.0: CAPACITY checkpoint before attribute merge
log_check("Combined tanks: CAPACITY column present", "CAPACITY" %in% names(combined_tanks))
log_check("Combined tanks: CAPACITY has non-NA values", sum(!is.na(combined_tanks$CAPACITY)) > 0)
message(sprintf("  CAPACITY coverage: %d of %d tanks (%.1f%%)", 
                sum(!is.na(combined_tanks$CAPACITY)), 
                nrow(combined_tanks),
                100 * sum(!is.na(combined_tanks$CAPACITY)) / nrow(combined_tanks)))

saveRDS(combined_tanks, "data/processed/combined_tanks_status.rds")
fwrite(combined_tanks, "data/processed/combined_tanks_status.csv", nThread = getDTthreads())

# ==============================================================================
# STEP 4: PROCESS TANK COMPONENT ATTRIBUTES (DUAL ENCODING + COMPLIANCE LOGIC)
# ==============================================================================
message("\n--- Step 4: Process Tank Component Attributes (Dual Encoding) ---")

comp_file <- "data/external/padep/allattributes(in).csv"
map_dir   <- "qmd/output/mappings" # Ensure this path is correct relative to project root
if(!dir.exists(map_dir)) map_dir <- "output/component_mapping/mappings"

log_check("Component attributes CSV exists", file.exists(comp_file))

# 4.1 Ingest Mappings & Identify Risk Terms (MIGRATED FROM 03)
# ------------------------------------------------------------------------------
map_files <- list.files(map_dir, pattern = "\\.csv$", full.names = TRUE)
message(sprintf("Found %d mapping files in %s", length(map_files), map_dir))

# Storage for "Bad" keywords extracted from metadata
risk_terms <- list(non_compliant = character(), legacy = character())

read_mapping <- function(fpath) {
  dt <- fread(fpath, colClasses = "character")
  setnames(dt, janitor::make_clean_names(names(dt)))
  
  # Infer component type from filename
  comp_key <- str_remove(basename(fpath), "map_") %>% str_remove(".csv")
  dt[, component_type_key := comp_key]
  
  # Handle potential header variation
  if("code" %in% names(dt)) setnames(dt, "code", "original_code")
  
  # --- COMPLIANCE LOGIC MIGRATION ---
  # Extract categories flagged as "Non-Compliant" or "Legacy" in metadata
  if("federal_status" %in% names(dt) && "coarsened_category" %in% names(dt)) {
    nc <- dt[grepl("Non-Compliant|High Risk", federal_status, ignore.case=TRUE), unique(coarsened_category)]
    leg <- dt[grepl("Legacy", federal_status, ignore.case=TRUE), unique(coarsened_category)]
    
    # Push to global list
    if(length(nc) > 0) risk_terms$non_compliant <<- c(risk_terms$non_compliant, nc)
    if(length(leg) > 0) risk_terms$legacy <<- c(risk_terms$legacy, leg)
  }
  
  if(!"coarsened_category" %in% names(dt)) return(NULL)
  
  return(dt[, .(original_code, coarsened_category, component_type_key)])
}

map_master <- rbindlist(lapply(map_files, read_mapping), fill = TRUE)

# Deduplicate Risk Terms
risk_terms$non_compliant <- unique(risk_terms$non_compliant)
risk_terms$legacy <- unique(risk_terms$legacy)
message(sprintf("Identified %d Non-Compliant and %d Legacy category terms.", 
                length(risk_terms$non_compliant), length(risk_terms$legacy)))

# 4.2 Load Raw Components & Prep Join Keys (Optimized)
# ------------------------------------------------------------------------------
components <- fread(comp_file, na.strings = c("", "NA", "NULL"))
setnames(components, janitor::make_clean_names(names(components)))

# FIX 1: Handle non-numeric Tank IDs (e.g., "001A") to prevent NA warning
components[, tank_clean := gsub("[^0-9]", "", tank_name)] # Remove non-digits

# FIX: Added COMPONENT_DETAIL (description_1) for ML labels
tanks_long <- components[, .(
  FAC_ID             = trimws(as.character(fac_id)),
  TANK_ID            = as.integer(tank_clean),
  COMPONENT_CATEGORY = description,
  COMPONENT_VALUE    = attribute,
  COMPONENT_DETAIL   = description_1
)]

# FIX 2: Performance Optimization (Clean Unique Values Only)
cat_lookup <- unique(tanks_long[, .(COMPONENT_CATEGORY)])
cat_lookup[, component_key := make_clean_names(COMPONENT_CATEGORY)]

val_lookup <- unique(tanks_long[, .(COMPONENT_VALUE)])
val_lookup[, code_clean := make_clean_names(COMPONENT_VALUE)]

tanks_long <- merge(tanks_long, cat_lookup, by = "COMPONENT_CATEGORY", all.x = TRUE)
tanks_long <- merge(tanks_long, val_lookup, by = "COMPONENT_VALUE", all.x = TRUE)

# Prepare mapping master keys
map_master[, code_clean := make_clean_names(original_code)]

message("Keys prepared via optimized lookup.")

# 4.3 Merge Mappings
# ------------------------------------------------------------------------------
tanks_mapped <- merge(
  tanks_long, 
  map_master, 
  by.x = c("component_key", "code_clean"), 
  by.y = c("component_type_key", "code_clean"), 
  all.x = TRUE
)

# --- CRITICAL FIX: Ensure Unknown/Missing are tagged "Unmapped" ---
tanks_mapped[is.na(coarsened_category) | coarsened_category == "", coarsened_category := "Unmapped"]

# 4.4 DUAL ENCODING STRATEGY
# ------------------------------------------------------------------------------

# STRATEGY A: CATEGORICAL (Factors for Tables)
message("  -> Generating Categorical Factors (cat_)...")
dt_cat <- dcast(
  tanks_mapped, 
  FAC_ID + TANK_ID ~ paste0("cat_", component_key), 
  value.var = "coarsened_category",
  fun.aggregate = function(x) paste(unique(x), collapse = " | ")
)

# STRATEGY B: INDICATORS (One-Hot for ML)
message("  -> Generating Raw Indicators (ind_)...")

# v8.0: Construct feature_name explicitly for diagnostic
tanks_mapped[, feature_name := paste0("ind_", component_key, "_", code_clean)]

# --- v8.0: DUPLICATE DIAGNOSTIC (Validate, Don't Hide) ---
message("\n  --- Duplicate Diagnostic ---")
dup_check <- tanks_mapped[, .N, by = .(FAC_ID, TANK_ID, feature_name)]
duplicate_audit <- dup_check[N > 1]

if(nrow(duplicate_audit) > 0) {
  message(sprintf("  WARNING: %d tank-feature combinations have duplicate entries", nrow(duplicate_audit)))
  message(sprintf("  Affected unique tanks: %d", uniqueN(duplicate_audit[, .(FAC_ID, TANK_ID)])))
  
  # Save diagnostic table for review
  saveRDS(duplicate_audit, "data/processed/DIAGNOSTIC_duplicate_tank_features.rds")
  
  # Sample of problematic entries for logging
  sample_dups <- head(duplicate_audit[order(-N)], 10)
  message("\n  Top 10 duplicated feature entries:")
  print(sample_dups)
  
  # Detailed audit: what raw values caused duplicates?
  dup_detail <- merge(
    duplicate_audit,
    tanks_mapped[, .(FAC_ID, TANK_ID, feature_name, COMPONENT_CATEGORY, COMPONENT_VALUE, COMPONENT_DETAIL, coarsened_category)],
    by = c("FAC_ID", "TANK_ID", "feature_name")
  )
  saveRDS(dup_detail, "data/processed/DIAGNOSTIC_duplicate_detail.rds")
  message("  Saved: DIAGNOSTIC_duplicate_tank_features.rds")
  message("  Saved: DIAGNOSTIC_duplicate_detail.rds")
} else {
  message("  OK: No duplicate tank-feature combinations detected")
}

# v8.0: dcast uses feature_name column; fun.aggregate = length handles duplicates
dt_ind <- dcast(
  tanks_mapped,
  FAC_ID + TANK_ID ~ feature_name,
  fun.aggregate = length
)

# Normalize Counts to Binary (1/0) - binarize AFTER pivot to handle duplicate counts
ind_cols <- names(dt_ind)[-c(1:2)]
for(col in ind_cols) {
  set(dt_ind, j = col, value = as.numeric(dt_ind[[col]] > 0))
}

message(sprintf("  Generated %d binary indicator features", length(ind_cols)))

# 4.5 Final Merge
# ------------------------------------------------------------------------------
components_wide <- merge(dt_cat, dt_ind, by = c("FAC_ID", "TANK_ID"), all = TRUE)
setkey(components_wide, FAC_ID, TANK_ID)

saveRDS(components_wide, "data/processed/facility_attributes.rds") 
log_check("Dual Encoded Attributes Saved", file.exists("data/processed/facility_attributes.rds"))


# ==============================================================================
# 4.6 GENERATE ML FEATURE DICTIONARY (ENHANCED - v8.0 STRICT UNIQUENESS)
# ==============================================================================
message("  -> Generating Enhanced ML Feature Dictionary for Visualization Labels...")

# 1. Extract unique mappings from the mapped long-format data
# Include COMPONENT_DETAIL (description_1) for specific labeling
feature_map <- unique(tanks_mapped[, .(
  component_key,
  code_clean,
  original_code = COMPONENT_VALUE,
  category_label = COMPONENT_CATEGORY,
  detail_label = COMPONENT_DETAIL
)])

# 2. Reconstruct the exact column names used in dcast
# Format must match: paste0("ind_", component_key, "_", code_clean)
feature_map[, feature_name := paste0("ind_", component_key, "_", code_clean)]

# 3. v8.0 FIX: Create GUARANTEED UNIQUE Audience-Friendly Label
# Format: "Detail [Category] (CODE)" - CODE suffix ensures uniqueness
# This prevents the "duplicated name" error in rpart when renaming columns
feature_map[, clean_label := ifelse(
  !is.na(detail_label) & detail_label != "",
  paste0(str_to_title(detail_label), " [", str_to_title(category_label), "] (", toupper(code_clean), ")"),
  paste0(original_code, " [", category_label, "] (", toupper(code_clean), ")")
)]

# 4. Deduplicate: Ensure 1:1 mapping for feature_name
# If multiple details exist for one code (rare), pick the longest string
feature_map <- feature_map[order(feature_name, -nchar(clean_label))]
feature_map <- unique(feature_map, by = "feature_name")

# 5. v8.0: Verify label uniqueness (belt-and-suspenders)
label_dup_check <- feature_map[, .N, by = clean_label][N > 1]
if(nrow(label_dup_check) > 0) {
  warning(sprintf("Dictionary still has %d duplicate labels - adding row index as final fallback", nrow(label_dup_check)))
  feature_map[, row_idx := .I]
  feature_map[clean_label %in% label_dup_check$clean_label, 
              clean_label := paste0(clean_label, " #", row_idx)]
  feature_map[, row_idx := NULL]
}

log_check("Dictionary: All feature_names unique", uniqueN(feature_map$feature_name) == nrow(feature_map))
log_check("Dictionary: All clean_labels unique", uniqueN(feature_map$clean_label) == nrow(feature_map))

# 6. Save to Processed Data
saveRDS(feature_map, "data/processed/ml_feature_dictionary.rds")
fwrite(feature_map, "data/processed/ml_feature_dictionary.csv")

log_check("ML Feature Dictionary Saved", file.exists("data/processed/ml_feature_dictionary.csv"))

# ==============================================================================
# STEP 5: BUILD MASTER DATASET & CALCULATE RISK FLAGS
# ==============================================================================
message("\n--- Step 5: Master Dataset Build & Risk Calculation ---")

# 5.1 Execute Left Join
master_dataset <- merge(combined_tanks, components_wide, by = c("FAC_ID", "TANK_ID"), all.x = TRUE)

# v8.0: CAPACITY checkpoint after merge
log_check("Master: CAPACITY column present after merge", "CAPACITY" %in% names(master_dataset))

# 5.2 CALCULATE RISK FLAGS (MIGRATED FROM 03)
# ------------------------------------------------------------------------------
message("  -> Calculating Regulatory Risk Flags...")

# Helper: Check specific column for regex
check_feat <- function(col, regex) {
  if(!col %in% names(master_dataset)) return(rep(0L, nrow(master_dataset)))
  as.integer(grepl(regex, master_dataset[[col]], ignore.case = TRUE))
}

# Helper: Check specific column for list of risk terms
check_terms <- function(col, terms) {
  if(!col %in% names(master_dataset) || length(terms) == 0) return(rep(0L, nrow(master_dataset)))
  pattern <- paste(terms, collapse = "|")
  as.integer(grepl(pattern, master_dataset[[col]], ignore.case = TRUE))
}

master_dataset[, `:=`(
  # Construction Risks
  flag_bare_steel       = check_feat("cat_tank_construction", "Bare Steel|Unprotected"),
  flag_single_wall      = check_feat("cat_tank_construction", "Single Wall") | 
                          check_feat("cat_ug_piping", "Single Wall"),
  flag_secondary_containment = check_feat("cat_tank_construction", "Secondary Containment") |
                               check_feat("cat_ust_secondary", "Secondary Containment"),
  
  # Piping Risks
  flag_pressure_piping  = check_feat("cat_pump_delivery", "Pressurized"),
  flag_suction_piping   = check_feat("cat_pump_delivery", "Suction"),
  flag_galvanized_piping = check_feat("cat_ug_piping", "Galvanized"),
  
  # Detection Risks
  flag_electronic_atg   = check_feat("cat_tank_release_detection", "ATG|Automatic Tank Gauging"),
  flag_manual_detection = check_feat("cat_tank_release_detection", "Manual|Stick|Visual"),
  flag_overfill_alarm   = check_feat("cat_overfill_prevention", "Alarm|Electronic.*Overfill"),
  
  # Compliance Risks (Dynamic from Mappings)
  flag_noncompliant     = check_terms("cat_tank_construction", risk_terms$non_compliant) |
                          check_terms("cat_ug_piping", risk_terms$non_compliant) |
                          check_terms("cat_tank_release_detection", risk_terms$non_compliant),
  
  flag_legacy           = check_terms("cat_tank_construction", risk_terms$legacy),
  
# --- DATA QUALITY RISK (Unknown/Missing Info) - BROKEN OUT BY CATEGORY ---
  flag_tank_construction_unknown   = check_feat("cat_tank_construction", "Unknown|Unmapped|Data Gap"),
  flag_ug_piping_unknown           = check_feat("cat_ug_piping", "Unknown|Unmapped|Data Gap"),
  flag_tank_release_detection_unknown = check_feat("cat_tank_release_detection", "Unknown|Unmapped|Data Gap"),
  
  # Composite flag (preserves backward compatibility)
  flag_data_unknown = check_feat("cat_tank_construction", "Unknown|Unmapped|Data Gap") | 
                      check_feat("cat_ug_piping", "Unknown|Unmapped|Data Gap") | 
                      check_feat("cat_tank_release_detection", "Unknown|Unmapped|Data Gap")
)]

# Clean up spurious columns if any
if ("NA" %in% names(master_dataset)) master_dataset[, `NA` := NULL]

log_check("Merge executed and Flags calculated", nrow(master_dataset) == nrow(combined_tanks))

# 5.3 Final Keys
setkey(master_dataset, FAC_ID, TANK_ID, DATE_INSTALLED)
setorder(master_dataset, FAC_ID, TANK_ID, DATE_INSTALLED)

# 5.4 Save Master Dataset
saveRDS(master_dataset, "data/processed/pa_ust_master_facility_tank_database.rds")
fwrite(master_dataset, "data/processed/pa_ust_master_facility_tank_database.csv", nThread = getDTthreads())
log_check("Master dataset saved", file.exists("data/processed/pa_ust_master_facility_tank_database.rds"))

# ==============================================================================
# STEP 6: GENERATE DATA DICTIONARY
# ==============================================================================
message("\n--- Step 6: Generating Data Dictionary ---")

build_data_dictionary <- function(dt) {
  dict <- data.table(
    variable_name = names(dt),
    data_type = sapply(dt, function(x) class(x)[1]),
    n_missing = sapply(dt, function(x) sum(is.na(x))),
    n_unique = sapply(dt, uniqueN)
  )
  dict[, pct_missing := round((n_missing / nrow(dt)) * 100, 2)]
  
  # Source Logic
  dict[, origin := fcase(
    grepl("^cat_", variable_name), "Component Attribute (Categorical)",
    grepl("^ind_", variable_name), "Component Attribute (Indicator)",
    grepl("^flag_", variable_name), "Derived Risk Flag",
    default = "Base Facility Record"
  )]
  
  # Descriptions
  dict[variable_name == "FAC_ID", description := "Unique Facility Identifier (PADEP Other ID)"]
  dict[variable_name == "TANK_ID", description := "Unique Tank Sequence Number (Integer)"]
  dict[variable_name %like% "^cat_", description := "Coarsened Factor (Text) for Tables"]
  dict[variable_name %like% "^ind_", description := "Raw Indicator (Binary) for ML"]
  dict[variable_name %like% "^flag_", description := "Regulatory Risk Flag (Calculated from Cats)"]
  dict[variable_name == "flag_data_unknown", description := "Indicator: Key Attributes (Construction/Piping/Detection) are Unknown/Unmapped"]
  
  return(dict)
}

data_dictionary <- build_data_dictionary(master_dataset)
fwrite(data_dictionary, "data/processed/master_dataset_data_dictionary.csv")
log_check("Data dictionary generated", file.exists("data/processed/master_dataset_data_dictionary.csv"))

# ==============================================================================
# APPENDIX B: VALIDATION CHECKLIST EXPORT
# ==============================================================================
checklist_path <- "data/processed/validation_checklist.txt"
sink(checklist_path)
cat("PA UST DATABASE BUILD - VALIDATION CHECKLIST\n")
cat(sprintf("Date: %s\n\n", Sys.time()))
for (entry in validation_log) {
  mark <- if(entry$status) "[x]" else "[ ]"
  cat(sprintf("%s %s\n", mark, entry$item))
}
sink()
message(sprintf("\nValidation Checklist written to: %s", checklist_path))
message("MASTER DATASET BUILD COMPLETE (v8.0)")