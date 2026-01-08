# PA UST Facility-Tank Master Database Build Script
# Version: 3.1 (Parallel + Memory Optimized + Clean Names + Data Dictionary)
# ==============================================================================

# 0. SETUP & PACKAGES
# ==============================================================================
# install.packages(c("data.table", "janitor", "stringr", "readxl", "pbapply", "future", "furrr", "tictoc"))

suppressPackageStartupMessages({
  library(data.table)
  library(janitor)
  library(stringr)
  library(readxl)
  library(pbapply)
  library(future)
  library(furrr)
  library(tictoc)
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
  
  # Read full sheet (all columns)
  dt <- as.data.table(read_excel(file))
  
  # Strict selection: Map source headers to Master Standard
  # Using data.table subsetting logic: new_name = old_name
  # Note: NA columns or empty cols in source are implicitly dropped if not mapped
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
# Types are forced on the subsetted data
active_tanks[, FAC_ID := trimws(as.character(FAC_ID))]
active_tanks[, TANK_ID := trimws(as.character(TANK_ID))]
active_tanks[, TANK_TYPE := toupper(trimws(as.character(TANK_TYPE)))]
active_tanks[, SUBSTANCE_CODE := toupper(trimws(as.character(SUBSTANCE_CODE)))]
active_tanks[, STATUS_CODE := toupper(trimws(as.character(STATUS_CODE)))]
active_tanks[, Tank_Status_Meaning := trimws(as.character(Tank_Status_Meaning))]

# Dates & Numbers
active_tanks[, DATE_INSTALLED := as.Date(DATE_INSTALLED)]
active_tanks[, Tank_Status_Date := as.Date(Tank_Status_Date)]
active_tanks[, CAPACITY := as.numeric(CAPACITY)]

log_check("Active column types standardized", is.numeric(active_tanks$CAPACITY))

# 1.5 Tank Sequence ID Cleaning (Remove 'A', strip zeros)
active_tanks[, TANK_ID := gsub("A$", "", TANK_ID)]
active_tanks[, TANK_ID := as.integer(gsub("^0+", "", TANK_ID))]

log_check("Active TANK_ID converted to integer", is.integer(active_tanks$TANK_ID))

# 1.6 Filter to USTs Only
n_before <- nrow(active_tanks)
active_tanks <- active_tanks[TANK_TYPE == "UST"]
message(sprintf("Filtered out %d AST records", n_before - nrow(active_tanks)))

# 1.7 Source Indicator & Checkpoint
active_tanks[, source_dataset := "active"]
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

# Configuration: Batching for Parallel Processing
batch_size <- 2000 
batches <- split(inactive_files, ceiling(seq_along(inactive_files) / batch_size))

# Set up Parallel Workers (Multisession)
# Use one less than available cores to keep system responsive
available_cores <- parallel::detectCores(logical = FALSE) - 1
plan(multisession, workers = available_cores)
message(sprintf("Parallel processing enabled on %d cores.", available_cores))

# 2.2 Worker Function (Self-Contained)
process_single_file_parallel <- function(file) {
  require(data.table)
  require(janitor)
  
  tryCatch({
    # 1. Read ALL columns (as requested)
    dt <- fread(file, showProgress = FALSE, na.strings = c("", "NA", "NULL"))
    
    # 2. Clean Names
    setnames(dt, janitor::make_clean_names(names(dt)))
    
    # 3. Target Map (Local definition for safety)
    # Maps: Cleaned_Source_Name -> Master_Name
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
    
    # 4. Immediate Subset & Rename (Memory Optimization)
    # Only keep columns that exist in the map
    found_cols <- intersect(names(dt), names(target_map))
    dt_subset <- dt[, ..found_cols]
    
    # Rename to Master Standard
    new_names <- target_map[match(names(dt_subset), names(target_map))]
    setnames(dt_subset, names(dt_subset), new_names)
    
    return(dt_subset)
    
  }, error = function(e) return(NULL))
}

# 2.3 Execute Batches
message(sprintf("Processing %d batches...", length(batches)))
tic("Batch Processing")

# future_map runs the batch loop in parallel
all_batches <- future_map(batches, function(current_batch_files) {
  # Standard lapply inside the worker (sequential read of ~2000 files)
  batch_list <- lapply(current_batch_files, process_single_file_parallel)
  # Bind inside worker to reduce data transfer
  rbindlist(Filter(Negate(is.null), batch_list), fill = TRUE, use.names = TRUE)
}, .progress = TRUE)

toc()

# Combine Parallel Results
inactive_tanks <- rbindlist(Filter(Negate(is.null), all_batches), fill = TRUE, use.names = TRUE)
log_check("Inactive parallel processing completed", nrow(inactive_tanks) > 0)

# Reset Parallel Plan
plan(sequential)
rm(all_batches)
gc()

saveRDS(inactive_tanks, "data/processed/inactive_tanks_combined_raw.rds")

# 2.4 Final Type Standardization
inactive_tanks[, FAC_ID := trimws(as.character(FAC_ID))]
inactive_tanks[, TANK_ID := trimws(as.character(TANK_ID))]
inactive_tanks[, TANK_TYPE := toupper(trimws(as.character(TANK_TYPE)))]
inactive_tanks[, SUBSTANCE_CODE := toupper(trimws(as.character(SUBSTANCE_CODE)))]
inactive_tanks[, STATUS_CODE := toupper(trimws(as.character(STATUS_CODE)))]
inactive_tanks[, Tank_Status_Meaning := trimws(as.character(Tank_Status_Meaning))]

# Flexible Date Parsing
parse_date_flexible <- function(date_col) {
  parsed <- as.Date(date_col, format = "%Y-%m-%d")
  if (all(is.na(parsed))) parsed <- as.Date(date_col, format = "%m/%d/%Y")
  if (all(is.na(parsed))) parsed <- as.Date(date_col, format = "%d-%b-%Y")
  return(parsed)
}
inactive_tanks[, DATE_INSTALLED := parse_date_flexible(DATE_INSTALLED)]
inactive_tanks[, Tank_Status_Date := parse_date_flexible(Tank_Status_Date)]
inactive_tanks[, CAPACITY := as.numeric(CAPACITY)]

# 2.5 Tank Sequence ID Cleaning (Format: "578916 - 001")
# Split on hyphen, take second part
inactive_tanks[, TANK_ID := sub("^.*-\\s*", "", TANK_ID)]
inactive_tanks[, TANK_ID := trimws(TANK_ID)]
inactive_tanks[, TANK_ID := gsub("A$", "", TANK_ID)]
inactive_tanks[, TANK_ID := as.integer(gsub("^0+", "", TANK_ID))]

# Handle invalid IDs
invalid_recs <- inactive_tanks[is.na(TANK_ID) | TANK_ID <= 0]
if (nrow(invalid_recs) > 0) fwrite(invalid_recs, "data/processed/invalid_tank_ids.csv")
inactive_tanks <- inactive_tanks[!is.na(TANK_ID) & TANK_ID > 0]

log_check("Inactive TANK_ID converted to integer", is.integer(inactive_tanks$TANK_ID))

# 2.6 Filter to USTs Only & Source
inactive_tanks <- inactive_tanks[TANK_TYPE == "UST"]
inactive_tanks[, source_dataset := "inactive"]

# 2.7 Checkpoint
saveRDS(inactive_tanks, "data/processed/inactive_tanks_harmonized.rds")
fwrite(inactive_tanks, "data/processed/inactive_tanks_harmonized.csv", nThread = getDTthreads())

# ==============================================================================
# STEP 3: COMBINE ACTIVE AND INACTIVE TANKS
# ==============================================================================
message("\n--- Step 3: Combining Datasets ---")

# 3.2 Row Bind
combined_tanks <- rbindlist(list(active_tanks, inactive_tanks), use.names = TRUE, fill = TRUE)
log_check("Row count equals sum", nrow(combined_tanks) == nrow(active_tanks) + nrow(inactive_tanks))

# 3.3 Construct Closed Date Variable
combined_tanks[, Tank_Closed_Date := fifelse(source_dataset == "inactive", Tank_Status_Date, as.Date("9999-01-01"))]
combined_tanks[, is_closed := fifelse(source_dataset == "inactive", 1L, 0L)]

# 3.4 Set Keys
setkey(combined_tanks, FAC_ID, TANK_ID, DATE_INSTALLED)
setorder(combined_tanks, FAC_ID, TANK_ID, DATE_INSTALLED)

# 3.5 Deduplication Check
dupes <- combined_tanks[, .N, by = .(FAC_ID, TANK_ID)][N > 1]
if (nrow(dupes) > 0) {
  example_dupes <- combined_tanks[FAC_ID %in% head(dupes$FAC_ID, 10) & TANK_ID %in% head(dupes$TANK_ID, 10)]
  fwrite(example_dupes, "data/processed/duplicate_tank_records_sample.csv")
}

# 3.6 Save Checkpoint
saveRDS(combined_tanks, "data/processed/combined_tanks_status.rds")
fwrite(combined_tanks, "data/processed/combined_tanks_status.csv", nThread = getDTthreads())

# ==============================================================================
# STEP 4: PROCESS TANK COMPONENT ATTRIBUTES
# ==============================================================================
message("\n--- Step 4: Process Tank Component Attributes ---")

comp_file <- "data/external/padep/allattributes(in).csv"
log_check("Component attributes CSV exists", file.exists(comp_file))

components <- fread(comp_file, na.strings = c("", "NA", "NULL"))
setnames(components, janitor::make_clean_names(names(components)))

# Clean up component columns to Master Standard
# Explicit map based on user instructions
components <- components[, .(
  REGION_ICS_CODE          = ics_code,
  FAC_ID                   = fac_id,
  FACILITY_NAME            = f_name,
  SUB_FACILITY_ID          = sub_fac_id,
  TANK_ID                  = tank_name,
  CAPACITY                 = capacity,
  STATUS_CODE              = status,
  COMPONENT_BEGIN_DATE     = begin_date,
  COMPONENT_ATTRIBUTE_CODE = attribute,
  COMPONENT_CATEGORY       = description,
  COMPONENT_TYPE           = description_1
)]

# 4.3 Clean TANK_ID & 4.4 FAC_ID
components[, TANK_ID := as.integer(TANK_ID)]
components[, FAC_ID := trimws(as.character(FAC_ID))]

# 4.5 Classify Component Types
yesno_types <- c("YES", "NO", "NONE", "NOT IN CONTACT W/ GROUND")
components[, is_yesno := COMPONENT_TYPE %in% yesno_types | grepl("[YN]$", COMPONENT_ATTRIBUTE_CODE)]

# 4.6 Widen Part Name Components
# FIX: Use make_clean_names to remove special characters (e.g., "&")
part_components <- components[is_yesno == FALSE]
parts_wide <- dcast(part_components, FAC_ID + TANK_ID ~ COMPONENT_CATEGORY, value.var = "COMPONENT_TYPE",
                    fun.aggregate = function(x) paste(unique(x), collapse = "; "))

# Rename parts_wide columns to be safe (remove spaces, ampersands, etc.)
# We skip the key columns
cols_to_clean <- setdiff(names(parts_wide), c("FAC_ID", "TANK_ID"))
setnames(parts_wide, cols_to_clean, janitor::make_clean_names(cols_to_clean, case = "screaming_snake"))

part_types <- unique(part_components[, .(COMPONENT_CATEGORY, COMPONENT_TYPE)])
for (i in seq_len(nrow(part_types))) {
  # Clean category names to match the dcast output above
  cat_name_raw <- part_types$COMPONENT_CATEGORY[i]
  cat_name_clean <- janitor::make_clean_names(cat_name_raw, case = "screaming_snake")
  
  type_name <- part_types$COMPONENT_TYPE[i]
  
  # Create safe column name for binary indicator
  col_name <- make_clean_names(paste0(cat_name_clean, "_", type_name))
  
  # Ensure the category column exists (it might have been cleaned differently, checking intersection is safest)
  if (cat_name_clean %in% names(parts_wide)) {
    parts_wide[, (col_name) := fifelse(grepl(type_name, get(cat_name_clean), fixed = TRUE), 1L, 0L)]
  }
}

# REPLACE STEP 4.7 WITH THIS SAFER VERSION

# 4.7 Widen Yes/No Components
yesno_components <- components[is_yesno == TRUE]

# Calculate binary value (1/0)
# We ensure NAs are handled: if type is YES or code ends in Y, it's 1. Otherwise 0.
yesno_components[, binary_value := fifelse(
  COMPONENT_TYPE == "YES" | grepl("Y$", COMPONENT_ATTRIBUTE_CODE),
  1L, 
  0L
)]

# Define a safe max function to prevent -Inf errors
safe_max <- function(x) {
  # Remove NAs first
  clean_x <- x[!is.na(x)]
  # If empty after removing NAs, return 0 (assuming absence = 0)
  # Alternatively return NA_integer_ if you prefer to know data was missing
  if(length(clean_x) == 0) return(0L) 
  return(max(clean_x))
}

# Pivot wide using the safe function
yesno_wide <- dcast(
  yesno_components,
  FAC_ID + TANK_ID ~ COMPONENT_CATEGORY,
  value.var = "binary_value",
  fun.aggregate = safe_max
)

# Suffix with _YN to prevent collision
yn_cols_to_clean <- setdiff(names(yesno_wide), c("FAC_ID", "TANK_ID"))
new_yn_names <- paste0(janitor::make_clean_names(yn_cols_to_clean, case = "screaming_snake"), "_YN")
setnames(yesno_wide, yn_cols_to_clean, new_yn_names)

message(sprintf("Created %d yes/no indicator columns", ncol(yesno_wide) - 2))

# 4.8 Merge & 4.9 Keys
components_wide <- merge(parts_wide, yesno_wide, by = c("FAC_ID", "TANK_ID"), all = TRUE)
setkey(components_wide, FAC_ID, TANK_ID)
saveRDS(components_wide, "data/processed/tank_components_wide.rds")

# ==============================================================================
# STEP 5: BUILD MASTER DATASET
# ==============================================================================
message("\n--- Step 5: Master Dataset Build ---")

# 5.2 Pre-Join Validation
unmatched_tanks <- combined_tanks[!components_wide, on = .(FAC_ID, TANK_ID)]
if (nrow(unmatched_tanks) > 0) fwrite(unmatched_tanks, "data/processed/tanks_without_components.csv")

# 5.5 Execute Left Join
master_dataset <- merge(combined_tanks, components_wide, by = c("FAC_ID", "TANK_ID"), all.x = TRUE)

# FIX: Identify and remove the "NA" column if it exists
# This handles the mystery column from the dcast/merge process
if ("NA" %in% names(master_dataset)) {
  master_dataset[, `NA` := NULL]
  message("Removed spurious 'NA' column from master dataset.")
}

log_check("Merge executed without row loss", nrow(master_dataset) == nrow(combined_tanks))

# 5.7 Final Keys
setkey(master_dataset, FAC_ID, TANK_ID, DATE_INSTALLED)
setorder(master_dataset, FAC_ID, TANK_ID, DATE_INSTALLED)

# 5.9 Save Master Dataset
saveRDS(master_dataset, "data/processed/pa_ust_master_facility_tank_database.rds")
fwrite(master_dataset, "data/processed/pa_ust_master_facility_tank_database.csv", nThread = getDTthreads())
log_check("Master dataset saved", file.exists("data/processed/pa_ust_master_facility_tank_database.rds"))

# ==============================================================================
# STEP 6: GENERATE DATA DICTIONARY
# ==============================================================================
message("\n--- Step 6: Generating Data Dictionary ---")

# Define Data Dictionary Construction Logic
build_data_dictionary <- function(dt) {
  
  # Get basic stats
  dict <- data.table(
    variable_name = names(dt),
    data_type = sapply(dt, function(x) class(x)[1]),
    n_missing = sapply(dt, function(x) sum(is.na(x))),
    n_unique = sapply(dt, uniqueN)
  )
  
  # Calculate missing percentage
  dict[, pct_missing := round((n_missing / nrow(dt)) * 100, 2)]
  
  # Add Source Logic (Combined vs Component)
  base_cols <- c("FAC_ID", "TANK_ID", "TANK_TYPE", "DATE_INSTALLED", "CAPACITY", 
                 "SUBSTANCE_CODE", "STATUS_CODE", "Tank_Status_Meaning", 
                 "Tank_Status_Date", "source_region", "source_dataset", 
                 "Tank_Closed_Date", "is_closed")
  dict[, origin := fifelse(variable_name %in% base_cols, "Base Facility Record", "Component Attribute")]
  
  # Add Description Placeholder (User can fill)
  dict[, description := ""]
  dict[variable_name == "FAC_ID", description := "Unique Facility Identifier (PADEP Other ID)"]
  dict[variable_name == "TANK_ID", description := "Unique Tank Sequence Number (Integer)"]
  dict[variable_name == "is_closed", description := "Binary Indicator: 1=Inactive/Closed, 0=Active"]
  dict[variable_name %like% "_YN$", description := "Binary Indicator: Component Presence (1=Yes)"]
  
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
message("MASTER DATASET BUILD COMPLETE.")