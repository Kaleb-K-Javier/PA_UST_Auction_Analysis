# R/etl/create_verification_subset.R
# ============================================================================
# Purpose: Generate verification sample with collapsed URLs (No Bloat Version)
# ============================================================================

library(data.table)
library(cli)

# 1. LOAD DATA
# ============================================================================
cli_h1("Generating Verification Sample")

# Paths
path_active   <- "data/external/padep/pasda_facilities_active.rds"
path_inactive <- "data/external/padep/pasda_facilities_inactive.rds"
path_attribs  <- "data/external/padep/allattributes(in).csv"

# Load Lists
if (!file.exists(path_active)) cli_abort("Active facilities list missing.")
active_facs <- readRDS(path_active) %>% as.data.table()
setDT(active_facs) # Ensure data.table for easy grouping

if (!file.exists(path_inactive)) cli_abort("Inactive facilities list missing.")
inactive_facs <- readRDS(path_inactive) %>% as.data.table()
setDT(inactive_facs) 

# 2. RANDOM SAMPLING
# ============================================================================
set.seed(99) 

# Sample 5 distinct IDs using the specific column 'attributes_facility_i'
samp_active   <- sample(unique(active_facs$attributes_facility_i), 5)
samp_inactive <- sample(unique(inactive_facs$attributes_facility_i), 5)

target_ids <- c(samp_active, samp_inactive)

cli_h2("Selected Target IDs")
cat("Active:  ", paste(samp_active, collapse=", "), "\n")
cat("Inactive:", paste(samp_inactive, collapse=", "), "\n")

# 3. EXTRACT ATTRIBUTES
# ============================================================================
cli_h2("Extracting from All Attributes File")

if (!file.exists(path_attribs)) cli_abort("Attribute file missing.")

# Load big file
dt_attr <- fread(path_attribs, colClasses = "character", fill = TRUE)

L_GOLL_comp = dt_attr[FAC_ID == '51-09156']
L_GOLL_ACTIVE = active_facs[attributes_facility_i == '51-09156']
L_GOLL_INACTIVE = inactive_facs[attributes_facility_i == '51-09156']
View(L_GOLL_comp)
View(L_GOLL_ACTIVE)
View(L_GOLL_INACTIVE)

nrow(active_facs)
nrow(inactive_facs)# Filter using 'FAC_ID'
subset_dt <- dt_attr[FAC_ID %in% target_ids]

if (nrow(subset_dt) == 0) {
  cli_alert_danger("No matching rows found! Check ID formats.")
} else {
  cli_alert_success("Extracted {nrow(subset_dt)} rows.")
  
  # 4. COLLAPSE URLs & JOIN
  # ==========================================================================
  cli_alert_info("Collapsing and Joining URLs...")

  # collapse active URLs by facility ID
  act_map <- active_facs[, .(url_active_info = paste(unique(attributes_tank_infor), collapse = "|")), 
                         by = .(FAC_ID = attributes_facility_i)]

  # collapse inactive URLs by facility ID
  inact_map <- inactive_facs[, .(url_inactive_info = paste(unique(attributes_tank_infor), collapse = "|")), 
                             by = .(FAC_ID = attributes_facility_i)]

  # Left Join Active URLs
  subset_dt <- merge(subset_dt, act_map, by = "FAC_ID", all.x = TRUE)
  
  # Left Join Inactive URLs
  subset_dt <- merge(subset_dt, inact_map, by = "FAC_ID", all.x = TRUE)

  # 5. SAVE
  out_path <- "data/processed/verification_subset.csv"
  fwrite(subset_dt, out_path)
  cli_alert_success("Saved: {.path {out_path}}")
  cli_alert_info("Ready for upload.")
}