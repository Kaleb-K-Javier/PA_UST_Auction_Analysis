# R/etl/04_construct_hazard_panel.R
# ============================================================================
# Pennsylvania UST Analysis - ETL Step 4: Construct Facility-Year Panel
# ============================================================================
# Purpose: Build a "Hazard Rate" dataset (One row per Facility per Year).
# Methodology: High-Performance Vectorized Construction (data.table)
# Update: FULL DYNAMIC FEATURE EXTRACTION (150+ Cols)
#   1. Skeleton: Cross-Join Facilities x Years (1990-2025).
#   2. History: Cumulative sums for Claims and Closed Tanks.
#   3. Active Profile: Dynamic One-Hot Encoding of ALL components.
# Output: data/processed/panel_hazard_dataset.rds
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(janitor)
  library(stringr)
  library(here)
})

# ============================================================================
# 1. SETUP & LOAD
# ============================================================================
paths <- list(
  processed = here("data/processed"),
  tanks     = here("data/processed/pa_ust_master_facility_tank_database.rds"),
  claims    = here("data/processed/claims_clean.rds"),
  raw_comps = here("data", "external", "padep", "allattributes(in).csv") 
)

cat("--- Loading Data ---\n")
tanks <- readRDS(paths$tanks)
setDT(tanks)
claims <- readRDS(paths$claims)
setDT(claims)

# Clean IDs & Dates
tanks[, FAC_ID := trimws(as.character(FAC_ID))]
tanks[, TANK_ID := trimws(as.character(TANK_ID))]
tanks[, install_year := year(as.IDate(DATE_INSTALLED))]
tanks[, close_year := year(as.IDate(Tank_Closed_Date))]
tanks[is.na(close_year), close_year := 9999] 

claims[, department := trimws(as.character(department))]
claims[, claim_year := year(as.IDate(claim_date))]

# ============================================================================
# 2. BUILD THE SKELETON (Vectorized Grid)
# ============================================================================
cat("--- Building Time Grid ---\n")

START_YEAR <- 1990
END_YEAR   <- 2025

facilities <- unique(tanks[, .(FAC_ID)])
panel <- CJ(FAC_ID = facilities$FAC_ID, Year = START_YEAR:END_YEAR)
setkey(panel, FAC_ID, Year)

cat(sprintf("Skeleton created: %d rows (Facilities x Years)\n", nrow(panel)))

# ============================================================================
# 3. CLAIMS & CLOSED TANK HISTORY (Cumulative)
# ============================================================================
cat("--- Vectorizing History ---\n")

# A. Claims
annual_claims <- claims[, .(n_new_claims = .N, cost = sum(total_paid, na.rm=TRUE)), 
                        by = .(FAC_ID = department, Year = claim_year)]
panel <- merge(panel, annual_claims, by = c("FAC_ID", "Year"), all.x = TRUE)
panel[is.na(n_new_claims), n_new_claims := 0]

panel[, `:=`(
  cumulative_claims = cumsum(n_new_claims),
  has_claim_event = as.integer(n_new_claims > 0)
), by = FAC_ID]

# B. Closed Tanks
closed_counts <- tanks[close_year <= END_YEAR, .(n_closed = .N), by = .(FAC_ID, Year = close_year)]
panel <- merge(panel, closed_counts, by = c("FAC_ID", "Year"), all.x = TRUE)
panel[is.na(n_closed), n_closed := 0]
panel[, cumulative_closed_tanks := cumsum(n_closed), by = FAC_ID]

# ============================================================================
# 4. ACTIVE TANK PROFILE (FULL DYNAMIC EXPANSION)
# ============================================================================
cat("--- Constructing Full Granular History (All Components) ---\n")

# A. Load & Clean Components
if (file.exists(paths$raw_comps)) {
  # Load raw
  comps <- fread(paths$raw_comps, colClasses = "character")
  
  # Standardize names
  comps <- clean_names(comps)
  
  # Robust Renaming (Using 'comps', not 'components')
  # 1. Facility ID
  if("fac_id" %in% names(comps)) setnames(comps, "fac_id", "FAC_ID")
  if("facility_id" %in% names(comps)) setnames(comps, "facility_id", "FAC_ID")
  
  # 2. Tank ID
  if("tank_name" %in% names(comps)) setnames(comps, "tank_name", "TANK_ID")
  if("tank_id" %in% names(comps)) setnames(comps, "tank_id", "TANK_ID")
  
  # 3. Attribute/Code
  if("attribute" %in% names(comps)) setnames(comps, "attribute", "CODE")
  if("code" %in% names(comps)) setnames(comps, "code", "CODE")
  
  # Filter Empty Codes
  comps <- comps[!is.na(CODE) & CODE != ""]
  
  # Create Feature Names
  # Clean Description: "Double Walled" -> "DOUBLE_WALLED"
  comps[, clean_desc := str_replace_all(toupper(description), "[^A-Z0-9]+", "_")]
  comps[, clean_desc := str_trunc(clean_desc, 30, ellipsis = "")] 
  comps[, feature_name := paste0("feat_", CODE, "_", clean_desc)]
  
  # B. DCAST to Tank Level (Wide Format)
  # 150+ columns: One row per Tank, 1 if component present, 0 if not
  cat("   Pivoting components to wide format...\n")
  tank_features_wide <- dcast(comps, FAC_ID + TANK_ID ~ feature_name, 
                              fun.aggregate = length, fill = 0)
  
  # Convert counts (>1) to binary (1)
  feat_cols <- names(tank_features_wide)[-(1:2)] # Skip FAC_ID, TANK_ID
  for(col in feat_cols) {
    set(tank_features_wide, i=which(tank_features_wide[[col]] > 0), j=col, value=1)
  }
  
} else {
  stop("Component file missing")
}

# C. Merge Features to Tank Master
# We attach dates (Install/Close) to the features so we know WHEN they were active
tanks_rich <- merge(tanks[, .(FAC_ID, TANK_ID, install_year, close_year)], 
                    tank_features_wide, by = c("FAC_ID", "TANK_ID"), all.x = TRUE)

# Fill NAs in feature columns with 0 (Tanks with no components found = 0 risk)
for(col in feat_cols) {
  set(tanks_rich, i=which(is.na(tanks_rich[[col]])), j=col, value=0)
}

# D. Expand to Time Grid (The "Heavy" Step)
# Join Panel (Fac, Year) to Rich Tanks. 
cat("   Expanding to Facility-Year Panel (Active Tanks Only)...\n")
panel_active_tanks <- merge(panel[, .(FAC_ID, Year)], 
                            tanks_rich, 
                            by = "FAC_ID", all.x = TRUE, allow.cartesian = TRUE)

# Filter for Active Years
panel_active_tanks <- panel_active_tanks[Year >= install_year & Year <= close_year]

# E. Aggregate to Facility-Year (Calculate Shares)
# "In Year X, what % of active tanks had Pressure Piping?"
cat("   Aggregating shares for", length(feat_cols), "features...\n")

# Dynamic Aggregation using .SD (Fastest Method)
risk_profile <- panel_active_tanks[, lapply(.SD, mean, na.rm=TRUE), 
                                   by = .(FAC_ID, Year), 
                                   .SDcols = feat_cols]

# Calculate Meta Stats (Count & Age)
meta_stats <- panel_active_tanks[, .(
  n_active_tanks = uniqueN(TANK_ID),
  avg_tank_age = mean(Year - install_year, na.rm = TRUE)
), by = .(FAC_ID, Year)]
# ============================================================================
# 5. FINAL MERGE
# ============================================================================
cat("--- Final Merge ---\n")

# Merge Risk Shares
panel <- merge(panel, risk_profile, by = c("FAC_ID", "Year"), all.x = TRUE)

# Merge Meta Stats
panel <- merge(panel, meta_stats, by = c("FAC_ID", "Year"), all.x = TRUE)

# Fill NAs for years with 0 tanks
# (Features become 0, Count becomes 0)
for(col in feat_cols) set(panel, which(is.na(panel[[col]])), col, 0)
panel[is.na(n_active_tanks), n_active_tanks := 0]
panel[is.na(avg_tank_age), avg_tank_age := 0]

# ============================================================================
# 6. SAVE
# ============================================================================
cat("--- Saving High-Dimensional Panel ---\n")
if(!dir.exists(paths$processed)) dir.create(paths$processed, recursive=TRUE)
saveRDS(panel, file.path(paths$processed, "panel_hazard_dataset.rds"))

cat(sprintf("COMPLETE. Panel: %d rows x %d columns.\n", nrow(panel), ncol(panel)))