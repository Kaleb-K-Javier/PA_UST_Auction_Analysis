# R/analysis/00_raw_data_summary.R
# ==============================================================================
# Purpose: Generate robust descriptive stats, Tables, and Descriptive Plots
#          * Flat directory structure (output/tables/ & output/figures/)
#          * FIXED: Uses writeLines() to bypass Pandoc dependency error
#          * INTEGRATED: Owner Linkage & Firm Classification
#          * ENHANCED: Actual KNN Date Imputation (Diagnostics only)
#          * ENHANCED: Deep Fleet Analytics (Risk, Standardization)
#          * ENHANCED: Closure Dynamics by Facility Type
# ==============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(janitor)
  library(stringr)
  library(knitr)
  library(kableExtra) 
  library(ggplot2)
  library(scales)
  library(lubridate)
  library(viridis)
  library(xtable)
  library(FNN) # Required for Actual KNN Regression
})

# 1. SETUP & DATA LOADING
# ==============================================================================
path_combined   <- "data/processed/combined_tanks_status.csv"
path_components <- "data/external/padep/allattributes(in).csv"
path_linkage    <- "data\\external\\padep\\facility_linkage_table.csv" 

# Update: Output directories
out_dir_tables <- "output/tables"
out_dir_figs   <- "output/figures"

if(!dir.exists(out_dir_tables)) dir.create(out_dir_tables, recursive = TRUE)
if(!dir.exists(out_dir_figs))   dir.create(out_dir_figs, recursive = TRUE)

# A. Load Main Data
if (!file.exists(path_combined)) stop("Combined tanks file not found.")
combined_tanks <- fread(path_combined)

# B. Load Components
if (!file.exists(path_components)) stop("Raw components file not found.")
raw_comps <- fread(path_components, na.strings = c("", "NA", "NULL"))
setnames(raw_comps, janitor::make_clean_names(names(raw_comps)))

components <- raw_comps[, .(
  FAC_ID                   = fac_id,
  TANK_ID                  = tank_name,
  COMPONENT_CATEGORY       = description,
  COMPONENT_TYPE           = description_1,
  COMPONENT_ATTRIBUTE_CODE = attribute
)]

# C. Load Harmonized Context Data (Reference)
if(file.exists("data/processed/inactive_tanks_harmonized.csv")) {
  harmon_inactive = fread("data/processed/inactive_tanks_harmonized.csv", nThread = getDTthreads())
}
if(file.exists("data/processed/active_tanks_harmonized.csv")) {
  harmon_active = fread("data/processed/active_tanks_harmonized.csv", nThread = getDTthreads())
}

# 2. HELPER FUNCTIONS
# ==============================================================================
save_pub_table <- function(dt, filename_base, caption, col_names = NULL) {
  if(is.null(col_names)) col_names <- names(dt)
  
  # HTML Fragment
  k_html <- kbl(dt, format = "html", caption = caption, col.names = col_names) %>%
    kable_styling(bootstrap_options = c("striped", "hover", "condensed"), full_width = F, position = "left")
  writeLines(as.character(k_html), file.path(out_dir_tables, paste0(filename_base, ".html")))
  
  # LaTeX Fragment
  k_tex <- kbl(dt, format = "latex", caption = caption, booktabs = TRUE, col.names = col_names) %>%
    kable_styling(latex_options = c("striped", "hold_position"))
  writeLines(as.character(k_tex), file.path(out_dir_tables, paste0(filename_base, ".tex")))
  
  message(sprintf("Table Saved: %s", filename_base))
}

save_pub_figure <- function(plot_obj, filename_base) {
  final_plot <- plot_obj + 
    theme_minimal(base_size = 14) +
    theme(
      plot.title = element_blank(),
      plot.subtitle = element_blank(),
      panel.grid.minor = element_line(color = "#ecf0f1", linewidth = 0.5),
      axis.title = element_text(face = "bold", color = "#2c3e50"),
      legend.position = "bottom",
      legend.title = element_text(face = "bold")
    )
  
  ggsave(
    filename = file.path(out_dir_figs, paste0(filename_base, ".png")),
    plot = final_plot, width = 10, height = 6, dpi = 300, bg = "white"
  )
  message(sprintf("Figure Saved: %s", filename_base))
}

# 3. OWNER & SECTOR CLASSIFICATION
# ==============================================================================
message("\nRunning Owner Linkage & Classification...")

if (file.exists(path_linkage)) {
  # Load Linkage Table
  df_link <- fread(path_linkage)
  
  # Clean owner names & IDs
  df_link[, owner_name := toupper(trimws(as.character(owner_name)))]
  df_link[, client_id := as.numeric(client_id)]
  
  # A. Define Sector Logic
  classify_pa_sector <- function(name) {
    fcase(
      # --- MAJOR CHAINS ---
      grepl("SHEETZ", name), "Major Chain (Sheetz)",
      grepl("WAWA", name), "Major Chain (Wawa)",
      grepl("RUTTER|CHR CORP", name), "Major Chain (Rutters)",
      grepl("GETGO|GIANT EAGLE|GIANT FOOD|GIANT CO LLC", name), "Major Chain (GetGo/Giant)",
      grepl("TURKEY HILL|KROGER|EG GROUP", name), "Major Chain (Turkey Hill/EG)",
      grepl("SUNOCO|SUN OIL", name), "Major Chain (Sunoco)",
      grepl("7 ELEVEN|7-11|7 11", name), "Major Chain (7-Eleven)",
      grepl("SPEEDWAY", name), "Major Chain (Speedway)",
      grepl("PILOT TRAVEL|FLYING J|LOVES TRAVEL|TRAVEL CENTERS|TA OPERATING", name), "Major Chain (Travel Center)",
      grepl("ROYAL FARMS", name), "Major Chain (Royal Farms)",
      grepl("COUNTRY FAIR", name), "Major Chain (Country Fair)",
      grepl("UNITED REF|KWIK FILL|RED APPLE", name), "Major Chain (United Refining)",
      grepl("WEIS MKT|WEIS MARKETS", name), "Major Chain (Weis)",
      grepl("EXXON|MOBIL|SHELL|BP |GULF |TEXACO|CITGO|VALERO|LUKOIL|DELTA", name), "Major Chain (Other Fuel Brand)",
      # --- MAJOR RETAIL ---
      grepl("WAL-MART|WALMART|SAMS CLUB", name), "Major Retail (Walmart)",
      grepl("HOME DEPOT|LOWES|TARGET|COSTCO|BJS WHOLESALE", name), "Major Retail (Big Box)",
      # --- PUBLIC SECTOR ---
      grepl("COMMONWEALTH OF PA|PA DEPT|PENNDOT|PA DOT|TURNPIKE", name), "State Govt/Agency",
      grepl("SCHOOL|SCH DIST|ACADEMY|COLLEGE|\\bUNIV\\b|UNIVERSITY|INSTITUTE|EDUCATION", name), "Education/School",
      grepl("BORO|BOROUGH|TWP|TOWNSHIP|CITY OF|COUNTY|MUNICIPAL|MUNI AUTH|SEWER AUTH|WATER AUTH|COMMISSION|AUTHORITY", name), "Local Govt/Muni",
      grepl("FIRE|VOLUNTEER|EMS|AMBULANCE|RESCUE", name), "Emergency Services",
      grepl("FEDERAL|US GOVT|POSTAL|VETERANS ADMIN", name), "Federal Govt",
      # --- UTILITIES ---
      grepl("VERIZON|BELL ATLANTIC|COMCAST|AT&T", name), "Utility/Telecom",
      grepl("PP&L|PPL|PECO|DUQUESNE LIGHT|PENN POWER|FIRSTENERGY|UGI|MET-ED|PENELEC|ELECTRIC|ENERGY", name), "Utility/Energy",
      grepl("AQUA PA|PA AMERICAN WATER|WATER CO|SANITARY|WASTEWATER", name), "Utility/Water & Sewer",
      # --- COMMERCIAL ---
      grepl("HOSPITAL|HEALTH|MEDICAL|CLINIC|UPMC|GEISINGER|AHN |JEFFERSON HEALTH|PENN MEDICINE", name), "Healthcare",
      grepl("AUTO|MOTORS|FORD|CHEVY|TOYOTA|HONDA|NISSAN|DODGE|CHRYSLER|JEEP|KIA|HYUNDAI|BMW|MERCEDES|CAR |SERVICE CENTER|TIRE", name), "Auto Dealership/Repair",
      grepl("TRUCK|HAULING|TRANSPORT|LOGISTICS|EXPRESS|TERMINAL|FREIGHT|DELIVERY|PENSKE|RYDER", name), "Trucking/Logistics",
      grepl("CONST|EXCAVATING|PAVING|BUILDERS|CONTRACTOR|DEVELOPMENT|ASPHALT|CONCRETE|STONE|QUARRY", name), "Construction/Development",
      grepl("REALTY|PROPERTIES|MANAGEMENT|APARTMENTS|ESTATE|RENTAL|HOUSING|LP|LLC|PARTNERS|ASSOCIATES|GROUP|HOLDINGS", name), "Real Estate/Property Mgmt",
      grepl("FARM|DAIRY|NURSERY|GREENHOUSE|AGRI", name), "Agriculture",
      grepl("MFG|MANUFACTURING|STEEL|IRON|WORKS|PRODUCTS|INDUSTRIAL|CHEMICAL|AIR PRODUCTS|ALCOA|US STEEL", name), "Manufacturing/Industrial",
      grepl("GOLF|CLUB|PARK|CAMP|REC|BOWLING|THEATER|CASINO|RESORT|HOTEL|MOTEL|INN", name), "Recreation/Hospitality",
      grepl("BANK|CREDIT UNION|FINANCIAL|PNC BANK|WELLS FARGO|CITIZENS BANK", name), "Financial Services",
      grepl("CHURCH|PARISH|RELIGIOUS|CATHEDRAL|TEMPLE|MINISTRIES", name), "Religious/Non-Profit",
      default = "Private Commercial/Other"
    )
  }
  
  df_link[, owner_sector := classify_pa_sector(owner_name)]
  df_link[is.na(owner_name) | owner_name == "", owner_sector := "Unknown/Missing"]
  
  # B. Build Owner Profile
  getmode <- function(v) {
    uniqv <- unique(v)
    uniqv[which.max(tabulate(match(v, uniqv)))]
  }
  
  owner_stats <- df_link[!is.na(client_id), .(
    owner_sector_mode = getmode(owner_sector),
    facility_count = uniqueN(facility_id)
  ), by = client_id]
  
  # C. Business Model Logic
  categorize_business_model <- function(sector, count) {
    fcase(
      sector %in% c("State Govt/Agency", "Local Govt/Muni", "Federal Govt", 
                    "Education/School", "Emergency Services", "Utility/Water & Sewer"), "Publicly Owned",
      grepl("Major Chain|Major Retail", sector), "Retail Gas (Branded Commercial)",
      sector %in% c("Utility/Telecom", "Utility/Energy", "Trucking/Logistics", 
                    "Construction/Development", "Auto Dealership/Repair"), "Non-Retail: Fleet Fuel Facility",
      sector %in% c("Manufacturing/Industrial", "Agriculture", "Healthcare", 
                    "Real Estate/Property Mgmt", "Financial Services", 
                    "Religious/Non-Profit", "Recreation/Hospitality"), "Private Firm - Non-motor fuel seller",
      sector == "Private Commercial/Other" & count == 1, "Retail Gas - Single Proprietor",
      sector == "Private Commercial/Other" & count > 1, "Retail Gas - Multi-property Not Branded",
      default = "Unknown/Unclassified"
    )
  }
  
  owner_stats[, business_category := categorize_business_model(owner_sector_mode, facility_count)]
  owner_stats[, Owner_Size_Class := fcase(
    facility_count == 1, "Single-Site Owner (Mom & Pop)",
    facility_count >= 2 & facility_count <= 9, "Small Fleet (2-9)",
    facility_count >= 10 & facility_count <= 49, "Medium Fleet (10-49)",
    facility_count >= 50, "Large Fleet/Corporate (50+)",
    default = "Unknown"
  )]
  
  # D. Merge Logic
  df_link[owner_stats, on = "client_id", `:=`(
    final_owner_sector = i.owner_sector_mode,
    business_category = i.business_category,
    Owner_Size_Class = i.Owner_Size_Class
  )]
  
  setnames(df_link, "facility_id", "FAC_ID")
  cols_to_merge <- df_link[, .(FAC_ID, client_id, final_owner_sector, business_category, Owner_Size_Class)]
  
  combined_tanks <- merge(combined_tanks, cols_to_merge, by = "FAC_ID", all.x = TRUE)
  
  combined_tanks[is.na(business_category), business_category := "Unknown/Unclassified"]
  combined_tanks[is.na(Owner_Size_Class), Owner_Size_Class := "Unknown/Unlinked"]
  combined_tanks[is.na(final_owner_sector), final_owner_sector := "Unknown"]
} else {
  message("Warning: 'facility_linkage_table.csv' not found. Skipping Owner Classification.")
  combined_tanks[, `:=`(Owner_Size_Class = "Unknown", business_category = "Unknown", final_owner_sector = "Unknown")]
}

# 4. BASIC DATA PREP & ACTUAL KNN DIAGNOSTIC
# ==============================================================================
# Standardize Date
if(is.character(combined_tanks$DATE_INSTALLED)) {
  combined_tanks[, DATE_INSTALLED := as.Date(DATE_INSTALLED, format = "%m/%d/%Y")]
}
combined_tanks[, install_year := year(DATE_INSTALLED)]

# Standardize Tank Closure Date (Moved up for availability)
if(is.character(combined_tanks$Tank_Closed_Date)) {
  combined_tanks[, Tank_Closed_Date := as.Date(Tank_Closed_Date, format = "%m/%d/%Y")]
}

# Boolean Flags
combined_tanks[, `:=`(
  Gasoline      = SUBSTANCE_CODE %in% c("GAS", "GASOL"),
  Diesel        = SUBSTANCE_CODE == "DIESL",
  Other_Substance = !SUBSTANCE_CODE %in% c("GAS", "GASOL", "DIESL")
)]

# --- Actual KNN Imputation Diagnostic ---
message("\nRunning Actual KNN Date Imputation (Diagnostics)...")

# 1. Prepare Data for KNN
knn_data <- combined_tanks[, .(FAC_ID, TANK_ID, install_year, CAPACITY, SUBSTANCE_CODE, source_region)]
knn_data[, is_valid := !is.na(install_year) & install_year >= 1920]
knn_data <- knn_data[!is.na(CAPACITY) & !is.na(SUBSTANCE_CODE) & !is.na(source_region)]

# 2. Feature Engineering
knn_data[, scaled_cap := scale(CAPACITY)]
knn_data[, sub_fac := as.factor(SUBSTANCE_CODE)]
knn_data[, reg_fac := as.factor(source_region)]
mm_features <- model.matrix(~ scaled_cap + sub_fac + reg_fac - 1, data = knn_data)

# 3. Train/Test Split
train_idx <- which(knn_data$is_valid)
test_idx  <- which(!knn_data$is_valid)

if(length(train_idx) > 0 && length(test_idx) > 0) {
  # Run KNN (k=5)
  knn_fit <- knn.reg(train = mm_features[train_idx, ], test = mm_features[test_idx, ], y = knn_data$install_year[train_idx], k = 5)
  knn_data[test_idx, KNN_Predicted_Year := floor(knn_fit$pred)]
  combined_tanks <- merge(combined_tanks, knn_data[, .(FAC_ID, TANK_ID, KNN_Predicted_Year)], by = c("FAC_ID", "TANK_ID"), all.x = TRUE)
  
  # Diagnostic Plot
  viz_data <- rbind(
    combined_tanks[install_year >= 1960, .(Year = install_year, Type = "Reported (Raw)")],
    combined_tanks[!is.na(KNN_Predicted_Year), .(Year = KNN_Predicted_Year, Type = "KNN Imputed (Proposed)")]
  )
  p_imp <- ggplot(viz_data, aes(x = Year, fill = Type)) +
    geom_density(alpha = 0.4) + scale_fill_manual(values = c("Reported (Raw)" = "#2c3e50", "KNN Imputed (Proposed)" = "#e74c3c")) +
    labs(x = "Installation Year", y = "Density", title = "Date Imputation Diagnostics (Actual KNN k=5)")
  save_pub_figure(p_imp, "003b_knn_imputation_diagnostics")
}

combined_tanks[install_year >= 1980, `:=`(Decade = floor(install_year / 10) * 10, Bin_5yr = floor(install_year / 5) * 5)]

# --- Generate Facility Summary ---
message("Generating Facility-Level Aggregates...")
fac_summary <- combined_tanks[, .(
  n_tanks = uniqueN(TANK_ID),
  n_active = sum(is_closed == 0),
  n_closed = sum(is_closed == 1),
  vintage_year = min(install_year, na.rm=TRUE) 
), by = FAC_ID]

fac_summary[, facility_status := fcase(
  n_active == 0, "Fully Closed",
  n_closed == 0, "Fully Active",
  default = "Mixed Status"
)]
fac_summary[, facility_age := 2025 - vintage_year]
fac_summary[vintage_year >= 1960, Vintage_Decade := floor(vintage_year / 10) * 10]


# 5. OUTPUT GENERATION (TABLES)
# ==============================================================================

# --- 001. Status Frequency ---
combined_tanks[STATUS_CODE == "C" & (is.na(Tank_Status_Meaning) | Tank_Status_Meaning == ""), Tank_Status_Meaning := "Currently In Use"]
combined_tanks[STATUS_CODE == "T" & (is.na(Tank_Status_Meaning) | Tank_Status_Meaning == ""), Tank_Status_Meaning := "Temporarily Out of Use"]
status_freq <- combined_tanks[, .N, by = .(STATUS_CODE, Tank_Status_Meaning)][order(-N)]
status_freq[, Pct := sprintf("%.1f%%", (N/sum(N)*100))]
save_pub_table(status_freq, "001_tank_status_freq", "Tank Status Distribution")

# --- 002. Substance Summary ---
sub_counts <- melt(combined_tanks, measure.vars = c("Gasoline", "Diesel", "Other_Substance"), variable.name="Fuel", value.name="Present")
sub_counts <- sub_counts[Present == TRUE, .N, by = Fuel]
save_pub_table(sub_counts, "002_substance_counts", "Fuel Types")

# --- 003. Date Diagnostics ---
date_freq <- combined_tanks[!is.na(DATE_INSTALLED), .N, by = DATE_INSTALLED][order(-N)]
top_dates <- head(date_freq, 10)
top_dates[, DATE_INSTALLED := as.character(DATE_INSTALLED)] 
save_pub_table(top_dates, "003_date_defaults", "Top 10 Most Frequent Installation Dates")

# --- 004. Component Universe (Loop) ---
unique_cats <- unique(components$COMPONENT_CATEGORY)
message("Generating Component Universe Tables...")
for(cat in unique_cats) {
  safe_name <- str_trunc(janitor::make_clean_names(cat), 40)
  filename <- paste0("004_comp_", safe_name)
  dt_sub <- components[COMPONENT_CATEGORY == cat]
  freq_table <- dt_sub[, .N, by = .(COMPONENT_ATTRIBUTE_CODE, COMPONENT_TYPE)]
  setorder(freq_table, COMPONENT_ATTRIBUTE_CODE, COMPONENT_TYPE)
  save_pub_table(freq_table, filename, caption = paste0("Component Universe: ", cat))
}

# --- 005. Business Model Summary ---
if("business_category" %in% names(combined_tanks)) {
  biz_counts <- combined_tanks[, .N, by = business_category][order(-N)]
  save_pub_table(biz_counts, "005_business_model_counts", "Facility Counts by Business Category")
}

# --- 006. Owner Fleet Size Frequency ---
if("Owner_Size_Class" %in% names(combined_tanks)) {
  os_table <- combined_tanks[, .(Facilities = uniqueN(FAC_ID), Tanks = .N), by = Owner_Size_Class][order(-Tanks)]
  os_table[, Pct_Tanks := sprintf("%.1f%%", Tanks/sum(Tanks)*100)]
  save_pub_table(os_table, "006_owner_size_class_freq", "Distribution by Owner Fleet Size")
}

# --- 007. Owner Sector Frequency (Top 25) ---
if("final_owner_sector" %in% names(combined_tanks)) {
  sec_table <- combined_tanks[, .(Facilities = uniqueN(FAC_ID), Tanks = .N), by = final_owner_sector][order(-Tanks)]
  save_pub_table(head(sec_table, 25), "007_owner_sector_freq", "Top 25 Owner Sectors by Tank Count")
}

# --- 008. Facility Operational Status ---
fs_table <- fac_summary[, .N, by = facility_status][order(-N)]
fs_table[, Pct := sprintf("%.1f%%", N/sum(N)*100)]
save_pub_table(fs_table, "008_facility_status_freq", "Facility Operational Status")

# --- 009. Cross-Tab: Owner Size vs Business Type ---
if("Owner_Size_Class" %in% names(combined_tanks)) {
  crosstab <- dcast(combined_tanks, Owner_Size_Class ~ business_category, value.var = "FAC_ID", fun.aggregate = uniqueN)
  save_pub_table(crosstab, "009_owner_size_vs_business_crosstab", "Facility Counts: Owner Size vs Business Model")
}

# --- 010. Closure Rates by Facility Type ---
if("business_category" %in% names(combined_tanks)) {
  closure_summary <- combined_tanks[, .(
    Total_Tanks = .N,
    Closed_Tanks = sum(is_closed, na.rm = TRUE),
    Active_Tanks = sum(is_closed == 0, na.rm = TRUE)
  ), by = business_category][order(-Total_Tanks)]
  closure_summary[, Closure_Rate := sprintf("%.1f%%", Closed_Tanks / Total_Tanks * 100)]
  save_pub_table(closure_summary, "010_closure_rates_by_facility_type", "Tank Closure Rates by Business Category")
}


# 6. OUTPUT GENERATION: FIGURES (ENHANCED)
# ==============================================================================

# --- A. Capacity Analytics (10k Major / 2k Minor) ---
cap_data <- combined_tanks[CAPACITY > 0 & CAPACITY <= 40000]
p_cap <- ggplot(cap_data, aes(x = CAPACITY)) +
  geom_histogram(binwidth = 1000, fill = "#3498db", color = "white", alpha = 0.8) +
  scale_x_continuous(breaks = seq(0, 40000, 10000), minor_breaks = seq(0, 40000, 2000), labels = comma) +
  labs(x = "Tank Capacity (Gallons)", y = "Number of Tanks")
save_pub_figure(p_cap, "005_capacity_distribution")

cap_time <- combined_tanks[!is.na(Decade) & CAPACITY > 0 & CAPACITY <= 40000]
p_cap_evol <- ggplot(cap_time, aes(x = CAPACITY, color = factor(Decade), fill = factor(Decade))) +
  geom_density(alpha = 0.1, linewidth = 1) +
  scale_x_continuous(breaks = seq(0, 40000, 10000), minor_breaks = seq(0, 40000, 2000), labels = comma) +
  scale_color_viridis_d() + scale_fill_viridis_d() +
  labs(x = "Capacity", y = "Density", color = "Decade", fill = "Decade")
save_pub_figure(p_cap_evol, "005b_capacity_evolution_by_decade")

# --- B. Fuel Mix Evolution ---
fuel_time_data <- combined_tanks[!is.na(Bin_5yr), .(Gasoline = sum(Gasoline, na.rm=T), Diesel = sum(Diesel, na.rm=T), Other = sum(Other_Substance, na.rm=T)), by = Bin_5yr]
fuel_long <- melt(fuel_time_data, id.vars = "Bin_5yr", variable.name = "Fuel", value.name = "Count")
p_fuel_evol <- ggplot(fuel_long, aes(x = Bin_5yr, y = Count, fill = Fuel)) +
  geom_col(position = "fill", width = 4) + scale_y_continuous(labels = percent) +
  scale_fill_manual(values = c("Gasoline"="#e74c3c", "Diesel"="#2c3e50", "Other"="#95a5a6")) +
  labs(x = "Installation Period", y = "Share", fill = NULL)
save_pub_figure(p_fuel_evol, "006_fuel_mix_evolution")

# --- C. Lifespan Analysis ---
closed_tanks <- combined_tanks[is_closed == 1 & !is.na(DATE_INSTALLED) & !is.na(Tank_Closed_Date) & year(DATE_INSTALLED) > 1950]
closed_tanks[, age_years := as.numeric(Tank_Closed_Date - DATE_INSTALLED) / 365.25]
closed_tanks <- closed_tanks[age_years > 0 & age_years < 60] 

p_life <- ggplot(closed_tanks, aes(x = age_years)) +
  geom_histogram(binwidth = 2, fill = "#8e44ad", color = "white", alpha = 0.8) +
  geom_vline(aes(xintercept = median(age_years)), linetype="dashed", linewidth=1) +
  labs(x = "Tank Age at Closure (Years)", y = "Number of Tanks")
save_pub_figure(p_life, "007_tank_lifespan_distribution")

# --- D. Facility Intelligence ---
p_fac_size_evol <- ggplot(fac_summary[!is.na(Vintage_Decade)], aes(x = n_tanks, color = factor(Vintage_Decade))) +
  geom_density(alpha = 0.1, linewidth = 1, adjust = 2) +
  scale_x_continuous(limits = c(0, 15), breaks = 1:15) +
  scale_color_viridis_d() +
  labs(x = "Tanks per Facility", y = "Density", color = "Vintage")
save_pub_figure(p_fac_size_evol, "008_facility_size_evolution_by_decade")

# 009: Facility Status by Vintage (STRICT HARDENING)
# 1. Ensure numeric/finite Decade and non-empty status
status_by_vintage <- fac_summary[
  is.finite(Vintage_Decade) & 
  !is.na(facility_status) & 
  facility_status != "", 
  .(N = .N), by = .(Vintage_Decade, facility_status)
]

# 2. Safety Check: Only attempt plot if rows exist
if(nrow(status_by_vintage) > 0) {
  p_fac_surv <- ggplot(status_by_vintage, aes(x = Vintage_Decade, y = N, fill = facility_status)) +
    geom_col(position = "fill", width = 8) + 
    scale_y_continuous(labels = percent) +
    scale_x_continuous(breaks = seq(1960, 2020, 10)) +
    scale_fill_manual(values = c(
      "Fully Active" = "#27ae60", 
      "Fully Closed" = "#c0392b", 
      "Mixed Status" = "#f39c12"
    )) +
    labs(x = "Facility Vintage", y = "Proportion", fill = "Status")

  save_pub_figure(p_fac_surv, "009_facility_status_by_vintage")
} else {
  message("Warning: Skipping Figure 009 - No valid data found after filtering.")
}

p_fac_scatter <- ggplot(fac_summary, aes(x = facility_age, y = n_tanks)) +
  geom_jitter(alpha = 0.1, color = "#2c3e50", height = 0.2) +
  geom_smooth(method = "gam", color = "#e74c3c", se = FALSE) +
  scale_x_continuous(breaks = seq(0, 60, 10)) + scale_y_continuous(limits = c(0, 20)) +
  labs(x = "Facility Age (Years)", y = "Facility Size")
save_pub_figure(p_fac_scatter, "010_facility_age_vs_size_scatter")

# --- E. Deep Fleet Analytics: Risk & Standardization ---
tank_constr <- components[COMPONENT_CATEGORY == "TANK CONSTRUCTION"]
tank_constr[, Risk_Tier := fcase(
  grepl("STEEL", COMPONENT_TYPE) & !grepl("DOUBLE|JACKETED|COMPOSITE", COMPONENT_TYPE), "High Risk (Bare/Single Steel)",
  grepl("FIBERGLASS", COMPONENT_TYPE) & !grepl("DOUBLE", COMPONENT_TYPE), "Medium Risk (Single Fiberglass)",
  grepl("DOUBLE", COMPONENT_TYPE) | grepl("JACKETED", COMPONENT_TYPE), "Low Risk (Double-Walled)",
  default = "Unknown/Other"
)]
risk_merged <- merge(combined_tanks, tank_constr[, .(FAC_ID, TANK_ID = as.integer(TANK_ID), Risk_Tier)], by = c("FAC_ID", "TANK_ID"))
risk_trend <- risk_merged[!is.na(Bin_5yr) & Bin_5yr >= 1980, .N, by = .(Bin_5yr, Risk_Tier)]

p_risk <- ggplot(risk_trend, aes(x = Bin_5yr, y = N, fill = Risk_Tier)) +
  geom_col(position = "fill", width = 4) + scale_y_continuous(labels = percent) +
  scale_fill_manual(values = c("High Risk (Bare/Single Steel)"="#c0392b", "Medium Risk (Single Fiberglass)"="#f39c12", "Low Risk (Double-Walled)"="#27ae60", "Unknown/Other"="#bdc3c7")) +
  labs(x = "Installation Period", y = "Share", fill = "Risk Tier")
save_pub_figure(p_risk, "011_fleet_risk_transition")

common_caps <- c(4000, 6000, 8000, 10000, 12000, 15000, 20000)
cap_std <- combined_tanks[CAPACITY %in% common_caps & install_year >= 1980]
p_std <- ggplot(cap_std, aes(x = install_year, color = factor(CAPACITY))) +
  geom_freqpoly(binwidth = 2, linewidth = 1) + scale_color_viridis_d(name = "Standard Capacity") +
  labs(x = "Installation Year", y = "Installations")
save_pub_figure(p_std, "013_capacity_standardization_trends")

# --- F. Business Model Visuals (Owner/Sector) ---
if("Owner_Size_Class" %in% names(combined_tanks)) {
  p_own <- ggplot(combined_tanks[!is.na(Owner_Size_Class)], aes(x = Owner_Size_Class)) +
    geom_bar(fill = "#34495e") + coord_flip() + labs(x = NULL, y = "Count")
  save_pub_figure(p_own, "014_owner_size_distribution")
  
  mp_sect <- combined_tanks[Owner_Size_Class == "Single-Site Owner (Mom & Pop)", .N, by = final_owner_sector][order(-N)]
  p_mp <- ggplot(head(mp_sect, 10), aes(x = reorder(final_owner_sector, N), y = N)) +
    geom_col(fill = "#e67e22") + coord_flip() + labs(x = NULL, y = "Count", title = "Mom & Pop Sectors")
  save_pub_figure(p_mp, "015_mom_pop_sector_breakdown")
  
  chain_data <- combined_tanks[grepl("Major Chain", final_owner_sector), .N, by = final_owner_sector][order(-N)]
  p_chains <- ggplot(chain_data, aes(x = reorder(final_owner_sector, N), y = N)) +
    geom_col(fill = "#27ae60") + coord_flip() + labs(x = NULL, y = "Count", title = "Major Chains Market Share")
  save_pub_figure(p_chains, "016_major_chains_market_share")
  
# FIXED: Added explicit filtering to prevent position="fill" crash
# Temporal Evolution of Owner Size
size_time <- combined_tanks[!is.na(Bin_5yr) & !is.na(Owner_Size_Class), .N, by = .(Bin_5yr, Owner_Size_Class)]
p_size_time <- ggplot(size_time, aes(x = Bin_5yr, y = N, fill = Owner_Size_Class)) +
  geom_col(position = "fill", width = 4) +
  scale_y_continuous(labels = percent) +
  scale_fill_viridis_d(option = "magma", begin = 0.2, end = 0.9) +
  labs(x = "Installation Period", y = "Share")
save_pub_figure(p_size_time, "017_owner_size_evolution")

# Temporal Evolution of Business Types
biz_time <- combined_tanks[!is.na(Bin_5yr) & !is.na(business_category), .N, by = .(Bin_5yr, business_category)]
p_biz_time <- ggplot(biz_time, aes(x = Bin_5yr, y = N, fill = business_category)) +
  geom_col(position = "fill", width = 4) +
  scale_y_continuous(labels = percent) +
  scale_fill_viridis_d(option = "turbo") +
  labs(x = "Installation Period", y = "Share")
save_pub_figure(p_biz_time, "018_business_category_evolution")



}

# --- G. Closure Trends by Facility Type ---
if("business_category" %in% names(combined_tanks) && "Tank_Closed_Date" %in% names(combined_tanks)) {
  closures_time <- combined_tanks[is_closed == 1 & !is.na(Tank_Closed_Date)]
  closures_time[, Closure_Year := year(Tank_Closed_Date)]
  closures_time <- closures_time[Closure_Year >= 1985 & Closure_Year <= 2025] 
  
  closure_trend <- closures_time[, .N, by = .(Closure_Year, business_category)]
  p_close_type <- ggplot(closure_trend, aes(x = Closure_Year, y = N, fill = business_category)) +
    geom_area(alpha = 0.8, size = 0.5, color = "white") +
    scale_fill_viridis_d(option = "turbo", name = "Facility Type") +
    scale_x_continuous(breaks = seq(1990, 2025, 5)) +
    labs(x = "Year of Closure", y = "Number of Tanks Closed", title = "Tank Closures by Facility Type")
  save_pub_figure(p_close_type, "019_tank_closures_by_facility_type_timeline")
}

# 7. SAVE FINAL DATA OBJECT
# ==============================================================================
ds_stats <- list(
  status = status_freq,
  substance = sub_counts,
  date_fillers = top_dates,
  comp_universe = comp_universe_list,
  fac_summary = fac_summary,
  life_stats = summary(closed_tanks$age_years),
  risk_trend = risk_trend,
  biz_counts = if(exists("biz_counts")) biz_counts else NULL
)

saveRDS(ds_stats, "data/processed/raw_data_descriptives.rds")
message("\nProcessing Complete.")
message(sprintf("Tables saved to: %s/", out_dir_tables))
message(sprintf("Figures saved to: %s/", out_dir_figs))