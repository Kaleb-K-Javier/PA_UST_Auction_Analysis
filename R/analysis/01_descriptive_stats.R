# R/analysis/01_descriptive_stats.R
# ==============================================================================
# Pennsylvania UST Analysis - Descriptive Statistics
# ==============================================================================
# PURPOSE: Characterize the claim universe and market structure
#
# SECTIONS (from 1/16 Policy Brief Goals):
#   1) BROAD CLAIMS ANALYSIS
#      1.1) Correlates of Eligibility (Denied vs Withdrawn vs Eligible)
#      1.2) Denied Claims and ALAE ("Phantom Spend")
#      1.3) Closed Claims: Cost Drivers, Closing Time, Reporting Lag
#      1.4) Remediation Trends (Costs & Durations Over Time)
#      1.5) Spatial/Temporal Visualization (Maps, Time Series)
#
#   3) MARKET CONCENTRATION
#      3.1) Consultant Concentration (HHI by Region)
#      3.2) Adjuster & Auction Concentration
#
# OUTPUTS:
#   - Tables: output/tables/1XX_*.{html,tex}
#   - Figures: output/figures/1XX_*.{png,pdf}
# ==============================================================================


# # Claim Management (Administrative Profile)
#    has_fixed_price + has_tm + has_pfp_contract + has_facility_match ---are not about facility this describes contracts.
# dont put in faciliyt stuff
# recent closure is within 90 of replred date
suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(modelsummary)
  library(kableExtra)
  library(janitor)
  library(stringr)
  library(ggplot2)
  library(scales)
  library(patchwork)
  library(here)
  # NEW: Visualize Non-Linear Logic with a Surrogate Tree
# "What rules best approximate the Random Forest's predictions?"
library(rpart)
library(rpart.plot)
})


source(here("R/functions/style_guide.R"))
source(here("R/functions/save_utils.R"))

# ==============================================================================
# 0. SETUP
# ==============================================================================
paths <- list(
  master   = here("data/processed/master_analysis_dataset.rds"),
  contracts = here("data/processed/contracts_with_real_values.rds"),
  tables   = here("output/tables"),
  figures  = here("output/figures")
)

# Standard directory creation is handled by save_utils, but keeping this is safe
dir.create(paths$tables, recursive = TRUE, showWarnings = FALSE)
dir.create(paths$figures, recursive = TRUE, showWarnings = FALSE)

# REMOVED: Local save_table() and save_figure() definitions
# Using functions from R/functions/save_utils.R instead
source(here('R\\functions\\save_utils.R'))

# ==============================================================================
# 1. LOAD DATA
# ==============================================================================
message("\n--- Loading Data ---")

master <- readRDS(paths$master)
setDT(master)

contracts <- readRDS(paths$contracts)
setDT(contracts)



# Merge region to contracts for HHI
contracts <- merge(contracts, 
                   master[, .(claim_number, dep_region, county)], 
                   by = "claim_number", all.x = TRUE)

message(sprintf("Master: %d claims | Contracts: %d records", nrow(master), nrow(contracts)))



# ==============================================================================
# 1. FACILITY CHARACTERISTICS (X-Variables: Physical & Owner Traits)
# Source: 03_merge_master_dataset.R (Sections 3, 4, 6, 7)
# ==============================================================================
facility_vars <- c(
  # --- Census & Scale ---
  "n_tanks_total",            # Historical total tanks (Census baseline)
  "n_tanks_active",           # Count of tanks active at claim date
  "facility_age",             # Age of oldest tank ever installed
  "avg_tank_age",             # Average age of active tanks at claim date
  "region_cluster",           # Geographic grouping (Southeast, Northwest, etc.)

  # --- Capacity Metrics (v8.0) ---
  "total_capacity_gal",       # Sum of capacity (active tanks)
  "avg_tank_capacity_gal",    # Average tank capacity
  "max_tank_capacity_gal",    # Size of largest tank on site

  # --- Owner Profile ---
  "business_category",        # Broad sector (Retail Gas vs. Public vs. Industrial)
  "final_owner_sector",       # Granular owner type (e.g., "Major Chain (Sheetz)")
  "Owner_Size_Class",         # Categorical size (Single-Site vs. Corp Fleet)
  "facility_count",           # Number of sites owned (Sophistication proxy)

  # --- Physical Risk Flags (Binary: Did ANY active tank have this?) ---
  "has_bare_steel",           # 1 if any active tank is Bare Steel
  "has_single_walled",        # 1 if any active tank/piping is Single Walled
  "has_secondary_containment",# 1 if secondary containment exists
  "has_pressure_piping",      # 1 if pressurized piping exists
  "has_suction_piping",       # 1 if suction piping exists
  "has_galvanized_piping",    # 1 if galvanized piping exists
  "has_electronic_atg",       # 1 if Electronic Automatic Tank Gauging used
  "has_manual_detection",     # 1 if Manual/Stick/Visual detection used
  "has_overfill_alarm",       # 1 if overfill alarm present
  "has_gasoline",             # 1 if substance is Gasoline
  "has_diesel",               # 1 if substance is Diesel
  "has_noncompliant",         # 1 if any active tank is flagged Non-Compliant

  # --- Risk Proportions (Continuous: % of active tanks) ---
  "share_bare_steel",         # % Bare Steel
  "share_single_wall",        # % Single Walled
  "share_pressure_piping",    # % Pressure Piping
  "share_noncompliant",       # % Non-Compliant
  "share_data_unknown",       # % General Unknown Data
  "share_tank_construction_unknown",      # % Unknown Tank Construction
  "share_ug_piping_unknown",              # % Unknown Piping Material
  "share_tank_release_detection_unknown", # % Unknown Detection Method

  # --- Historical Churn/Closure Activity ---
  "has_recent_closure",       # 1 if tank closed <90 days before claim
  "n_closed_0_30d",           # Count closed 0-30 days prior
  "n_closed_31_90d",          # Count closed 31-90 days prior
  "n_closed_1yr",             # Count closed within 1 year prior
  "n_closed_total"            # Cumulative historical closures
)

# ==============================================================================
# 2. CLAIM CHARACTERISTICS (X-Variables: Event Specifics & History)
# Source: 03_merge_master_dataset.R (Section 5)
# ==============================================================================
claim_vars <- c(
  # --- Reporting Behavior ---
  "reporting_lag_days",       # Time from Loss_Date to Report_Date (Speed of reporting)
  "Year",                     # Calendar year of claim (Trend control)
  
  # --- Filer History (Behavioral Risk) ---
  "is_repeat_filer",          # 1 if this facility has filed claims before
  "prior_claims_n",           # Exact count of previous claims
  "days_since_prior_claim",   # Recency: Days since the last claim was filed
  "avg_prior_claim_cost",     # Severity History: Average cost of past claims
  
  # --- Data Quality/Legacy ---
  "has_legacy_grandfathered", # 1 if tanks are pre-1988/Grandfathered
  "has_unknown_data"          # 1 if key attributes are missing (Binary Flag)
)

# ==============================================================================
# 3. OUTCOME VARIABLES (Y-Variables: Costs, Durations, Decisions)
# Source: 01_descriptive_stats.R & 02_cost_correlates.R
# ==============================================================================
outcome_vars <- c(
  # --- Financial Outcomes (Inflation Adjusted) ---
  "total_paid_real",          # Total Claim Cost (Indemnity + Expense) in 2025$
  "paid_loss_real",           # Indemnity/Remediation portion
  "paid_alae_real",           # Legal/Adjuster expense portion
  "total_contract_value_real",# Value of specific contracts (if auctioned)
  "log_cost",                 # Log(total_paid_real + 1) for regressions
  
  # --- Operational Outcomes ---
  "claim_duration_days",      # Days from Claim_Date to Closed_Date
  "intervention_lag_days",    # Days from Claim to First Contract (Auction timing)
  
  # --- Binary/Classification Outcomes ---
  "status_group",             # "Denied", "Eligible", "Withdrawn"
  "went_to_auction",          # 1 if claim had a competitive bid (PFP/SOW)
  "is_high_cost",             # 1 if total_paid > 90th percentile
  "is_long_duration"          # 1 if duration > 90th percentile
)


# ==============================================================================
# SECTION 1.1: CORRELATES OF ELIGIBILITY
# ==============================================================================
message("\n--- 1.1 Correlates of Eligibility ---")

# 1.1a Define Analysis Variables & Nice Labels
# ------------------------------------------------------------------------------
# Map ETL variable names (left) to Publication Labels (right)
var_map <- c(
  # Facility / Physical
  "n_tanks_total"       = "Total Tanks on Site",
  "avg_tank_age"        = "Average Tank Age (Years)",
  "paid_alae_real"      = "ALAE Costs (2025$)",        # Added base year
  "reporting_lag_days"  = "Reporting Delay (Days)",    # Fixed: "Duration" = open time; "Lag" = delay
  
  # Regulatory / Human Flags (Checklist Items)
  "has_single_walled"   = "Risk: Single-Walled System",
  "has_unknown_data"    = "Risk: Missing/Unknown Tank Data",
  "is_repeat_filer"     = "Operator History: Repeat Filer",
  "prior_claims_n"      = "Operator History: Prior Claim Count" # Restored variable, fixed label
)

# 1.1b Prepare Data Summary
# ------------------------------------------------------------------------------
# Filter Groups
target_groups <- c("Denied", "Eligible/Active")
master[, eligibility_group := fcase(
  status_group == "Denied", "Denied",
  status_group %in% c("Eligible", "Post Remedial", "Open"), "Eligible/Active",
  default = NA_character_
)]

# Summarize Means
risk_summary <- master[!is.na(eligibility_group), 
                       lapply(.SD, mean, na.rm = TRUE), 
                       by = eligibility_group, 
                       .SDcols = names(var_map)]

# Reshape & Format
risk_long <- melt(risk_summary, id.vars = "eligibility_group", 
                  variable.name = "var_code", value.name = "mean_val")

# Attach Readable Labels
risk_long[, Metric := var_map[as.character(var_code)]]

# Pivot Wider for Table
risk_wide <- dcast(risk_long, Metric ~ eligibility_group, value.var = "mean_val")

# Calculate % Difference
risk_wide[, Diff_Pct := (`Denied` - `Eligible/Active`) / `Eligible/Active`]

# 1.1c Generate Publication Table (gt)
# ------------------------------------------------------------------------------
library(gt)

human_risk_tbl <- risk_wide %>%
  gt() %>%
  cols_label(
    Metric = "Characteristic",
    Diff_Pct = "% Difference"
  ) %>%
  # Number Formatting
  fmt_number(
    columns = c("Denied", "Eligible/Active"),
    rows = !grepl("\\$", Metric), # Non-currency rows
    decimals = 2
  ) %>%
  fmt_currency(
    columns = c("Denied", "Eligible/Active"),
    rows = grepl("\\$", Metric), # Currency rows
    decimals = 0
  ) %>%
  fmt_percent(
    columns = "Diff_Pct",
    decimals = 1
  ) %>%
  # Styling
  tab_header(
    title = "Profile of Denied vs. Eligible Claims",
    subtitle = "Comparison of key facility risks and cost indicators"
  ) %>%
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_labels()
  ) %>%
  # Highlight large differences
  tab_style(
    style = list(cell_text(color = "#e74c3c", weight = "bold")),
    locations = cells_body(
      columns = Diff_Pct,
      rows = abs(Diff_Pct) > 0.5
    )
  ) %>%
  tab_source_note(
    source_note = "Source: PA USTIF Claims Database (1994-2025)"
  )

# 1.1d Save Outputs
# ------------------------------------------------------------------------------
# 1. Save HTML for quick viewing
save_table((human_risk_tbl), "101_eligibility_profile")
human_risk_tbl
# 2. Save RDS for QMD programmatic loading (Recommended for Policy Brief)
saveRDS(human_risk_tbl, here("output/tables/101_eligibility_profile.rds"))
message("Saved 101_eligibility_profile.rds for QMD integration.")


# -------------------------------------------------------------------------
# 1.1b Formal Balance Test (T-Tests)
# -------------------------------------------------------------------------
# Ensure analysis_vars comes from the map defined in 1.1a
analysis_vars <- names(var_map)

bal_data <- master[eligibility_group %in% c("Denied", "Eligible/Active"), 
                   c("eligibility_group", analysis_vars), with = FALSE]

balance_results <- lapply(analysis_vars, function(v) {
  denied <- bal_data[eligibility_group == "Denied", get(v)]
  eligible <- bal_data[eligibility_group == "Eligible/Active", get(v)]
  
  # Safety checks for NAs or zero variance
  if(all(is.na(denied)) || all(is.na(eligible))) return(NULL)
  if(uniqueN(na.omit(denied)) < 2 && uniqueN(na.omit(eligible)) < 2) return(NULL)
  
  tryCatch({
    tt <- t.test(denied, eligible)
    
    data.table(
      Variable = v,
      Metric = var_map[v], # Use the readable label
      Mean_Denied = mean(denied, na.rm = TRUE),
      Mean_Eligible = mean(eligible, na.rm = TRUE),
      Difference = mean(denied, na.rm = TRUE) - mean(eligible, na.rm = TRUE),
      P_Value = tt$p.value
    )
  }, error = function(e) return(NULL))
})

balance_dt <- rbindlist(balance_results)

# Generate GT Table
bal_tbl <- balance_dt[, .(Metric, Mean_Denied, Mean_Eligible, Difference, P_Value)] %>%
  gt() %>%
  cols_label(
    Metric = "Characteristic",
    Mean_Denied = "Denied (Mean)",
    Mean_Eligible = "Eligible (Mean)",
    P_Value = "P-Value"
  ) %>%
  # Format numeric rows
  fmt_number(
    columns = c("Mean_Denied", "Mean_Eligible", "Difference"),
    rows = !grepl("\\$", Metric),
    decimals = 2
  ) %>%
  # Format currency rows
  fmt_currency(
    columns = c("Mean_Denied", "Mean_Eligible", "Difference"),
    rows = grepl("\\$", Metric),
    decimals = 0
  ) %>%
  # Format P-Values
  fmt_number(
    columns = "P_Value",
    decimals = 4
  ) %>%
  # Highlight Significant Differences
  tab_style(
    style = list(cell_text(color = "#e74c3c", weight = "bold")),
    locations = cells_body(
      columns = P_Value,
      rows = P_Value < 0.05
    )
  ) %>%
  tab_header(
    title = "Statistical Balance Test: Denied vs. Eligible",
    subtitle = "T-Test comparing facility characteristics and risk flags"
  ) %>%
  tab_source_note("Source: PA USTIF Claims Database")
bal_tbl
# Save Outputs
save_table((bal_tbl), "102_eligibility_balance_test")
saveRDS(bal_tbl, here("output/tables/102_eligibility_balance_test.rds"))
# ==============================================================================
# 1.1e Descriptive Regression: Drivers of Denial (Linear Probability Model)
# ==============================================================================
message("\n--- 1.1e Modeling Denial Probability (LPM) ---")

# Define Outcome: 1 = Denied, 0 = Eligible/Active
denial_data <- master[eligibility_group %in% c("Denied", "Eligible/Active")]
denial_data[, is_denied := as.integer(eligibility_group == "Denied")]

# Comprehensive Formula (Human + Physical Flags)
f_string <- paste(
  "is_denied ~",
  "avg_tank_age + n_tanks_total + business_category +",
  # --- Physical Risks ---
  "has_bare_steel + has_single_walled + has_secondary_containment +",
  "has_pressure_piping + has_suction_piping + has_galvanized_piping +",
  "has_electronic_atg + has_manual_detection + has_overfill_alarm +",
  "has_gasoline + has_diesel +",
  
  # --- Human/Behavioral Risks ---
  "I(reporting_lag_days/30) + is_repeat_filer + has_legacy_grandfathered + has_unknown_data +",
  "has_recent_closure",
  
  # --- Fixed Effects ---
  "| region_cluster + claim_year"
)

f_denial <- as.formula(f_string)

# Run LPM (Linear Probability Model)
denial_model <- feols(f_denial, data = denial_data, vcov = 'hetero')

# Output Table
denial_tbl <- modelsummary(
  list("Pr(Denied)" = denial_model),
  stars = c('*' = .1, '**' = .05, '***' = .01),
  gof_map = c("nobs", "r.squared.adj", "fe_region_cluster", "fe_claim_year"),
  output = "gt"
) %>%
  tab_header(
    title = "Correlates of Claim Denial", 
    subtitle = "Linear Probability Model (LPM) on Physical & Behavioral Flags"
  ) %>%
  tab_source_note("Controls: Region & Year FE.")

save_table(as_raw_html(denial_tbl), "103_denial_drivers_regression")
saveRDS(denial_tbl, here("output/tables/103_denial_drivers_regression.rds"))


# ==============================================================================
# 1.1f ML Discovery: Denial Rules (Fixed Ordering & Duplicates)
# ==============================================================================
message("\n--- 1.1f ML Discovery: Denial Rules ---")
library(stringr)
library(rpart)
library(rpart.plot)

# 1. Load Dictionary
feature_dict <- readRDS("data/processed/ml_feature_dictionary.rds")
setDT(feature_dict)

# 2. Define Features & Matrix
ml_features <- c(
  "avg_tank_age", "n_tanks_total", "is_repeat_filer",
  "reporting_lag_days", 
  grep("^bin_", names(denial_data), value = TRUE),
  grep("^qty_", names(denial_data), value = TRUE)
)

ml_df <- denial_data[complete.cases(denial_data[, ..ml_features])]
X_denial <- model.matrix(as.formula(paste("~", paste(ml_features, collapse="+"), "-1")), data = ml_df)
Y_denial <- as.factor(ml_df$is_denied)

# 3. Train Probability Forest
set.seed(999)
pf_denial <- grf::probability_forest(
  X = X_denial, 
  Y = Y_denial, 
  num.trees = 2000,
  tune.parameters = "all",
  seed = 999
)

# 4. Variable Importance (Sorted & Deduplicated)
# ------------------------------------------------------------------------------
imp_denial <- grf::variable_importance(pf_denial)
dt_imp_denial <- data.table(Feature = colnames(X_denial), Importance = imp_denial[,1])

# Match Labels
dt_imp_denial[, Match_Key := gsub("^(bin_|qty_)", "", Feature)]
dt_imp_denial <- merge(dt_imp_denial, feature_dict[, .(feature_name, clean_label)], 
                       by.x = "Match_Key", by.y = "feature_name", all.x = TRUE)

dt_imp_denial[, Display_Label := fcase(
  !is.na(clean_label), clean_label,
  Feature == "avg_tank_age", "Average Tank Age (Years)",
  Feature == "reporting_lag_days", "Reporting Delay (Days)",
  Feature == "is_repeat_filer", "Repeat Filer History",
  Feature == "n_tanks_total", "Total Tanks at Facility",
  default = Feature
)]

# --- FIX 1: DEDUPLICATE LABELS ---
# Detects if multiple codes map to the same text (e.g. "Other") and appends code to make unique
dt_imp_denial[, label_count := .N, by = Display_Label]
dt_imp_denial[label_count > 1, Display_Label := paste0(Display_Label, " (", sub(".*_", "", Feature), ")")]
dt_imp_denial[, label_count := NULL]

# --- FIX 2: FORCE SORTING IN PLOT ---
# Filter Top 15 -> Reorder Factor by Importance
top_features <- tail(dt_imp_denial[order(Importance)], 15)

p_ml_denial <- ggplot(top_features, aes(x = reorder(Display_Label, Importance), y = Importance)) +
  geom_col(fill = "#c0392b", alpha = 0.85, width = 0.7) + 
  coord_flip() +
  labs(
    x = NULL, 
    y = "Predictive Importance (Probability Forest)"
  ) +
  theme_minimal() +
  theme(
    axis.text.y = element_text(size = 10, color = "black"),
    panel.grid.major.y = element_blank()
  )

save_figure(p_ml_denial, "104_ml_denial_importance")

# 5. Policy Tree (Modern Style)
# ------------------------------------------------------------------------------
# Add predictions
ml_df[, prob_denied := predict(pf_denial)$predictions[, 2]]
tree_data <- ml_df[, c("prob_denied", ml_features), with = FALSE]

# Rename Columns using the UNIQUE Display_Labels
name_map <- setNames(dt_imp_denial$Display_Label, dt_imp_denial$Feature)
cols_to_rename <- intersect(names(tree_data), names(name_map))
setnames(tree_data, old = cols_to_rename, new = name_map[cols_to_rename])

# Fit Tree
tree_denial <- rpart(
  prob_denied ~ ., 
  data = tree_data,
  method = "anova",
  control = rpart.control(maxdepth = 3, cp = 0.01)
)

# Plot
png(here("output/figures/104_denial_policy_tree.png"), width = 1400, height = 900, res = 150)

rpart.plot(
  tree_denial,
  type = 4,
  extra = 101,
  under = TRUE,
  box.palette = "OrRd",
  shadow.col = NULL,
  border.col = "gray80",
  branch.lty = 1,
  branch.col = "gray60",
  nn = TRUE,
  varlen = 0,
  faclen = 0,
  roundint = FALSE,
  cex = 0.8,
  main = NULL,
  sub = "How to read: Top Value = Predicted Denial Probability | Bottom Value = % of Claims in this Group"
)

dev.off()
message("Saved 104_denial_policy_tree.png (Fixed Labels)")


# ==============================================================================
# SECTION 1.2: DENIED CLAIMS AND ALAE ("Phantom Spend")
# ==============================================================================
message("\n--- 1.2 Denied Claims and ALAE Analysis ---")

# Filter to Denied Claims
denied_claims <- master[status_group == "Denied"]

# -------------------------------------------------------------------------
# 1.2a Descriptive: "Phantom Spend" by Business Sector
# -------------------------------------------------------------------------
phantom_sector <- denied_claims[!is.na(business_category), .(
  N_Claims = .N,
  Total_ALAE = sum(paid_alae_real, na.rm = TRUE),
  Mean_ALAE  = mean(paid_alae_real, na.rm = TRUE),
  Max_ALAE   = max(paid_alae_real, na.rm = TRUE)
), by = business_category]

total_phantom_alae <- sum(phantom_sector$Total_ALAE)
phantom_sector[, Share_Pct := Total_ALAE / total_phantom_alae]
setorder(phantom_sector, -Total_ALAE)

# GT Table
phantom_tbl <- phantom_sector %>%
  gt() %>%
  cols_label(
    business_category = "Business Sector",
    N_Claims = "Denied Claims",
    Total_ALAE = "Total Legal Costs",
    Mean_ALAE = "Avg Cost/Claim",
    Share_Pct = "% of Total Spend"
  ) %>%
  fmt_number(columns = "N_Claims", decimals = 0) %>%
  fmt_currency(columns = c("Total_ALAE", "Mean_ALAE", "Max_ALAE"), decimals = 0) %>%
  fmt_percent(columns = "Share_Pct", decimals = 1) %>%
  tab_header(
    title = "Phantom Spend: Legal Costs on Denied Claims",
    subtitle = sprintf("Total Expenditures: $%s across %d denied claims", 
                       format(total_phantom_alae, big.mark = ",", nsmall=0),
                       nrow(denied_claims))
  ) %>%
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_labels()
  ) %>%
  data_color(columns = "Share_Pct", palette = "Reds") %>%
  tab_source_note("Source: PA USTIF Claims Database (Denied Claims Only)")


phantom_tbl

save_table((phantom_tbl), "103_phantom_spend_by_sector")
saveRDS(phantom_tbl, here("output/tables/103_phantom_spend_by_sector.rds"))

# -------------------------------------------------------------------------
# 1.2b Figure: ALAE vs. Reporting Lag
# -------------------------------------------------------------------------
plot_data_scatter <- master[paid_alae_real > 0 & reporting_lag_days > 0 & reporting_lag_days < 1825]

p_alae_lag <- ggplot(plot_data_scatter, aes(x = reporting_lag_days, y = paid_alae_real)) +
  geom_point(alpha = 0.4, color = "#2c3e50") +
  geom_smooth(method = "gam", color = "#e74c3c", fill = "#e74c3c", alpha = 0.2) +
  scale_y_continuous(labels = dollar_format()) +
  scale_x_continuous(breaks = seq(0, 1800, 365), labels = 0:4) +
  labs(x = "Reporting Delay (Years)", y = "Legal & Adjustment Costs ($)") +
  theme_minimal() 

save_figure(p_alae_lag, "104_alae_vs_reporting_lag")

# ==============================================================================
# SECTION 1.2c: REGRESSION ANALYSIS (ALAE COST DRIVERS)
# ==============================================================================
message("\n--- 1.2c Regression Analysis: Drivers of Phantom Spend ---")

library(fixest)
library(modelsummary)
library(gt)

# 1. Prepare Data: Filter and Fill NAs
# ------------------------------------------------------------------------------
# Filter to positive ALAE
reg_data <- denied_claims[paid_alae_real > 0]


# List of all risk flags to ensure they are clean (NA -> 0)
risk_flags <- c("has_bare_steel", "has_single_walled", "has_secondary_containment",
                "has_pressure_piping", "has_suction_piping", "has_galvanized_piping",
                "has_electronic_atg", "has_manual_detection", "has_overfill_alarm",
                "has_gasoline", "has_diesel", "has_noncompliant",
                "has_legacy_grandfathered", "has_unknown_data", "has_recent_closure")

# Loop to replace NAs with 0 (FALSE)
for (col in risk_flags) {
  if (col %in% names(reg_data)) {
    set(reg_data, which(is.na(reg_data[[col]])), col, 0)
  }
}

# 2. Define Formula
# ------------------------------------------------------------------------------
f_string <- paste(
  "log(paid_alae_real) ~",
  # --- Physical Risks ---
  "has_bare_steel + has_single_walled + has_secondary_containment +",
  "has_pressure_piping + has_suction_piping + has_galvanized_piping +",
  "has_electronic_atg + has_manual_detection + has_overfill_alarm +",
  "has_gasoline + has_diesel +",
  
  # --- Human/Behavioral Risks ---
  "reporting_lag_days + is_repeat_filer +",
  "has_noncompliant + has_legacy_grandfathered + has_unknown_data +",
  "has_recent_closure",
  
  # --- Fixed Effects ---
  "| region_cluster + claim_year"
)

f_human <- as.formula(f_string)

# 3. Run Model (Robust Standard Errors)
# ------------------------------------------------------------------------------
# Switched from cluster="region_cluster" to vcov="hetero" to avoid semi-definite warning
alae_human_model <- feols(f_human, data = reg_data, vcov = "hetero")

message("Regression Complete. Variables kept in model:")
print(names(coef(alae_human_model)))

# 4. Output Table (GT)
# ------------------------------------------------------------------------------
human_tbl <- modelsummary(
  list("Log(ALAE Cost)" = alae_human_model),
  coef_map = c(
    # Human/Behavioral
    "reporting_lag_days" = "Reporting Delay (Days)",
    "is_repeat_filerTRUE" = "Repeat Filer",
    "has_recent_closureTRUE" = "Tank Pull (Recent Closure)",
    "has_noncompliant" = "Flag: Non-Compliant",
    "has_unknown_data" = "Flag: Unknown Data",
    
    # Physical/Tank
    "has_single_walled" = "Flag: Single-Walled",
    "has_bare_steel" = "Flag: Bare Steel",
    "has_secondary_containment" = "Flag: Secondary Containment",
    "has_gasoline" = "Substance: Gasoline"
  ),
  stars = c('*' = .1, '**' = .05, '***' = .01),
  gof_map = c("nobs", "r.squared.adj", "fe_region_cluster", "fe_claim_year"),
  output = "gt"
) %>%
  tab_header(
    title = "Drivers of Legal Costs (ALAE)", 
    subtitle = "Regression on Risk Flags (Denied Claims Only)"
  ) %>%
  tab_source_note("Controls: Region & Year FE. Standard Errors: Heteroskedasticity-robust (White).")

# Save Outputs
save_table((human_tbl), "105_alae_human_drivers")
saveRDS(human_tbl, here("output/tables/105_alae_human_drivers.rds"))


# -------------------------------------------------------------------------
# 1.2d FWL Partial Regression Plot (Reporting Lag)
# -------------------------------------------------------------------------
# Visualize the unique effect of delay after controlling for all human flags
p_fwl <- modelplot(alae_human_model, coef_map = c("reporting_lag_days" = "Reporting Delay")) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
  labs(
    x = "Effect on Log(ALAE) (95% CI)", 
    y = NULL,
    caption = "Coefficient estimate controlling for all regulatory flags + Region/Year FE"
  ) +
  theme_minimal()

save_figure(p_fwl, "106_alae_fwl_coefplot")
# -------------------------------------------------------------------------
# 1.2e ML Discovery: Data-Driven Drivers (GRF)
# -------------------------------------------------------------------------
# Goal: Do granular signals (bin_/qty_) predict ALAE better than human flags?
library(stringr) # Ensure stringr is loaded for label wrapping

feature_dict <- readRDS("data/processed/ml_feature_dictionary.rds")
setDT(feature_dict)

message("Running ML ALAE Driver Discovery (Regression Forest)...")

# 1. Define Feature Set & Matrix
# -------------------------------------------------------------------------
# Use all granular signals + lag/history
ml_features <- c(
  "reporting_lag_days", "is_repeat_filer",
  grep("^bin_", names(reg_data), value = TRUE),
  grep("^qty_", names(reg_data), value = TRUE)
)

# Filter complete cases first
ml_df <- reg_data[complete.cases(reg_data[, ..ml_features])]

# Create Model Matrix (Handle factors/interactions if any, usually identity for binary)
X_ml <- model.matrix(as.formula(paste("~", paste(ml_features, collapse="+"), "-1")), data = ml_df)
Y_ml <- log(ml_df$paid_alae_real)

# 2. Train Forest (Optimized for Inference)
# -------------------------------------------------------------------------
# GRF Best Practices:
# - tune.parameters = "all": Cross-validates mtry, min.node.size, sample.fraction, etc.
# - num.trees = 2000: Recommended minimum for stable variable importance & CIs
set.seed(789)
rf_alae <- grf::regression_forest(
  X = X_ml, 
  Y = Y_ml, 
  num.trees = 2000, 
  tune.parameters = "all", 
  seed = 789
)

# 3. Variable Importance (Human-Readable)
# -------------------------------------------------------------------------
imp_alae <- grf::variable_importance(rf_alae)
dt_imp <- data.table(Feature = colnames(X_ml), Importance = imp_alae[,1])

# -- LABEL CLEANING LOGIC --
# 1. Extract the core key (remove bin_/qty_ prefix) to match Dictionary
dt_imp[, Match_Key := gsub("^(bin_|qty_)", "", Feature)]

# 2. Merge with Dictionary
dt_imp <- merge(dt_imp, feature_dict[, .(feature_name, original_description, category_label)], 
                by.x = "Match_Key", by.y = "feature_name", all.x = TRUE)

# 3. Define Clean Labels (Mapped vs. Manual)
dt_imp[, Clean_Label := fcase(
  !is.na(original_description), paste0(original_description, " (", category_label, ")"),
  Feature == "reporting_lag_days", "Reporting Delay (Days)",
  Feature == "is_repeat_filer", "Prior Claims History",
  default = Feature # Fallback
)]

# 4. Cleanup: Remove codes like "1E - " for cleaner plots
dt_imp[, Clean_Label := gsub("^[A-Z0-9]+ - ", "", Clean_Label)]

# -- PLOT --
# Note: Titles removed for Quarto compatibility (captions used instead)
p_ml_alae <- ggplot(head(dt_imp[order(-Importance)], 15), 
                    aes(x = reorder(str_wrap(Clean_Label, 40), Importance), y = Importance)) +
  geom_col(fill = "#2c3e50", alpha = 0.9, width = 0.75) + # Professional Slate Blue
  coord_flip() +
  labs(
    x = NULL, 
    y = "Relative Predictive Importance"
  ) +
  theme_minimal() +
  theme(
    axis.text.y = element_text(size = 10, color = "black"), # Readable labels
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank()
  )

save_figure(p_ml_alae, "107_ml_alae_drivers_importance")

# 4. Surrogate Tree (Flowchart with Readable Splits)
# -------------------------------------------------------------------------
# Add predictions to data
ml_df[, rf_pred := predict(rf_alae)$predictions]

# Prepare data subset for the Tree
tree_data <- ml_df[, c("rf_pred", ml_features), with = FALSE]

# -- RENAME COLUMNS FOR PLOT --
# Create mapping vector: Technical Name -> Clean Label
# (We filter dt_imp to only rows that exist in ml_features to be safe)
name_map <- setNames(dt_imp$Clean_Label, dt_imp$Feature)

# Identify columns in tree_data that have a mapping
cols_to_rename <- intersect(names(tree_data), names(name_map))
new_names <- name_map[cols_to_rename]

# Apply renaming
setnames(tree_data, old = cols_to_rename, new = new_names)

# Fit Tree
tree_surrogate <- rpart(
  rf_pred ~ ., 
  data = tree_data,
  method = "anova",
  control = rpart.control(maxdepth = 3, cp = 0.01)
)

# Save Plot
png(here("output/figures/107_alae_policy_tree.png"), width = 1400, height = 900, res = 150)
rpart.plot(
  tree_surrogate,
  type = 4, 
  extra = 101, 
  under = TRUE,
  box.palette = "Blues",
  branch.lty = 3,
  shadow.col = "gray",
  nn = TRUE,
  varlen = 0, # Use full variable names (our Clean Labels)
  faclen = 0,
  main = "Pathways to High Legal Costs (Surrogate Model)"
)
dev.off()

message("Saved 107_alae_policy_tree.png")


# ==============================================================================
# SECTION 1.3: CLOSED CLAIMS - Cost Drivers
# ==============================================================================
message("\n--- 1.3 Closed Claims Analysis ---")

# Filter to closed, eligible claims with real costs
closed_claims <- master[status_group %in% c("Eligible", "Post Remedial") & 
                          total_paid_real > 1000]

# Cost distribution
p_cost_dist <- ggplot(closed_claims, aes(x = total_paid_real)) +
  geom_histogram(bins = 50, fill = "#2c3e50", alpha = 0.9) +
  scale_x_log10(labels = dollar_format()) +
  labs(title = "Total Claim Cost Distribution (Closed Eligible)",
       subtitle = sprintf("N = %s claims", format(nrow(closed_claims), big.mark = ",")),
       x = "Total Cost (Real $, Log Scale)", y = "Count")

save_figure(p_cost_dist, "104_cost_distribution")

# Cost drivers regression
cost_reg <- feols(
  log(total_paid_real) ~ avg_tank_age + n_tanks_total + 
    share_bare_steel + share_pressure_piping + 
    business_category | dep_region,
  data = closed_claims[!is.na(avg_tank_age)],
  cluster = "region_cluster"
)

coef_map <- c(
  "avg_tank_age" = "Tank Age (Years)",
  "n_tanks_total" = "N Tanks at Facility",
  "share_bare_steel" = "Share: Bare Steel Tanks",
  "share_pressure_piping" = "Share: Pressure Piping"
)

cost_tbl <- modelsummary(
  list("Log(Total Cost)" = cost_reg),
  coef_map = coef_map,
  stars = c('*' = .1, '**' = .05, '***' = .01),
  gof_map = c("nobs", "r.squared"),
  output = "kableExtra"
) %>% kable_styling()

save_table(cost_tbl, "105_cost_drivers_regression")

# Duration analysis
p_duration <- ggplot(closed_claims[claim_duration_days > 0 & claim_duration_days < 10000], 
                     aes(x = claim_duration_days / 365)) +
  geom_histogram(bins = 40, fill = "#27ae60", alpha = 0.8) +
  labs(title = "Claim Duration Distribution",
       x = "Duration (Years)", y = "Count")

save_figure(p_duration, "106_duration_distribution")

# ==============================================================================
# SECTION 1.4: REMEDIATION TRENDS
# ==============================================================================
message("\n--- 1.4 Remediation Trends ---")

# Annual trends
annual_trends <- master[claim_year >= 1990 & claim_year <= 2024, .(
  N_Claims = .N,
  Mean_Cost_Real = mean(total_paid_real, na.rm = TRUE),
  Median_Cost_Real = median(total_paid_real, na.rm = TRUE),
  Mean_ALAE_Real = mean(paid_alae_real, na.rm = TRUE),
  Mean_Duration_Days = mean(claim_duration_days, na.rm = TRUE),
  Share_Denied = mean(status_group == "Denied", na.rm = TRUE)
), by = claim_year]

# Cost trends
p_cost_trend <- ggplot(annual_trends, aes(x = claim_year)) +
  geom_line(aes(y = Mean_Cost_Real, color = "Mean"), linewidth = 1) +
  geom_line(aes(y = Median_Cost_Real, color = "Median"), linewidth = 1) +
  scale_y_continuous(labels = dollar_format()) +
  scale_color_manual(values = c("Mean" = "#e74c3c", "Median" = "#3498db")) +
  labs(title = "Claim Costs Over Time (Real $)",
       x = "Year", y = "Cost", color = NULL)

save_figure(p_cost_trend, "107_cost_trends")

# Duration trends
p_duration_trend <- ggplot(annual_trends[claim_year >= 1995], 
                           aes(x = claim_year, y = Mean_Duration_Days / 365)) +
  geom_line(color = "#27ae60", linewidth = 1) +
  geom_smooth(method = "loess", se = TRUE, alpha = 0.2) +
  labs(title = "Average Claim Duration Over Time",
       x = "Year", y = "Duration (Years)")

save_figure(p_duration_trend, "108_duration_trends")

# Denial rate trends
p_denial_trend <- ggplot(annual_trends[claim_year >= 1995], 
                         aes(x = claim_year, y = Share_Denied)) +
  geom_line(color = "#9b59b6", linewidth = 1) +
  geom_smooth(method = "loess", se = TRUE, alpha = 0.2) +
  scale_y_continuous(labels = percent_format()) +
  labs(title = "Denial Rate Over Time",
       x = "Year", y = "Share Denied")

save_figure(p_denial_trend, "109_denial_rate_trends")

# Combined trends panel
trends_long <- melt(annual_trends[claim_year >= 1995], 
                    id.vars = "claim_year",
                    measure.vars = c("N_Claims", "Mean_Cost_Real", "Mean_Duration_Days"))

p_trends_panel <- ggplot(trends_long, aes(x = claim_year, y = value)) +
  geom_line(color = "#2c3e50", linewidth = 0.8) +
  geom_smooth(method = "loess", se = TRUE, alpha = 0.2, color = "#e74c3c") +
  facet_wrap(~ variable, scales = "free_y", 
             labeller = labeller(variable = c(
               "N_Claims" = "Number of Claims",
               "Mean_Cost_Real" = "Mean Cost (Real $)",
               "Mean_Duration_Days" = "Duration (Days)"
             ))) +
  labs(title = "Remediation Trends: 1995-2024", x = "Year", y = NULL)

save_figure(p_trends_panel, "110_trends_panel", width = 12, height = 6)

# ==============================================================================
# SECTION 1.5: SPATIAL VISUALIZATION
# ==============================================================================
message("\n--- 1.5 Spatial Analysis ---")

# Regional summary
regional_summary <- master[!is.na(dep_region) & total_paid_real > 0, .(
  N_Claims = .N,
  Total_Cost_Real = sum(total_paid_real, na.rm = TRUE),
  Mean_Cost_Real = mean(total_paid_real, na.rm = TRUE),
  Mean_Duration_Years = mean(claim_duration_days, na.rm = TRUE) / 365
), by = dep_region][order(-N_Claims)]

region_tbl <- kbl(regional_summary, digits = 0, format = "html",
                  caption = "Claims Summary by DEP Region") %>%
  kable_styling()

save_table(region_tbl, "111_regional_summary")

# Regional bar chart
p_region <- ggplot(regional_summary, aes(x = reorder(dep_region, N_Claims), y = N_Claims)) +
  geom_col(fill = "#3498db", alpha = 0.8) +
  coord_flip() +
  labs(title = "Claims by DEP Region",
       x = NULL, y = "Number of Claims")

save_figure(p_region, "111_regional_claims_bar")

# County concentration
county_summary <- master[!is.na(county) & total_paid_real > 0, .(
  N_Claims = .N,
  Total_Cost = sum(total_paid_real, na.rm = TRUE)
), by = county][order(-N_Claims)]

top_counties <- head(county_summary, 20)

p_counties <- ggplot(top_counties, aes(x = reorder(county, N_Claims), y = N_Claims)) +
  geom_col(fill = "#e67e22", alpha = 0.8) +
  coord_flip() +
  labs(title = "Top 20 Counties by Claims",
       x = NULL, y = "Number of Claims")

save_figure(p_counties, "112_county_claims_bar")

# ==============================================================================
# SECTION 3.1: CONSULTANT CONCENTRATION (HHI)
# ==============================================================================
message("\n--- 3.1 Consultant Concentration ---")

# HHI by region
consultant_shares <- contracts[!is.na(consultant) & !is.na(dep_region) & 
                                 total_contract_value_real > 0, .(
  contract_value = sum(total_contract_value_real, na.rm = TRUE)
), by = .(dep_region, consultant)]

consultant_shares[, market_share := contract_value / sum(contract_value), by = dep_region]
consultant_shares[, share_sq := market_share^2]

hhi_by_region <- consultant_shares[, .(
  HHI = sum(share_sq) * 10000,
  N_Consultants = uniqueN(consultant),
  Top_Consultant_Share = max(market_share)
), by = dep_region][order(-HHI)]

# Add interpretation
hhi_by_region[, Market_Structure := fcase(
  HHI < 1500, "Competitive",
  HHI < 2500, "Moderately Concentrated",
  default = "Highly Concentrated"
)]

hhi_tbl <- kbl(hhi_by_region, digits = 0, format = "html",
               caption = "Market Concentration (HHI) by Region") %>%
  kable_styling() %>%
  column_spec(5, background = ifelse(hhi_by_region$HHI > 2500, "#ffcccc", "white"))

save_table(hhi_tbl, "301_hhi_by_region")

# Top consultants overall
top_consultants <- contracts[!is.na(consultant) & total_contract_value_real > 0, .(
  N_Contracts = .N,
  Total_Value = sum(total_contract_value_real, na.rm = TRUE),
  Mean_Value = mean(total_contract_value_real, na.rm = TRUE)
), by = consultant][order(-Total_Value)][1:15]

top_consultants[, Market_Share := Total_Value / sum(Total_Value)]

consultant_tbl <- kbl(top_consultants, digits = 0, format = "html",
                      caption = "Top 15 Consultants by Contract Value") %>%
  kable_styling()

save_table(consultant_tbl, "302_top_consultants")

# ==============================================================================
# HHI ANALYSIS & MAPPING
# ==============================================================================

# 1. Define PA DEP Regions (County Lookup)
# This mapping ensures counties are grouped correctly for the outline
dep_regions <- data.table(
  region = c(rep("Southeast", 5), rep("Northeast", 11), rep("Southcentral", 15), 
             rep("Northcentral", 14), rep("Southwest", 10), rep("Northwest", 12)),
  subregion = tolower(c(
    "Bucks", "Chester", "Delaware", "Montgomery", "Philadelphia",
    "Carbon", "Lackawanna", "Lehigh", "Luzerne", "Monroe", "Northampton", "Pike", "Schuylkill", "Susquehanna", "Wayne", "Wyoming",
    "Adams", "Bedford", "Berks", "Blair", "Cumberland", "Dauphin", "Franklin", "Fulton", "Huntingdon", "Juniata", "Lancaster", "Lebanon", "Mifflin", "Perry", "York",
    "Bradford", "Cameron", "Centre", "Clearfield", "Clinton", "Columbia", "Lycoming", "Montour", "Northumberland", "Potter", "Snyder", "Sullivan", "Tioga", "Union",
    "Allegheny", "Armstrong", "Beaver", "Cambria", "Fayette", "Greene", "Indiana", "Somerset", "Washington", "Westmoreland",
    "Butler", "Clarion", "Crawford", "Elk", "Erie", "Forest", "Jefferson", "Lawrence", "McKean", "Mercer", "Venango", "Warren"
  ))
)

# 2. Calculate HHI by County
contracts_merged <- merge(contracts, master[, .(claim_number, county)], by = "claim_number")
contracts_merged[, county_clean := tolower(county)]

county_hhi <- contracts_merged[!is.na(consultant) & total_contract_value_real > 0, .(
  val = sum(total_contract_value_real)
), by = .(county_clean, consultant)]
county_hhi[, share := val / sum(val), by = county_clean]
county_hhi_final <- county_hhi[, .(HHI = sum(share^2) * 10000), by = county_clean]

# 3. Join with Map Data
pa_map <- map_data("county", "pennsylvania")
setDT(pa_map)
map_data_full <- merge(pa_map, dep_regions, by = "subregion", all.x = TRUE)
map_data_full <- merge(map_data_full, county_hhi_final, by = "subregion", all.x = TRUE)
setorder(map_data_full, group, order)

# 4. Generate the Map
p_hhi_map <- ggplot(map_data_full, aes(x = long, y = lat, group = group)) +
  # A. County Fill (The Heatmap)
  geom_polygon(aes(fill = HHI), color = NA) +
  # B. County Borders (Thin, White)
  geom_polygon(fill = NA, color = "white", linewidth = 0.1) +
  # C. Region Outlines (Thick, Dark) - trick: union polygons implicitly by grouping
  geom_polygon(data = map_data_full, aes(color = region), fill = NA, linewidth = 0.8) +
  scale_fill_ustif_c(name = "Market Concentration (HHI)", limits = c(0, 10000)) +
  scale_color_ustif(name = "DEP Region") +
  coord_fixed(1.3) +
  theme_void() + # Clean map theme
  theme(legend.position = "bottom", 
        legend.title = element_text(face="bold"),
        legend.key.width = unit(1.5, "cm"))

# 5. Save Artifact
save_plot_pub(p_hhi_map, "301_hhi_county_map_regions", width = 8, height = 5)

# ==============================================================================
# SECTION 3.2: ADJUSTER CONCENTRATION
# ==============================================================================
message("\n--- 3.2 Adjuster Concentration ---")

# Adjuster activity by region
adjuster_activity <- contracts[!is.na(adjuster) & !is.na(dep_region), .(
  N_Contracts = .N,
  N_Regions = uniqueN(dep_region),
  Total_Value = sum(total_contract_value_real, na.rm = TRUE)
), by = adjuster][order(-N_Contracts)]

# How many adjusters work across regions?
multi_region_adjusters <- adjuster_activity[N_Regions > 1]
message(sprintf("%d of %d adjusters work across multiple regions (%.1f%%)",
                nrow(multi_region_adjusters),
                nrow(adjuster_activity),
                100 * nrow(multi_region_adjusters) / nrow(adjuster_activity)))

# Auction propensity by adjuster
auction_by_adjuster <- contracts[!is.na(adjuster), .(
  N_Contracts = .N,
  N_PFP = sum(auction_type == "Bid-to-Result", na.rm = TRUE),
  Share_PFP = mean(auction_type == "Bid-to-Result", na.rm = TRUE)
), by = adjuster][N_Contracts >= 5][order(-Share_PFP)]

p_adjuster_pfp <- ggplot(auction_by_adjuster[N_Contracts >= 10], 
                          aes(x = N_Contracts, y = Share_PFP)) +
  geom_point(alpha = 0.6, size = 3, color = "#9b59b6") +
  scale_y_continuous(labels = percent_format()) +
  labs(title = "Adjuster Auction Propensity",
       subtitle = "Adjusters with 10+ contracts",
       x = "Number of Contracts", y = "Share: Bid-to-Result")

save_figure(p_adjuster_pfp, "303_adjuster_auction_propensity")

# ==============================================================================
# SUMMARY OUTPUT
# ==============================================================================
message("\n========================================")
message("ANALYSIS 01 COMPLETE: Descriptive Statistics")
message("========================================")
message(sprintf("Key Findings:"))
message(sprintf("  - Phantom Spend (ALAE on Denied): $%s", 
                format(phantom_spend$Total_ALAE_Real, big.mark = ",", nsmall = 0)))
message(sprintf("  - Highest HHI Region: %s (%.0f)", 
                hhi_by_region$dep_region[1], hhi_by_region$HHI[1]))
message(sprintf("  - Multi-Region Adjusters: %d (%.1f%%)", 
                nrow(multi_region_adjusters),
                100 * nrow(multi_region_adjusters) / nrow(adjuster_activity)))
