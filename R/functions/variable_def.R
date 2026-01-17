# ==============================================================================
# 00_variable_definitions.R
# ==============================================================================
# PURPOSE: Standardized variable definitions for PA UST Analysis.
# SOURCE: 03_merge_master_dataset.R
# USAGE: Source this script at the top of 01_descriptive and 02_models.
#        Use 'fixed_effects' vector for FE specifications.
# ==============================================================================

# ==============================================================================
# 1. MASTER VARIABLE LISTS (The "Source of Truth")
# ==============================================================================

# A. OUTCOMES (Y-Variables)
outcome_vars <- c(
  # --- Financial (Inflation Adjusted 2025$) ---
  "total_paid_real",            # Total Cost (Indemnity + Expense)
  "paid_loss_real",             # Indemnity/Remediation portion
  "paid_alae_real",             # Legal/Adjuster expense portion
  "total_contract_value_real",  # Auction contract value
  "log_cost",                   # log(total_paid_real + 1)
  
  # --- Operational ---
  "claim_duration_days",        # Cycle time (Open to Close)
  "intervention_lag_days",      # Time to first contract (Auction lag)
  
  # --- Classification Targets ---
  "status_group",               # Denied / Eligible / Withdrawn
  "went_to_auction",            # Binary: 1 if Competitive Bid
  "is_high_cost",               # Binary: >90th Percentile Cost
  "is_long_duration"            # Binary: >90th Percentile Time
)

# B. FIXED EFFECTS (Control Variables)
fixed_effects <- c(
  "Year",                       # Time FE (Calendar Year)
  "region_cluster"              # Spatial FE (DEP Region Group)
)

# ==============================================================================
# 2. FACILITY CHARACTERISTICS (X-Variables)
# ==============================================================================

# --- Binary Flags (1/0) ---
facility_vars_binary <- c(
  # Physical Risks (Did ANY active tank have this?)
  "has_bare_steel", 
  "has_single_walled", 
  "has_secondary_containment",
  "has_pressure_piping", 
  "has_suction_piping", 
  "has_galvanized_piping",
  "has_electronic_atg", 
  "has_manual_detection", 
  "has_overfill_alarm",
  
  # Substance
  "has_gasoline", 
  "has_diesel", 
  
  # Compliance & Data Quality
  "has_noncompliant",           # Flagged violation
  "has_legacy_grandfathered",   # Pre-1988 tanks
  "has_unknown_data",           # General data gap flag
  "has_recent_closure"          # Tank closed <90 days prior
)

# --- Continuous Metrics (Counts, Ages, Shares) ---
facility_vars_continuous <- c(
  # Scale & Age
  "n_tanks_total",              # Historical census count
  "n_tanks_active",             # Active at claim date
  "facility_age",               # Age of facility (years)
  "avg_tank_age",               # Average age of active tanks
  
  # Capacity (v8.0)
  "total_capacity_gal", 
  "avg_tank_capacity_gal", 
  "max_tank_capacity_gal",
  
  # Sophistication
  "facility_count",             # Portfolio size
  
  # Risk Shares (% of active tanks)
  "share_bare_steel",
  "share_single_wall",
  "share_pressure_piping",
  "share_noncompliant",
  "share_data_unknown",
  "share_tank_construction_unknown",
  "share_ug_piping_unknown",
  "share_tank_release_detection_unknown",
  
  # Churn Activity (Counts)
  "n_closed_0_30d",
  "n_closed_31_90d",
  "n_closed_1yr",
  "n_closed_total"
)

# --- Categorical (Factors) ---
# NOTE: 'region_cluster' removed (moved to fixed_effects)
facility_vars_categorical <- c(
  "business_category",          # Broad (e.g., "Retail Gas")
  "final_owner_sector",         # Granular (e.g., "Sheetz")
  "Owner_Size_Class"            # Size Buckets
)

# ==============================================================================
# 3. CLAIM CHARACTERISTICS (History & Behavior)
# ==============================================================================

claim_vars_binary <- c(
  "is_repeat_filer"             # Has prior claims
)

# NOTE: 'Year' removed (moved to fixed_effects)
claim_vars_continuous <- c(
  "reporting_lag_days",         # Days: Loss -> Report
  "prior_claims_n",             # Count of prior claims
  "days_since_prior_claim",     # Recency of last claim
  "avg_prior_claim_cost"        # Severity history
)

# ==============================================================================
# 4. MODELING FEATURE SETS (Ready-to-Use Lists)
# ==============================================================================

# Set A: OLS / Linear Models (Avoid Multicollinearity)
# Usage: formula = outcome ~ paste(c(features_ols, fixed_effects), collapse = " + ")
features_ols <- c(
  # Key Continuous
  "avg_tank_age", "n_tanks_total", "reporting_lag_days", "prior_claims_n",
  
  # Key Binary Flags
  facility_vars_binary, 
  "is_repeat_filer"
)

# Set B: ML / Random Forest (High Dimensionality)
# Usage: X <- build_ml_matrix(dt, c(features_ml_base, fixed_effects))
features_ml_base <- c(
  facility_vars_binary,
  facility_vars_continuous,
  claim_vars_binary,
  claim_vars_continuous,
  facility_vars_categorical
)


# ==============================================================================
# 5. HELPER FUNCTIONS
# ==============================================================================

#' Impute NA -> 0 for specific numeric columns
impute_human_na <- function(dt, vars) {
  dt_copy <- copy(dt)
  valid_vars <- intersect(vars, names(dt_copy))
  for (v in valid_vars) {
    if (is.numeric(dt_copy[[v]])) {
      set(dt_copy, which(is.na(dt_copy[[v]])), v, 0)
    }
  }
  return(dt_copy)
}

#' Build ML Model Matrix (Handles Categoricals + ETL Signals)
build_ml_matrix <- function(dt, base_vars, include_bin_qty = TRUE) {
  
  # 1. Identify ETL Signals (High-dim indicators)
  etl_vars <- character(0)
  if (include_bin_qty) {
    etl_vars <- grep("^(bin_|qty_)", names(dt), value = TRUE)
  }
  
  # 2. Combine with Base Vars
  all_vars <- unique(c(intersect(base_vars, names(dt)), etl_vars))
  
  # 3. Formula Construction
  f_str <- paste("~", paste(all_vars, collapse = " + "), "- 1")
  
  # 4. Generate Matrix
  X <- model.matrix(as.formula(f_str), data = dt, na.action = na.pass)
  
  return(X)
}