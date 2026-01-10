# R/analysis/01_risk_probability_modeling.R
# ==============================================================================
# Purpose: Comprehensive Risk & Cost Analysis (Panel Data Edition)
#          PART A: Descriptive Statistics (Costs/Contracts from Claims Master)
#          PART B: Hazard Modeling (Probability of Claim from Facility Panel)
# ==============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)       # Fast OLS / Fixed Effects
  library(grf)          # Generalized Random Forests
  library(modelsummary) # Tables
  library(kableExtra)
  library(janitor)
  library(stringr)
  library(ggplot2)
  library(scales)
  library(patchwork)
})

# 1. SETUP & PATHS
# ==============================================================================
paths <- list(
  panel     = "data/processed/panel_hazard_dataset.rds",
  master    = "data/processed/master_analysis_dataset.rds",
  tables    = "output/tables",
  figures   = "output/figures"
)

dir.create(paths$tables, recursive = TRUE, showWarnings = FALSE)
dir.create(paths$figures, recursive = TRUE, showWarnings = FALSE)

# --- Output Helpers ---
save_pub_table <- function(table_obj, filename_base) {
  # If it's a kable/gt object, convert to string
  if (!is.character(table_obj)) {
    table_str <- as.character(table_obj)
  } else {
    table_str <- table_obj
  }
  writeLines(table_str, file.path(paths$tables, paste0(filename_base, ".html")))
}

save_pub_figure <- function(plot_obj, filename_base) {
  clean_plot <- plot_obj + 
    theme_minimal(base_size = 14) +
    theme(
      plot.title    = element_blank(),
      plot.subtitle = element_blank(),
      axis.title    = element_text(face = "bold", color = "#2c3e50"),
      axis.text     = element_text(color = "#2c3e50"),
      legend.position = "bottom",
      panel.grid.minor = element_blank()
    )
  ggsave(file.path(paths$figures, paste0(filename_base, ".png")), clean_plot, width=9, height=6, bg="white")
  ggsave(file.path(paths$figures, paste0(filename_base, ".pdf")), clean_plot, width=9, height=6, bg="white")
}

# 2. DATA LOADING & PREP
# ==============================================================================
message("\n--- Loading Data ---")
claims <- readRDS(paths$master) # For Part A (Costs)
panel  <- readRDS(paths$panel)  # For Part B (Risk)

# --- FIX: Create Factor for Visualization ---
if ("contract_type" %in% names(claims)) {
  claims[, auction_type_factor := factor(contract_type, 
                                         levels = c("No Contract", "Other Contract", "Scope of Work", "Bid-to-Result"))]
} else {
  claims[, auction_type_factor := factor("Unknown")]
}

# Filter Claims for Analysis
analysis_claims <- claims[total_paid_real > 1000]

# ==============================================================================
# PART A: THE COST OF RISK (Descriptive)
# ==============================================================================
message("\n--- Part A: Descriptive Statistics (Costs & Contracts) ---")

# 1. Cost Distribution
p_cost <- ggplot(analysis_claims, aes(x = total_paid_real)) +
  geom_histogram(bins = 45, fill = "#2c3e50", color = "white", alpha = 0.9) +
  scale_x_log10(labels = dollar_format()) +
  labs(x = "Total Real Cost (2024 Dollars, Log Scale)", y = "Number of Claims")
save_pub_figure(p_cost, "201_real_cost_distribution")

# 2. Temporal Trends
annual_stats <- analysis_claims[claim_year >= 1995, .(
  Median_Cost = median(total_paid_real, na.rm=TRUE),
  Mean_Cost   = mean(total_paid_real, na.rm=TRUE)
), by = claim_year]

p_trend <- ggplot(annual_stats, aes(x = claim_year)) +
  geom_line(aes(y = Mean_Cost, color = "Mean"), linewidth = 1.2) +
  geom_line(aes(y = Median_Cost, color = "Median"), linewidth = 1.2) +
  scale_y_continuous(labels = dollar_format()) +
  scale_color_manual(values = c("Mean" = "#e67e22", "Median" = "#2980b9")) +
  labs(x = "Year", y = "Real Cost ($)", color = NULL)
save_pub_figure(p_trend, "202_cost_trends")

# 3. Contract Mechanisms
p_mech <- ggplot(analysis_claims[!is.na(auction_type_factor)], 
                 aes(x = auction_type_factor, y = total_paid_real, fill = auction_type_factor)) +
  geom_boxplot(outlier.shape = NA, alpha = 0.7) +
  scale_y_log10(labels = dollar_format()) +
  scale_fill_viridis_d(option = "mako", begin = 0.3, end = 0.8) +
  labs(x = NULL, y = "Total Real Cost (Log Scale)") +
  theme(legend.position = "none")
save_pub_figure(p_mech, "203_contract_mechanisms")

# ==============================================================================
# PART B: THE DRIVERS OF RISK (Hazard Modeling)
# ==============================================================================
message("\n--- Part B: Hazard Modeling (Panel Data) ---")

# 1. Panel Filtering & Feature Aggregation
risk_panel <- panel[Year >= 1995 & Year <= 2024 & n_active_tanks > 0]

# Feature Construction (Safe Row Means)
cols_bare_steel <- grep("feat_(1A|2A|UNPROTECTED|BARE)", names(risk_panel), value=TRUE, ignore.case=TRUE)
cols_dbl_wall   <- grep("feat_.*DOUBLE", names(risk_panel), value=TRUE, ignore.case=TRUE)
cols_pressure   <- grep("feat_.*PRESSURE", names(risk_panel), value=TRUE, ignore.case=TRUE)

risk_panel[, `:=`(
  share_bare_steel      = if(length(cols_bare_steel) > 0) rowMeans(.SD, na.rm=TRUE) else 0, 
  share_double_wall     = if(length(cols_dbl_wall) > 0) rowMeans(.SD, na.rm=TRUE) else 0,
  share_pressure_piping = if(length(cols_pressure) > 0) rowMeans(.SD, na.rm=TRUE) else 0
), .SDcols = c(cols_bare_steel, cols_dbl_wall, cols_pressure)]

# Zero-fill NAs explicitly to prevent regression drops
for(v in c("share_bare_steel", "share_double_wall", "share_pressure_piping")) {
  if(v %in% names(risk_panel)) {
    set(risk_panel, i=which(is.na(risk_panel[[v]])), j=v, value=0)
  }
}

# Age Bins
risk_panel[, age_bin := cut(avg_tank_age, 
                            breaks = c(-Inf, 5, 10, 15, 20, 25, 30, 35, 40, 45, Inf),
                            labels = c("0-5", "05-10", "10-15", "15-20", "20-25", 
                                       "25-30", "30-35", "35-40", "40-45", "45+"))]
risk_panel[, age_bin := relevel(age_bin, ref = "0-5")]

message(sprintf("Analysis Panel: %d facility-years. Total Events: %d", 
                nrow(risk_panel), sum(risk_panel$has_claim_event)))

# ------------------------------------------------------------------------------
# 2. COMPARATIVE PROFILE
# ------------------------------------------------------------------------------
ever_claimant_ids <- unique(analysis_claims$department)
risk_panel[, is_ever_claimant := FAC_ID %in% ever_claimant_ids]
last_active <- risk_panel[, .SD[which.max(Year)], by = FAC_ID]

vars_compare <- c("n_active_tanks", "avg_tank_age", "share_bare_steel", "share_double_wall")
bal_tab <- last_active[, lapply(.SD, mean, na.rm=TRUE), by = is_ever_claimant, .SDcols = vars_compare]

# Formatting
bal_melt <- melt(bal_tab, id.vars = "is_ever_claimant")
bal_cast <- dcast(bal_melt, variable ~ is_ever_claimant, value.var = "value")
setnames(bal_cast, c("FALSE", "TRUE"), c("Never_Claimant", "Ever_Claimant"))

bal_cast[, `:=`(
  Metric = fcase(variable == "n_active_tanks", "Facility Size (Tanks)",
                 variable == "avg_tank_age", "Avg Tank Age",
                 variable == "share_bare_steel", "Share: Bare Steel",
                 variable == "share_double_wall", "Share: Double Wall"),
  Diff_Pct = (Ever_Claimant - Never_Claimant) / Never_Claimant
)]

bal_out <- bal_cast[, .(Metric, 
                        No_Claim = round(Never_Claimant, 2), 
                        Has_Claim = round(Ever_Claimant, 2), 
                        Diff = percent(Diff_Pct, accuracy=0.1))]

# Save with Kable styling
save_pub_table(kbl(bal_out, format="html") %>% kable_styling(), "301_claimant_vs_nonclaimant_profile")

# ------------------------------------------------------------------------------
# 3. EXPLANATORY MODELING (OLS)
# ------------------------------------------------------------------------------
# We model the hazard rate.
# Note: share_double_wall and share_pressure_piping might be dropped if collinear.
# This is expected if they are perfectly correlated with bare_steel or each other.

m_ols <- feols(has_claim_event ~ age_bin + n_active_tanks + 
                 share_bare_steel + share_double_wall + share_pressure_piping, 
               data = risk_panel, 
               cluster = "FAC_ID")

coef_map <- c(
  "n_active_tanks" = "Facility Size",
  "share_bare_steel" = "Share: Bare Steel",
  "share_double_wall" = "Share: Double Wall",
  "share_pressure_piping" = "Share: Pressure Piping",
  "age_bin05-10" = "Age: 05-10", "age_bin10-15" = "Age: 10-15",
  "age_bin15-20" = "Age: 15-20", "age_bin20-25" = "Age: 20-25",
  "age_bin25-30" = "Age: 25-30", "age_bin30-35" = "Age: 30-35",
  "age_bin35-40" = "Age: 35-40", "age_bin40-45" = "Age: 40-45",
  "age_bin45+"   = "Age: 45+"
)

# FIX: output="kableExtra" ensures it returns an object compatible with kable_styling
tab_ols <- modelsummary(
  list("Annual Claim Probability (LPM)" = m_ols),
  coef_map = coef_map,
  stars = c('*' = .1, '**' = .05, '***' = .01),
  gof_map = c("nobs", "r.squared"),
  output = "kableExtra" 
) %>% kable_styling(bootstrap_options = c("striped", "hover"), full_width = F)

save_pub_table(tab_ols, "304_hazard_model_ols")

# ------------------------------------------------------------------------------
# 4. PREDICTIVE MODELING (GRF)
# ------------------------------------------------------------------------------
message("Training Probability Forest (Clustered)...")

feat_cols <- grep("^feat_", names(risk_panel), value = TRUE)
meta_cols <- c("avg_tank_age", "n_active_tanks")
ml_data <- risk_panel[, c(feat_cols, meta_cols, "has_claim_event", "FAC_ID"), with=FALSE]
ml_data <- na.omit(ml_data)

# Use a subsample for training if N is very large (>200k), to keep it snappy
# But 270k is manageable for GRF.
set.seed(42)
if(nrow(ml_data) > 100000) {
  train_idx <- sample(nrow(ml_data), 100000)
  ml_train <- ml_data[train_idx]
} else {
  ml_train <- ml_data
}

X <- as.matrix(ml_train[, .SD, .SDcols = c(feat_cols, meta_cols)])
Y <- as.factor(ml_train$has_claim_event)
Clusters <- as.numeric(as.factor(ml_train$FAC_ID))

# Train Forest
rf_prob <- probability_forest(X, Y, clusters = Clusters, num.trees = 1000, seed = 42)

# Variable Importance
imp <- variable_importance(rf_prob)
imp_dt <- data.table(Feature = colnames(X), Importance = imp[,1])
setorder(imp_dt, -Importance)

imp_dt[, Label := gsub("feat_[0-9A-Z]+_", "", Feature)]
imp_dt[, Label := str_to_title(gsub("_", " ", Label))]
imp_dt[Feature == "avg_tank_age", Label := "Tank Age"]
imp_dt[Feature == "n_active_tanks", Label := "Facility Size"]

p_imp <- ggplot(head(imp_dt, 15), aes(x = reorder(Label, Importance), y = Importance)) +
  geom_col(fill = "#2c3e50", width = 0.7) +
  coord_flip() +
  labs(x = NULL, y = "Importance (Claim Probability)")
save_pub_figure(p_imp, "305_grf_hazard_importance")

# Surrogate Projection
ml_train[, risk_score := predict(rf_prob, OOB = TRUE)$predictions[, 2]]
surrogate <- feols(risk_score ~ avg_tank_age + n_active_tanks, 
                   data = ml_train, cluster = "FAC_ID")

tab_surr <- modelsummary(
  list("ML Surrogate (Projection)" = surrogate),
  coef_map = c("avg_tank_age" = "Age", "n_active_tanks" = "Size"),
  output = "kableExtra"
) %>% kable_styling()
save_pub_table(tab_surr, "306_grf_surrogate_projection")

# PDP
grid_age <- seq(0, 50, by = 2)
pdp_base <- ml_train[sample(.N, min(2000, .N))]
pdp_res <- lapply(grid_age, function(a) {
  dt <- copy(pdp_base)
  dt[, avg_tank_age := a]
  preds <- predict(rf_prob, newdata = as.matrix(dt[, .SD, .SDcols = colnames(X)]))$predictions[,2]
  data.table(Age = a, Mean_Risk = mean(preds))
})
pdp_dt <- rbindlist(pdp_res)

p_curve <- ggplot(pdp_dt, aes(x = Age, y = Mean_Risk)) +
  geom_line(color = "#e74c3c", linewidth = 1.5) +
  scale_y_continuous(labels = percent_format(accuracy=0.01)) +
  labs(x = "Average Tank Age (Years)", y = "Predicted Annual Claim Probability")
save_pub_figure(p_curve, "307_pdp_age_curve")

message("\n========================================")
message("ANALYSIS 01 (PANEL) COMPLETE")
message("========================================")