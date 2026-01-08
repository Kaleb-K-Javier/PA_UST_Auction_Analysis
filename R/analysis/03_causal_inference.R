# R/analysis/03_causal_inference.R
# ============================================================================
# Pennsylvania UST Auction Analysis - Causal Inference
# ============================================================================
# Purpose: Estimate the causal effect of PFP intervention on remediation costs
#          using methods that correct for dynamic selection bias
# Input: data/processed/analysis_panel.rds
# Output (Multi-Format):
#   Figures: output/figures/*.png, *.pdf
#   Tables:  output/tables/*.html, *.tex, *.pdf
#   Models:  output/models/*.rds
# ============================================================================
# IDENTIFICATION STRATEGY:
#   1. The "Tacit Rule": Learn what predicts PFP assignment (RF propensity)
#   2. IV Estimation: Use adjuster leniency as instrument for quasi-random variation
#   3. DML: Use flexible ML to partial out confounders under conditional ignorability
# ============================================================================

cat("\n========================================\n")
cat("Causal Inference Analysis\n")
cat("========================================\n\n")

# ============================================================================
# SETUP
# ============================================================================

pacman::p_load(
 tidyverse,
 fixest,
 ranger,
 modelsummary,
 DoubleML,
 mlr3,
 mlr3learners,
 data.table,
 ggplot2,
 scales,
 gt
)

# Define paths
paths <- list(
 processed = "data/processed",
 figures = "output/figures",
 tables = "output/tables",
 models = "output/models"
)

# Ensure output directories exist
for (p in paths) {
 if (!dir.exists(p)) dir.create(p, recursive = TRUE)
}

# Load output helper functions
source("R/functions/output_helpers.R")

# Custom theme
theme_ustif <- function() {
 theme_minimal(base_size = 12) +
   theme(
     plot.title = element_text(face = "bold", size = 14),
     plot.subtitle = element_text(color = "gray40", size = 11),
     plot.caption = element_text(color = "gray50", hjust = 0, size = 9),
     panel.grid.minor = element_blank()
   )
}

# ============================================================================
# LOAD DATA
# ============================================================================

cat("Loading analysis panel...\n")

panel <- readRDS(file.path(paths$processed, "analysis_panel.rds"))

# Also load IV-specific panel
iv_panel <- readRDS(file.path(paths$processed, "iv_analysis_panel.rds"))

cat(paste(" Full panel observations:", nrow(panel), "\n"))
cat(paste(" IV-valid observations:", nrow(iv_panel), "\n\n"))

# ============================================================================
# PART 1: THE "TACIT RULE" - RANDOM FOREST PROPENSITY MODEL
# ============================================================================

cat("========================================\n")
cat("PART 1: Learning the Tacit Rule\n")
cat("========================================\n\n")

# Prepare data for RF (complete cases for key predictors)
rf_data <- panel %>%
 filter(!is.na(tank_age_imputed) & !is.na(duration_years) & !is.na(region)) %>%
 mutate(
   is_PFP_numeric = as.numeric(is_PFP),
   region_factor = factor(region)
 ) %>%
 select(
   is_PFP_numeric,
   tank_age_imputed,
   duration_years,
   capacity_imputed,
   has_single_wall,
   n_tanks,
   claim_year,
   region_factor
 ) %>%
 drop_na()

cat(paste("RF training sample:", nrow(rf_data), "observations\n\n"))

# Fit Random Forest for propensity
cat("Training Random Forest propensity model...\n")

set.seed(42)
rf_model <- ranger(
 is_PFP_numeric ~ .,
 data = rf_data,
 num.trees = 500,
 importance = "impurity",
 probability = FALSE,
 min.node.size = 10
)

# Extract variable importance
importance_df <- data.frame(
 variable = names(rf_model$variable.importance),
 importance = rf_model$variable.importance
) %>%
 arrange(desc(importance)) %>%
 mutate(
   variable_clean = case_when(
     variable == "tank_age_imputed" ~ "Tank Age (Years)",
     variable == "duration_years" ~ "Claim Duration (Years)",
     variable == "capacity_imputed" ~ "Tank Capacity (Gallons)",
     variable == "has_single_wall" ~ "Has Single-Wall Tank",
     variable == "n_tanks" ~ "Number of Tanks",
     variable == "claim_year" ~ "Claim Year",
     variable == "region_factor" ~ "DEP Region",
     TRUE ~ variable
   )
 )

cat("\nVariable Importance (Gini Impurity):\n")
print(importance_df)

# Plot variable importance
p_importance <- ggplot(importance_df, aes(x = reorder(variable_clean, importance), y = importance)) +
 geom_col(fill = "#2166AC", alpha = 0.8) +
 coord_flip() +
 labs(
   title = "What Drives PFP Intervention? (The Tacit Rule)",
   subtitle = "Random Forest variable importance for predicting auction assignment",
   x = NULL,
   y = "Importance (Gini Impurity Decrease)",
   caption = paste0(
     "Note: Higher values indicate stronger prediction of PFP assignment.\n",
     "This reveals the de facto policy rule used by TPA adjusters.\n",
     "N = ", format(nrow(rf_data), big.mark = ","), " observations"
   )
 ) +
 theme_ustif()

# Save in multiple formats
save_figure(p_importance, "rf_importance",
           output_dir = paths$figures,
           width = 9, height = 6,
           formats = c("png", "pdf"))

# Save RF model
saveRDS(rf_model, file.path(paths$models, "rf_propensity_model.rds"))
cat("✓ Saved: output/models/rf_propensity_model.rds\n\n")

# ============================================================================
# PART 2: INSTRUMENTAL VARIABLE ESTIMATION
# ============================================================================

cat("========================================\n")
cat("PART 2: Instrumental Variable Estimation\n")
cat("========================================\n\n")

# Prepare IV data
iv_data <- iv_panel %>%
 filter(has_valid_iv == TRUE) %>%
 filter(!is.na(region) & region != "Unknown") %>%
 mutate(
   region_factor = factor(region),
   year_factor = factor(claim_year)
 )

cat(paste("IV estimation sample:", nrow(iv_data), "observations\n"))
cat(paste("Unique adjusters:", n_distinct(iv_data$adjuster), "\n\n"))

# First Stage
cat("First Stage Regression: is_PFP ~ adjuster_leniency | region + year\n")

first_stage <- feols(
 is_PFP ~ adjuster_leniency | region_factor + year_factor,
 data = iv_data,
 vcov = "hetero"
)

cat("\nFirst Stage Results:\n")
print(summary(first_stage))

first_stage_f <- fitstat(first_stage, "ivf")
cat(paste("\nFirst Stage F-statistic:", round(first_stage_f$ivf$stat, 2), "\n"))
if (first_stage_f$ivf$stat < 10) {
 cat("WARNING: Weak instrument (F < 10). IV estimates may be biased.\n")
} else {
 cat("Instrument appears strong (F > 10).\n")
}

# IV (2SLS) Estimation
cat("\n\nIV (2SLS) Estimation: log_total_paid ~ 1 | region + year | is_PFP ~ adjuster_leniency\n")

iv_model <- feols(
 log_total_paid ~ 1 | region_factor + year_factor | is_PFP ~ adjuster_leniency,
 data = iv_data,
 vcov = "hetero"
)

cat("\nIV Results (Log Total Paid):\n")
print(summary(iv_model))

# IV in levels
iv_model_levels <- feols(
 total_paid ~ 1 | region_factor + year_factor | is_PFP ~ adjuster_leniency,
 data = iv_data,
 vcov = "hetero"
)

# OLS comparison (biased)
ols_model <- feols(
 log_total_paid ~ is_PFP | region_factor + year_factor,
 data = iv_data,
 vcov = "hetero"
)

cat("\nOLS Results (for comparison - BIASED):\n")
print(summary(ols_model))

# Save IV results
iv_results <- list(
 iv_model = iv_model,
 iv_model_levels = iv_model_levels,
 first_stage = first_stage,
 ols_model = ols_model,
 first_stage_f = first_stage_f,
 n_obs = nrow(iv_data),
 n_adjusters = n_distinct(iv_data$adjuster)
)

saveRDS(iv_results, file.path(paths$models, "iv_model.rds"))
cat("\n✓ Saved: output/models/iv_model.rds\n\n")

# ============================================================================
# PART 3: DOUBLE MACHINE LEARNING (DML)
# ============================================================================

cat("========================================\n")
cat("PART 3: Double Machine Learning (DML)\n")
cat("========================================\n\n")

# Prepare data for DML
dml_data <- panel %>%
 filter(!is.na(tank_age_imputed) & !is.na(duration_years) & !is.na(region)) %>%
 filter(!is.na(total_paid) & total_paid > 0) %>%
 mutate(
   log_total_paid = log(total_paid + 1),
   is_PFP_numeric = as.numeric(is_PFP)
 ) %>%
 select(
   log_total_paid,
   is_PFP_numeric,
   tank_age_imputed,
   duration_years,
   capacity_imputed,
   n_tanks,
   claim_year
 ) %>%
 drop_na() %>%
 as.data.table()

cat(paste("DML estimation sample:", nrow(dml_data), "observations\n\n"))

# Define variable roles
y_col <- "log_total_paid"
d_col <- "is_PFP_numeric"
x_cols <- c("tank_age_imputed", "duration_years", "capacity_imputed", "n_tanks", "claim_year")

# Create DoubleML data object
cat("Creating DoubleML data backend...\n")

dml_data_obj <- DoubleMLData$new(
 dml_data,
 y_col = y_col,
 d_cols = d_col,
 x_cols = x_cols
)

# Define ML learners
cat("Configuring Random Forest learners...\n")

ml_l <- lrn("regr.ranger",
           num.trees = 200,
           min.node.size = 5,
           num.threads = 1)

ml_m <- lrn("classif.ranger",
           num.trees = 200,
           min.node.size = 5,
           num.threads = 1,
           predict_type = "prob")

# Fit DML PLR model
cat("Fitting Double ML Partially Linear Regression model...\n")
cat("(This may take a few minutes)\n\n")

set.seed(42)
dml_plr <- DoubleMLPLR$new(
 dml_data_obj,
 ml_l = ml_l,
 ml_m = ml_m,
 n_folds = 5,
 n_rep = 1
)

dml_plr$fit()

# Extract results
cat("\nDML Results:\n")
print(dml_plr)

dml_coef <- dml_plr$coef
dml_se <- dml_plr$se
dml_pval <- 2 * (1 - pnorm(abs(dml_coef / dml_se)))

cat("\n========================================\n")
cat("DML Treatment Effect Estimate:\n")
cat(paste(" Coefficient (is_PFP):", round(dml_coef, 4), "\n"))
cat(paste(" Std. Error:", round(dml_se, 4), "\n"))
cat(paste(" t-statistic:", round(dml_coef / dml_se, 2), "\n"))
cat(paste(" p-value:", round(dml_pval, 4), "\n"))
cat(paste(" 95% CI: [", round(dml_coef - 1.96 * dml_se, 4), ", ",
         round(dml_coef + 1.96 * dml_se, 4), "]\n"))
cat("========================================\n\n")

# Save DML results
dml_results <- list(
 model = dml_plr,
 coef = dml_coef,
 se = dml_se,
 pval = dml_pval,
 ci_lower = dml_coef - 1.96 * dml_se,
 ci_upper = dml_coef + 1.96 * dml_se,
 n_obs = nrow(dml_data)
)

saveRDS(dml_results, file.path(paths$models, "dml_model.rds"))
cat("✓ Saved: output/models/dml_model.rds\n\n")

# ============================================================================
# PART 4: COMPARISON TABLE
# ============================================================================

cat("========================================\n")
cat("PART 4: Comparison of Estimates\n")
cat("========================================\n\n")

# Create comparison table
comparison_df <- tibble(
 Method = c("OLS (Biased)", "IV (2SLS)", "Double ML"),
 Coefficient = c(
   coef(ols_model)["is_PFPTRUE"],
   coef(iv_model)["fit_is_PFP"],
   dml_coef
 ),
 `Std. Error` = c(
   se(ols_model)["is_PFPTRUE"],
   se(iv_model)["fit_is_PFP"],
   dml_se
 ),
 `Sample Size` = c(
   nobs(ols_model),
   nobs(iv_model),
   nrow(dml_data)
 )
) %>%
 mutate(
   `t-stat` = Coefficient / `Std. Error`,
   `p-value` = 2 * (1 - pnorm(abs(`t-stat`))),
   `95% CI` = paste0("[", round(Coefficient - 1.96 * `Std. Error`, 3), ", ",
                     round(Coefficient + 1.96 * `Std. Error`, 3), "]"),
   `% Effect` = paste0(round((exp(Coefficient) - 1) * 100, 1), "%")
 )

cat("\nComparison of Treatment Effect Estimates:\n")
print(comparison_df)

# Create gt table
comparison_table <- comparison_df %>%
 gt() %>%
 tab_header(
   title = "Causal Effect of PFP Intervention on Remediation Costs",
   subtitle = "Comparison of estimation methods"
 ) %>%
 fmt_number(
   columns = c(Coefficient, `Std. Error`, `t-stat`),
   decimals = 3
 ) %>%
 fmt_number(
   columns = `p-value`,
   decimals = 4
 ) %>%
 fmt_number(
   columns = `Sample Size`,
   use_seps = TRUE,
   decimals = 0
 ) %>%
 tab_footnote(
   footnote = "OLS is biased due to selection on unobservables",
   locations = cells_body(columns = Method, rows = 1)
 ) %>%
 tab_footnote(
   footnote = "IV uses adjuster leniency as instrument",
   locations = cells_body(columns = Method, rows = 2)
 ) %>%
 tab_footnote(
   footnote = "DML uses Random Forest for nuisance parameters",
   locations = cells_body(columns = Method, rows = 3)
 ) %>%
 tab_source_note("Source: USTIF Administrative Data. Dependent variable is log(total_paid).")

# Save in multiple formats
save_gt_table(comparison_table, "causal_estimates",
             output_dir = paths$tables,
             formats = c("html", "latex", "pdf"))

# ============================================================================
# PART 5: COEFFICIENT PLOT
# ============================================================================

cat("Creating coefficient comparison plot...\n")

coef_plot_data <- comparison_df %>%
 mutate(
   ci_lower = Coefficient - 1.96 * `Std. Error`,
   ci_upper = Coefficient + 1.96 * `Std. Error`,
   Method = factor(Method, levels = c("OLS (Biased)", "IV (2SLS)", "Double ML"))
 )

p_coef <- ggplot(coef_plot_data, aes(x = Method, y = Coefficient, color = Method)) +
 geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
 geom_point(size = 4) +
 geom_errorbar(aes(ymin = ci_lower, ymax = ci_upper), width = 0.2, linewidth = 1) +
 scale_color_manual(values = c("OLS (Biased)" = "#D6604D",
                                "IV (2SLS)" = "#2166AC",
                                "Double ML" = "#4DAF4A")) +
 labs(
   title = "Treatment Effect Estimates: PFP vs. T&M",
   subtitle = "Point estimates with 95% confidence intervals",
   x = NULL,
   y = "Effect on Log(Total Paid)",
   caption = paste0(
     "Note: Negative values indicate PFP REDUCES costs relative to T&M.\n",
     "OLS is biased (selection); IV and DML correct for this bias."
   )
 ) +
 theme_ustif() +
 theme(legend.position = "none")

save_figure(p_coef, "causal_estimates_plot",
           output_dir = paths$figures,
           width = 8, height = 6,
           formats = c("png", "pdf"))

# ============================================================================
# PART 6: FIRST STAGE VISUALIZATION
# ============================================================================

cat("Creating first stage diagnostic plot...\n")

# Binned scatter plot of adjuster leniency vs PFP assignment
p_first_stage <- iv_data %>%
 mutate(leniency_bin = cut(adjuster_leniency, breaks = 10)) %>%
 group_by(leniency_bin) %>%
 summarise(
   mean_leniency = mean(adjuster_leniency),
   mean_pfp = mean(is_PFP),
   n = n(),
   .groups = "drop"
 ) %>%
 ggplot(aes(x = mean_leniency, y = mean_pfp)) +
 geom_point(aes(size = n), color = "#2166AC", alpha = 0.7) +
 geom_smooth(method = "lm", se = TRUE, color = "#B2182B", linetype = "dashed") +
 scale_size_continuous(name = "N Observations", range = c(3, 10)) +
 scale_y_continuous(labels = scales::percent_format()) +
 labs(
   title = "First Stage: Adjuster Leniency Predicts PFP Assignment",
   subtitle = paste0("F-statistic = ", round(first_stage_f$ivf$stat, 1),
                     " (instrument is ", ifelse(first_stage_f$ivf$stat >= 10, "strong", "weak"), ")"),
   x = "Adjuster Leniency (Leave-One-Out Mean)",
   y = "Probability of PFP Assignment",
   caption = "Note: Each point represents a decile of adjuster leniency. Line shows linear fit."
 ) +
 theme_ustif()

save_figure(p_first_stage, "first_stage_plot",
           output_dir = paths$figures,
           width = 9, height = 6,
           formats = c("png", "pdf"))

# ============================================================================
# SUMMARY
# ============================================================================

cat("\n========================================\n")
cat("CAUSAL INFERENCE SUMMARY\n")
cat("========================================\n\n")

cat("1. THE TACIT RULE (What triggers intervention?):\n")
cat("  ", paste(head(importance_df$variable_clean, 3), collapse = ", "), "\n")
cat("  These are the primary drivers of PFP assignment.\n\n")

cat("2. SELECTION BIAS CORRECTION:\n")
cat(paste("  OLS estimate:", round(coef(ols_model)["is_PFPTRUE"], 3), "(BIASED)\n"))
cat(paste("  IV estimate:", round(coef(iv_model)["fit_is_PFP"], 3), "\n"))
cat(paste("  DML estimate:", round(dml_coef, 3), "\n\n"))

cat("========================================\n")
cat("OUTPUTS CREATED:\n")
cat("========================================\n")
cat("Figures (PNG + PDF):\n")
cat(" • rf_importance\n")
cat(" • causal_estimates_plot\n")
cat(" • first_stage_plot\n\n")
cat("Tables (HTML + LaTeX + PDF):\n")
cat(" • causal_estimates\n\n")
cat("Models (RDS):\n")
cat(" • rf_propensity_model\n")
cat(" • iv_model\n")
cat(" • dml_model\n")
cat("========================================\n\n")
