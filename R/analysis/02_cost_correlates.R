# R/analysis/02_cost_correlates.R
# ============================================================================
# Pennsylvania UST Auction Analysis - Cost Correlates Analysis
# ============================================================================
# Purpose: Explore correlations between costs and facility characteristics
# 
# IMPORTANT METHODOLOGICAL NOTE:
# This analysis is DESCRIPTIVE and EXPLORATORY. The regression models document
# conditional correlations - they should NOT be interpreted as causal effects.
# Selection into auction vs. non-auction procurement is endogenous.
#
# Updates: Implements multi-format output (HTML, LaTeX, PDF, PNG)
# ============================================================================

cat("\n========================================\n")
cat("Cost Correlates Analysis (Descriptive)\n")
cat("========================================\n\n")

# Load dependencies
library(tidyverse)
library(fixest)
library(modelsummary)
library(scales)
library(patchwork)
library(kableExtra) # For PDF table generation

# Source helper functions for multi-format saving
# (Ensure R/functions/output_helpers.R exists)
if (file.exists("R/functions/output_helpers.R")) {
  source("R/functions/output_helpers.R")
} else {
  # Fallback: simple wrappers if helper missing
  save_figure_multi <- function(p, f, w, h) ggsave(paste0(f, ".png"), p, width=w, height=h)
  warning("R/functions/output_helpers.R not found. Using basic PNG saving.")
}

# Load paths
if (!exists("paths")) {
  paths <- list(
    processed = "data/processed",
    figures = "output/figures",
    tables = "output/tables",
    models = "output/models"
  )
}

# Ensure output directories exist
dir.create(paths$figures, recursive = TRUE, showWarnings = FALSE)
dir.create(paths$tables, recursive = TRUE, showWarnings = FALSE)
dir.create(paths$models, recursive = TRUE, showWarnings = FALSE)

# Load master dataset
master <- readRDS(file.path(paths$processed, "master_analysis_dataset.rds"))

cat(paste("Loaded:", nrow(master), "claims\n"))
cat("\n*** IMPORTANT: All regression results are DESCRIPTIVE ***\n")
cat("*** Coefficients represent conditional correlations, NOT causal effects ***\n\n")

# ============================================================================
# SECTION 1: Data Preparation for Regression
# ============================================================================

cat("Section 1: Preparing Regression Data\n")
cat("--------------------------------------\n")

# Create analysis sample with clean variables
analysis_data <- master %>%
  filter(
    total_cost > 0,
    !is.na(claim_year),
    !is.na(county)
  ) %>%
  mutate(
    # Log cost (main outcome)
    log_cost = log(total_cost),
    
    # Time variables
    claim_year_factor = factor(claim_year),
    
    # Era indicators
    era_factor = factor(era, levels = c("Pre-2000", "2000-2004", "2005-2009", "2010-2014", "2015+")),
    
    # County factor
    county_factor = factor(county),
    
    # DEP Region factor
    region_factor = factor(dep_region),
    
    # Contract type for regression
    contract_type = case_when(
      is_bid_to_result ~ "Bid-to-Result",
      is_sow ~ "Scope of Work",
      has_contract & !is_auction ~ "Other Contract",
      TRUE ~ "No Contract"
    ) %>% factor(levels = c("No Contract", "Scope of Work", "Bid-to-Result", "Other Contract")),
    
    # Closure indicator
    brings_to_closure = ifelse(has_contract, brings_to_closure, NA)
  )

cat(paste("Analysis sample:", nrow(analysis_data), "claims\n"))
cat(paste("Counties:", n_distinct(analysis_data$county), "\n"))
cat(paste("Year range:", min(analysis_data$claim_year), "-", max(analysis_data$claim_year), "\n"))

# ============================================================================
# SECTION 2: Bivariate Correlations
# ============================================================================

cat("\nSection 2: Bivariate Correlations\n")
cat("----------------------------------\n")

# Cost vs. Contract Type
cat("\nMean Cost by Contract Type:\n")
analysis_data %>%
  group_by(contract_type) %>%
  summarise(
    n = n(),
    mean_cost = mean(total_cost),
    median_cost = median(total_cost),
    sd_cost = sd(total_cost),
    mean_log_cost = mean(log_cost),
    .groups = "drop"
  ) %>%
  print()

# Cost vs. Era
cat("\nMean Cost by Era:\n")
analysis_data %>%
  group_by(era_factor) %>%
  summarise(
    n = n(),
    mean_cost = mean(total_cost),
    median_cost = median(total_cost),
    .groups = "drop"
  ) %>%
  print()

# Cost vs. Region
cat("\nMean Cost by DEP Region:\n")
analysis_data %>%
  group_by(region_factor) %>%
  summarise(
    n = n(),
    mean_cost = mean(total_cost),
    median_cost = median(total_cost),
    .groups = "drop"
  ) %>%
  arrange(desc(mean_cost)) %>%
  print()

# ============================================================================
# SECTION 3: Descriptive Regression Models
# ============================================================================

cat("\nSection 3: Descriptive Regression Models\n")
cat("-----------------------------------------\n")
cat("(Conditional correlations, NOT causal effects)\n\n")

# Model 1: Baseline - Contract Type Only
model1 <- feols(
  log_cost ~ contract_type,
  data = analysis_data,
  vcov = "hetero"
)

# Model 2: Add Year Fixed Effects
model2 <- feols(
  log_cost ~ contract_type | claim_year_factor,
  data = analysis_data,
  vcov = "hetero"
)

# Model 3: Add County Fixed Effects
model3 <- feols(
  log_cost ~ contract_type | claim_year_factor + county_factor,
  data = analysis_data,
  vcov = "hetero"
)

# Model 4: Add Region Fixed Effects (alternative geographic control)
model4 <- feols(
  log_cost ~ contract_type | claim_year_factor + region_factor,
  data = analysis_data,
  vcov = "hetero"
)

# Model 5: Era instead of Year Fixed Effects (more parsimonious)
model5 <- feols(
  log_cost ~ contract_type + era_factor,
  data = analysis_data,
  vcov = "hetero"
)

# ============================================================================
# SECTION 4: Regression Table Output (Multi-Format)
# ============================================================================

cat("\nSection 4: Regression Results Table\n")
cat("------------------------------------\n")

# Create coefficient map for cleaner labels
coef_map <- c(
  "contract_typeScope of Work" = "Scope of Work Contract",
  "contract_typeBid-to-Result" = "Bid-to-Result Auction",
  "contract_typeOther Contract" = "Other Contract",
  "era_factor2000-2004" = "Era: 2000-2004",
  "era_factor2005-2009" = "Era: 2005-2009",
  "era_factor2010-2014" = "Era: 2010-2014",
  "era_factor2015+" = "Era: 2015+",
  "(Intercept)" = "Constant"
)

# Generate regression table list
models_list <- list(
  "(1)" = model1,
  "(2)" = model2,
  "(3)" = model3,
  "(4)" = model4,
  "(5)" = model5
)

# Common notes
table_notes <- c(
  "Dependent variable: Log(Total Cost)",
  "Reference category: No Contract",
  "Heteroskedasticity-robust standard errors in parentheses",
  "*** p < 0.01, ** p < 0.05, * p < 0.1",
  "NOTE: Coefficients represent conditional correlations, NOT causal effects"
)

# 1. Save HTML (Best for viewing)
modelsummary(
  models_list,
  stars = c('*' = 0.1, '**' = 0.05, '***' = 0.01),
  coef_map = coef_map,
  gof_map = c("nobs", "r.squared", "adj.r.squared"),
  title = "Correlates of Remediation Costs (Descriptive)",
  notes = table_notes,
  output = file.path(paths$tables, "02_cost_correlates_regression.html")
)
cat("✓ Saved: output/tables/02_cost_correlates_regression.html\n")

# 2. Save LaTeX (For academic appendix)
modelsummary(
  models_list,
  stars = c('*' = 0.1, '**' = 0.05, '***' = 0.01),
  coef_map = coef_map,
  gof_map = c("nobs", "r.squared", "adj.r.squared"),
  title = "Correlates of Remediation Costs (Descriptive)",
  output = file.path(paths$tables, "02_cost_correlates_regression.tex")
)
cat("✓ Saved: output/tables/02_cost_correlates_regression.tex\n")

# 3. Save PDF (Optional - requires TinyTeX)
# modelsummary can output PDF directly via kableExtra
tryCatch({
  modelsummary(
    models_list,
    stars = c('*' = 0.1, '**' = 0.05, '***' = 0.01),
    coef_map = coef_map,
    gof_map = c("nobs", "r.squared", "adj.r.squared"),
    title = "Correlates of Remediation Costs (Descriptive)",
    output = file.path(paths$tables, "02_cost_correlates_regression.pdf")
  )
  cat("✓ Saved: output/tables/02_cost_correlates_regression.pdf\n")
}, error = function(e) {
  cat("! PDF Table generation failed (LaTeX/TinyTeX missing). See .tex file.\n")
})

# Print to console
cat("\nRegression Results (Console View):\n")
etable(model1, model2, model3, model4, model5,
       headers = c("Baseline", "+ Year FE", "+ County FE", "+ Region FE", "Era Specification"))

# ============================================================================
# SECTION 5: Coefficient Visualization
# ============================================================================

cat("\nSection 5: Coefficient Visualization\n")
cat("-------------------------------------\n")

# Extract coefficients from preferred specification (Model 3)
coef_data <- broom::tidy(model3, conf.int = TRUE) %>%
  filter(str_detect(term, "contract_type")) %>%
  mutate(
    term_clean = case_when(
      term == "contract_typeScope of Work" ~ "Scope of Work",
      term == "contract_typeBid-to-Result" ~ "Bid-to-Result",
      term == "contract_typeOther Contract" ~ "Other Contract"
    )
  )

# Figure: Coefficient Plot
fig_coef <- ggplot(coef_data, aes(x = reorder(term_clean, estimate), y = estimate)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
  geom_point(size = 3, color = "#1f77b4") +
  geom_errorbar(aes(ymin = conf.low, ymax = conf.high), width = 0.2, color = "#1f77b4") +
  coord_flip() +
  labs(
    title = "Cost Correlates by Contract Type",
    subtitle = "Relative to claims without contracts (Reference group)\nWith year and county fixed effects",
    x = NULL,
    y = "Log Cost Difference (Conditional Correlation)",
    caption = "Note: 95% confidence intervals shown. These are descriptive correlations, not causal effects."
  )

# Save Figure 5 (Multi-format)
save_figure_multi(
  plot = fig_coef,
  filename = "05_coefficient_plot",
  output_dir = paths$figures,
  width = 8,
  height = 4
)

# ============================================================================
# SECTION 6: Heterogeneity Exploration
# ============================================================================

cat("\nSection 6: Exploring Heterogeneity\n")
cat("-----------------------------------\n")

# Cost distribution by contract type and era
fig_hetero <- analysis_data %>%
  filter(contract_type != "Other Contract") %>%
  ggplot(aes(x = era_factor, y = total_cost, fill = contract_type)) +
  geom_boxplot(alpha = 0.7, outlier.alpha = 0.2) +
  scale_y_log10(labels = dollar_format()) +
  scale_fill_manual(values = c("#7f7f7f", "#1f77b4", "#ff7f0e")) +
  labs(
    title = "Cost Distribution by Contract Type and Era",
    subtitle = "Descriptive comparison across time periods",
    x = "Era",
    y = "Total Cost (Log Scale)",
    fill = "Contract Type"
  ) +
  theme(legend.position = "bottom")

# Save Figure 6 (Multi-format)
save_figure_multi(
  plot = fig_hetero,
  filename = "06_heterogeneity_by_era",
  output_dir = paths$figures,
  width = 10,
  height = 6
)

# Era-specific correlations
cat("\nContract Type Correlations by Era:\n")
for (e in levels(analysis_data$era_factor)) {
  era_data <- analysis_data %>% filter(era_factor == e)
  if (nrow(era_data) > 50) {
    era_model <- feols(log_cost ~ contract_type, data = era_data, vcov = "hetero")
    cat(paste("\n", e, ":\n"))
    print(coef(era_model)[2:4])
  }
}

# ============================================================================
# SECTION 7: Save Model Objects
# ============================================================================

cat("\nSection 7: Saving Model Objects\n")
cat("--------------------------------\n")

# Save all models for potential future use
model_list <- list(
  baseline = model1,
  year_fe = model2,
  county_fe = model3,
  region_fe = model4,
  era_spec = model5
)

saveRDS(model_list, file.path(paths$models, "cost_correlates_models.rds"))
cat("✓ Saved: output/models/cost_correlates_models.rds\n")

# ============================================================================
# SECTION 8: Interpretation Notes
# ============================================================================

cat("\n========================================\n")
cat("INTERPRETATION NOTES\n")
cat("========================================\n")
cat("\n")
cat("KEY CAVEATS (PLEASE READ):\n")
cat("--------------------------\n")
cat("1. These regression results are DESCRIPTIVE, not CAUSAL.\n")
cat("\n")
cat("2. The coefficients represent conditional correlations between\n")
cat("   contract types and remediation costs, controlling for year\n")
cat("   and geographic factors.\n")
cat("\n")
cat("3. Selection into auction/contract procurement is ENDOGENOUS:\n")
cat("   - USTIF selects which claims go to auction\n")
cat("   - Selection criteria include complexity, delays, cost concerns\n")
cat("   - Observed cost differences may reflect site selection, not\n")
cat("     auction treatment effects\n")
cat("\n")
cat("4. These estimates SHOULD NOT be used to claim that auctions\n")
cat("   'cause' higher or lower costs. They only describe patterns\n")
cat("   in the data.\n")
cat("\n")
cat("5. A causal analysis would require:\n")
cat("   - A credible identification strategy (e.g., randomized assignment)\n")
cat("   - Instrumental variables or regression discontinuity design\n")
cat("   - Difference-in-differences with parallel trends validation\n")
cat("\n")

# ============================================================================
# SUMMARY
# ============================================================================

cat("\n========================================\n")
cat("COST CORRELATES ANALYSIS COMPLETE\n")
cat("========================================\n")
cat("\nSummary of Descriptive Findings:\n")

# Extract key coefficients from preferred model
key_coefs <- coef(model3)
cat(paste("\nConditional correlation (vs. No Contract, with Year + County FE):\n"))
cat(paste("  • Scope of Work:", round(key_coefs["contract_typeScope of Work"], 3), 
          "(", round(exp(key_coefs["contract_typeScope of Work"]), 2), "x in levels)\n"))
cat(paste("  • Bid-to-Result:", round(key_coefs["contract_typeBid-to-Result"], 3),
          "(", round(exp(key_coefs["contract_typeBid-to-Result"]), 2), "x in levels)\n"))
cat("\n")
cat("Outputs:\n")
cat("  • Figures: output/figures/ (PNG + PDF)\n")
cat("  • Tables: output/tables/ (HTML + TeX + PDF)\n")
cat("  • Models: output/models/cost_correlates_models.rds\n")
cat("\n")
cat("NEXT STEP:\n")
cat("  Render policy brief: quarto render qmd/policy_brief.qmd\n")
cat("========================================\n\n")