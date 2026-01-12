# R/analysis/02_cost_drivers_and_hte.R
# ==============================================================================
# Pennsylvania UST Analysis - Step 2: Cost Drivers & Heterogeneous Effects
# ==============================================================================
# Purpose:
#   1. Cost Drivers (Regression): What features predict high baseline costs?
#   2. Causal HTE (Causal Forest): Do Auctions save more money on specific sites?
#      (Uses Best Linear Projection to validate heterogeneity)
# ==============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(fixest)       # Fixed-Effects OLS
  library(grf)          # Generalized Random Forests
  library(modelsummary) # Tables
  library(kableExtra)   # Styling
  library(scales)       # Currency formatting
  library(patchwork)    # Plot layout
  library(here)
})

# ==============================================================================
# 0. CONFIGURATION & PATHS
# ==============================================================================
paths <- list(
  master  = here("data/processed/master_analysis_dataset.rds"),
  tables  = here("output/tables"),
  figures = here("output/figures"),
  models  = here("output/models")
)

walk(paths[-1], ~dir.create(., recursive = TRUE, showWarnings = FALSE))
# ==============================================================================
# 1. DATA PREPARATION
# ==============================================================================
cat("\n--- Loading & Preparing Data ---\n")

master <- readRDS(paths$master)
setDT(master)

# Filter: Real Cost > $1k, Valid Business Category
df_reg <- master[total_paid_real > 1000 & !is.na(business_category)]

# Feature Engineering
# Age Bins for plotting
df_reg[, age_bin := cut(avg_tank_age, breaks = c(seq(0, 40, by = 10), Inf),
                        labels = c("0-10", "11-20", "21-30", "31-40", "40+"),
                        include.lowest = TRUE)]

# Risk Shares Imputation
risk_cols <- c("share_pressure_piping", "share_bare_steel", "share_no_electronic_detection")
for(col in risk_cols) set(df_reg, i = which(is.na(df_reg[[col]])), j = col, value = 0)

# --- CRITICAL FIX: Handle Missing Data for ML ---
# Define variables intended for the model
x_vars <- c("avg_tank_age", "n_tanks_total", risk_cols, 
            "business_category", "region_cluster")

# Filter df_reg to keep only rows where ALL x_vars are observed
# This ensures X and Y stay perfectly aligned
df_reg <- df_reg[complete.cases(df_reg[, ..x_vars])]

cat(sprintf("Final Analysis N: %d (Dropped rows with missing age/characteristics)\n", nrow(df_reg)))

# Create Model Matrix (One-Hot Encoding)
X <- model.matrix(as.formula(paste("~", paste(x_vars, collapse="+"), "-1")), df_reg)

# Define Outcome Y (Must match X dimensions)
Y_cost <- log(df_reg$total_paid_real)

# Train Regression Forest (predicts E[Y|X])
rf_cost <- regression_forest(X, Y_cost, num.trees = 1000, seed = 123)

# Variable Importance
var_imp <- variable_importance(rf_cost)
df_imp <- data.table(Feature = colnames(X), Importance = var_imp[,1])[order(-Importance)]

# Plot Top Drivers
p_imp <- ggplot(head(df_imp, 15), aes(x = reorder(Feature, Importance), y = Importance)) +
  geom_col(fill = "#2c3e50", width = 0.7) +
  coord_flip() +
  labs(title = "What features best predict total cost?", 
       subtitle = "Variable Importance (Regression Forest)", x = NULL, y = "Importance") +
  theme_minimal()

ggsave(file.path(paths$figures, "drivers_ml_importance.png"), p_imp, width = 8, height = 6)

# ==============================================================================
# 3. CAUSAL HETEROGENEITY (BLP ANALYSIS)
# ==============================================================================
cat("\n--- 3. Analyzing Treatment Heterogeneity (The Auction Effect) ---\n")
cat("Hypothesis: Do Auctions (Bid-to-Result) work better for certain sites?\n")

# 3.1 Define Treatment & Propensity
# Treatment: 1 = Bid-to-Result, 0 = Non-Auction (Pooling others for contrast)
# Note: In a rigorous study, you might drop 'Scope of Work' to compare Auction vs No-Contract purely.
W <- as.numeric(df_reg$contract_type == "Bid-to-Result")

# 3.2 Train Causal Forest
# Estimates tau(x) = E[Y(1) - Y(0) | X=x]
# Tuning parameters explicitly for stability
cf <- causal_forest(X, Y_cost, W, 
                    num.trees = 2000, 
                    min.node.size = 10,
                    tune.parameters = "all",
                    seed = 123)

# 3.3 Best Linear Projection (BLP) of the CATE
# This fits: tau(Xi) = beta_0 + beta_1 * X_1 + ... + epsilon
# It tells us if the treatment effect is significantly correlated with features.
blp <- best_linear_projection(cf, X)

# Format BLP Results
blp_res <- as.data.table(blp, keep.rownames = "Feature")
setnames(blp_res, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"), 
         c("estimate", "std_error", "t_stat", "p_value"))

# Filter for significant modifiers (p < 0.1) or top factors
blp_sig <- blp_res[p_value < 0.1 | Feature == "(Intercept)"]

cat("\n--- Best Linear Projection Results ---\n")
print(blp_sig)

# ==============================================================================
# 4. OUTPUTS & INTERPRETATION
# ==============================================================================

# Save BLP Table
# A negative estimate for a feature means: 
# "As this feature increases, the Cost SAVINGS from Auction increase (Cost goes down more)"
# (Assuming the Average Treatment Effect is negative, i.e., Auctions save money)

blp_table <- kable(blp_res, digits = 4, format = "latex", booktabs = TRUE,
      caption = "Best Linear Projection of Auction Treatment Effect") %>%
  kable_styling(latex_options = "hold_position")

writeLines(blp_table, file.path(paths$tables, "304_blp_heterogeneity.tex"))

# Visualizing the HTE Distribution
# Histogram of predicted CATEs (Treatment Effects)
preds <- predict(cf)$predictions
df_reg[, cate := preds]

p_hte <- ggplot(df_reg, aes(x = cate)) +
  geom_histogram(bins = 30, fill = "#e67e22", color = "white", alpha = 0.8) +
  geom_vline(xintercept = mean(df_reg$cate), linetype = "dashed", size = 1) +
  labs(
    title = "Distribution of Auction Treatment Effects",
    subtitle = "Negative values = Cost Savings. Spread = Heterogeneity.",
    x = "Estimated Effect on Log Cost (CATE)",
    y = "Frequency"
  ) +
  theme_minimal()

ggsave(file.path(paths$figures, "304_hte_distribution.png"), p_hte, width = 8, height = 5)

# Visualizing Interaction: Effect vs. Tank Age (if Age was significant in BLP)
p_interact <- ggplot(df_reg, aes(x = avg_tank_age, y = cate)) +
  geom_point(alpha = 0.2, color = "grey50") +
  geom_smooth(method = "loess", color = "#e74c3c") +
  labs(
    title = "Does Tank Age Influence Auction Savings?",
    x = "Average Tank Age",
    y = "Estimated Auction Effect (Log Cost)"
  ) +
  theme_minimal()

ggsave(file.path(paths$figures, "304_hte_by_age.png"), p_interact, width = 8, height = 5)

cat("\nAnalysis Complete. Check output/tables/304_blp_heterogeneity.tex for the BLP results.\n")