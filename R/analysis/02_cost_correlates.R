# R/analysis/02_cost_correlates.R
# ==============================================================================
# Pennsylvania UST Analysis - Auction Mechanics & Procurement Design
# ==============================================================================
# PURPOSE: Evaluate the efficacy of the current auction system (Descriptive)
#
# SECTIONS (from 1/16 Policy Brief Goals):
#   2) AUCTION MECHANICS & PROCUREMENT
#      2.1) Intervention Timing: When does USTIF send claims to auction?
#      2.2) Costs of Work in Auctions: Contract vs ALAE vs Loss breakdown
#      2.3) Procurement Design Comparison: PFP vs SOW vs T&M
#      2.4) Contract Trends Over Time
#
# NOTE: This script is DESCRIPTIVE. It documents correlations and patterns
#       but does NOT establish causality. See 03_causal_inference.R for IV/DML.
#
# OUTPUTS:
#   - Tables: output/tables/2XX_*.{html,tex}
#   - Figures: output/figures/2XX_*.{png,pdf}
# ==============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(modelsummary)
  library(kableExtra)
  library(ggplot2)
  library(scales)
  library(patchwork)
  library(stringr)
  library(here)
})

# ==============================================================================
# 0. SETUP
# ==============================================================================
paths <- list(
  master    = here("data/processed/master_analysis_dataset.rds"),
  contracts = here("data/processed/contracts_with_real_values.rds"),
  tables    = here("output/tables"),
  figures   = here("output/figures")
)

dir.create(paths$tables, recursive = TRUE, showWarnings = FALSE)
dir.create(paths$figures, recursive = TRUE, showWarnings = FALSE)

save_table <- function(tbl, name) {
  writeLines(as.character(tbl), file.path(paths$tables, paste0(name, ".html")))
  message(sprintf("Saved: %s.html", name))
}

save_figure <- function(p, name, width = 9, height = 6) {
  p_clean <- p + 
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold", size = 14),
      axis.title = element_text(face = "bold"),
      legend.position = "bottom",
      panel.grid.minor = element_blank()
    )
  ggsave(file.path(paths$figures, paste0(name, ".png")), p_clean, 
         width = width, height = height, bg = "white")
  ggsave(file.path(paths$figures, paste0(name, ".pdf")), p_clean, 
         width = width, height = height, bg = "white")
  message(sprintf("Saved: %s.{png,pdf}", name))
}

# ==============================================================================
# 1. LOAD DATA
# ==============================================================================
message("\n--- Loading Data ---")

master <- readRDS(paths$master)
setDT(master)

contracts <- readRDS(paths$contracts)
setDT(contracts)

# Merge claim info to contracts
contracts <- merge(contracts, 
                   master[, .(claim_number, dep_region, county, claim_date, 
                              total_paid_real, paid_alae_real, claim_duration_days,
                              avg_tank_age, n_tanks_total, business_category)],
                   by = "claim_number", all.x = TRUE)

# Create auction type factor
master[, auction_type_factor := factor(contract_type, 
                                        levels = c("No Contract", "Other Contract", 
                                                   "Scope of Work", "Bid-to-Result"))]

# Filter to claims with contracts for auction analysis
auction_claims <- master[contract_type != "No Contract" & total_paid_real > 1000]

message(sprintf("Master: %d claims | With Contracts: %d | Contracts Table: %d",
                nrow(master), nrow(auction_claims), nrow(contracts)))

# ==============================================================================
# SECTION 2.1: INTERVENTION TIMING
# ==============================================================================
message("\n--- 2.1 Intervention Timing ---")

# When does USTIF intervene with an auction?
# intervention_lag_days = date_first_contract - claim_date

intervention_summary <- auction_claims[!is.na(intervention_lag_days) & 
                                         intervention_lag_days > 0, .(
  N = .N,
  Mean_Days = mean(intervention_lag_days),
  Median_Days = median(intervention_lag_days),
  SD_Days = sd(intervention_lag_days),
  P25_Days = quantile(intervention_lag_days, 0.25),
  P75_Days = quantile(intervention_lag_days, 0.75),
  Mean_Years = mean(intervention_lag_days) / 365,
  Median_Years = median(intervention_lag_days) / 365
), by = contract_type]

intervention_tbl <- kbl(intervention_summary, digits = 1, format = "html",
                        caption = "Time from Claim to First Contract (Days)") %>%
  kable_styling()

save_table(intervention_tbl, "201_intervention_timing_summary")

# Distribution plot
p_intervention <- ggplot(auction_claims[intervention_lag_days > 0 & 
                                          intervention_lag_days < 5000], 
                          aes(x = intervention_lag_days / 365)) +
  geom_histogram(bins = 40, fill = "#3498db", alpha = 0.8, color = "white") +
  geom_vline(aes(xintercept = median(intervention_lag_days, na.rm = TRUE) / 365),
             linetype = "dashed", color = "#e74c3c", linewidth = 1) +
  labs(title = "Time to Auction Intervention",
       subtitle = sprintf("Median: %.1f years (dashed line)", 
                          median(auction_claims$intervention_lag_days, na.rm = TRUE) / 365),
       x = "Years from Claim to First Contract",
       y = "Number of Claims")

save_figure(p_intervention, "201_intervention_timing_histogram")

# By contract type
p_intervention_by_type <- ggplot(auction_claims[intervention_lag_days > 0 & 
                                                  intervention_lag_days < 5000],
                                  aes(x = intervention_lag_days / 365, 
                                      fill = contract_type)) +
  geom_density(alpha = 0.5) +
  scale_fill_viridis_d(option = "mako", begin = 0.2, end = 0.8) +
  labs(title = "Intervention Timing by Contract Type",
       x = "Years from Claim to First Contract",
       y = "Density", fill = "Contract Type")

save_figure(p_intervention_by_type, "202_intervention_by_type")

# What predicts going to auction?
auction_pred <- feols(
  I(contract_type == "Bid-to-Result") ~ avg_tank_age + n_tanks_total + 
    share_bare_steel + share_pressure_piping | dep_region,
  data = master[total_paid_real > 1000 & !is.na(avg_tank_age)],
  cluster = "dep_region"
)

pred_tbl <- modelsummary(
  list("Pr(Bid-to-Result)" = auction_pred),
  coef_map = c(
    "avg_tank_age" = "Tank Age",
    "n_tanks_total" = "N Tanks",
    "share_bare_steel" = "Share: Bare Steel",
    "share_pressure_piping" = "Share: Pressure Piping"
  ),
  stars = c('*' = .1, '**' = .05, '***' = .01),
  output = "kableExtra"
) %>% kable_styling()

save_table(pred_tbl, "203_auction_predictors_lpm")

# ==============================================================================
# SECTION 2.2: COSTS OF WORK IN AUCTIONS
# ==============================================================================
message("\n--- 2.2 Cost Breakdown ---")

# How much of claim cost is contract work vs ALAE vs other loss?
cost_breakdown <- auction_claims[, .(
  claim_number,
  total_paid = total_paid_real,
  contract_value = total_contract_value_real,
  alae = paid_alae_real,
  loss = paid_loss_real
)]

cost_breakdown[, `:=`(
  contract_share = contract_value / total_paid,
  alae_share = alae / total_paid,
  loss_share = loss / total_paid,
  other = total_paid - contract_value - alae
)]

# Cap shares at 1 for display
cost_breakdown[contract_share > 1, contract_share := 1]
cost_breakdown[alae_share > 1, alae_share := 1]

cost_shares_summary <- cost_breakdown[, .(
  N = .N,
  Mean_Contract_Share = mean(contract_share, na.rm = TRUE),
  Median_Contract_Share = median(contract_share, na.rm = TRUE),
  Mean_ALAE_Share = mean(alae_share, na.rm = TRUE),
  Mean_Total_Paid = mean(total_paid, na.rm = TRUE),
  Mean_Contract_Value = mean(contract_value, na.rm = TRUE)
)]

cost_breakdown_tbl <- kbl(cost_shares_summary, digits = 2, format = "html",
                          caption = "Cost Component Breakdown (Auction Claims)") %>%
  kable_styling()

save_table(cost_breakdown_tbl, "204_cost_breakdown_summary")

# Scatter: Contract Value vs Total Paid
p_cost_scatter <- ggplot(auction_claims[total_contract_value_real > 0], 
                          aes(x = total_contract_value_real, y = total_paid_real)) +
  geom_point(alpha = 0.4, color = "#3498db") +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "#e74c3c") +
  scale_x_log10(labels = dollar_format()) +
  scale_y_log10(labels = dollar_format()) +
  labs(title = "Contract Value vs Total Claim Cost",
       subtitle = "Dashed line = 45° (Contract = Total Cost)",
       x = "Total Contract Value (Real $)",
       y = "Total Claim Cost (Real $)")

save_figure(p_cost_scatter, "204_contract_vs_total_scatter")

# ==============================================================================
# SECTION 2.3: PROCUREMENT DESIGN COMPARISON
# ==============================================================================
message("\n--- 2.3 Procurement Design Comparison ---")

# Descriptive comparison: PFP vs SOW vs Other
procurement_summary <- master[contract_type != "No Contract" & total_paid_real > 1000, .(
  N_Claims = .N,
  Mean_Cost_Real = mean(total_paid_real, na.rm = TRUE),
  Median_Cost_Real = median(total_paid_real, na.rm = TRUE),
  Mean_Duration_Days = mean(claim_duration_days, na.rm = TRUE),
  Median_Duration_Days = median(claim_duration_days, na.rm = TRUE),
  Mean_Intervention_Days = mean(intervention_lag_days, na.rm = TRUE),
  Mean_Tank_Age = mean(avg_tank_age, na.rm = TRUE)
), by = contract_type][order(-N_Claims)]

proc_tbl <- kbl(procurement_summary, digits = 0, format = "html",
                caption = "Procurement Design Comparison (Descriptive)") %>%
  kable_styling() %>%
  footnote(general = "WARNING: These are RAW comparisons. Selection bias likely present.")

save_table(proc_tbl, "205_procurement_comparison")

# Boxplot comparison
p_cost_by_type <- ggplot(master[contract_type != "No Contract" & total_paid_real > 100],
                          aes(x = auction_type_factor, y = total_paid_real, 
                              fill = auction_type_factor)) +
  geom_boxplot(outlier.alpha = 0.2) +
  scale_y_log10(labels = dollar_format()) +
  scale_fill_viridis_d(option = "mako", begin = 0.2, end = 0.8) +
  labs(title = "Total Cost by Contract Type",
       subtitle = "CAUTION: Selection bias - complex sites go to auction",
       x = NULL, y = "Total Cost (Real $, Log Scale)") +
  theme(legend.position = "none")

save_figure(p_cost_by_type, "205_cost_by_contract_type")

# Duration comparison
p_duration_by_type <- ggplot(master[contract_type != "No Contract" & 
                                      claim_duration_days > 0 & 
                                      claim_duration_days < 10000],
                              aes(x = auction_type_factor, y = claim_duration_days / 365,
                                  fill = auction_type_factor)) +
  geom_boxplot(outlier.alpha = 0.2) +
  scale_fill_viridis_d(option = "plasma", begin = 0.2, end = 0.8) +
  labs(title = "Claim Duration by Contract Type",
       x = NULL, y = "Duration (Years)") +
  theme(legend.position = "none")

save_figure(p_duration_by_type, "206_duration_by_contract_type")

# Naive regression (WITH CAVEATS)
naive_reg <- feols(
  log(total_paid_real) ~ contract_type + avg_tank_age + n_tanks_total + 
    share_bare_steel | dep_region,
  data = master[total_paid_real > 1000 & !is.na(avg_tank_age)],
  cluster = "dep_region"
)

naive_tbl <- modelsummary(
  list("Log(Total Cost) - NAIVE" = naive_reg),
  coef_map = c(
    "contract_typeScope of Work" = "Scope of Work (vs No Contract)",
    "contract_typeBid-to-Result" = "Bid-to-Result (vs No Contract)",
    "contract_typeOther Contract" = "Other Contract (vs No Contract)",
    "avg_tank_age" = "Tank Age",
    "n_tanks_total" = "N Tanks",
    "share_bare_steel" = "Share: Bare Steel"
  ),
  stars = c('*' = .1, '**' = .05, '***' = .01),
  output = "kableExtra"
) %>% kable_styling() %>%
  footnote(general = "CAUTION: OLS coefficient on contract type is NOT causal. Selection bias.")

save_table(naive_tbl, "207_naive_cost_regression")

# ==============================================================================
# SECTION 2.4: CONTRACT TRENDS OVER TIME
# ==============================================================================
message("\n--- 2.4 Contract Trends ---")

# Contract type usage over time
contract_trends <- contracts[!is.na(Year) & Year >= 2000 & Year <= 2024, .(
  N_Contracts = .N,
  Total_Value = sum(total_contract_value_real, na.rm = TRUE)
), by = .(Year, auction_type)]

contract_trends_wide <- dcast(contract_trends, Year ~ auction_type, 
                               value.var = "N_Contracts", fill = 0)

# Stacked area plot
p_contract_trend <- ggplot(contract_trends[!is.na(auction_type)], 
                            aes(x = Year, y = N_Contracts, fill = auction_type)) +
  geom_area(alpha = 0.8) +
  scale_fill_viridis_d(option = "mako", begin = 0.2, end = 0.9) +
  labs(title = "Contract Types Over Time",
       x = "Year", y = "Number of Contracts", fill = "Auction Type")

save_figure(p_contract_trend, "208_contract_trends_stacked")

# Line plot
p_contract_lines <- ggplot(contract_trends[!is.na(auction_type) & auction_type != ""], 
                            aes(x = Year, y = N_Contracts, color = auction_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  scale_color_viridis_d(option = "mako", begin = 0.2, end = 0.9) +
  labs(title = "Contract Types Over Time",
       x = "Year", y = "Number of Contracts", color = "Auction Type")

save_figure(p_contract_lines, "209_contract_trends_lines")

# PFP share over time
pfp_share_trend <- contracts[Year >= 2000 & Year <= 2024, .(
  N_Total = .N,
  N_PFP = sum(auction_type == "Bid-to-Result", na.rm = TRUE),
  Share_PFP = mean(auction_type == "Bid-to-Result", na.rm = TRUE)
), by = Year]

p_pfp_share <- ggplot(pfp_share_trend, aes(x = Year, y = Share_PFP)) +
  geom_line(color = "#e74c3c", linewidth = 1) +
  geom_point(color = "#e74c3c", size = 2) +
  scale_y_continuous(labels = percent_format()) +
  labs(title = "Share of Bid-to-Result (PFP) Contracts Over Time",
       x = "Year", y = "Share PFP")

save_figure(p_pfp_share, "210_pfp_share_trend")

# ==============================================================================
# SELECTION BIAS DIAGNOSTIC
# ==============================================================================
message("\n--- Selection Bias Diagnostic ---")

# Compare characteristics: Auction vs Non-Auction claims
selection_comparison <- master[total_paid_real > 1000, .(
  N = .N,
  Mean_Cost = mean(total_paid_real, na.rm = TRUE),
  Mean_Age = mean(avg_tank_age, na.rm = TRUE),
  Mean_Tanks = mean(n_tanks_total, na.rm = TRUE),
  Share_BareSteel = mean(share_bare_steel, na.rm = TRUE),
  Mean_Duration = mean(claim_duration_days, na.rm = TRUE)
), by = .(Has_Auction = contract_type != "No Contract")]

selection_tbl <- kbl(selection_comparison, digits = 2, format = "html",
                     caption = "Selection Bias Evidence: Auction vs Non-Auction Claims") %>%
  kable_styling() %>%
  footnote(general = "Auction claims are systematically different - older tanks, higher costs")

save_table(selection_tbl, "211_selection_bias_evidence")

# ==============================================================================
# SUMMARY
# ==============================================================================
message("\n========================================")
message("ANALYSIS 02 COMPLETE: Auction Mechanics")
message("========================================")
message("KEY FINDINGS (DESCRIPTIVE ONLY):")
message(sprintf("  - Median Intervention Lag: %.1f years", 
                median(auction_claims$intervention_lag_days, na.rm = TRUE) / 365))
message(sprintf("  - PFP Claims: N=%d, Mean Cost=$%s",
                sum(master$contract_type == "Bid-to-Result"),
                format(mean(master[contract_type == "Bid-to-Result", total_paid_real], na.rm = TRUE), 
                       big.mark = ",", nsmall = 0)))
message("\nCAUTION: All comparisons subject to selection bias.")
message("See 03_causal_inference.R for IV/DML treatment effects.")
