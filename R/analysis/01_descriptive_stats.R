# R/analysis/01_descriptive_stats.R
# ============================================================================
# Pennsylvania UST Auction Analysis - Descriptive Statistics
# ============================================================================
# Purpose: Generate comprehensive summary statistics and visualizations
# Note: This is DESCRIPTIVE analysis - no causal claims
# ============================================================================

cat("\n========================================\n")
cat("Descriptive Statistics Analysis\n")
cat("========================================\n\n")

# Load dependencies
library(tidyverse)
library(scales)
library(patchwork)
library(gt)
library(gtsummary)

# Load paths and theme
if (!exists("paths")) {
  paths <- list(
    processed = "data/processed",
    figures = "output/figures",
    tables = "output/tables"
  )
}

# Ensure output directories exist
dir.create(paths$figures, recursive = TRUE, showWarnings = FALSE)
dir.create(paths$tables, recursive = TRUE, showWarnings = FALSE)

# Load master dataset
master <- readRDS(file.path(paths$processed, "master_analysis_dataset.rds"))

cat(paste("Loaded:", nrow(master), "claims\n\n"))

# ============================================================================
# SECTION 1: Overall Cost Distribution
# ============================================================================

cat("Section 1: Cost Distribution Analysis\n")
cat("--------------------------------------\n")

# Summary statistics
cost_summary <- master %>%
  summarise(
    N = n(),
    Mean = mean(total_cost, na.rm = TRUE),
    SD = sd(total_cost, na.rm = TRUE),
    Min = min(total_cost, na.rm = TRUE),
    P25 = quantile(total_cost, 0.25, na.rm = TRUE),
    Median = quantile(total_cost, 0.50, na.rm = TRUE),
    P75 = quantile(total_cost, 0.75, na.rm = TRUE),
    P95 = quantile(total_cost, 0.95, na.rm = TRUE),
    Max = max(total_cost, na.rm = TRUE),
    Total = sum(total_cost, na.rm = TRUE)
  )

cat("\nOverall Cost Summary:\n")
print(cost_summary %>% 
        mutate(across(where(is.numeric), ~format(., big.mark = ",", digits = 0))))

# Figure 1: Cost Distribution (Histogram)
fig1 <- ggplot(master, aes(x = total_cost)) +
  geom_histogram(bins = 50, fill = "#1f77b4", alpha = 0.7, color = "white") +
  scale_x_log10(labels = dollar_format()) +
  geom_vline(aes(xintercept = median(total_cost, na.rm = TRUE)), 
             linetype = "dashed", color = "red", linewidth = 1) +
  annotate("text", x = median(master$total_cost, na.rm = TRUE) * 1.5, 
           y = Inf, vjust = 2, hjust = 0,
           label = paste("Median:", dollar(median(master$total_cost, na.rm = TRUE))),
           color = "red", size = 3.5) +
  labs(
    title = "Distribution of UST Remediation Costs",
    subtitle = "Log scale, USTIF claims 1994-2025",
    x = "Total Cost (Log Scale)",
    y = "Number of Claims",
    caption = "Source: USTIF Administrative Data"
  )

ggsave(file.path(paths$figures, "01_cost_distribution.png"), 
       fig1, width = 8, height = 5, dpi = 300)
cat("✓ Saved: output/figures/01_cost_distribution.png\n")

# ============================================================================
# SECTION 2: Temporal Trends
# ============================================================================

cat("\nSection 2: Temporal Trends\n")
cat("---------------------------\n")

# Annual summary
annual_summary <- master %>%
  group_by(claim_year) %>%
  summarise(
    n_claims = n(),
    total_paid = sum(total_cost, na.rm = TRUE),
    mean_cost = mean(total_cost, na.rm = TRUE),
    median_cost = median(total_cost, na.rm = TRUE),
    n_with_contract = sum(has_contract),
    pct_with_contract = mean(has_contract) * 100,
    .groups = "drop"
  ) %>%
  filter(!is.na(claim_year))

# Figure 2: Claims Over Time
fig2a <- ggplot(annual_summary, aes(x = claim_year, y = n_claims)) +
  geom_col(fill = "#1f77b4", alpha = 0.7) +
  geom_smooth(se = FALSE, color = "red", linewidth = 1, method = "loess") +
  labs(
    title = "Number of UST Claims by Year",
    x = "Year",
    y = "Number of Claims"
  )

# Figure 2b: Cost Trends
fig2b <- ggplot(annual_summary, aes(x = claim_year)) +
  geom_line(aes(y = median_cost, color = "Median"), linewidth = 1) +
  geom_line(aes(y = mean_cost, color = "Mean"), linewidth = 1) +
  scale_y_continuous(labels = dollar_format()) +
  scale_color_manual(values = c("Median" = "#1f77b4", "Mean" = "#ff7f0e")) +
  labs(
    title = "Average Remediation Cost by Year",
    x = "Year",
    y = "Cost ($)",
    color = "Statistic"
  ) +
  theme(legend.position = "bottom")

# Combine
fig2 <- fig2a / fig2b

ggsave(file.path(paths$figures, "02_temporal_trends.png"), 
       fig2, width = 10, height = 8, dpi = 300)
cat("✓ Saved: output/figures/02_temporal_trends.png\n")

# ============================================================================
# SECTION 3: Geographic Distribution
# ============================================================================

cat("\nSection 3: Geographic Distribution\n")
cat("-----------------------------------\n")

# County summary
county_stats <- master %>%
  group_by(county) %>%
  summarise(
    n_claims = n(),
    total_cost = sum(total_cost, na.rm = TRUE),
    mean_cost = mean(total_cost, na.rm = TRUE),
    median_cost = median(total_cost, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(n_claims)) %>%
  mutate(
    pct_claims = n_claims / sum(n_claims) * 100,
    cum_pct = cumsum(pct_claims)
  )

cat("\nTop 15 Counties by Number of Claims:\n")
print(head(county_stats, 15))

# Figure 3: Top Counties
top_counties <- county_stats %>% head(15)

fig3 <- ggplot(top_counties, aes(x = reorder(county, n_claims), y = n_claims)) +
  geom_col(fill = "#1f77b4", alpha = 0.7) +
  geom_text(aes(label = n_claims), hjust = -0.2, size = 3) +
  coord_flip() +
  labs(
    title = "UST Claims by County (Top 15)",
    subtitle = paste0("These counties account for ", 
                      round(sum(top_counties$pct_claims), 1), "% of all claims"),
    x = NULL,
    y = "Number of Claims"
  ) +
  theme(axis.text.y = element_text(size = 9))

ggsave(file.path(paths$figures, "03_county_distribution.png"), 
       fig3, width = 8, height = 6, dpi = 300)
cat("✓ Saved: output/figures/03_county_distribution.png\n")

# ============================================================================
# SECTION 4: Contract/Auction Analysis
# ============================================================================

cat("\nSection 4: Contract/Auction Characteristics\n")
cat("--------------------------------------------\n")

# Contract type summary
contract_stats <- master %>%
  group_by(auction_type_factor) %>%
  summarise(
    n = n(),
    pct = n() / nrow(master) * 100,
    mean_cost = mean(total_cost, na.rm = TRUE),
    median_cost = median(total_cost, na.rm = TRUE),
    sd_cost = sd(total_cost, na.rm = TRUE),
    .groups = "drop"
  )

cat("\nCost Summary by Contract Type:\n")
print(contract_stats)

# Figure 4: Cost by Contract Type (Box Plot)
fig4 <- master %>%
  filter(auction_type_factor != "Other Contract") %>%  # Focus on main categories
  ggplot(aes(x = auction_type_factor, y = total_cost, fill = auction_type_factor)) +
  geom_boxplot(alpha = 0.7, outlier.alpha = 0.3) +
  scale_y_log10(labels = dollar_format()) +
  scale_fill_manual(values = c("#7f7f7f", "#1f77b4", "#ff7f0e")) +
  labs(
    title = "Remediation Costs by Contract Type",
    subtitle = "Note: Descriptive comparison only - not causal",
    x = NULL,
    y = "Total Cost (Log Scale)",
    caption = "Box shows IQR with median; whiskers extend to 1.5 × IQR"
  ) +
  theme(legend.position = "none")

ggsave(file.path(paths$figures, "04_cost_by_contract_type.png"), 
       fig4, width = 8, height = 5, dpi = 300)
cat("✓ Saved: output/figures/04_cost_by_contract_type.png\n")

# ============================================================================
# SECTION 5: Claim Duration Analysis
# ============================================================================

cat("\nSection 5: Claim Duration Analysis\n")
cat("-----------------------------------\n")

# Duration statistics (closed claims only)
duration_stats <- master %>%
  filter(is_closed & !is.na(claim_duration_years)) %>%
  summarise(
    n_closed = n(),
    mean_duration = mean(claim_duration_years, na.rm = TRUE),
    median_duration = median(claim_duration_years, na.rm = TRUE),
    sd_duration = sd(claim_duration_years, na.rm = TRUE),
    min_duration = min(claim_duration_years, na.rm = TRUE),
    max_duration = max(claim_duration_years, na.rm = TRUE)
  )

cat("\nClaim Duration Statistics (Closed Claims):\n")
print(duration_stats)

# Duration by contract type
duration_by_contract <- master %>%
  filter(is_closed & !is.na(claim_duration_years)) %>%
  group_by(auction_type_factor) %>%
  summarise(
    n = n(),
    mean_duration = mean(claim_duration_years, na.rm = TRUE),
    median_duration = median(claim_duration_years, na.rm = TRUE),
    .groups = "drop"
  )

cat("\nDuration by Contract Type:\n")
print(duration_by_contract)

# ============================================================================
# SECTION 6: Summary Statistics Table (for Policy Brief)
# ============================================================================

cat("\nSection 6: Generating Summary Tables\n")
cat("--------------------------------------\n")

# Create publication-ready summary table
summary_table <- master %>%
  select(
    total_cost, 
    has_contract, 
    is_auction,
    claim_duration_years,
    is_closed,
    county,
    dep_region
  ) %>%
  tbl_summary(
    statistic = list(
      all_continuous() ~ "{mean} ({sd}) | {median} [{p25}, {p75}]",
      all_categorical() ~ "{n} ({p}%)"
    ),
    label = list(
      total_cost ~ "Total Remediation Cost ($)",
      has_contract ~ "Has Contract Record",
      is_auction ~ "Procured via Auction",
      claim_duration_years ~ "Claim Duration (Years)",
      is_closed ~ "Claim Closed",
      county ~ "County",
      dep_region ~ "DEP Region"
    ),
    missing = "no"
  ) %>%
  modify_header(label ~ "**Variable**") %>%
  bold_labels()

# Save as HTML
summary_table %>%
  as_gt() %>%
  gtsave(file.path(paths$tables, "01_summary_statistics.html"))

cat("✓ Saved: output/tables/01_summary_statistics.html\n")

# ============================================================================
# SECTION 7: Export Results
# ============================================================================

# Compile all descriptive results
descriptive_results <- list(
  cost_summary = cost_summary,
  annual_summary = annual_summary,
  county_stats = county_stats,
  contract_stats = contract_stats,
  duration_stats = duration_stats,
  duration_by_contract = duration_by_contract,
  timestamp = Sys.time()
)

saveRDS(descriptive_results, file.path(paths$processed, "descriptive_results.rds"))

# ============================================================================
# SUMMARY
# ============================================================================

cat("\n========================================\n")
cat("DESCRIPTIVE ANALYSIS COMPLETE\n")
cat("========================================\n")
cat("\nKey Findings (Descriptive Only):\n")
cat(paste("  • Total claims analyzed:", nrow(master), "\n"))
cat(paste("  • Median remediation cost:", dollar(cost_summary$Median), "\n"))
cat(paste("  • Mean remediation cost:", dollar(cost_summary$Mean), "\n"))
cat(paste("  • Claims with contract data:", sum(master$has_contract), 
          "(", round(mean(master$has_contract) * 100, 1), "%)\n"))
cat(paste("  • Claims procured via auction:", sum(master$is_auction),
          "(", round(mean(master$is_auction) * 100, 1), "%)\n"))
cat("\n")
cat("Outputs:\n")
cat("  • Figures saved to: output/figures/\n")
cat("  • Tables saved to: output/tables/\n")
cat("  • Results saved to: data/processed/descriptive_results.rds\n")
cat("\n")
cat("NEXT STEP:\n")
cat("  Run: source('R/analysis/02_cost_correlates.R')\n")
cat("========================================\n\n")
