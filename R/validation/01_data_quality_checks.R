# R/validation/01_data_quality_checks.R
# ============================================================================
# Pennsylvania UST Auction Analysis - Data Quality Validation
# ============================================================================
# Purpose: Comprehensive data quality assessment before analysis
# Run after: ETL pipeline (Steps 1-3)
# ============================================================================

cat("\n========================================\n")
cat("Data Quality Validation\n")
cat("========================================\n\n")

# Load dependencies
library(tidyverse)

# Load paths
if (!exists("paths")) {
  paths <- list(
    processed = "data/processed",
    figures = "output/figures",
    tables = "output/tables"
  )
}

# Load master dataset
master <- readRDS(file.path(paths$processed, "master_analysis_dataset.rds"))

# ============================================================================
# CHECK 1: Missing Data Assessment
# ============================================================================

cat("CHECK 1: Missing Data Assessment\n")
cat("---------------------------------\n")

missing_summary <- master %>%
  summarise(across(everything(), ~sum(is.na(.)))) %>%
  pivot_longer(everything(), names_to = "variable", values_to = "n_missing") %>%
  mutate(
    n_total = nrow(master),
    pct_missing = round(n_missing / n_total * 100, 2)
  ) %>%
  arrange(desc(pct_missing)) %>%
  filter(pct_missing > 0)

if (nrow(missing_summary) > 0) {
  cat("\nVariables with missing values:\n")
  print(missing_summary, n = 20)
} else {
  cat("\n✓ No missing values detected in key variables.\n")
}

# ============================================================================
# CHECK 2: Cost Distribution Validation
# ============================================================================

cat("\n\nCHECK 2: Cost Distribution Validation\n")
cat("--------------------------------------\n")

cost_stats <- master %>%
  summarise(
    n = n(),
    n_zero = sum(total_cost == 0),
    n_negative = sum(total_cost < 0),
    min = min(total_cost, na.rm = TRUE),
    p01 = quantile(total_cost, 0.01, na.rm = TRUE),
    p05 = quantile(total_cost, 0.05, na.rm = TRUE),
    p25 = quantile(total_cost, 0.25, na.rm = TRUE),
    median = median(total_cost, na.rm = TRUE),
    mean = mean(total_cost, na.rm = TRUE),
    p75 = quantile(total_cost, 0.75, na.rm = TRUE),
    p95 = quantile(total_cost, 0.95, na.rm = TRUE),
    p99 = quantile(total_cost, 0.99, na.rm = TRUE),
    max = max(total_cost, na.rm = TRUE),
    sd = sd(total_cost, na.rm = TRUE)
  )

cat("\nCost Distribution Summary:\n")
print(as.data.frame(t(cost_stats)))

# Flag extreme outliers (>3 IQR from median)
iqr <- cost_stats$p75 - cost_stats$p25
upper_fence <- cost_stats$p75 + 3 * iqr
lower_fence <- cost_stats$p25 - 3 * iqr

n_outliers <- sum(master$total_cost > upper_fence | master$total_cost < lower_fence)
cat(paste("\nExtreme outliers (>3 IQR):", n_outliers, "claims\n"))

# ============================================================================
# CHECK 3: Temporal Coverage
# ============================================================================

cat("\n\nCHECK 3: Temporal Coverage\n")
cat("---------------------------\n")

temporal_summary <- master %>%
  group_by(claim_year) %>%
  summarise(
    n_claims = n(),
    n_with_contract = sum(has_contract),
    n_auctions = sum(is_auction),
    mean_cost = mean(total_cost, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(claim_year)

cat("\nClaims by Year:\n")
print(temporal_summary, n = 30)

# Check for gaps in temporal coverage
years_present <- sort(unique(master$claim_year))
expected_years <- seq(min(years_present), max(years_present))
missing_years <- setdiff(expected_years, years_present)

if (length(missing_years) > 0) {
  cat(paste("\n⚠ Missing years:", paste(missing_years, collapse = ", "), "\n"))
} else {
  cat("\n✓ No gaps in temporal coverage.\n")
}

# ============================================================================
# CHECK 4: Geographic Distribution
# ============================================================================

cat("\n\nCHECK 4: Geographic Distribution\n")
cat("---------------------------------\n")

county_summary <- master %>%
  group_by(county) %>%
  summarise(
    n_claims = n(),
    pct_claims = round(n() / nrow(master) * 100, 2),
    mean_cost = mean(total_cost, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(n_claims))

cat("\nTop 10 Counties by Claim Count:\n")
print(head(county_summary, 10))

# Check for missing county
n_missing_county <- sum(is.na(master$county) | master$county == "")
cat(paste("\nClaims with missing county:", n_missing_county, "\n"))

# ============================================================================
# CHECK 5: Auction/Contract Data Quality
# ============================================================================

cat("\n\nCHECK 5: Auction/Contract Data Quality\n")
cat("---------------------------------------\n")

contract_summary <- master %>%
  group_by(auction_type_factor) %>%
  summarise(
    n = n(),
    pct = round(n() / nrow(master) * 100, 2),
    mean_cost = mean(total_cost, na.rm = TRUE),
    median_cost = median(total_cost, na.rm = TRUE),
    .groups = "drop"
  )

cat("\nDistribution by Contract Type:\n")
print(contract_summary)

# ============================================================================
# CHECK 6: Date Consistency
# ============================================================================

cat("\n\nCHECK 6: Date Consistency\n")
cat("--------------------------\n")

date_issues <- master %>%
  mutate(
    loss_before_claim = loss_reported_date > claim_date,
    closure_before_claim = closed_date < claim_date,
    contract_before_claim = first_contract_date < claim_date
  ) %>%
  summarise(
    n_loss_after_claim = sum(loss_before_claim, na.rm = TRUE),
    n_closure_before_claim = sum(closure_before_claim, na.rm = TRUE),
    n_contract_before_claim = sum(contract_before_claim, na.rm = TRUE)
  )

cat("\nDate Consistency Issues:\n")
print(as.data.frame(date_issues))

if (sum(date_issues) == 0) {
  cat("\n✓ No date consistency issues detected.\n")
}

# ============================================================================
# CHECK 7: Duplicate Detection
# ============================================================================

cat("\n\nCHECK 7: Duplicate Detection\n")
cat("-----------------------------\n")

n_unique_claims <- n_distinct(master$claim_number)
n_rows <- nrow(master)

cat(paste("Total rows:", n_rows, "\n"))
cat(paste("Unique claim numbers:", n_unique_claims, "\n"))

if (n_rows != n_unique_claims) {
  cat(paste("⚠ Duplicate claim numbers detected:", n_rows - n_unique_claims, "\n"))
} else {
  cat("✓ No duplicate claim numbers.\n")
}

# ============================================================================
# GENERATE VALIDATION REPORT
# ============================================================================

validation_report <- list(
  timestamp = Sys.time(),
  dataset_name = "master_analysis_dataset",
  n_records = nrow(master),
  date_range = c(min(master$claim_date), max(master$claim_date)),
  missing_summary = missing_summary,
  cost_stats = cost_stats,
  temporal_summary = temporal_summary,
  county_summary = county_summary,
  contract_summary = contract_summary,
  date_issues = date_issues,
  n_outliers = n_outliers,
  passed_all_checks = (n_outliers < nrow(master) * 0.05)  # Less than 5% outliers
)

saveRDS(validation_report, file.path(paths$processed, "validation_report.rds"))

# ============================================================================
# SUMMARY
# ============================================================================

cat("\n========================================\n")
cat("VALIDATION COMPLETE\n")
cat("========================================\n")
cat("\nOverall Assessment:\n")
cat(paste("  • Total records:", nrow(master), "\n"))
cat(paste("  • Date range:", min(master$claim_date), "to", max(master$claim_date), "\n"))
cat(paste("  • Missing values in key fields: ", sum(missing_summary$pct_missing > 5), "fields >5% missing\n"))
cat(paste("  • Extreme cost outliers:", n_outliers, "(", round(n_outliers/nrow(master)*100, 2), "%)\n"))
cat(paste("  • Data quality:", ifelse(validation_report$passed_all_checks, "GOOD", "REVIEW RECOMMENDED"), "\n"))
cat("\n")
cat("✓ Saved: data/processed/validation_report.rds\n")
cat("========================================\n\n")
