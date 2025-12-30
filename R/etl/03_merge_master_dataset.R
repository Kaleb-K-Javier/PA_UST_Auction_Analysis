# R/etl/03_merge_master_dataset.R
# ============================================================================
# Pennsylvania UST Auction Analysis - ETL Step 3: Construct Master Dataset
# ============================================================================
# Purpose: Merge USTIF data sources into unified analysis panel
# Input: Cleaned .rds files from Step 1
# Output: Master analysis dataset
# ============================================================================

cat("\n========================================\n")
cat("ETL Step 3: Constructing Master Dataset\n")
cat("========================================\n\n")

# Load dependencies
library(tidyverse)
library(lubridate)

# Load paths
if (!exists("paths")) {
  paths <- list(
    processed = "data/processed",
    external = "data/external"
  )
}

# ============================================================================
# LOAD CLEANED DATASETS
# ============================================================================

cat("Loading cleaned datasets...\n")

# Load USTIF data (required)
contracts <- readRDS(file.path(paths$processed, "contracts_clean.rds"))
tanks <- readRDS(file.path(paths$processed, "tanks_clean.rds"))
claims <- readRDS(file.path(paths$processed, "claims_clean.rds"))

cat(paste("  • Contracts:", nrow(contracts), "records\n"))
cat(paste("  • Tanks:", nrow(tanks), "records\n"))
cat(paste("  • Claims:", nrow(claims), "records\n"))

# Load PA DEP data (optional - may not exist)
tanks_padep <- NULL
cleanup_padep <- NULL

padep_tanks_path <- file.path(paths$external, "padep_tanks_raw.rds")
padep_cleanup_path <- file.path(paths$external, "padep_cleanup_raw.rds")

if (file.exists(padep_tanks_path)) {
  tanks_padep <- readRDS(padep_tanks_path)
  cat(paste("  • PA DEP Tanks:", nrow(tanks_padep), "records (optional)\n"))
}

if (file.exists(padep_cleanup_path)) {
  cleanup_padep <- readRDS(padep_cleanup_path)
  cat(paste("  • PA DEP Cleanup:", nrow(cleanup_padep), "records (optional)\n"))
}

cat("\n")

# ============================================================================
# STEP 1: Create Claims-Level Master Dataset
# ============================================================================
# Primary unit of analysis: Individual claims
# Enriched with contract/auction information and facility characteristics

cat("Step 1: Building claims-level master dataset...\n")

# Identify merge keys between datasets
# Claims use: claim_number
# Contracts use: claim_number
# Tanks use: facility_id (need to link via claims if possible)

# First, merge claims with contracts
master_claims <- claims %>%
  left_join(
    contracts %>%
      # Keep relevant contract fields
      select(
        claim_number,
        contract_id,
        auction_type,
        bid_type,
        contract_type,
        contract_category,
        base_price,
        amendments_total,
        total_contract_value,
        paid_to_date,
        contract_start,
        contract_end,
        brings_to_closure_flag,
        consultant,
        adjuster,
        site_name
      ),
    by = "claim_number",
    relationship = "many-to-many"  # One claim may have multiple contracts
  )

cat(paste("  • Claims merged with contracts:", nrow(master_claims), "records\n"))

# Check merge rate
claims_with_contract <- master_claims %>%
  filter(!is.na(contract_id)) %>%
  distinct(claim_number) %>%
  nrow()

cat(paste("  • Claims with contract data:", claims_with_contract, "/", 
          n_distinct(claims$claim_number), 
          "(", round(claims_with_contract / n_distinct(claims$claim_number) * 100, 1), "%)\n"))

# ============================================================================
# STEP 2: Create Analysis Variables
# ============================================================================

cat("\nStep 2: Creating analysis variables...\n")

master_claims <- master_claims %>%
  mutate(
    # ---- Cost Variables ----
    # Total cost = max of incurred loss and total paid
    total_cost = pmax(coalesce(incurred_loss, 0), coalesce(total_paid, 0), na.rm = TRUE),
    
    # Log transformation (add $1 to handle zeros)
    log_total_cost = log(total_cost + 1),
    
    # Cost categories
    cost_category = cut(
      total_cost,
      breaks = c(0, 10000, 50000, 100000, 250000, 500000, Inf),
      labels = c("$0-10K", "$10-50K", "$50-100K", "$100-250K", "$250-500K", "$500K+"),
      include.lowest = TRUE
    ),
    
    # ---- Auction/Contract Variables ----
    # Has any contract (auction or otherwise)
    has_contract = !is.na(contract_id),
    
    # Auction type indicators
    is_auction = has_contract & auction_type %in% c("Scope of Work", "Bid-to-Result"),
    is_sow = auction_type == "Scope of Work",
    is_bid_to_result = auction_type == "Bid-to-Result",
    
    # Contract completion
    contract_complete = !is.na(contract_end),
    
    # ---- Temporal Variables ----
    # Fiscal year (July 1 - June 30)
    fiscal_year = ifelse(month(claim_date) >= 7, year(claim_date) + 1, year(claim_date)),
    
    # Era groupings for trend analysis
    era = case_when(
      claim_year < 2000 ~ "Pre-2000",
      claim_year >= 2000 & claim_year < 2005 ~ "2000-2004",
      claim_year >= 2005 & claim_year < 2010 ~ "2005-2009",
      claim_year >= 2010 & claim_year < 2015 ~ "2010-2014",
      claim_year >= 2015 ~ "2015+",
      TRUE ~ "Unknown"
    ),
    
    # ---- Geographic Variables ----
    # Clean county (already done, but ensure consistency)
    county_clean = str_to_title(str_trim(county)),
    
    # DEP region factor
    dep_region_factor = factor(dep_region)
  )

cat("  • Analysis variables created\n")

# ============================================================================
# STEP 3: Aggregate to Claim Level (One Row per Claim)
# ============================================================================

cat("\nStep 3: Aggregating to claim level...\n")

# Since a claim can have multiple contracts, we need to decide how to aggregate
# Strategy: Keep one row per claim, summarize contract info

master_claims_agg <- master_claims %>%
  group_by(claim_number) %>%
  summarise(
    # ---- Core Claim Info (first occurrence) ----
    department = first(department),
    claimant_name = first(claimant_name),
    county = first(county_clean),
    dep_region = first(dep_region),
    location_desc = first(location_desc),
    products = first(products),
    
    # ---- Dates ----
    claim_date = first(claim_date),
    loss_reported_date = first(loss_reported_date),
    closed_date = first(closed_date),
    claim_year = first(claim_year),
    fiscal_year = first(fiscal_year),
    era = first(era),
    
    # ---- Claim Status ----
    claim_status = first(claim_status),
    is_closed = first(is_closed),
    is_open = first(is_open),
    claim_duration_years = first(claim_duration_years),
    
    # ---- Cost Variables ----
    paid_loss = first(paid_loss),
    paid_alae = first(paid_alae),
    incurred_loss = first(incurred_loss),
    total_paid = first(total_paid),
    total_cost = first(total_cost),
    log_total_cost = first(log_total_cost),
    cost_category = first(cost_category),
    
    # ---- Contract/Auction Variables (aggregated) ----
    n_contracts = sum(!is.na(contract_id)),
    has_contract = any(!is.na(contract_id)),
    is_auction = any(is_auction, na.rm = TRUE),
    is_sow = any(is_sow, na.rm = TRUE),
    is_bid_to_result = any(is_bid_to_result, na.rm = TRUE),
    brings_to_closure = any(brings_to_closure_flag, na.rm = TRUE),
    
    # Contract cost info (sum across contracts)
    total_base_price = sum(base_price, na.rm = TRUE),
    total_amendments = sum(amendments_total, na.rm = TRUE),
    total_contract_value = sum(total_contract_value, na.rm = TRUE),
    
    # First contract date
    first_contract_date = min(contract_start, na.rm = TRUE),
    
    # Site name (from contract if available)
    site_name = first(na.omit(site_name)),
    
    .groups = "drop"
  ) %>%
  # Clean up Inf values from min/sum on empty sets
  mutate(
    first_contract_date = if_else(is.infinite(first_contract_date), NA_Date_, first_contract_date),
    total_base_price = if_else(total_base_price == 0 & !has_contract, NA_real_, total_base_price),
    total_contract_value = if_else(total_contract_value == 0 & !has_contract, NA_real_, total_contract_value)
  )

cat(paste("  • Aggregated to:", nrow(master_claims_agg), "unique claims\n"))

# ============================================================================
# STEP 4: Link Facility/Tank Information
# ============================================================================

cat("\nStep 4: Linking facility information...\n")

# The tank data uses PF_OTHER_ID (facility_id), claims use claim_number
# We may not have a direct link - explore potential linkage via site_name or claimant_name

# For now, create facility-level summary from tanks data
facility_summary <- tanks %>%
  group_by(facility_id) %>%
  summarise(
    facility_name = first(facility_name),
    client_name = first(client_name),
    region = first(region),
    n_tanks = n(),
    total_capacity = sum(capacity_gallons, na.rm = TRUE),
    min_install_year = min(install_year, na.rm = TRUE),
    max_install_year = max(install_year, na.rm = TRUE),
    mean_tank_age = mean(tank_age_years, na.rm = TRUE),
    pct_single_wall = mean(is_single_wall, na.rm = TRUE),
    pct_double_wall = mean(is_double_wall, na.rm = TRUE),
    pct_fiberglass = mean(is_fiberglass, na.rm = TRUE),
    .groups = "drop"
  )

cat(paste("  • Facility summary:", nrow(facility_summary), "unique facilities\n"))

# Note: Direct facility-claim linkage requires identifier matching
# This would typically be done via facility_id in claims or fuzzy name matching
# For now, we'll flag this as a data limitation

cat("  NOTE: Direct facility-claim linkage not available in current data structure.\n")
cat("        Analysis will proceed at claim level without tank-level attributes.\n")

# ============================================================================
# STEP 5: Create Final Analysis Dataset
# ============================================================================

cat("\nStep 5: Finalizing analysis dataset...\n")

# Final cleaning and validation
analysis_dataset <- master_claims_agg %>%
  # Remove claims with zero or missing cost (if any)
  filter(total_cost > 0) %>%
  # Ensure valid dates
  filter(!is.na(claim_date)) %>%
  # Create additional analysis flags
  mutate(
    # Valid for cost analysis
    valid_for_cost_analysis = total_cost > 0 & !is.na(total_cost),
    
    # Valid for auction analysis
    valid_for_auction_analysis = has_contract,
    
    # Auction type factor for modeling
    auction_type_factor = case_when(
      is_bid_to_result ~ "Bid-to-Result",
      is_sow ~ "Scope of Work",
      has_contract ~ "Other Contract",
      TRUE ~ "No Contract"
    ) %>% factor(levels = c("No Contract", "Scope of Work", "Bid-to-Result", "Other Contract"))
  )

cat(paste("  • Final analysis dataset:", nrow(analysis_dataset), "claims\n"))

# ============================================================================
# STEP 6: Generate Merge Diagnostics
# ============================================================================

cat("\nStep 6: Generating merge diagnostics...\n")

merge_diagnostics <- tibble(
  dataset = c("Claims", "Contracts", "Tanks"),
  source_records = c(
    nrow(claims),
    nrow(contracts),
    nrow(tanks)
  ),
  unique_key = c(
    n_distinct(claims$claim_number),
    n_distinct(contracts$claim_number),
    n_distinct(tanks$facility_id)
  ),
  merged_records = c(
    nrow(analysis_dataset),
    sum(analysis_dataset$has_contract),
    NA  # Tank linkage not implemented
  ),
  match_rate = c(
    100,
    round(sum(analysis_dataset$has_contract) / nrow(analysis_dataset) * 100, 1),
    NA
  )
)

print(merge_diagnostics)

# ============================================================================
# STEP 7: Save Outputs
# ============================================================================

cat("\nStep 7: Saving datasets...\n")

# Save main analysis dataset
saveRDS(analysis_dataset, file.path(paths$processed, "master_analysis_dataset.rds"))
write_csv(analysis_dataset, file.path(paths$processed, "master_analysis_dataset.csv"))

# Save facility summary (for potential future use)
saveRDS(facility_summary, file.path(paths$processed, "facility_summary.rds"))

# Save merge diagnostics
saveRDS(merge_diagnostics, file.path(paths$processed, "merge_diagnostics.rds"))

cat("✓ Saved: data/processed/master_analysis_dataset.rds\n")
cat("✓ Saved: data/processed/master_analysis_dataset.csv\n")
cat("✓ Saved: data/processed/facility_summary.rds\n")
cat("✓ Saved: data/processed/merge_diagnostics.rds\n")

# ============================================================================
# SUMMARY
# ============================================================================

cat("\n========================================\n")
cat("ETL STEP 3 COMPLETE\n")
cat("========================================\n")
cat("\nMaster Analysis Dataset Summary:\n")
cat(paste("  • Total claims:", nrow(analysis_dataset), "\n"))
cat(paste("  • Claims with contracts:", sum(analysis_dataset$has_contract), 
          "(", round(mean(analysis_dataset$has_contract) * 100, 1), "%)\n"))
cat(paste("  • Auction claims:", sum(analysis_dataset$is_auction), 
          "(", round(mean(analysis_dataset$is_auction) * 100, 1), "%)\n"))
cat(paste("  • Date range:", min(analysis_dataset$claim_date), "to", 
          max(analysis_dataset$claim_date), "\n"))
cat(paste("  • Total paid (all claims): $", 
          format(sum(analysis_dataset$total_cost, na.rm = TRUE), big.mark = ","), "\n"))
cat(paste("  • Median cost per claim: $", 
          format(median(analysis_dataset$total_cost, na.rm = TRUE), big.mark = ","), "\n"))
cat("\n")
cat("NEXT STEPS:\n")
cat("  • Run: source('R/validation/01_data_quality_checks.R')\n")
cat("  • Run: source('R/analysis/01_descriptive_stats.R')\n")
cat("========================================\n\n")
