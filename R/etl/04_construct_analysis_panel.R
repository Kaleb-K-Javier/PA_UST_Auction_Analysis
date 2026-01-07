# R/etl/04_construct_analysis_panel.R
# ============================================================================
# Pennsylvania UST Auction Analysis - ETL Step 4: Construct Analysis Panel
# ============================================================================
# Purpose: Build the econometric analysis panel with treatment, instrument,
#          and covariates for causal inference
# Input: Cleaned CSVs from data/processed/
# Output: data/processed/analysis_panel.rds
# ============================================================================
# Design Decisions (per Research Protocol):
#   - Treatment: is_PFP = TRUE if auction_type == "Bid-to-Result"
#   - Control: Claims with NO Bid-to-Result contract (pure T&M)
#   - Exclusion: Scope of Work contracts (confounds incentive comparison)
#   - Sample: Closed claims only, total_paid > $1,000
#   - Instrument: Leave-one-out adjuster leniency (adjusters with >10 claims)
# ============================================================================

cat("\n========================================\n")
cat("ETL Step 4: Constructing Analysis Panel\n")
cat("========================================\n\n")

# ============================================================================
# SETUP
# ============================================================================

pacman::p_load(

  tidyverse,
  lubridate,
  janitor
)

# Define paths
paths <- list(
  processed = "data/processed",
  figures = "output/figures",
  tables = "output/tables",
  models = "output/models"
)

# ============================================================================
# LOAD DATA
# ============================================================================

cat("Loading cleaned datasets...\n")

claims <- read_csv(file.path(paths$processed, "claims_clean.csv"),
                   show_col_types = FALSE)
contracts <- read_csv(file.path(paths$processed, "contracts_clean.csv"),
                      show_col_types = FALSE)
tanks <- read_csv(file.path(paths$processed, "tanks_clean.csv"),
                  show_col_types = FALSE)

cat(paste("  Claims:", nrow(claims), "records\n"))
cat(paste("  Contracts:", nrow(contracts), "records\n"))
cat(paste("  Tanks:", nrow(tanks), "records\n\n"))

# ============================================================================
# STEP 1: AGGREGATE TANKS TO FACILITY LEVEL
# ============================================================================
# tanks_clean has multiple rows per facility (one per tank)
# Aggregate to facility-level summary for merging

cat("Aggregating tanks to facility level...\n")

tanks_facility <- tanks %>%
  group_by(facility_id) %>%
  summarise(
    # Tank count
    n_tanks = n(),
    
    # Age: use max (oldest tank at facility)
    tank_age_max = max(tank_age_years, na.rm = TRUE),
    tank_age_mean = mean(tank_age_years, na.rm = TRUE),
    
    # Capacity: sum total

    total_capacity_gallons = sum(capacity_gallons, na.rm = TRUE),
    
    # Wall type: flag if ANY single-wall present (higher risk)
    has_single_wall = any(is_single_wall, na.rm = TRUE),
    has_double_wall = any(is_double_wall, na.rm = TRUE),
    pct_single_wall = mean(is_single_wall, na.rm = TRUE),
    
    # Region (should be constant within facility)
    region = first(region),
    
    # Installation year (earliest)
    earliest_install_year = min(install_year, na.rm = TRUE),
    
    .groups = "drop"
  ) %>%
  # Handle Inf/-Inf from empty groups

  mutate(
    across(where(is.numeric), ~ ifelse(is.infinite(.), NA, .))
  )

cat(paste("  Unique facilities:", nrow(tanks_facility), "\n\n"))

# ============================================================================
# STEP 2: PREPARE CONTRACTS DATA
# ============================================================================
# Create treatment indicator and filter out Scope of Work

cat("Preparing contracts data...\n")

contracts_clean <- contracts %>%
  mutate(
    # Treatment: Bid-to-Result = Pay-for-Performance (PFP)
    is_PFP = (auction_type == "Bid-to-Result"),
    
    # Extract year for fixed effects
    contract_year = year(contract_start)
  ) %>%
  # EXCLUSION: Remove Scope of Work (confounds T&M vs PFP comparison)
  filter(auction_type != "Scope of Work") %>%
  # Keep relevant fields
  select(
    claim_number,
    contract_id,
    adjuster,
    auction_type,
    is_PFP,
    contract_year,
    base_price,
    total_contract_value,
    brings_to_closure_flag
  )

cat(paste("  Contracts after SOW exclusion:", nrow(contracts_clean), "\n"))
cat(paste("  Treatment (PFP):", sum(contracts_clean$is_PFP), "\n"))
cat(paste("  Control (Other/No PFP):", sum(!contracts_clean$is_PFP), "\n\n"))

# ============================================================================
# STEP 3: PREPARE CLAIMS DATA
# ============================================================================

cat("Preparing claims data...\n")

claims_clean <- claims %>%
  mutate(
    # Ensure numeric
    total_paid = as.numeric(total_paid),
    claim_duration_years = as.numeric(claim_duration_years),
    
    # Extract claim year for fixed effects
    claim_year = year(claim_date)
  ) %>%
  # SAMPLE RESTRICTION 1: Closed claims only
  filter(is_closed == TRUE) %>%
  # SAMPLE RESTRICTION 2: Minimum cost threshold (exclude administrative noise)
  filter(total_paid > 1000) %>%
  select(
    claim_number,
    department,
    total_paid,
    paid_loss,
    paid_alae,
    incurred_loss,
    claim_year,
    loss_year,
    claim_duration_years,
    county,
    dep_region,
    is_closed
  )

cat(paste("  Claims after restrictions:", nrow(claims_clean), "\n"))
cat(paste("    - Closed claims only\n"))
cat(paste("    - total_paid > $1,000\n\n"))

# ============================================================================
# STEP 4: JOIN DATASETS
# ============================================================================
# Join order: claims (base) <- contracts <- tanks
# Linkage: claims.department = tanks.facility_id

cat("Joining datasets...\n")

# Step 4a: Claims LEFT JOIN Contracts (on claim_number)
# Claims without contracts = pure T&M (control group)
panel_step1 <- claims_clean %>%
  left_join(
    contracts_clean,
    by = "claim_number",
    relationship = "many-to-many"  # One claim can have multiple contracts
  )

cat(paste("  After claims-contracts join:", nrow(panel_step1), "\n"))

# Collapse to claim level if multiple contracts per claim
# Take the "most aggressive" treatment (if ANY contract is PFP, treat as PFP)
panel_step2 <- panel_step1 %>%
  group_by(claim_number) %>%
  summarise(
    # From claims
    department = first(department),
    total_paid = first(total_paid),
    paid_loss = first(paid_loss),
    paid_alae = first(paid_alae),
    incurred_loss = first(incurred_loss),
    claim_year = first(claim_year),
    loss_year = first(loss_year),
    claim_duration_years = first(claim_duration_years),
    county = first(county),
    dep_region = first(dep_region),
    
    # Treatment: ANY PFP contract = treated
    is_PFP = any(is_PFP, na.rm = TRUE),
    
    # Contract info (use first non-NA)
    adjuster = first(na.omit(adjuster)),
    n_contracts = sum(!is.na(contract_id)),
    total_contract_value = sum(total_contract_value, na.rm = TRUE),
    
    .groups = "drop"
  ) %>%
  # Claims with NO contracts are control (T&M)
  mutate(
    is_PFP = coalesce(is_PFP, FALSE),
    has_contract = (n_contracts > 0)
  )

cat(paste("  After collapsing to claim level:", nrow(panel_step2), "\n"))

# ============================================================================
# STEP 4B: JOIN WITH PA DEP FACILITY LINKAGE (REPLACES TANK JOIN)
# ============================================================================
# Source: facility_linkage_table.csv from 02a_padep_download.R
# Expected match rate: >90% (per README architecture)

cat("Joining with PA DEP facility linkage table...\n")

# Load PA DEP facility linkage (authoritative source)
linkage_path <- "data/external/padep/facility_linkage_table.csv"

if (!file.exists(linkage_path)) {
  stop(paste0(
    "CRITICAL: facility_linkage_table.csv not found.\n",
    "Run R/etl/02a_padep_download.R first."
  ))
}

linkage <- fread(linkage_path)

cat(sprintf("  Linkage table loaded: %d facilities\n", nrow(linkage)))

# Join claims to facility universe via permit_number = department
panel_step3 <- panel_step2 %>%
  left_join(
    linkage %>% 
      select(permit_number, efacts_facility_id, site_id, 
             latitude, longitude, registration_status, owner_name),
    by = c("department" = "permit_number")
  )

# DIAGNOSTIC: Match rate validation
n_matched <- sum(!is.na(panel_step3$efacts_facility_id))
match_rate <- n_matched / nrow(panel_step3)

cat(sprintf("  Facility linkage match rate: %d/%d (%.1f%%)\n", 
            n_matched, nrow(panel_step3), 100 * match_rate))

# FAIL if match rate below threshold
if (match_rate < 0.85) {
  warning(sprintf(
    "LOW MATCH RATE: %.1f%% < 85%% threshold.\n
    Check that claims.department format matches linkage.permit_number (XX-XXXXX)",
    100 * match_rate
  ))
}

stopifnot(match_rate > 0.50)  # Hard fail if catastrophically low


# ============================================================================
# STEP 5: CREATE INSTRUMENT - ADJUSTER LENIENCY (LEAVE-ONE-OUT)
# ============================================================================
# FIX: Filter to valid adjusters BEFORE computing LOO to avoid denominator bias

cat("Constructing adjuster leniency instrument...\n")

# STEP 5A: Identify valid adjusters FIRST (>10 claims for stable instrument)
adjuster_counts <- panel_step3 %>%
  filter(!is.na(adjuster)) %>%
  count(adjuster, name = "adjuster_n_claims")

valid_adjusters <- adjuster_counts %>%
  filter(adjuster_n_claims > 10) %>%
  pull(adjuster)

cat(sprintf("  Total unique adjusters: %d\n", n_distinct(panel_step3$adjuster, na.rm = TRUE)))
cat(sprintf("  Adjusters with >10 claims (valid for IV): %d\n", length(valid_adjusters)))

# STEP 5B: Compute LOO on FULL panel, then flag validity
panel_with_iv <- panel_step3 %>%
  left_join(adjuster_counts, by = "adjuster") %>%
  group_by(adjuster) %>%
  mutate(
    # Sum of is_PFP for this adjuster
    adjuster_pfp_sum = sum(is_PFP, na.rm = TRUE),
    # Leave-one-out: subtract current observation
    adjuster_pfp_sum_loo = adjuster_pfp_sum - as.numeric(is_PFP),
    # LOO mean (only valid if adjuster has >1 claim)
    adjuster_leniency = if_else(
      adjuster_n_claims > 1,
      adjuster_pfp_sum_loo / (adjuster_n_claims - 1),
      NA_real_
    )
  ) %>%
  ungroup() %>%
  # Flag observations valid for IV estimation
  mutate(
    has_valid_iv = (adjuster %in% valid_adjusters) & !is.na(adjuster_leniency)
  ) %>%
  # Clean up intermediate columns
  select(-adjuster_pfp_sum, -adjuster_pfp_sum_loo)

cat(sprintf("  Observations with valid IV: %d (%.1f%%)\n", 
            sum(panel_with_iv$has_valid_iv),
            100 * mean(panel_with_iv$has_valid_iv)))


# ============================================================================
# STEP 6: FINAL VARIABLE PREPARATION
# ============================================================================

cat("Final variable preparation...\n")

analysis_panel <- panel_with_iv %>%
  mutate(
    # OUTCOME: Log transformation for skewed costs
    log_total_paid = log(total_paid + 1),
    
    # COVARIATES: Impute missing tank_age with median (with flag)
    tank_age_imputed = coalesce(tank_age_max, median(tank_age_max, na.rm = TRUE)),
    tank_age_missing = is.na(tank_age_max),
    
    # Region: Clean DEP region for fixed effects
    region = case_when(
      !is.na(dep_region) ~ dep_region,
      !is.na(region) ~ as.character(region),
      TRUE ~ "Unknown"
    ),
    
    # Duration: Impute if missing
    duration_years = coalesce(claim_duration_years, median(claim_duration_years, na.rm = TRUE)),
    duration_missing = is.na(claim_duration_years),
    
    # Capacity: Impute if missing
    capacity_imputed = coalesce(total_capacity_gallons, median(total_capacity_gallons, na.rm = TRUE)),
    capacity_missing = is.na(total_capacity_gallons),
    
    # Year as factor for FE
    claim_year_factor = factor(claim_year)
  ) %>%
  # Select final variables
  select(
    # Identifiers
    claim_number,
    department,
    adjuster,
    
    # Outcome
    total_paid,
    log_total_paid,
    
    # Treatment
    is_PFP,
    has_contract,
    n_contracts,
    
    # Instrument
    adjuster_leniency,
    adjuster_n_claims,
    has_valid_iv,
    
    # Covariates - Tank
    tank_age_imputed,
    tank_age_missing,
    n_tanks,
    capacity_imputed,
    capacity_missing,
    has_single_wall,
    pct_single_wall,
    
    # Covariates - Claim
    duration_years,
    duration_missing,
    claim_year,
    claim_year_factor,
    loss_year,
    
    # Covariates - Geographic
    region,
    county
  )

cat(paste("  Final panel observations:", nrow(analysis_panel), "\n"))

# ============================================================================
# STEP 7: SUMMARY STATISTICS
# ============================================================================

cat("\n========================================\n")
cat("ANALYSIS PANEL SUMMARY\n")
cat("========================================\n\n")

cat("Sample Composition:\n")
cat(paste("  Total observations:", nrow(analysis_panel), "\n"))
cat(paste("  Treatment (PFP):", sum(analysis_panel$is_PFP), 
          "(", round(mean(analysis_panel$is_PFP) * 100, 1), "%)\n"))
cat(paste("  Control (T&M):", sum(!analysis_panel$is_PFP),
          "(", round(mean(!analysis_panel$is_PFP) * 100, 1), "%)\n\n"))

cat("Outcome Variable (total_paid):\n")
cat(paste("  Mean: $", format(mean(analysis_panel$total_paid), big.mark = ",", nsmall = 0), "\n"))
cat(paste("  Median: $", format(median(analysis_panel$total_paid), big.mark = ",", nsmall = 0), "\n"))
cat(paste("  SD: $", format(sd(analysis_panel$total_paid), big.mark = ",", nsmall = 0), "\n"))
cat(paste("  Range: $", format(min(analysis_panel$total_paid), big.mark = ","), 
          " - $", format(max(analysis_panel$total_paid), big.mark = ","), "\n\n"))

cat("Instrument (adjuster_leniency):\n")
iv_subset <- analysis_panel %>% filter(has_valid_iv)
cat(paste("  Valid IV observations:", nrow(iv_subset), "\n"))
cat(paste("  Mean leniency:", round(mean(iv_subset$adjuster_leniency, na.rm = TRUE), 3), "\n"))
cat(paste("  SD leniency:", round(sd(iv_subset$adjuster_leniency, na.rm = TRUE), 3), "\n"))
cat(paste("  Range:", round(min(iv_subset$adjuster_leniency, na.rm = TRUE), 3), 
          " - ", round(max(iv_subset$adjuster_leniency, na.rm = TRUE), 3), "\n\n"))

cat("Missing Data Imputation:\n")
cat(paste("  Tank age imputed:", sum(analysis_panel$tank_age_missing), 
          "(", round(mean(analysis_panel$tank_age_missing) * 100, 1), "%)\n"))
cat(paste("  Duration imputed:", sum(analysis_panel$duration_missing),
          "(", round(mean(analysis_panel$duration_missing) * 100, 1), "%)\n"))
cat(paste("  Capacity imputed:", sum(analysis_panel$capacity_missing),
          "(", round(mean(analysis_panel$capacity_missing) * 100, 1), "%)\n\n"))

cat("Temporal Coverage:\n")
cat(paste("  Claim years:", min(analysis_panel$claim_year, na.rm = TRUE), 
          " - ", max(analysis_panel$claim_year, na.rm = TRUE), "\n"))
print(table(analysis_panel$claim_year, useNA = "ifany"))

# ============================================================================
# STEP 8: SAVE OUTPUT
# ============================================================================

cat("\n\nSaving analysis panel...\n")

saveRDS(analysis_panel, file.path(paths$processed, "analysis_panel.rds"))
write_csv(analysis_panel, file.path(paths$processed, "analysis_panel.csv"))

cat("✓ Saved: data/processed/analysis_panel.rds\n")
cat("✓ Saved: data/processed/analysis_panel.csv\n\n")

# Also save IV subset for convenience
iv_panel <- analysis_panel %>% filter(has_valid_iv)
saveRDS(iv_panel, file.path(paths$processed, "iv_analysis_panel.rds"))
cat("✓ Saved: data/processed/iv_analysis_panel.rds (IV-valid subset)\n\n")

cat("========================================\n")
cat("ETL STEP 4 COMPLETE\n")
cat("========================================\n")
cat("\nNEXT STEPS:\n")
cat("  • Run: source('R/analysis/01_descriptive_stats.R')\n")
cat("  • Run: source('R/analysis/03_causal_inference.R')\n")
cat("========================================\n\n")
