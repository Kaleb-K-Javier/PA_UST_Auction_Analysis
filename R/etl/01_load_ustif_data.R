# R/etl/01_load_ustif_data.R
# ============================================================================
# Pennsylvania UST Auction Analysis - ETL Step 1: Load USTIF Data
# ============================================================================
# Purpose: Import and clean proprietary USTIF Excel datasets
# Input: Raw Excel files in data/raw/
# Output: Cleaned .rds and .csv files in data/processed/
# ============================================================================

cat("\n========================================\n")
cat("ETL Step 1: Loading USTIF Proprietary Data\n")
cat("========================================\n\n")

# Load dependencies
library(tidyverse)
library(readxl)
library(janitor)
library(lubridate)

# Load paths from master script (or define if running standalone)
if (!exists("paths")) {
  paths <- list(
    raw = "data/raw",
    processed = "data/processed"
  )
}

# Load data dictionary
data_dict <- readRDS(file.path(paths$processed, "data_dictionary.rds"))

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

# Add entries to data dictionary
add_to_dictionary <- function(df, source_file, description_prefix = "") {
  new_entries <- tibble(
    variable_name = names(df),
    description = paste(description_prefix, "Variable from", source_file),
    source_file = source_file,
    data_type = sapply(df, function(x) class(x)[1]),
    n_records = nrow(df),
    n_missing = sapply(df, function(x) sum(is.na(x))),
    unique_values = sapply(df, n_distinct),
    notes = ""
  )
  return(new_entries)
}

# Save dataset as both RDS and CSV
save_dataset <- function(df, name, path = paths$processed) {
  saveRDS(df, file.path(path, paste0(name, ".rds")))
  write_csv(df, file.path(path, paste0(name, ".csv")))
  cat(paste0("✓ Saved: ", path, "/", name, ".rds\n"))
  cat(paste0("✓ Saved: ", path, "/", name, ".csv\n\n"))
}

# ============================================================================
# DATASET 1: Actuarial Contract Data (Auction/Bid Records)
# ============================================================================

cat("Loading Actuarial Contract Data...\n")

contracts_raw <- read_excel(
  file.path(paths$raw, "Actuarial_Contract_Data_2.xlsx"),
  sheet = "Report 1"
) %>%
  clean_names()

cat(paste("  Raw records:", nrow(contracts_raw), "\n"))
cat(paste("  Columns:", ncol(contracts_raw), "\n"))

# Data cleaning
contracts <- contracts_raw %>%
  rename(
    claim_number = contract_jobs_claim_number,
    contract_id = contract_job_identifier,
    adjuster = adjuster_full_name,
    site_name = cts_site_name,
    department = department_name,
    brings_to_closure = contract_will_bring_site_to_closure_desc,
    consultant = consultant_full_name,
    contract_start = contract_effective_date,
    contract_end = contract_end_date,
    contract_category = contract_category_desc,
    bid_approval_date = bid_approval_letter_to_claimant,
    bid_type = bid_type_desc,
    contract_type = contract_type_desc,
    base_price = contract_base_price,
    amendments_total = total_price_of_amendments,
    paid_to_date = amount_paid_to_date
  ) %>%
  mutate(
    # Convert dates
    contract_start = as.Date(contract_start),
    contract_end = as.Date(contract_end),
    bid_approval_date = as.Date(bid_approval_date),
    
    # Create contract year for temporal analysis
    contract_year = year(contract_start),
    
    # Calculate total contract value
    total_contract_value = coalesce(base_price, 0) + coalesce(amendments_total, 0),
    
    # Create bid type indicators
    is_bid_to_result = str_detect(tolower(coalesce(bid_type, "")), "bid|result"),
    is_scope_of_work = str_detect(tolower(coalesce(bid_type, "")), "scope|sow"),
    
    # Auction type categorization based on USTIF institutional knowledge
    auction_type = case_when(
      is_scope_of_work ~ "Scope of Work",
      is_bid_to_result ~ "Bid-to-Result",
      TRUE ~ "Other/Unknown"
    ),
    
    # Create closure indicator
    brings_to_closure_flag = str_detect(tolower(coalesce(brings_to_closure, "")), "yes|true|closure")
  ) %>%
  filter(!is.na(claim_number) & claim_number != "")

cat(paste("  Cleaned records:", nrow(contracts), "\n"))

# Summary statistics
cat("\n  Contract Summary:\n")
cat(paste("    - Date range:", min(contracts$contract_start, na.rm = TRUE), "to", 
          max(contracts$contract_start, na.rm = TRUE), "\n"))
cat(paste("    - Unique claims:", n_distinct(contracts$claim_number), "\n"))
cat(paste("    - Bid types:\n"))
print(table(contracts$auction_type, useNA = "ifany"))

# Update data dictionary
data_dict <- bind_rows(data_dict, add_to_dictionary(contracts, "Actuarial_Contract_Data_2.xlsx"))

# Save cleaned data
save_dataset(contracts, "contracts_clean")

# ============================================================================
# DATASET 2: Tank Construction/Closure Data
# ============================================================================

cat("Loading Tank Construction Data...\n")

tanks_raw <- read_excel(
  file.path(paths$raw, "Tank_Construction_Closed.xlsx"),
  sheet = "data"
) %>%
  clean_names()

cat(paste("  Raw records:", nrow(tanks_raw), "\n"))
cat(paste("  Columns:", ncol(tanks_raw), "\n"))

# Data cleaning
tanks <- tanks_raw %>%
  rename(
    facility_id = pf_other_id,
    facility_name = facility_name,
    client_id = client_id,
    client_name = client_search_name,
    region = region_code,
    tank_type = ast_ust,
    tank_seq = seq_number,
    capacity_gallons = capacity,
    install_date = date_installed,
    substance = substance_stored_desc,
    component_code = sys_comp,
    component_desc = sys_comp_desc,
    construction_desc = comp_desc,
    construction_comment = comp_desc_comment,
    construction_detail = comp_desc_description,
    permit_status = tank_permit_status_code,
    tank_status = tank_status_code
  ) %>%
  mutate(
    # Convert dates
    install_date = as.Date(install_date),
    
    # Calculate tank age (as of current date)
    tank_age_years = as.numeric(difftime(Sys.Date(), install_date, units = "days")) / 365.25,
    
    # Installation year for cohort analysis
    install_year = year(install_date),
    
    # Create tank construction indicators
    is_single_wall = str_detect(tolower(coalesce(construction_desc, "")), "single|bare|steel") &
                     !str_detect(tolower(coalesce(construction_desc, "")), "double"),
    is_double_wall = str_detect(tolower(coalesce(construction_desc, "")), "double|secondary"),
    is_fiberglass = str_detect(tolower(coalesce(construction_desc, "")), "fiberglass|frp|composite"),
    
    # Tank type categorization
    wall_type = case_when(
      is_double_wall ~ "Double-Wall",
      is_single_wall ~ "Single-Wall",
      TRUE ~ "Unknown/Other"
    ),
    
    # Tank status interpretation
    status_desc = case_when(
      tank_status == "W" ~ "Withdrawn",
      tank_status == "UR" ~ "Under Review",
      tank_status == "A" ~ "Active",
      tank_status == "C" ~ "Closed",
      TRUE ~ as.character(tank_status)
    ),
    
    # Underground vs aboveground
    is_underground = (tank_type == "UST")
  ) %>%
  filter(!is.na(facility_id) & facility_id != "")

cat(paste("  Cleaned records:", nrow(tanks), "\n"))

# Summary statistics
cat("\n  Tank Summary:\n")
cat(paste("    - Unique facilities:", n_distinct(tanks$facility_id), "\n"))
cat(paste("    - Install date range:", min(tanks$install_date, na.rm = TRUE), "to",
          max(tanks$install_date, na.rm = TRUE), "\n"))
cat(paste("    - Mean tank age:", round(mean(tanks$tank_age_years, na.rm = TRUE), 1), "years\n"))
cat(paste("    - Wall types:\n"))
print(table(tanks$wall_type, useNA = "ifany"))
cat(paste("    - Tank status:\n"))
print(table(tanks$status_desc, useNA = "ifany"))

# Update data dictionary
data_dict <- bind_rows(data_dict, add_to_dictionary(tanks, "Tank_Construction_Closed.xlsx"))

# Save cleaned data
save_dataset(tanks, "tanks_clean")

# ============================================================================
# DATASET 3: Individual Claims Data
# ============================================================================

cat("Loading Individual Claims Data...\n")

claims_raw <- read_excel(
  file.path(paths$raw, "Actuarial_UST_Individual_Claim_Data_thru_63020_4.xlsx"),
  sheet = "Report 1",
  skip = 3  # Header is on row 4
) %>%
  clean_names()

cat(paste("  Raw records:", nrow(claims_raw), "\n"))
cat(paste("  Columns:", ncol(claims_raw), "\n"))

# Data cleaning
claims <- claims_raw %>%
  select(-starts_with("unnamed")) %>%
  rename(
    department = event_department_abbreviation,
    loss_reported_date = date_of_loss_reported,
    claim_date = date_of_claim,
    claim_status = claim_status_as_of_6_10_2025,
    closed_date = date_closed,
    claimant_name = claimant_full_name,
    location_desc = event_primary_loc_desc,
    products = product_s,
    product_other = product_type_other_description
  ) %>%
  mutate(
    # Convert dates
    loss_reported_date = as.Date(loss_reported_date),
    claim_date = as.Date(claim_date),
    closed_date = as.Date(closed_date),
    
    # Extract year for temporal analysis
    claim_year = year(claim_date),
    loss_year = year(loss_reported_date),
    closed_year = year(closed_date),
    
    # Calculate total paid (loss + ALAE)
    total_paid = coalesce(paid_loss, 0) + coalesce(paid_alae, 0),
    
    # Claim status indicators
    is_closed = !is.na(closed_date) | str_detect(tolower(coalesce(claim_status, "")), "closed"),
    is_open = str_detect(tolower(coalesce(claim_status, "")), "open"),
    
    # Duration metrics (if closed)
    claim_duration_days = as.numeric(difftime(closed_date, claim_date, units = "days")),
    claim_duration_years = claim_duration_days / 365.25,
    
    # Clean county names
    county = str_to_title(str_trim(county)),
    
    # Clean DEP region
    dep_region = str_trim(dep_region)
  ) %>%
  filter(!is.na(claim_number) & claim_number != "") %>%
  distinct(claim_number, .keep_all = TRUE)

cat(paste("  Cleaned records:", nrow(claims), "\n"))

# Summary statistics
cat("\n  Claims Summary:\n")
cat(paste("    - Unique claims:", n_distinct(claims$claim_number), "\n"))
cat(paste("    - Claim date range:", min(claims$claim_date, na.rm = TRUE), "to",
          max(claims$claim_date, na.rm = TRUE), "\n"))
cat(paste("    - Total paid (all claims): $", format(sum(claims$total_paid, na.rm = TRUE), big.mark = ","), "\n"))
cat(paste("    - Median paid per claim: $", format(median(claims$total_paid, na.rm = TRUE), big.mark = ","), "\n"))
cat(paste("    - Mean paid per claim: $", format(mean(claims$total_paid, na.rm = TRUE), big.mark = ","), "\n"))
cat(paste("    - Claim status:\n"))
cat(paste("        Closed:", sum(claims$is_closed, na.rm = TRUE), "\n"))
cat(paste("        Open:", sum(claims$is_open, na.rm = TRUE), "\n"))
cat(paste("    - Claims by DEP Region:\n"))
print(table(claims$dep_region, useNA = "ifany"))

# Update data dictionary
data_dict <- bind_rows(data_dict, add_to_dictionary(claims, "Actuarial_UST_Individual_Claim_Data_thru_63020_4.xlsx"))

# Save cleaned data
save_dataset(claims, "claims_clean")

# ============================================================================
# DATASET 4: Institutional Knowledge (Text Data)
# ============================================================================

cat("Processing Institutional Documentation...\n")

# Read and parse the Q&A document
qa_text <- readLines(file.path(paths$raw, "USTIF_Auction_Q_A.txt"), warn = FALSE)

# Extract key institutional facts
institutional_knowledge <- list(
  source_file = "USTIF_Auction_Q_A.txt",
  date_processed = Sys.Date(),
  
  # Auction formats
  auction_formats = c(
    "Scope of Work (SOW)" = "Detailed specifications; cost heavily weighted in scoring",
    "Bid-to-Result" = "Outcome-based; technical proposal emphasized in scoring"
  ),
  
  # Scoring mechanism
  scoring_rules = list(
    cost_scoring = "Lowest bid receives maximum cost points",
    viability_threshold = "70% of max cost score qualifies bid as viable",
    technical_vs_cost = "SOW weights cost; Bid-to-Result weights technical"
  ),
  
  # Administrative process
  administrative_evaluation = list(
    purpose = "Rule compliance check",
    cost_changes = "Not permitted after submission",
    late_bids = "Disqualified"
  ),
  
  # Site selection
  site_selection = list(
    decision_maker = "USTIF decides which sites go to auction",
    criteria = "Existing claims with delays or cost estimate concerns",
    note = "Stuff not getting done or cost estimates too high"
  ),
  
  # Historical notes
  history = list(
    pre_2009 = "Manual process; required signup for bid list",
    post_2009 = "Automated notification system"
  )
)

# Save institutional knowledge
saveRDS(institutional_knowledge, file.path(paths$processed, "institutional_knowledge.rds"))

# Also save as markdown for human reference
cat(qa_text, file = "docs/USTIF_auction_design.md", sep = "\n")

cat("✓ Saved: data/processed/institutional_knowledge.rds\n")
cat("✓ Saved: docs/USTIF_auction_design.md\n\n")

# ============================================================================
# SAVE UPDATED DATA DICTIONARY
# ============================================================================

saveRDS(data_dict, file.path(paths$processed, "data_dictionary.rds"))
write_csv(data_dict, file.path(paths$processed, "data_dictionary.csv"))

cat("✓ Updated: data/processed/data_dictionary.rds\n")
cat("✓ Updated: data/processed/data_dictionary.csv\n\n")

# ============================================================================
# SUMMARY
# ============================================================================

cat("========================================\n")
cat("ETL STEP 1 COMPLETE\n")
cat("========================================\n")
cat("\nDatasets created:\n")
cat(paste("  1. contracts_clean (.rds/.csv):", nrow(contracts), "records\n"))
cat(paste("  2. tanks_clean (.rds/.csv):", nrow(tanks), "records\n"))
cat(paste("  3. claims_clean (.rds/.csv):", nrow(claims), "records\n"))
cat("  4. institutional_knowledge.rds\n")
cat("\n")
cat("NEXT STEPS:\n")
cat("  • (Optional) Run: source('R/etl/02_padep_acquisition.R')\n")
cat("  • Run: source('R/etl/03_merge_master_dataset.R')\n")
cat("========================================\n\n")