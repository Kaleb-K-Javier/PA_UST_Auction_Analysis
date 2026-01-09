# R/etl/01_load_ustif_data.R
# ============================================================================
# Pennsylvania UST Auction Analysis - ETL Step 1: Load USTIF Data (CSV, exact)
# - Recreates variables & names from older Excel-based script
# - Robust, NA-safe date parsing
# - Head + str printed after each step for rapid debugging
# - Produces data dictionary entries for both datasets
# ============================================================================

cat("\n========================================\n")
cat("ETL Step 1: Loading USTIF Proprietary Data (CSV, exact-schema)\n")
cat("========================================\n\n")

suppressPackageStartupMessages({
  library(data.table)
  library(janitor)
  library(lubridate)
  library(stringr)
  library(here)
})

# -------------------------
# Paths (data/raw or /mnt/data)
# -------------------------
paths <- list(
  raw = here("data/raw"),
  uploads = "/mnt/data",
  processed = here("data/processed")
)
dir.create(paths$processed, recursive = TRUE, showWarnings = FALSE)

find_raw_file <- function(fn) {
  p1 <- file.path(paths$raw, fn)
  p2 <- file.path(paths$uploads, fn)
  if (file.exists(p1)) return(p1)
  if (file.exists(p2)) return(p2)
  return(NULL)
}

# -------------------------
# Helpers
# -------------------------
# Robust, NA-safe date parser
parse_dates <- function(col) {
  n <- length(col)
  out <- rep(as.Date(NA), n)

  # quick returns
  if (inherits(col, "Date")) return(as.Date(col))
  if (inherits(col, "POSIXt")) return(as.Date(col))

  # factors -> char
  if (is.factor(col)) col <- as.character(col)

  # build char vector safely
  col_chr <- rep(NA_character_, n)
  non_na <- !is.na(col)
  if (any(non_na)) col_chr[non_na] <- trimws(as.character(col[non_na]))

  # numeric detection (numeric or numeric-strings)
  numeric_vals <- suppressWarnings(as.numeric(col_chr))
  is_num <- !is.na(numeric_vals)

  # numeric handling: epoch vs excel serial vs NA
  if (any(is_num, na.rm = TRUE)) {
    idx_num <- which(is_num)
    for (i in idx_num) {
      v <- numeric_vals[i]
      if (is.na(v)) { out[i] <- as.Date(NA); next }
      if (v > 1e9) {
        out[i] <- as.Date(as.POSIXct(v, origin = "1970-01-01", tz = "UTC"))
      } else if (v >= 20000 && v <= 60000) {
        out[i] <- as.Date(v, origin = "1899-12-30")
      } else if (v > 0 && v < 20000) {
        out[i] <- as.Date(v, origin = "1899-12-30")
      } else {
        out[i] <- as.Date(NA)
      }
    }
  }

  # textual parsing (remaining non-empty)
  text_idx <- which(is.na(out) & !is.na(col_chr) & col_chr != "")
  if (length(text_idx) > 0) {
    attempts <- c("mdy", "mdy HMS", "mdy HM", "dmy", "ymd", "ymd HMS", "BdY", "bdy", "Ymd")
    parsed <- suppressWarnings(parse_date_time(col_chr[text_idx], orders = attempts, exact = FALSE, tz = "UTC"))
    parsed_date <- as.Date(parsed)
    ok <- !is.na(parsed_date)
    if (any(ok)) out[text_idx[ok]] <- parsed_date[ok]
  }

  # final explicit formats fallback
  still_na <- which(is.na(out) & !is.na(col_chr) & col_chr != "")
  if (length(still_na) > 0) {
    fmts <- c("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%Y/%m/%d",
              "%d-%b-%Y", "%d-%B-%Y", "%b %d, %Y", "%B %d, %Y", "%Y%m%d")
    for (fmt in fmts) {
      if (length(still_na) == 0) break
      try_parsed <- suppressWarnings(as.Date(col_chr[still_na], format = fmt))
      ok2 <- !is.na(try_parsed)
      if (any(ok2)) out[still_na[ok2]] <- try_parsed[ok2]
      still_na <- which(is.na(out) & !is.na(col_chr) & col_chr != "")
    }
  }

  return(as.Date(out))
}

# Quick print helper for debugging
print_block <- function(dt, name, n = 6) {
  cat("\n---", name, "---\n")
  if (is.null(dt)) { cat("NULL\n"); return(invisible(NULL)) }
  cat("Rows:", nrow(dt), "Cols:", ncol(dt), "\n")
  if (is.data.table(dt) || is.data.frame(dt)) {
    print(head(dt, n))
    cat("\nStructure:\n")
    print(str(dt))
  } else {
    print(dt)
  }
  invisible(NULL)
}

# Save helper
save_dataset <- function(dt, name, path = paths$processed) {
  if (nrow(dt) == 0) warning(sprintf("Dataset '%s' has 0 rows!", name))
  saveRDS(dt, file.path(path, paste0(name, ".rds")))
  fwrite(dt, file.path(path, paste0(name, ".csv")))
  cat(sprintf("✓ Saved: %s/%s (.rds/.csv) — %d rows, %d cols\n", path, name, nrow(dt), ncol(dt)))
  invisible(NULL)
}

# Data dictionary helper (returns data.table)
add_to_dictionary <- function(dt, source_file) {
  data.table(
    variable_name = names(dt),
    description = paste("Variable from", source_file),
    source_file = source_file,
    data_type = sapply(dt, function(x) class(x)[1]),
    n_records = nrow(dt),
    n_missing = sapply(dt, function(x) sum(is.na(x))),
    unique_values = sapply(dt, uniqueN),
    notes = NA_character_
  )
}

# -------------------------
# LOAD & PROCESS CLAIMS (exact columns)
# -------------------------
cat("\n--- Loading Individual Claims Data ---\n")
claims_filename <- "Actuarial_UST_Individual_Claim_Data_thru_63020_4.csv"
claims_file <- find_raw_file(claims_filename)
if (is.null(claims_file)) stop("CRITICAL: Claims file not found at: ", claims_filename)

claims_raw <- fread(claims_file, na.strings = c("", "NA", "N/A", "n/a"), encoding = "UTF-8", showProgress = FALSE)
claims_raw <- clean_names(claims_raw)
print_block(claims_raw, "claims_raw (post-read)")

# exact columns expected (from the CSV you provided)
expected_claim_cols <- c(
  "event_department_abbreviation", "claim_number", "paid_loss", "paid_alae", "incurred_loss",
  "date_of_loss_reported", "date_of_claim", "date_closed", "claim_status_as_of_6_10_2025",
  "dep_region", "claimant_full_name", "county", "event_primary_loc_desc",
  "product_s", "product_type_other_description"
)
missing_claim_cols <- setdiff(expected_claim_cols, names(claims_raw))
if (length(missing_claim_cols) > 0) {
  stop("CLAIMS SCHEMA ERROR. Missing: ", paste(missing_claim_cols, collapse = ", "))
}

# Build claims with the original variable names and engineered fields
claims <- claims_raw[, .(
  # Keys & raw
  department = trimws(as.character(event_department_abbreviation)),
  claim_number = trimws(as.character(claim_number)),

  # Financials
  paid_loss = as.numeric(paid_loss),
  paid_alae = as.numeric(paid_alae),
  incurred_loss = as.numeric(incurred_loss),

  # Dates (raw -> will parse)
  loss_reported_date_raw = date_of_loss_reported,
  claim_date_raw = date_of_claim,
  closed_date_raw = date_closed,

  # Claim characteristics
  claim_status = as.character(claim_status_as_of_6_10_2025),
  dep_region = trimws(as.character(dep_region)),
  claimant_name = as.character(claimant_full_name),
  county = str_to_title(trimws(as.character(county))),
  location_desc = as.character(event_primary_loc_desc),
  products = as.character(product_s),
  product_other = as.character(product_type_other_description)
)]

print_block(claims, "claims (after projection, raw dates present)")

# Parse dates robustly and drop raw date cols
claims[, loss_reported_date := parse_dates(loss_reported_date_raw)]
claims[, claim_date := parse_dates(claim_date_raw)]
claims[, closed_date := parse_dates(closed_date_raw)]
claims[, c("loss_reported_date_raw", "claim_date_raw", "closed_date_raw") := NULL]

print_block(claims, "claims (dates parsed)")

# Feature Engineering (match old script exactly)
claims[, `:=`(
  claim_year = year(claim_date),
  loss_year = year(loss_reported_date),
  closed_year = year(closed_date),

  total_paid = fcoalesce(paid_loss, 0) + fcoalesce(paid_alae, 0),

  is_closed = !is.na(closed_date) | str_detect(tolower(coalesce(claim_status, "")), "closed"),
  is_open = str_detect(tolower(coalesce(claim_status, "")), "open"),

  claim_duration_days = as.numeric(difftime(closed_date, claim_date, units = "days")),
  claim_duration_years = as.numeric(difftime(closed_date, claim_date, units = "days")) / 365.25
)]

print_block(claims, "claims (feature engineered)")

# Remove garbage rows and deduplicate
initial_n_claims <- nrow(claims)
claims <- claims[!is.na(claim_number) & claim_number != ""]
claims <- claims[!str_detect(tolower(coalesce(claim_number, "")), "^sum")]
claims <- claims[!is.na(department) & department != ""]
if (nrow(claims) < initial_n_claims) {
  cat(sprintf("  Removed %d garbage/summary rows from claims\n", initial_n_claims - nrow(claims)))
}
claims <- unique(claims, by = "claim_number")
print_block(claims, "claims (cleaned & deduped)")

# Summary statistics (same as old script)
cat("\n  Claims Summary:\n")
cat(sprintf("    - Unique claims: %d\n", uniqueN(claims$claim_number)))
if ("claim_date" %in% names(claims) && any(!is.na(claims$claim_date))) {
  cat(sprintf("    - Date range: %s to %s\n", min(claims$claim_date, na.rm = TRUE), max(claims$claim_date, na.rm = TRUE)))
}
cat(sprintf("    - Total paid (all claims): $%s\n", format(sum(claims$total_paid, na.rm = TRUE), big.mark = ",")))
cat(sprintf("    - Median paid per claim: $%s\n", format(median(claims$total_paid, na.rm = TRUE), big.mark = ",")))
cat(sprintf("    - Closed claims: %d (%.1f%%)\n", sum(claims$is_closed, na.rm = TRUE), 100 * mean(claims$is_closed, na.rm = TRUE)))

# Schema validation (hard)
required_claims_cols <- c("department", "claim_number", "total_paid", "claim_date", "is_closed", "claim_duration_years", "county", "dep_region")
missing_cols <- setdiff(required_claims_cols, names(claims))
if (length(missing_cols) > 0) {
  stop("SCHEMA ERROR: Missing required columns in claims: ", paste(missing_cols, collapse = ", "))
}

save_dataset(claims, "claims_clean")

# -------------------------
# LOAD & PROCESS CONTRACTS (exact columns)
# -------------------------
cat("\n--- Loading Actuarial Contract Data ---\n")
contracts_filename <- "Actuarial_Contract_Data_2.csv"
contracts_file <- find_raw_file(contracts_filename)
if (is.null(contracts_file)) {
  warning("Contracts file not found. Skipping contract processing.")
} else {
  contracts_raw <- fread(contracts_file, na.strings = c("", "NA", "N/A", "n/a"), encoding = "UTF-8", showProgress = FALSE)
  contracts_raw <- clean_names(contracts_raw)
  print_block(contracts_raw, "contracts_raw (post-read)")

  # Expect exact cleaned column names (as in your CSV)
  expected_contract_cols <- c(
    "contract_jobs_claim_number", "contract_job_identifier", "adjuster_full_name",
    "cts_site_name", "department_name", "contract_will_bring_site_to_closure_desc",
    "consultant_full_name", "contract_effective_date", "contract_end_date",
    "contract_category_desc", "bid_approval_letter_to_claimant", "bid_type_desc",
    "contract_type_desc", "contract_base_price", "total_price_of_amendments",
    "amount_paid_to_date", "notes"
  )
  missing_contract_cols <- setdiff(expected_contract_cols, names(contracts_raw))
  if (length(missing_contract_cols) > 0) {
    stop("CONTRACTS SCHEMA ERROR. Missing: ", paste(missing_contract_cols, collapse = ", "))
  }

  # Clean & Transform (original variable names + engineered)
  contracts <- contracts_raw[, .(
    # Keys
    claim_number = trimws(as.character(contract_jobs_claim_number)),
    contract_id = trimws(as.character(contract_job_identifier)),

    # Administrative
    adjuster = trimws(as.character(adjuster_full_name)),
    site_name = as.character(cts_site_name),
    department = trimws(as.character(department_name)),
    consultant = as.character(consultant_full_name),

    # Contract characteristics
    brings_to_closure = as.character(contract_will_bring_site_to_closure_desc),
    contract_category = as.character(contract_category_desc),
    bid_type = as.character(bid_type_desc),
    contract_type_raw = as.character(contract_type_desc),

    # Dates raw (parse after)
    contract_start_raw = contract_effective_date,
    contract_end_raw = contract_end_date,
    bid_approval_date_raw = bid_approval_letter_to_claimant,

    # Financials
    base_price = as.numeric(contract_base_price),
    amendments_total = as.numeric(total_price_of_amendments),
    paid_to_date = as.numeric(amount_paid_to_date),

    # Notes
    notes = as.character(notes)
  )]

  print_block(contracts, "contracts (after projection, raw dates present)")

  # Parse contract dates
  contracts[, contract_start := parse_dates(contract_start_raw)]
  contracts[, contract_end := parse_dates(contract_end_raw)]
  contracts[, bid_approval_date := parse_dates(bid_approval_date_raw)]
  contracts[, c("contract_start_raw", "contract_end_raw", "bid_approval_date_raw") := NULL]

  # Feature engineering (match old script)
  contracts[, `:=`(
    contract_year = year(contract_start),
    total_contract_value = fcoalesce(base_price, 0) + fcoalesce(amendments_total, 0),

    auction_type = fcase(
      str_detect(tolower(coalesce(bid_type, "")), "result") |
        str_detect(tolower(coalesce(contract_type_raw, "")), "performance"), "Bid-to-Result",
      str_detect(tolower(coalesce(bid_type, "")), "scope|sow|defined"), "Scope of Work",
      default = "Other/Unknown"
    ),

    is_bid_to_result = str_detect(tolower(coalesce(bid_type, "")), "result") |
      str_detect(tolower(coalesce(contract_type_raw, "")), "performance"),

    is_scope_of_work = str_detect(tolower(coalesce(bid_type, "")), "scope|sow|defined"),
    brings_to_closure_flag = str_detect(tolower(coalesce(brings_to_closure, "")), "yes|true")
  )]

  # Clean up & dedupe
  initial_n_contracts <- nrow(contracts)
  contracts <- contracts[!is.na(claim_number) & claim_number != ""]
  if (nrow(contracts) < initial_n_contracts) {
    cat(sprintf("  Removed %d rows with missing claim_number\n", initial_n_contracts - nrow(contracts)))
  }
  contracts <- unique(contracts, by = c("claim_number", "contract_id"))
  print_block(contracts, "contracts (cleaned & deduped)")

  # Summary (same style as old script)
  cat("\n  Contracts Summary:\n")
  cat(sprintf("    - Unique claims: %d\n", uniqueN(contracts$claim_number)))
  cat(sprintf("    - Unique adjusters: %d\n", uniqueN(contracts$adjuster)))
  if ("contract_start" %in% names(contracts) && any(!is.na(contracts$contract_start))) {
    cat(sprintf("    - Date range: %s to %s\n", min(contracts$contract_start, na.rm = TRUE), max(contracts$contract_start, na.rm = TRUE)))
  }
  cat("    - Auction types:\n")
  print(table(contracts$auction_type, useNA = "ifany"))

  # Schema validation (hard)
  required_contracts_cols <- c("claim_number", "contract_id", "adjuster", "auction_type", "contract_start", "total_contract_value", "is_bid_to_result")
  missing_cols <- setdiff(required_contracts_cols, names(contracts))
  if (length(missing_cols) > 0) {
    stop("SCHEMA ERROR: Missing required columns in contracts: ", paste(missing_cols, collapse = ", "))
  }

  save_dataset(contracts, "contracts_clean")
}

# -------------------------
# DATA DICTIONARY UPDATE (match original layout & variable names)
# -------------------------
cat("\n--- Updating Data Dictionary ---\n")
dict_path <- file.path(paths$processed, "data_dictionary.rds")

if (file.exists(dict_path)) {
  data_dict <- readRDS(dict_path)
} else {
  data_dict <- data.table(
    variable_name = character(),
    description = character(),
    source_file = character(),
    data_type = character(),
    n_records = integer(),
    n_missing = integer(),
    unique_values = integer(),
    notes = character()
  )
}

# Add claims
data_dict <- rbindlist(list(
  data_dict,
  add_to_dictionary(claims, claims_filename)
), fill = TRUE)

# Add contracts if present
if (exists("contracts")) {
  data_dict <- rbindlist(list(
    data_dict,
    add_to_dictionary(contracts, contracts_filename)
  ), fill = TRUE)
}

# Deduplicate dictionary and save
data_dict <- unique(data_dict, by = c("variable_name", "source_file"))
saveRDS(data_dict, dict_path)
fwrite(data_dict, file.path(paths$processed, "data_dictionary.csv"))
cat("✓ Updated: data/processed/data_dictionary.rds\n")
cat("✓ Updated: data/processed/data_dictionary.csv\n\n")

# -------------------------
# SUMMARY
# -------------------------
cat("========================================\n")
cat("ETL STEP 1 COMPLETE\n")
cat("========================================\n")
cat("\nDatasets created:\n")
cat(sprintf("  1. claims_clean (.rds/.csv): %d records\n", nrow(claims)))
if (exists("contracts")) cat(sprintf("  2. contracts_clean (.rds/.csv): %d records\n", nrow(contracts)))
cat("\nStable Keys:\n")
cat("  - claims: claim_number, department\n")
cat("  - contracts: claim_number, contract_id, adjuster\n")
cat("\nNEXT STEPS:\n")
cat("  • Run: source('R/etl/02_build_master_tank_list.R')\n")
cat("  • Run: source('R/etl/03_merge_master_dataset.R')\n")
cat("========================================\n\n")
