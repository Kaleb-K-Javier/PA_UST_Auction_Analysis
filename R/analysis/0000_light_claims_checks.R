library(data.table)
library(tidyverse)

# Define file paths (Adjust based on your working directory)
# Using standard repo structure identified in README
path_claims <- "data/processed/claims_clean.csv"
path_contracts <- "data/processed/contracts_clean.csv"

# 1. Load Data using data.table::fread for performance
# forcing 'claim_number' to character initially to prevent scientific notation issues
dt_claims <- fread(path_claims, colClasses = list(character = "claim_number"))
dt_contracts <- fread(path_contracts, colClasses = list(character = "claim_number"))

# 2. Key Standardization (Critical Step)
# The README identifies 'claim_number' as the primary key.
# Existing ETL scripts (03_merge_master_dataset.R) convert this to character and trim whitespace.

# Standardize Claims Table
dt_claims[, claim_number := trimws(as.character(claim_number))]

# Standardize Contracts Table
dt_contracts[, claim_number := trimws(as.character(claim_number))]

# 3. Perform Left Join
# Left Join: Keep all rows from Claims, match Contracts where available.
# data.table syntax: X[Y, on = "Key"] (This is actually a right join on X, so we use merge for explicit left join behavior similar to dplyr)
merged_data <- merge(
  x = dt_claims, 
  y = dt_contracts, 
  by = "claim_number", 
  all.x = TRUE,      # Left Join (Keep all x)
  suffixes = c("_claim", "_contract") # Handle duplicate column names
)

# 4. Verify Merge Statistics (Optional but recommended)
match_rate <- nrow(merged_data[!is.na(contract_id)]) / nrow(merged_data)
cat(sprintf("Merge Complete. Contract Match Rate: %.2f%%\n", match_rate * 100))

# Preview Data
print(head(merged_data))

# Calculate unique claim counts grouping by the specified contract dimensions
# Note: Using 'bid_type' instead of 'bid_type_raw' based on the dataframe preview provided.

summary_counts <- merged_data[, .(
  n_unique_claims = uniqueN(contract_id)
), by = .(contract_category, contract_type_raw, bid_type)]

# Order by count descending for readability
setorder(summary_counts, -n_unique_claims)

# Print result
print(summary_counts)