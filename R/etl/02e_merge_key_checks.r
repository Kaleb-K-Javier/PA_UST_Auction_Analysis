# R/validation/03e_key_only_merge_test.R
# ============================================================================
# Key-Only Merge Validation
# ============================================================================
# Purpose: Verify the "Merge Flow" by stripping datasets down to JUST their keys
#          and running standard Inner Joins.
#          This confirms the keys align and the merge function works at scale.
# ============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(janitor)
  library(here)
})

cat("================================================================\n")
cat("KEY-ONLY MERGE TEST (INNER JOINS)\n")
cat("================================================================\n\n")

# 1. Load & Clean
# ----------------------------------------------------------------------------
tanks   <- readRDS(here("data/processed/pa_ust_master_facility_tank_database.rds"))
claims  <- readRDS(here("data/processed/claims_clean.rds"))
linkage <- fread(here("data/external/padep/facility_linkage_table.csv"))

setDT(tanks); setDT(claims); setDT(linkage)
tanks   <- clean_names(tanks)
claims  <- clean_names(claims)
linkage <- clean_names(linkage)

# Fix Linkage Key Name
if ("facility_id" %in% names(linkage)) setnames(linkage, "facility_id", "permit_number")

# Standardize Keys (The Critical Fix)
tanks[, fac_id := trimws(as.character(fac_id))]
claims[, department := trimws(as.character(department))]
linkage[, permit_number := trimws(as.character(permit_number))]

# 2. Subset to Keys Only (The "Select Down" Step)
# ----------------------------------------------------------------------------
k_tanks   <- unique(tanks[, .(fac_id)])
k_claims  <- unique(claims[, .(department)])
k_linkage <- unique(linkage[, .(permit_number)])

cat(sprintf("Unique Keys Loaded:\n"))
cat(sprintf("  Tanks:   %d\n", nrow(k_tanks)))
cat(sprintf("  Claims:  %d\n", nrow(k_claims)))
cat(sprintf("  Linkage: %d\n\n", nrow(k_linkage)))

# 3. Execute Inner Joins (The "Merge Flow" Check)
# ----------------------------------------------------------------------------

# Test A: Tanks <-> Linkage
join_tl <- merge(k_tanks, k_linkage, by.x="fac_id", by.y="permit_number", all=FALSE)
rate_tl <- nrow(join_tl) / nrow(k_tanks)

cat("[TEST A] Inner Join: Tanks (L) + Linkage (R)\n")
cat(sprintf("  Result Rows: %d\n", nrow(join_tl)))
cat(sprintf("  Match Rate:  %.1f%% (of Tanks)\n\n", rate_tl * 100))

# Test B: Claims <-> Linkage
join_cl <- merge(k_claims, k_linkage, by.x="department", by.y="permit_number", all=FALSE)
rate_cl <- nrow(join_cl) / nrow(k_claims)

cat("[TEST B] Inner Join: Claims (L) + Linkage (R)\n")
cat(sprintf("  Result Rows: %d\n", nrow(join_cl)))
cat(sprintf("  Match Rate:  %.1f%% (of Claims)\n\n", rate_cl * 100))

# Test C: Claims <-> Tanks
join_ct <- merge(k_claims, k_tanks, by.x="department", by.y="fac_id", all=FALSE)
rate_ct <- nrow(join_ct) / nrow(k_claims)

cat("[TEST C] Inner Join: Claims (L) + Tanks (R)\n")
cat(sprintf("  Result Rows: %d\n", nrow(join_ct)))
cat(sprintf("  Match Rate:  %.1f%% (of Claims)\n", rate_ct * 100))

cat("================================================================\n")