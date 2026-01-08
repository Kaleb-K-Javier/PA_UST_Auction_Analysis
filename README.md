# Pennsylvania UST Auction Analysis

**Causal Inference Analysis of Pay-for-Performance Auctions in Environmental Remediation**

[![R Version](https://img.shields.io/badge/R-%3E%3D4.0-blue)](https://www.r-project.org/)
[![Status](https://img.shields.io/badge/Status-Production-green)]()

## Overview

This repository contains the analytical pipeline for evaluating Pennsylvania's Underground Storage Tank Indemnification Fund (USTIF) auction procurement program. The analysis examines whether Pay-for-Performance (PFP) auctions reduce remediation costs compared to Time-and-Materials (T&M) contracts, using instrumental variables (IV) and double machine learning (DML) to correct for selection bias.

### Key Research Question

> At what point in a cleanup's lifecycle should USTIF intervene with an auction to minimize total social cost?

---

## ⚠️ Important: Data Architecture Changes (v2.0)

### Facility Data Source Migration

**Previous Architecture (Deprecated):**
- Used `Tank_Construction_Closed.xlsx` (tanks_clean.csv) for facility characteristics
- Limited to 2,969 facilities with construction/closure records
- 18.7% match rate to claims

**Current Architecture (v2.0):**
- Uses **PA DEP Database** (`data/external/padep/`) for all facility characteristics
- `facility_linkage_table.csv` provides complete facility universe (38,545 facilities)
- 93.7% match rate to claims
- Integrates eFACTS compliance data via web scraping

### Excluded Dataset

| Dataset | Status | Reason |
|---------|--------|--------|
| `Tank_Construction_Closed.xlsx` | **EXCLUDED** | Replaced by PA DEP facility_linkage_table |
| `tanks_clean.csv` | **EXCLUDED** | Derived from above; incomplete coverage |

---

## Repository Structure

```
PA_UST_Auction_Analysis/
├── R/
│   ├── 00_master_script.R              # Central orchestration
│   ├── analysis/
│   │   ├── 01_descriptive_stats.R      # Selection bias visualization
│   │   ├── 02_cost_correlates.R        # Descriptive regressions
│   │   └── 03_causal_inference.R       # IV, DML estimation
│   ├── etl/
│   │   ├── 01_load_ustif_data.R        # Load USTIF Excel files
│   │   ├── 02a_padep_download.R        # PA DEP ArcGIS bulk download
│   │   ├── 02b_efacts_scrape.R         # eFACTS compliance scraper (v22)
│   │   ├── 03_merge_master_dataset.R   # Build analysis dataset
│   │   └── 04_construct_analysis_panel.R
│   ├── functions/
│   │   └── output_helpers.R            # Multi-format save utilities
│   └── validation/
│       └── 01_data_quality_checks.R
├── data/
│   ├── raw/                            # USTIF Excel files (DO NOT COMMIT)
│   ├── processed/                      # Cleaned datasets
│   └── external/
│       ├── padep/                      # ★ PRIMARY: PA DEP facility data
│       │   ├── facility_linkage_table.csv   # Master facility crosswalk
│       │   ├── pasda_tanks_active.csv
│       │   ├── pasda_tanks_inactive.csv
│       │   ├── emappa_tanks_active.csv
│       │   └── emappa_land_recycling.csv
│       └── efacts/                     # Scraped compliance data
│           ├── efacts_facility_meta.csv
│           ├── efacts_violations.csv
│           ├── efacts_inspections.csv
│           └── ...
├── output/
│   ├── figures/                        # PNG + PDF
│   ├── tables/                         # HTML + LaTeX + PDF
│   └── models/                         # RDS model objects
├── qmd/
│   ├── policy_brief.qmd
│   └── research_proposal.qmd
└── literature/
    └── references.bib
```

---

## Data Linkage Architecture

### Entity Relationship Diagram

```
┌─────────────────────┐     claim_number      ┌─────────────────────┐
│   claims_clean.csv  │◄────────────────────►│ contracts_clean.csv │
│     (7,793 rows)    │       100% match      │     (658 rows)      │
└─────────┬───────────┘                       └─────────────────────┘
          │
          │ department = permit_number (93.7% match)
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    facility_linkage_table.csv                       │
│                         (38,545 rows)                               │
│  ┌─────────────┬──────────────────┬─────────┬───────────────────┐  │
│  │permit_number│efacts_facility_id│ site_id │ registration_status│  │
│  │ (PK)        │ (FK→eFACTS)      │         │                    │  │
│  └─────────────┴──────────────────┴─────────┴───────────────────┘  │
└─────────┬───────────────────────────┬───────────────────────────────┘
          │                           │
          │ permit_number             │ efacts_facility_id
          ▼                           ▼
┌─────────────────────┐     ┌─────────────────────────────────────────┐
│  pasda_tanks_*.csv  │     │           eFACTS Tables                 │
│ (Active + Inactive) │     │  • efacts_facility_meta.csv             │
│   (45,104 rows)     │     │  • efacts_violations.csv                │
└─────────────────────┘     │  • efacts_inspections.csv               │
                            │  • efacts_permits_detail.csv            │
                            │  • efacts_remediation_*.csv             │
                            └─────────────────────────────────────────┘
```

### Merge Key Analysis Table

| Left Table.Column | Right Table.Column | Key Format | Match Rate | Coverage |
|---|---|---|---|---|
| `claims.claim_number` | `contracts.claim_number` | Integer | **100%** | 531/531 |
| `claims.department` | `facility_linkage.permit_number` | String (XX-XXXXX) | **93.7%** | 5,467/5,835 |
| `claims.department` | `pasda_combined.attributes_facility_i` | String (XX-XXXXX) | **93.7%** | 5,467/5,835 |
| `facility_linkage.efacts_facility_id` | `efacts_*.efacts_facility_id` | Integer | **100%** | 38,545/38,545 |
| `facility_linkage.permit_number` | `pasda_active.attributes_facility_i` | String | 29.4% | 11,339/38,545 |
| `facility_linkage.permit_number` | `pasda_inactive.attributes_facility_i` | String | 87.6% | 33,765/38,545 |

### Coverage Notes

| Join | Coverage | Explanation |
|------|----------|-------------|
| claims → contracts | 6.8% | Most claims never go to auction (expected) |
| claims → facility_linkage | 93.7% | 6.3% have legacy/unregistered permits |
| facility_linkage → eFACTS | 100% | All registered facilities have eFACTS pages |
| facility_linkage → PASDA | 100% | Combined active+inactive provides full coverage |

---

## Quick Start

### 1. Prerequisites

```bash
# Required
R >= 4.0
Quarto >= 1.3  # For document rendering

# Recommended
RStudio (latest)
screen or tmux  # For server scraping
```

### 2. Clone and Setup

```bash
git clone https://github.com/[username]/PA_UST_Auction_Analysis.git
cd PA_UST_Auction_Analysis
```

### 3. Add Proprietary Data

Place USTIF files in `data/raw/`:
- `Actuarial_Contract_Data_2.xlsx`
- `Actuarial_UST_Individual_Claim_Data_thru_63020_4.xlsx`
- `USTIF_Auction_Q_A.txt`

**Note:** `Tank_Construction_Closed.xlsx` is **no longer required**.

### 4. Install Dependencies and Run

```r
# In R console
source("R/00_master_script.R")

# Full pipeline
run_all()

# Or step-by-step:
run_etl()           # Load and clean data
run_external()      # Download PA DEP data (optional but recommended)
run_validation()    # Data quality checks
run_analysis()      # Descriptive + causal inference
run_render()        # Generate Quarto documents
```

---

## ETL Pipeline Details

### Step 1: Load USTIF Data
```r
source("R/etl/01_load_ustif_data.R")
# Output: claims_clean.csv, contracts_clean.csv
```

### Step 2a: Download PA DEP Data
```r
source("R/etl/02a_padep_download.R")
# Output: facility_linkage_table.csv, pasda_tanks_*.csv
# Runtime: ~5-10 minutes
```

### Step 2b: Scrape eFACTS Compliance Data
```r
source("R/etl/02b_efacts_scrape.R")
linkage <- read.csv("data/external/padep/facility_linkage_table.csv")
ids <- unique(linkage$efacts_facility_id[!is.na(linkage$efacts_facility_id)])
run_scraper(ids)
# Output: 9 eFACTS CSVs in data/external/efacts/
# Runtime: 16-24 hours for full 38,545 facilities
```

### Step 3: Build Master Dataset
```r
source("R/etl/03_merge_master_dataset.R")
# Joins: claims → contracts → facility_linkage → PASDA
# Output: master_analysis_dataset.rds
```

---

## eFACTS Scraper (v22)

### Features
- **Checkpoint/Resume**: Saves progress every 100 facilities; survives interruptions
- **Schema Enforcement**: All output tables match canonical schemas (fixes append bugs)
- **Rate Limiting**: Configurable delays prevent server blocking
- **Full Enforcement Parsing**: Extracts penalties, appeals, enforcement actions

### Configuration
```r
CONFIG <- list(
  delay_facility = 1.5,      # Seconds between facility pages
  delay_detail = 0.75,       # Seconds between detail pages
  batch_size = 100,          # Checkpoint frequency
  timeout = 45,              # HTTP timeout
  scrape_violations = TRUE,
  scrape_permit_details = TRUE,
  scrape_remediation_details = TRUE
)
```

### Output Schema
| Table | Rows (Est.) | Key Fields |
|-------|-------------|------------|
| efacts_facility_meta | 38,545 | efacts_facility_id, status, program |
| efacts_tanks | ~100K | efacts_facility_id, sub_facility_name, type |
| efacts_inspections | ~200K | inspection_id, inspection_type_clean, date |
| efacts_violations | ~50K | violation_id, description, penalty_assessed |
| efacts_permits_detail | ~80K | auth_id, permit_number, authorization_type |
| efacts_permits_tasks | ~150K | task, start_date, completion_date |
| efacts_remediation_summary | ~20K | lrpact_id, cleanup_status |
| efacts_remediation_substances | ~40K | substance_released, environmental_impact |
| efacts_remediation_milestones | ~60K | milestone_name, milestone_status |

---

## Server Deployment

### Pre-Flight Checklist
```bash
# 1. Verify linkage table exists
ls -la data/external/padep/facility_linkage_table.csv
wc -l data/external/padep/facility_linkage_table.csv  # Should be ~38,546

# 2. Clear stale data (IMPORTANT: prevents schema conflicts)
rm -rf data/external/efacts/*.csv data/external/efacts/*.rds
rm -f data/external/efacts/scrape_checkpoint_v22.rds

# 3. Test scraper on small sample
Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  run_scraper(c('575014', '575015', '575016'))
"
# Verify: head -2 data/external/efacts/efacts_inspections.csv
```

### Launch Production Scrape
```bash
# Create log directory
mkdir -p logs

# Start in screen session (recommended)
screen -S efacts_scrape

# Launch scraper
nohup Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  linkage <- read.csv('data/external/padep/facility_linkage_table.csv')
  ids <- unique(linkage\$efacts_facility_id[!is.na(linkage\$efacts_facility_id)])
  cat('Starting scrape of', length(ids), 'facilities\n')
  run_scraper(ids)
" > logs/efacts_scrape_$(date +%Y%m%d_%H%M%S).log 2>&1 &

# Detach from screen: Ctrl+A, D
# Reattach later: screen -r efacts_scrape
```

### Monitor Progress
```bash
# Live log
tail -f data/external/efacts/scrape_log.txt

# Progress check
Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  get_scrape_status()
"

# Output file sizes
ls -lh data/external/efacts/*.csv
```

### Runtime Estimates

| Delay Setting | Runtime (38,545 facilities) |
|---------------|----------------------------|
| 1.0s | ~11 hours |
| 1.5s (default) | ~16 hours |
| 2.0s | ~21 hours |

*Add 50-100% buffer for detail page scraping*

### Resume After Interruption
```bash
# Simply re-run the same command
# Checkpoint system automatically skips completed facilities
Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  linkage <- read.csv('data/external/padep/facility_linkage_table.csv')
  ids <- unique(linkage\$efacts_facility_id[!is.na(linkage\$efacts_facility_id)])
  run_scraper(ids)
"
```

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| `Error tokenizing efacts CSV` | Schema conflict from mixed scrape versions | Delete all files in `data/external/efacts/` and restart |
| `0% facility match rate` | Wrong join key | Verify `claims.department` format matches `XX-XXXXX` |
| `HTTP 429` during scrape | Rate limiting | Increase `delay_facility` to 2.0+ seconds |
| Low claims→linkage match | Legacy permits | Expected; 93.7% is normal coverage |
| Scrape stuck | Network timeout | Check `scrape_log.txt`; will auto-retry on resume |

---

## Output Formats

| Type | Formats | Location |
|------|---------|----------|
| Figures | `.png`, `.pdf` | `output/figures/` |
| Tables | `.html`, `.tex`, `.pdf` | `output/tables/` |
| Models | `.rds` | `output/models/` |

---

## Key Outputs

| Output | Description |
|--------|-------------|
| `cost_density_plot` | Survivorship bias visualization |
| `balance_table` | Covariate imbalance evidence |
| `rf_importance` | "Tacit Rule" - what drives PFP assignment |
| `causal_estimates` | IV vs. DML treatment effect comparison |
| `first_stage_plot` | Instrument strength diagnostic |

---

## Citation

```bibtex
@misc{javier2025ustif,
  author = {Javier, Kaleb K.},
  title = {Optimal Intervention in Environmental Remediation},
  year = {2025},
  institution = {UC Berkeley, Department of Agricultural and Resource Economics}
}
```

---

## License

MIT License. See `LICENSE` for details.

---

## Contact

Kaleb K. Javier  
kalebkja@berkeley.edu  
UC Berkeley, Agricultural and Resource Economics