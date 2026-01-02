# Pennsylvania UST Auction Analysis

**Causal Inference Analysis of Pay-for-Performance Auctions in Environmental Remediation**

[![R Version](https://img.shields.io/badge/R-%3E%3D4.0-blue)](https://www.r-project.org/)
[![License](https://img.shields.io/badge/License-MIT-green)]()

## Overview

This repository contains the complete analytical pipeline for evaluating Pennsylvania's Underground Storage Tank Indemnification Fund (USTIF) auction procurement program. The analysis examines whether Pay-for-Performance (PFP) auctions reduce remediation costs compared to Time-and-Materials (T&M) contracts, using instrumental variables (IV) and double machine learning (DML) to correct for selection bias.

## Key Research Question

> At what point in a cleanup's lifecycle should USTIF intervene with an auction to minimize total social cost?

## Repository Structure

```
PA_UST_Auction_Analysis/
├── R/
│   ├── 00_master_script.R          # Central orchestration
│   ├── analysis/
│   │   ├── 01_descriptive_stats.R  # Selection bias visualization
│   │   ├── 02_cost_correlates.R    # Descriptive regressions
│   │   └── 03_causal_inference.R   # IV, DML estimation
│   ├── etl/
│   │   ├── 01_load_ustif_data.R    # Load USTIF Excel files
│   │   ├── 02a_padep_download.R    # PA DEP ArcGIS bulk download
│   │   ├── 02b_efacts_scrape.R     # eFACTS compliance scraper
│   │   ├── 03_merge_master_dataset.R
│   │   └── 04_construct_analysis_panel.R
│   ├── functions/
│   │   └── output_helpers.R        # Multi-format save utilities
│   └── validation/
│       └── 01_data_quality_checks.R
├── data/
│   ├── raw/                        # USTIF Excel files (DO NOT COMMIT)
│   ├── processed/                  # Cleaned datasets
│   └── external/
│       ├── padep/                  # PA DEP facility data
│       └── efacts/                 # eFACTS compliance data
├── output/
│   ├── figures/                    # PNG + PDF
│   ├── tables/                     # HTML + LaTeX + PDF
│   └── models/                     # RDS model objects
├── qmd/
│   ├── policy_brief.qmd
│   └── research_proposal.qmd
├── literature/
│   └── references.bib
└── docs/
```

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/[username]/PA_UST_Auction_Analysis.git
cd PA_UST_Auction_Analysis
```

### 2. Install Dependencies

```r
# In R console
install.packages("pacman")
source("R/00_master_script.R")  # Auto-installs all dependencies
```

### 3. Add Data Files

Place the following in `data/raw/`:
- `Actuarial_Contract_Data_2.xlsx`
- `Actuarial_UST_Individual_Claim_Data_thru_63020_4.xlsx`
- `Tank_Construction_Closed.xlsx`
- `USTIF_Auction_Q_A.txt`


## Data Linkage Architecture

### Known Limitations

⚠️ **Critical Linkage Gap**: USTIF claims data does not contain facility permit numbers. 
The join between claims and tank characteristics (`department` = `facility_id`) in 
`04_construct_analysis_panel.R` is semantically invalid and produces ~21% spurious matches.

**Impact**: Tank-level covariates (age, wall type, capacity) are not reliably linked to claims.

**Workaround Options**:
1. Request USTIF export with `permit_number` field
2. Implement fuzzy matching on `claimant_name` + `location_desc`
3. Proceed with claims-only analysis (current default)

### Data Flow Diagram
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA PIPELINE FLOW                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │ USTIF Excel  │     │ PA DEP APIs  │     │ eFACTS Web   │                │
│  │ (Claims/     │     │ (ArcGIS REST)│     │ (Scraper)    │                │
│  │  Contracts/  │     │              │     │              │                │
│  │  Tanks)      │     │              │     │              │                │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                │
│         │                    │                    │                         │
│         ▼                    ▼                    ▼                         │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │ 01_load_     │     │ 02a_padep_   │     │ 02b_efacts_  │                │
│  │ ustif_data.R │     │ download.R   │     │ scrape.R     │                │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                │
│         │                    │                    │                         │
│         ▼                    ▼                    ▼                         │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │claims_clean  │     │facility_     │     │efacts_*      │                │
│  │contracts_    │     │linkage_table │     │(9 tables)    │                │
│  │clean         │     │              │     │              │                │
│  │tanks_clean   │     │              │     │              │                │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘                │
│         │                    │                    │                         │
│         │    ┌───────────────┴────────────────────┘                         │
│         │    │                                                              │
│         │    │  permit_number ↔ efacts_facility_id                          │
│         │    │  (WORKING LINK)                                              │
│         │    │                                                              │
│         ▼    ▼                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    03_merge_master_dataset.R                          │  │
│  │                                                                       │  │
│  │  claims ←─JOIN─→ contracts  ✓ (claim_number)                         │  │
│  │  claims ←─????─→ tanks      ✗ (department ≠ facility_id)             │  │
│  │  tanks  ←─JOIN─→ linkage    ✓ (facility_id = permit_number)          │  │
│  │  linkage←─JOIN─→ efacts     ✓ (efacts_facility_id)                   │  │
│  │                                                                       │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Server Deployment for eFACTS Scrape

**Estimated Runtime**: 8-12 days for full PA universe (~35,000 facilities)

**Resource Requirements**:
- RAM: 2GB minimum
- Disk: 5GB for output CSVs
- Network: Stable connection (scraper has retry logic)

**Checkpoint System**: 
- Saves every 50 facilities
- Auto-resumes from `scrape_checkpoint_v21.rds`
- Safe to interrupt with Ctrl+C

**Recommended**: Run in `screen` or `tmux` session on remote server.

### Troubleshooting

| Issue | Solution |
|-------|----------|
| `HTTP 429 Too Many Requests` | Increase `CONFIG$delay_facility` to 2.0 seconds |
| `JSON parse error` | Check `scrape_log.txt`; retry individual facility manually |
| `CRITICAL: No facilities have eFACTS IDs` | Verify `02a` ran successfully; check `facility_linkage_table.csv` |
| Low match rate in `04_construct_analysis_panel.R` | Expected behavior; see Linkage Limitations above |

### 4. Run Pipeline

```r
source("R/00_master_script.R")

# Option A: Full pipeline (recommended)
run_all()

# Option B: Step-by-step
run_etl()           # Data loading and cleaning
run_validation()    # Data quality checks
run_analysis()      # Descriptive + causal inference
run_render()        # Quarto documents
```

## Output Formats

All figures and tables are saved in multiple formats for flexibility:

| Type | Formats | Location |
|------|---------|----------|
| Figures | `.png`, `.pdf` | `output/figures/` |
| Tables | `.html`, `.tex`, `.pdf` | `output/tables/` |
| Models | `.rds` | `output/models/` |

## Identification Strategy

### The Selection Problem

Sites assigned to PFP auctions are **not randomly selected**. USTIF selects "problem" sites—those with cost overruns or technical stagnation under T&M. Naive cost comparisons conflate the **treatment effect** with the **selection effect**.

### Solution: Instrumental Variables

We exploit quasi-random variation in TPA adjuster assignment. Different adjusters have different propensities to use PFP ("hawks" vs. "doves"). The leave-one-out leniency measure instruments for treatment:

```
Z_i = (1/(N_j - 1)) × Σ_{k≠i} 1[PFP_k = 1]
```

### Robustness: Double Machine Learning

Under conditional ignorability given rich observables, DML uses Random Forest to flexibly partial out confounders while maintaining valid inference on the treatment effect.

## Key Outputs

| Output | Description |
|--------|-------------|
| `cost_density_plot` | Survivorship bias visualization |
| `balance_table` | Covariate imbalance evidence |
| `rf_importance` | "Tacit Rule" - what drives PFP assignment |
| `causal_estimates` | IV vs. DML treatment effect comparison |
| `first_stage_plot` | Instrument strength diagnostic |

## External Data Acquisition

### PA DEP Data (Automatic)

```r
run_external()  # Downloads from PASDA ArcGIS REST APIs
```

### eFACTS Compliance Data (Manual/Long-Running)

```bash
# Run on server (8-12 hours for full PA universe)
nohup Rscript R/etl/02b_efacts_scrape.R > scrape.log 2>&1 &
```

Features checkpoint/resume for interruptions.

## Citation

```bibtex
@misc{javier2025ustif,
  author = {Javier, Kaleb K.},
  title = {Optimal Intervention in Environmental Remediation},
  year = {2025},
  institution = {UC Berkeley, Department of Agricultural and Resource Economics}
}
```

## License

MIT License. See `LICENSE` for details.

## Contact

Kaleb K. Javier  
kalebkja@berkeley.edu  
UC Berkeley, Agricultural and Resource Economics
