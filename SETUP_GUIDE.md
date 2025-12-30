# PA UST Auction Analysis - Setup Guide

## Quick Start

### Step 1: Open R/RStudio
Navigate to the project directory and open RStudio (or R).

### Step 2: Install Dependencies
```r
source("R/00_master_script.R")
```

This will:
- Install all required R packages (~20 packages)
- Configure global settings and themes
- Create the data dictionary template

### Step 3: Run ETL Pipeline
Execute these scripts in order:

```r
# Step 3a: Load and clean USTIF proprietary data
source("R/etl/01_load_ustif_data.R")

# Step 3b: (Optional) Download PA DEP external data
source("R/etl/02_padep_acquisition.R")

# Step 3c: Merge datasets into analysis panel
source("R/etl/03_merge_master_dataset.R")
```

### Step 4: Validate Data Quality
```r
source("R/validation/01_data_quality_checks.R")
```

### Step 5: Run Analysis
```r
# Descriptive statistics and visualizations
source("R/analysis/01_descriptive_stats.R")

# Cost correlates regression (descriptive)
source("R/analysis/02_cost_correlates.R")
```

### Step 6: Render Policy Brief
In terminal:
```bash
quarto render qmd/policy_brief.qmd
```

Or in R:
```r
quarto::quarto_render("qmd/policy_brief.qmd")
```

---

## File Structure

```
PA_UST_Auction_Analysis/
│
├── data/
│   ├── raw/                  # Original Excel files (DO NOT MODIFY)
│   │   ├── Actuarial_Contract_Data_2.xlsx
│   │   ├── Actuarial_UST_Individual_Claim_Data_thru_63020_4.xlsx
│   │   ├── Tank_Construction_Closed.xlsx
│   │   └── USTIF_Auction_Q_A.txt
│   ├── processed/            # Cleaned .rds files (created by ETL)
│   └── external/             # PA DEP downloads (optional)
│
├── R/
│   ├── 00_master_script.R    # Package installation and config
│   ├── etl/
│   │   ├── 01_load_ustif_data.R       # Clean proprietary Excel files
│   │   ├── 02_padep_acquisition.R     # Download PA DEP data (optional)
│   │   └── 03_merge_master_dataset.R  # Build analysis panel
│   ├── analysis/
│   │   ├── 01_descriptive_stats.R     # Summary statistics & figures
│   │   └── 02_cost_correlates.R       # Descriptive regression
│   ├── functions/            # Reusable helper functions
│   └── validation/
│       └── 01_data_quality_checks.R   # Data quality assessment
│
├── qmd/
│   └── policy_brief.qmd      # Main output document
│
├── output/
│   ├── figures/              # Exported .png visualizations
│   ├── tables/               # HTML/LaTeX tables
│   └── models/               # Saved regression objects
│
├── literature/
│   └── references.bib        # BibTeX citations
│
└── docs/                     # Supplementary documentation
```

---

## Data Files Description

### 1. Actuarial_Contract_Data_2.xlsx
- **Records**: 658
- **Purpose**: Auction/bid records for USTIF contracts
- **Key Fields**:
  - `Contract Jobs.Claim Number`: Links to claims data
  - `Bid Type Desc`: SOW vs. Bid-to-Result
  - `Contract Base Price`: Original contract amount
  - `Amount Paid to Date`: Payments made

### 2. Actuarial_UST_Individual_Claim_Data_thru_63020_4.xlsx
- **Records**: 7,793
- **Purpose**: Individual claims payment history
- **Key Fields**:
  - `Claim Number`: Unique identifier
  - `Paid Loss`: Remediation costs paid
  - `County`: Geographic location
  - `DEP Region`: Regulatory region
- **Note**: Skip first 3 rows when reading (header on row 4)

### 3. Tank_Construction_Closed.xlsx
- **Records**: 6,914
- **Purpose**: Tank-level facility data (closed tanks)
- **Key Fields**:
  - `PF_OTHER_ID`: Facility identifier
  - `CAPACITY`: Tank capacity in gallons
  - `DATE_INSTALLED`: Installation date
  - `COMP_DESC`: Tank construction type

### 4. USTIF_Auction_Q_A.txt
- **Purpose**: Institutional knowledge about auction design
- **Key Information**:
  - SOW vs. Bid-to-Result differences
  - 70% viability threshold for bids
  - Selection criteria for auction sites

---

## Outputs Generated

After running the full pipeline, you'll have:

### Data Files (data/processed/)
- `contracts_clean.rds` - Cleaned contract data
- `claims_clean.rds` - Cleaned claims data
- `tanks_clean.rds` - Cleaned tank data
- `master_analysis_dataset.rds` - Final analysis panel
- `data_dictionary.rds` - Variable definitions
- `validation_report.rds` - Data quality metrics

### Figures (output/figures/)
- `01_cost_distribution.png` - Histogram of remediation costs
- `02_temporal_trends.png` - Claims and costs over time
- `03_county_distribution.png` - Geographic distribution
- `04_cost_by_contract_type.png` - Costs by procurement type
- `05_coefficient_plot.png` - Regression coefficients
- `06_heterogeneity_by_era.png` - Era-specific patterns

### Tables (output/tables/)
- `01_summary_statistics.html` - Descriptive summary
- `02_cost_correlates_regression.html` - Regression results
- `02_cost_correlates_regression.tex` - LaTeX version

### Policy Brief (qmd/)
- `policy_brief.pdf` or `policy_brief.html` - Final report

---

## Troubleshooting

### "Package not found" errors
Run `source("R/00_master_script.R")` to install missing packages.

### "File not found" errors
Ensure data files are in `data/raw/` with exact filenames.

### Quarto rendering fails
- Install Quarto: https://quarto.org/docs/get-started/
- Check for LaTeX errors in the .log file
- Try rendering to HTML first: `quarto render qmd/policy_brief.qmd --to html`

### Memory issues with large data
- Close other applications
- Consider subsetting data for initial exploration

---

## Important Notes

1. **Analysis is DESCRIPTIVE**: All findings represent correlations, not causal effects.

2. **Selection Bias**: USTIF selects which claims go to auction. Cost differences may reflect this selection process.

3. **Data Confidentiality**: USTIF data is proprietary. Do not share raw files publicly.

4. **Reproducibility**: All analysis is reproducible from raw data using the scripts provided.
