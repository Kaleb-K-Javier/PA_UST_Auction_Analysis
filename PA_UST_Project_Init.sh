#!/bin/bash
# PA_UST_Project_Init.sh
# Pennsylvania Underground Storage Tank Auction Analysis - Project Setup Script
# Execute this script to initialize the complete project structure

PROJECT_ROOT="PA_UST_Auction_Analysis"

echo "========================================"
echo "PA UST Auction Analysis - Project Setup"
echo "========================================"

# Create directory structure (if not already created)
mkdir -p $PROJECT_ROOT/{data/{raw,processed,external},R/{etl,analysis,functions,validation},qmd,output/{figures,tables,models},docs,literature}

# Create .gitignore
cat > $PROJECT_ROOT/.gitignore << 'EOF'
# R specific
.Rproj.user/
.Rhistory
.RData
.Ruserdata
*.Rproj

# Data exclusions (proprietary USTIF data)
data/raw/*
data/processed/*
!data/raw/.gitkeep
!data/processed/.gitkeep
*.csv
*.xlsx
*.rds

# Output exclusions
output/models/*.rds
*.pdf
*.html

# Quarto
/.quarto/
_site/
_book/
*_files/

# System
.DS_Store
Thumbs.db

# LaTeX auxiliary files
*.aux
*.log
*.out
*.toc
*.bbl
*.blg
EOF

# Create placeholder files
touch $PROJECT_ROOT/data/{raw,processed,external}/.gitkeep
touch $PROJECT_ROOT/output/{figures,tables,models}/.gitkeep

# Create README
cat > $PROJECT_ROOT/README.md << 'EOF'
# Pennsylvania UST Claims Auction Analysis

## Project Objective
Descriptive and exploratory analysis of auction-based cost control mechanisms in underground storage tank remediation claims administered by Pennsylvania's Underground Storage Tank Indemnification Fund (USTIF).

**Analysis Scope:** This project provides descriptive characterization of relationships between facility characteristics, auction mechanisms, and remediation costs. The analysis is exploratory and does not deploy formal causal inference designs.

## Data Sources

### Proprietary (USTIF Administrative Records)
1. `Actuarial_Contract_Data_2.xlsx` - Contractor auction participation and bid records (658 records, 17 fields)
2. `Tank_Construction_Closed.xlsx` - Facility closure and tank status data (6,914 records, 17 fields)
3. `Actuarial_UST_Individual_Claim_Data_thru_63020_4.xlsx` - Claims payment history through June 2020 (7,793 records, 16 fields)
4. `USTIF_Auction_Q_A.txt` - Institutional auction design documentation

### External (PA DEP Open Data - Optional Enrichment)
1. Active Storage Tanks Registry - Facility attributes
2. Cleanup Locations Registry - Remediation status

## Repository Structure
```
├── data/
│   ├── raw/          # Original USTIF datasets (Excel files, not version controlled)
│   ├── processed/    # Cleaned analysis datasets (.rds format)
│   └── external/     # PA DEP administrative downloads (optional)
├── R/
│   ├── etl/          # Data acquisition and harmonization scripts
│   ├── analysis/     # Statistical models and descriptive analysis
│   ├── functions/    # Reusable helper functions
│   └── validation/   # Data quality checks and diagnostics
├── qmd/
│   └── policy_brief.qmd   # Main output document (Quarto)
├── output/
│   ├── figures/      # Exported visualizations (.png)
│   ├── tables/       # Summary tables (LaTeX/HTML)
│   └── models/       # Saved model objects (.rds)
├── literature/
│   └── references.bib     # BibTeX citations
└── docs/
    └── USTIF_auction_design.md   # Institutional background notes
```

## Setup Instructions

### Step 1: Install R and Required Packages
```r
source("R/00_master_script.R")
```

### Step 2: Place Data Files
Copy the following proprietary Excel files to `data/raw/`:
- `Actuarial_Contract_Data_2.xlsx`
- `Tank_Construction_Closed.xlsx`
- `Actuarial_UST_Individual_Claim_Data_thru_63020_4.xlsx`
- `USTIF_Auction_Q_A.txt`

### Step 3: Execute ETL Pipeline
```r
source("R/etl/01_load_ustif_data.R")      # Load and clean USTIF data
source("R/etl/02_padep_acquisition.R")     # (Optional) Download PA DEP data
source("R/etl/03_merge_master_dataset.R")  # Construct analysis panel
```

### Step 4: Validate Data Quality
```r
source("R/validation/01_data_quality_checks.R")
```

### Step 5: Run Analysis
```r
source("R/analysis/01_descriptive_stats.R")
source("R/analysis/02_cost_correlates.R")
```

### Step 6: Render Policy Brief
```bash
quarto render qmd/policy_brief.qmd
```

## Key Research Questions (Descriptive Focus)

1. What is the distribution of remediation costs across USTIF claims?
2. How do costs vary by facility characteristics (county, tank type, claim year)?
3. What is the relationship between auction competition and bid amounts?
4. How do SOW versus Bid-to-Result contracts differ in observed costs?

## Important Notes

- This analysis is **descriptive and exploratory**. Observed correlations should not be interpreted as causal effects.
- All proprietary USTIF data must remain confidential and excluded from version control.
- The policy brief targets non-technical stakeholders; statistical details are in the Technical Appendix.

## Contact
[Your Name] - [your.email@institution.edu]
EOF

echo "✓ Project structure created"
echo ""
echo "NEXT STEPS:"
echo "  1. Copy proprietary Excel files to data/raw/"
echo "  2. Open R and run: source('R/00_master_script.R')"
echo "  3. Execute ETL scripts in R/etl/ sequentially"
echo "  4. Render policy brief: quarto render qmd/policy_brief.qmd"
echo ""
echo "========================================"
