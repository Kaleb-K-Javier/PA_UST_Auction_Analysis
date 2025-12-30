# Pennsylvania UST Claims Auction Analysis

## Project Overview

This repository contains a descriptive and exploratory analysis of auction-based cost control mechanisms in underground storage tank (UST) remediation claims administered by Pennsylvania's Underground Storage Tank Indemnification Fund (USTIF).

**Analysis Scope**: Descriptive characterization of cost distributions, temporal trends, and correlates. This analysis does NOT deploy causal inference designs—observed correlations should not be interpreted as causal effects.

## Quick Start

```r
# 1. Install dependencies
source("R/00_master_script.R")

# 2. Run ETL pipeline
source("R/etl/01_load_ustif_data.R")
source("R/etl/03_merge_master_dataset.R")

# 3. Run analysis
source("R/analysis/01_descriptive_stats.R")
source("R/analysis/02_cost_correlates.R")

# 4. Render policy brief
quarto::quarto_render("qmd/policy_brief.qmd")
```

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for detailed instructions.

## Data Sources

| Dataset | Records | Description |
|---------|---------|-------------|
| Contract Data | 658 | Auction participation and bid records |
| Claims Data | 7,793 | Individual claims payment history (1994-2020) |
| Tank Data | 6,914 | Facility and tank characteristics |

## Key Outputs

- **Policy Brief** (`qmd/policy_brief.qmd`): Executive summary and findings for non-technical stakeholders
- **Figures** (`output/figures/`): Visualizations of cost distributions, trends, and correlates
- **Tables** (`output/tables/`): Summary statistics and regression results

## Repository Structure

```
├── data/raw/           # Original USTIF Excel files
├── data/processed/     # Cleaned analysis datasets
├── R/etl/              # Data loading and cleaning scripts
├── R/analysis/         # Statistical analysis scripts
├── R/validation/       # Data quality checks
├── qmd/                # Quarto policy brief
├── output/             # Figures, tables, models
└── literature/         # BibTeX references
```

## Methodology

The analysis employs:
1. **Summary Statistics**: Measures of central tendency and dispersion
2. **Visualization**: Histograms, box plots, trend lines
3. **Descriptive Regression**: Fixed effects models characterizing conditional correlations

**Important Caveat**: All regression results are descriptive. Selection into auction procurement is endogenous, so coefficients represent conditional correlations, not causal effects.

## Requirements

- R ≥ 4.0
- Quarto (for rendering policy brief)
- R packages: tidyverse, fixest, modelsummary, gt, gtsummary, readxl, lubridate

## License

[To be specified]

## Contact

[Your Name] - [your.email@institution.edu]
