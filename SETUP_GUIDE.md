# Setup Guide: PA UST Auction Analysis

## Prerequisites

### Software Requirements

| Software | Version | Required | Notes |
|----------|---------|----------|-------|
| R | ≥ 4.0 | Yes | [Download](https://cran.r-project.org/) |
| RStudio | Latest | Recommended | [Download](https://posit.co/download/rstudio-desktop/) |
| Quarto | ≥ 1.3 | For rendering | [Download](https://quarto.org/docs/get-started/) |
| Git | Latest | For version control | [Download](https://git-scm.com/) |

### R Packages

Packages are automatically installed by `00_master_script.R`. Key dependencies:

**Core Analysis:**
- `tidyverse` - Data manipulation
- `fixest` - Fast fixed effects estimation
- `modelsummary` - Regression tables
- `ranger` - Random Forest

**Causal Inference:**
- `DoubleML` - Double Machine Learning
- `mlr3` + `mlr3learners` - ML framework

**Output:**
- `gt` - Publication tables
- `flextable` - Word/PDF tables
- `webshot2` - HTML to PDF conversion

## Step-by-Step Setup

### Step 1: Clone Repository

```bash
git clone https://github.com/[username]/PA_UST_Auction_Analysis.git
cd PA_UST_Auction_Analysis
```

Or download and extract the ZIP file.

### Step 2: Add Data Files

Place USTIF administrative data in `data/raw/`:

```
data/raw/
├── Actuarial_Contract_Data_2.xlsx
├── Actuarial_UST_Individual_Claim_Data_thru_63020_4.xlsx
├── Tank_Construction_Closed.xlsx
└── USTIF_Auction_Q_A.txt
```

**IMPORTANT:** Do NOT commit these files to Git (they contain confidential information).

### Step 3: Open Project

Open `PA_UST_Auction_Analysis.Rproj` in RStudio (if using RStudio), or set working directory:

```r
setwd("/path/to/PA_UST_Auction_Analysis")
```

### Step 4: Load Master Script

```r
source("R/00_master_script.R")
```

This will:
- Install missing packages automatically
- Create all necessary directories
- Load helper functions and themes
- Print available pipeline functions

### Step 5: Run Pipeline

**Option A: Full Pipeline (Recommended)**

```r
run_all()
```

This runs:
1. ETL (data loading and cleaning)
2. Validation (data quality checks)
3. Analysis (descriptive + causal inference)
4. Render (Quarto documents)

**Option B: Step-by-Step**

```r
# Run individual stages
run_etl()           # ~2 minutes
run_validation()    # ~30 seconds
run_analysis()      # ~5-10 minutes (DML is slowest)
run_render()        # ~1 minute
```

**Option C: Skip Slow Components**

```r
# Skip causal inference (IV/DML) for faster debugging
run_analysis(skip_causal = TRUE)

# Skip external PA DEP data (default)
run_etl(skip_external = TRUE)
```

## External Data Acquisition (Optional)

### PA DEP Facility Data

Downloads ~20K facility records from PASDA ArcGIS REST APIs:

```r
run_external()
```

Runtime: ~5-10 minutes

### eFACTS Compliance Data

Scrapes inspection/violation history from eFACTS web interface:

```r
run_external(run_scrape = TRUE)
```

**WARNING:** This takes **8-12 hours** for the full PA facility universe.

**Recommended:** Run on a server with checkpoint/resume:

```bash
nohup Rscript R/etl/02b_efacts_scrape.R > scrape.log 2>&1 &

# Monitor progress
tail -f data/external/efacts/scrape_log.txt
```

The scraper saves checkpoints every 100 facilities and can resume from interruptions.

## Output Locations

After running the pipeline, outputs are organized as follows:

```
output/
├── figures/
│   ├── cost_density_plot.png
│   ├── cost_density_plot.pdf
│   ├── cost_boxplot.png
│   ├── cost_boxplot.pdf
│   ├── temporal_trends.png
│   ├── temporal_trends.pdf
│   ├── geographic_distribution.png
│   ├── geographic_distribution.pdf
│   ├── rf_importance.png
│   ├── rf_importance.pdf
│   ├── causal_estimates_plot.png
│   ├── causal_estimates_plot.pdf
│   ├── first_stage_plot.png
│   ├── first_stage_plot.pdf
│   ├── coefficient_plot.png
│   ├── coefficient_plot.pdf
│   ├── heterogeneity_by_era.png
│   └── heterogeneity_by_era.pdf
├── tables/
│   ├── balance_table.html
│   ├── balance_table.tex
│   ├── balance_table.docx
│   ├── summary_statistics.html
│   ├── summary_statistics.tex
│   ├── summary_statistics.pdf
│   ├── causal_estimates.html
│   ├── causal_estimates.tex
│   ├── causal_estimates.pdf
│   ├── cost_correlates_regression.html
│   ├── cost_correlates_regression.tex
│   ├── cost_correlates_by_era.html
│   └── cost_correlates_by_era.tex
└── models/
    ├── rf_propensity_model.rds
    ├── iv_model.rds
    ├── dml_model.rds
    ├── cost_correlates_models.rds
    └── cost_correlates_by_era.rds
```

## Troubleshooting

### Common Issues

**1. Package Installation Failures**

```r
# If automatic installation fails, try manual:
install.packages("DoubleML", dependencies = TRUE)
install.packages("mlr3learners")
```

**2. LaTeX/PDF Generation Errors**

```r
# Install TinyTeX for LaTeX rendering
tinytex::install_tinytex()
```

**3. webshot2 PDF Errors**

```r
# webshot2 requires Chrome/Chromium
# Install with:
webshot2::install_chromote()
```

**4. "Working directory" Error**

```r
# Ensure you're in project root
setwd("/path/to/PA_UST_Auction_Analysis")
# Verify:
file.exists("R/00_master_script.R")  # Should be TRUE
```

**5. Memory Issues with DML**

```r
# Reduce DML folds if memory constrained
# Edit R/analysis/03_causal_inference.R:
# Change n_folds = 5 to n_folds = 3
```

### Getting Help

1. Check error messages in console
2. Review `data/external/efacts/scrape_log.txt` for scraping issues
3. Contact: kalebkja@berkeley.edu

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.0 | Dec 2024 | Multi-format output, IV/DML causal inference, PA DEP integration |
| 1.0 | Dec 2024 | Initial descriptive analysis |
