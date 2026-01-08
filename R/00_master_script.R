# R/00_master_script.R
# ============================================================================
# Pennsylvania UST Auction Analysis - Master Configuration
# ============================================================================
# Purpose: Install dependencies, set global options, and define project paths
# Run this script FIRST before any other analysis
# ============================================================================

cat("\n========================================\n")
cat("PA UST Auction Analysis - Environment Setup\n")
cat("========================================\n\n")

# ----------------------------------------------------------------------------
# SECTION 1: Package Installation
# ----------------------------------------------------------------------------

required_packages <- c(
  # Data manipulation
  "tidyverse",      # Core data wrangling (dplyr, ggplot2, tidyr, etc.)
  "data.table",     # Fast data operations
  "janitor",        # Clean column names
  "lubridate",      # Date handling
  
  # Excel data import/export
  "readxl",         # Read Excel files
  "writexl",        # Write Excel files
  
  # Data acquisition (for PA DEP APIs - optional)
  "httr",           # HTTP requests
  "jsonlite",       # JSON parsing
  
  # Statistical analysis
  "fixest",         # Fixed effects regression (fast, flexible)
  "broom",          # Tidy model outputs
  "modelsummary",   # Publication-ready regression tables
  
  # Tables and output
  "gt",             # Grammar of tables
  "gtsummary",      # Summary statistics tables
  "kableExtra",     # Enhanced kable tables
  
  # Visualization
  "ggplot2",        # Core plotting (in tidyverse)
  "patchwork",      # Combine ggplots
  "scales",         # Axis formatting
  "ggridges",       # Ridge plots for distributions
  "viridis",        # Color scales
  
  # Quarto/RMarkdown
  "quarto",         # Quarto rendering
  "knitr",          # Dynamic documents
  "rmarkdown"       # R Markdown
)

# Install missing packages
cat("Checking package dependencies...\n")
new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]

if(length(new_packages) > 0) {
  cat(paste("Installing:", paste(new_packages, collapse = ", "), "\n"))
  install.packages(new_packages, quiet = TRUE)
} else {
  cat("All required packages are installed.\n")
}

# Load all packages
cat("\nLoading packages...\n")
suppressPackageStartupMessages({
  invisible(lapply(required_packages, library, character.only = TRUE))
})
cat("✓ All packages loaded successfully.\n")

# ----------------------------------------------------------------------------
# SECTION 2: Global Options
# ----------------------------------------------------------------------------

cat("\nSetting global options...\n")

options(
  # Numeric display
  scipen = 999,                               # Disable scientific notation
  digits = 4,                                 # Default decimal places
  
  # Table defaults
  modelsummary_factory_default = "gt",        # Use gt for regression tables
  
  # Data handling
  stringsAsFactors = FALSE,                   # Strings stay as character
  
  # Error handling
  warn = 1                                    # Print warnings immediately
)

# ----------------------------------------------------------------------------
# SECTION 3: ggplot2 Theme Configuration
# ----------------------------------------------------------------------------

# Define custom theme for consistent visualizations
theme_ustif <- function(base_size = 11) {
  theme_minimal(base_size = base_size) +
    theme(
      # Title formatting
      plot.title = element_text(face = "bold", size = base_size + 1, hjust = 0),
      plot.subtitle = element_text(size = base_size - 1, color = "gray40"),
      plot.caption = element_text(size = base_size - 2, color = "gray50", hjust = 0),
      
      # Axis formatting
      axis.title = element_text(size = base_size - 1),
      axis.text = element_text(size = base_size - 2),
      
      # Legend formatting
      legend.position = "bottom",
      legend.title = element_text(face = "bold", size = base_size - 1),
      legend.text = element_text(size = base_size - 2),
      
      # Panel formatting
      panel.grid.minor = element_blank(),
      panel.grid.major = element_line(color = "gray90"),
      
      # Strip formatting (for facets)
      strip.text = element_text(face = "bold", size = base_size - 1),
      strip.background = element_rect(fill = "gray95", color = NA)
    )
}

# Set as default theme
theme_set(theme_ustif())

# Define color palette
ustif_colors <- c(
  "primary" = "#1f77b4",     # Blue
  "secondary" = "#ff7f0e",   # Orange
  "tertiary" = "#2ca02c",    # Green
  "quaternary" = "#d62728",  # Red
  "gray" = "#7f7f7f"         # Gray
)

# Set default discrete color scale
scale_color_ustif <- function(...) {
  scale_color_manual(values = unname(ustif_colors[1:4]), ...)
}

scale_fill_ustif <- function(...) {
  scale_fill_manual(values = unname(ustif_colors[1:4]), ...)
}

cat("✓ ggplot2 theme configured.\n")

# ----------------------------------------------------------------------------
# SECTION 4: Project Paths
# ----------------------------------------------------------------------------

cat("\nConfiguring project paths...\n")

# Define paths relative to project root
# NOTE: Adjust if your working directory differs
paths <- list(
  raw = "data/raw",
  processed = "data/processed",
  external = "data/external",
  figures = "output/figures",
  tables = "output/tables",
  models = "output/models"
)

# Create directories if they don't exist
invisible(lapply(paths, function(p) {
  if (!dir.exists(p)) dir.create(p, recursive = TRUE)
}))

cat("✓ Project paths configured.\n")

# ----------------------------------------------------------------------------
# SECTION 5: Helper Functions
# ----------------------------------------------------------------------------

cat("\nLoading helper functions...\n")

#' Format dollar amounts
#' @param x Numeric vector
#' @param digits Decimal places (default 0)
#' @return Character vector with dollar formatting
format_dollar <- function(x, digits = 0) {
  scales::dollar(x, accuracy = 10^(-digits))
}

#' Format percentages
#' @param x Numeric vector (as proportion, e.g., 0.25 for 25%)
#' @param digits Decimal places (default 1)
#' @return Character vector with percentage formatting
format_pct <- function(x, digits = 1) {
  scales::percent(x, accuracy = 10^(-digits))
}

#' Calculate summary statistics for a numeric variable
#' @param data Data frame
#' @param var Variable name (unquoted)
#' @param by Optional grouping variable (unquoted)
#' @return Data frame with summary statistics
calc_summary <- function(data, var, by = NULL) {
  var_enquo <- enquo(var)
  by_enquo <- enquo(by)
  
  if (quo_is_null(by_enquo)) {
    data %>%
      summarise(
        n = n(),
        n_missing = sum(is.na(!!var_enquo)),
        mean = mean(!!var_enquo, na.rm = TRUE),
        sd = sd(!!var_enquo, na.rm = TRUE),
        min = min(!!var_enquo, na.rm = TRUE),
        p25 = quantile(!!var_enquo, 0.25, na.rm = TRUE),
        median = median(!!var_enquo, na.rm = TRUE),
        p75 = quantile(!!var_enquo, 0.75, na.rm = TRUE),
        max = max(!!var_enquo, na.rm = TRUE)
      )
  } else {
    data %>%
      group_by(!!by_enquo) %>%
      summarise(
        n = n(),
        n_missing = sum(is.na(!!var_enquo)),
        mean = mean(!!var_enquo, na.rm = TRUE),
        sd = sd(!!var_enquo, na.rm = TRUE),
        min = min(!!var_enquo, na.rm = TRUE),
        p25 = quantile(!!var_enquo, 0.25, na.rm = TRUE),
        median = median(!!var_enquo, na.rm = TRUE),
        p75 = quantile(!!var_enquo, 0.75, na.rm = TRUE),
        max = max(!!var_enquo, na.rm = TRUE),
        .groups = "drop"
      )
  }
}

#' Save a ggplot with consistent dimensions
#' @param plot ggplot object
#' @param filename Filename without path (saved to output/figures/)
#' @param width Width in inches (default 8)
#' @param height Height in inches (default 5)
save_plot <- function(plot, filename, width = 8, height = 5) {
  filepath <- file.path(paths$figures, filename)
  ggsave(filepath, plot = plot, width = width, height = height, dpi = 300)
  cat(paste("✓ Saved:", filepath, "\n"))
}

cat("✓ Helper functions loaded.\n")

# ----------------------------------------------------------------------------
# SECTION 6: Data Dictionary Initialization
# ----------------------------------------------------------------------------

cat("\nInitializing data dictionary...\n")

# Create empty data dictionary template
data_dictionary <- tibble(
  variable_name = character(),
  description = character(),
  source_file = character(),
  data_type = character(),
  n_records = integer(),
  n_missing = integer(),
  unique_values = integer(),
  notes = character()
)

saveRDS(data_dictionary, file.path(paths$processed, "data_dictionary.rds"))

cat("✓ Data dictionary template created.\n")

# ----------------------------------------------------------------------------
# SECTION 7: Session Info Summary
# ----------------------------------------------------------------------------

cat("\n========================================\n")
cat("SETUP COMPLETE\n")
cat("========================================\n")
cat(paste("R Version:", R.version.string, "\n"))
cat(paste("Working Directory:", getwd(), "\n"))
cat(paste("Date:", Sys.Date(), "\n"))
cat("\n")
cat("NEXT STEPS:\n")
cat("  1. Copy proprietary Excel files to data/raw/\n")
cat("  2. Run: source('R/etl/01_load_ustif_data.R')\n")
cat("  3. Run: source('R/etl/03_merge_master_dataset.R')\n")
cat("  4. Run: source('R/analysis/01_descriptive_stats.R')\n")
cat("========================================\n\n")
