# R/functions/save_utils.R
library(ggplot2)
library(gt)
library(here)
library(kableExtra) # Needed for saving kable objects

ensure_dir <- function(path) {
  if (!dir.exists(dirname(path))) dir.create(dirname(path), recursive = TRUE)
}

# ==============================================================================
# 1. SAVE FIGURES (Fixes "could not find function save_figure")
# ==============================================================================
save_figure <- function(plot, filename, width = 7, height = 4.5) {
  # Construct paths
  path_pdf <- here("output/figures", paste0(filename, ".pdf"))
  path_png <- here("output/figures", paste0(filename, ".png"))
  
  ensure_dir(path_pdf)
  
  # Save PDF (Vector for final doc)
  ggsave(filename = path_pdf, plot = plot, width = width, height = height, 
         device = cairo_pdf) 
  
  # Save PNG (Raster for quick preview/Word)
  ggsave(filename = path_png, plot = plot, width = width, height = height, 
         dpi = 300)
  
  message(sprintf("Saved Figure: %s (PDF & PNG)", filename))
}

# Alias in case you use the old name elsewhere
save_plot_pub <- save_figure

# ==============================================================================
# 2. SAVE TABLES (Fixes "could not find function save_table")
# ==============================================================================
save_table <- function(obj, filename) {
  path_html <- here("output/tables", paste0(filename, ".html"))
  path_tex  <- here("output/tables", paste0(filename, ".tex"))
  path_rds  <- here("output/tables", paste0(filename, ".rds"))
  
  ensure_dir(path_html)
  
  # A. Handle 'gt' objects (Preferred)
  if (inherits(obj, "gt_tbl")) {
    gtsave(obj, filename = path_html) # Preview
    gtsave(obj, filename = path_tex)  # LaTeX for PDF
    saveRDS(obj, path_rds)            # RDS for Quarto
    message(sprintf("Saved GT Table: %s", filename))
    
  # B. Handle 'kable' objects
  } else if (inherits(obj, "knitr_kable") || inherits(obj, "kableExtra")) {
    save_kable(obj, file = path_html, self_contained = TRUE)
    saveRDS(obj, path_rds)
    message(sprintf("Saved Kable Table: %s", filename))
    
  # C. Handle Raw HTML Strings (Fixes calls using as_raw_html)
  } else if (is.character(obj) || inherits(obj, "html")) {
    writeLines(as.character(obj), path_html)
    message(sprintf("Saved HTML String: %s (HTML Only - Warning: No RDS/TeX saved)", filename))
    
  } else {
    warning(sprintf("Could not save %s: Unknown object class.", filename))
  }
}