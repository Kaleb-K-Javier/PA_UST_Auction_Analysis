# R/functions/save_utils.R
library(ggplot2)
library(gt)
library(here)

ensure_dir <- function(path) {
  if (!dir.exists(dirname(path))) dir.create(dirname(path), recursive = TRUE)
}

# Save a ggplot with publication standards
save_plot_pub <- function(plot, filename, width = 7, height = 4.5) {
  # Construct paths
  path_pdf <- here("output/figures", paste0(filename, ".pdf"))
  path_png <- here("output/figures", paste0(filename, ".png"))
  
  ensure_dir(path_pdf)
  
  # Save PDF (Vector for final doc)
  ggsave(filename = path_pdf, plot = plot, width = width, height = height, 
         device = cairo_pdf) # cairo handles fonts better
  
  # Save PNG (Raster for quick preview/Word)
  ggsave(filename = path_png, plot = plot, width = width, height = height, 
         dpi = 300)
  
  message(sprintf("Saved: %s (PDF & PNG)", filename))
}

# Save a GT table as LaTeX
save_table_pub <- function(gt_obj, filename) {
  path_tex <- here("output/tables", paste0(filename, ".tex"))
  ensure_dir(path_tex)
  
  gt_obj %>%
    gtsave(filename = path_tex)
  
  message(sprintf("Saved: %s (TeX)", filename))
}