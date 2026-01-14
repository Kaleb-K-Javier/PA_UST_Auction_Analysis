# R/functions/style_guide.R
library(ggplot2)
library(scales)

# --- 1. Master Theme ---
theme_ustif_pub <- function(base_size = 11, base_family = "sans") {
  theme_minimal(base_size = base_size, base_family = base_family) +
    theme(
      # No internal titles (handled by Quarto captions)
      plot.title = element_blank(),
      plot.subtitle = element_blank(),
      plot.caption = element_blank(),
      
      # Clean, bold axes
      axis.title = element_text(face = "bold", size = rel(0.9), color = "black"),
      axis.text = element_text(color = "#333333", size = rel(0.8)),
      
      # Legend: clean and bottom-aligned
      legend.position = "bottom",
      legend.title = element_text(face = "bold", size = rel(0.8)),
      legend.text = element_text(size = rel(0.8)),
      legend.key.width = unit(1.0, "cm"),
      legend.margin = margin(t = 0, b = 0),
      
      # Gridlines: subtle
      panel.grid.minor = element_blank(),
      panel.grid.major = element_line(color = "#e5e5e5", linewidth = 0.2),
      
      # Margins
      plot.margin = margin(t = 5, r = 10, b = 5, l = 5)
    )
}

# --- 2. Color Palettes ---
# Discrete (e.g., Regions, Contract Types)
scale_fill_ustif <- function(...) scale_fill_viridis_d(option = "mako", begin = 0.2, end = 0.8, ...)
scale_color_ustif <- function(...) scale_color_viridis_d(option = "mako", begin = 0.2, end = 0.8, ...)

# Continuous (e.g., Cost, HHI)
scale_fill_ustif_c <- function(...) scale_fill_viridis_c(option = "magma", direction = -1, ...)