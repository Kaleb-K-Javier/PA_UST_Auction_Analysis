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

  # Save PNG (Raster for preview)
  # Change: added type = "cairo" to match PDF capabilities
  # Change: added bg = "white" to prevent transparency issues in viewers
  ggsave(filename = path_png, plot = plot, width = width, height = height,
         dpi = 300, type = "cairo", bg = "white")

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


# ==============================================================================
# HELPER: Filter modelsummary to p < 0.10 only
# ==============================================================================
filter_significant_coefs <- function(model, threshold = 0.10, 
                                     vcov_type = "hetero",
                                     title = "Significant Predictors",
                                     subtitle = NULL) {
  library(broom)
  library(modelsummary)
  library(kableExtra)
  

  # Extract tidy coefficients with robust SEs
  tidy_df <- broom::tidy(model, conf.int = TRUE)
  
  # For fixest models, get robust SEs
  if (inherits(model, "fixest")) {
    se_robust <- sqrt(diag(vcov(model, vcov = vcov_type)))
    tidy_df$std.error <- se_robust[match(tidy_df$term, names(se_robust))]
    tidy_df$statistic <- tidy_df$estimate / tidy_df$std.error
    tidy_df$p.value <- 2 * pt(-abs(tidy_df$statistic), df = model$nobs - length(coef(model)))
  }
  
  # Filter to significant only
  sig_df <- tidy_df[tidy_df$p.value < threshold & tidy_df$term != "(Intercept)", ]
  
  if (nrow(sig_df) == 0) {
    message("No coefficients significant at p < ", threshold)
    return(NULL)
  }
  
  # Format for presentation
  sig_df$estimate_fmt <- sprintf("%.3f", sig_df$estimate)
  sig_df$se_fmt <- sprintf("(%.3f)", sig_df$std.error)
  sig_df$stars <- ifelse(sig_df$p.value < 0.01, "***",
                         ifelse(sig_df$p.value < 0.05, "**",
                                ifelse(sig_df$p.value < 0.10, "*", "")))
  sig_df$coef_display <- paste0(sig_df$estimate_fmt, sig_df$stars)
  
  # Create clean table
  out_df <- sig_df[, c("term", "coef_display", "se_fmt", "p.value")]
  names(out_df) <- c("Predictor", "Coefficient", "Std. Error", "P-Value")
  
  # Sort by absolute effect size
  out_df <- out_df[order(-abs(sig_df$estimate)), ]
  
  tbl <- kbl(out_df, digits = 3, format = "html",
             caption = title) %>%
    kable_styling(bootstrap_options = c("striped", "hover")) %>%
    footnote(general = paste0("Only showing predictors significant at p < ", 
                              threshold, ". Robust standard errors."))
  
  if (!is.null(subtitle)) {
    tbl <- tbl %>% add_header_above(c(" " = 1, subtitle = 3))
  }
  
  return(list(table = tbl, data = out_df, full_tidy = sig_df))
}