# R/functions/output_helpers.R
# ============================================================================
# Pennsylvania UST Auction Analysis - Output Helper Functions
# ============================================================================
# Purpose: Standardized functions for saving figures and tables in multiple
#          formats (LaTeX, HTML, PDF, PNG) for flexible downstream use
# ============================================================================

# ============================================================================
# SETUP
# ============================================================================

#' Ensure required packages are available
ensure_output_packages <- function() {
  required <- c("ggplot2", "gt", "modelsummary", "webshot2", "tinytex")
  missing <- required[!sapply(required, requireNamespace, quietly = TRUE)]
  if (length(missing) > 0) {
    message("Installing missing packages: ", paste(missing, collapse = ", "))
    install.packages(missing)
  }
}

# ============================================================================
# FIGURE OUTPUT FUNCTIONS
# ============================================================================

#' Save a ggplot figure in multiple formats
#' 
#' @param plot A ggplot2 object
#' @param filename Base filename without extension (e.g., "cost_density")
#' @param output_dir Directory for output (default: "output/figures")
#' @param width Figure width in inches (default: 10)
#' @param height Figure height in inches (default: 7)
#' @param dpi Resolution for raster formats (default: 300)
#' @param formats Vector of formats to save (default: c("png", "pdf"))
#' @return Invisible list of saved file paths
save_figure <- function(plot, 
                        filename, 
                        output_dir = "output/figures",
                        width = 10, 
                        height = 7, 
                        dpi = 300,
                        formats = c("png", "pdf")) {
  
  # Ensure output directory exists
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  saved_paths <- list()
  
  for (fmt in formats) {
    filepath <- file.path(output_dir, paste0(filename, ".", fmt))
    
    tryCatch({
      ggplot2::ggsave(
        filename = filepath,
        plot = plot,
        width = width,
        height = height,
        dpi = dpi,
        device = fmt
      )
      saved_paths[[fmt]] <- filepath
      cat(sprintf("✓ Saved: %s\n", filepath))
    }, error = function(e) {
      cat(sprintf("✗ Failed to save %s: %s\n", filepath, e$message))
    })
  }
  
  invisible(saved_paths)
}

#' Save figure with all standard formats (PNG, PDF, SVG)
#' 
#' @inheritParams save_figure
save_figure_all <- function(plot, filename, output_dir = "output/figures", 
                            width = 10, height = 7, dpi = 300) {
  save_figure(plot, filename, output_dir, width, height, dpi, 
              formats = c("png", "pdf"))
}

# ============================================================================
# TABLE OUTPUT FUNCTIONS
# ============================================================================

#' Save a gt table in multiple formats
#' 
#' @param table A gt table object
#' @param filename Base filename without extension
#' @param output_dir Directory for output (default: "output/tables")
#' @param formats Vector of formats: "html", "latex", "pdf", "docx", "rtf"
#' @return Invisible list of saved file paths
save_gt_table <- function(table, 
                          filename, 
                          output_dir = "output/tables",
                          formats = c("html", "latex", "pdf")) {
  
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  saved_paths <- list()
  
  for (fmt in formats) {
    filepath <- file.path(output_dir, paste0(filename, ".", 
                                              ifelse(fmt == "latex", "tex", fmt)))
    
    tryCatch({
      if (fmt == "html") {
        gt::gtsave(table, filepath)
        saved_paths[[fmt]] <- filepath
        cat(sprintf("✓ Saved: %s\n", filepath))
        
      } else if (fmt == "latex") {
        # Export LaTeX code
        latex_code <- gt::as_latex(table)
        writeLines(as.character(latex_code), filepath)
        saved_paths[[fmt]] <- filepath
        cat(sprintf("✓ Saved: %s\n", filepath))
        
      } else if (fmt == "pdf") {
        gt::gtsave(table, filepath)
        saved_paths[[fmt]] <- filepath
        cat(sprintf("✓ Saved: %s\n", filepath))
        
      } else if (fmt == "docx") {
        gt::gtsave(table, filepath)
        saved_paths[[fmt]] <- filepath
        cat(sprintf("✓ Saved: %s\n", filepath))
        
      } else if (fmt == "rtf") {
        gt::gtsave(table, filepath)
        saved_paths[[fmt]] <- filepath
        cat(sprintf("✓ Saved: %s\n", filepath))
      }
      
    }, error = function(e) {
      cat(sprintf("✗ Failed to save %s: %s\n", filepath, e$message))
    })
  }
  
  invisible(saved_paths)
}

#' Save gt table with all standard formats
save_gt_table_all <- function(table, filename, output_dir = "output/tables") {
  save_gt_table(table, filename, output_dir, formats = c("html", "latex", "pdf"))
}

#' Save a modelsummary table in multiple formats
#' 
#' @param models List of models or single model
#' @param filename Base filename without extension
#' @param output_dir Directory for output
#' @param formats Vector of formats
#' @param ... Additional arguments passed to modelsummary()
#' @return Invisible list of saved file paths
save_modelsummary <- function(models, 
                              filename, 
                              output_dir = "output/tables",
                              formats = c("html", "latex"),
                              ...) {
  
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  saved_paths <- list()
  
  for (fmt in formats) {
    ext <- ifelse(fmt == "latex", "tex", fmt)
    filepath <- file.path(output_dir, paste0(filename, ".", ext))
    
    tryCatch({
      if (fmt == "html") {
        modelsummary::modelsummary(models, output = filepath, ...)
      } else if (fmt == "latex") {
        modelsummary::modelsummary(models, output = filepath, ...)
      }
      saved_paths[[fmt]] <- filepath
      cat(sprintf("✓ Saved: %s\n", filepath))
    }, error = function(e) {
      cat(sprintf("✗ Failed to save %s: %s\n", filepath, e$message))
    })
  }
  
  invisible(saved_paths)
}

#' Save modelsummary with all standard formats
save_modelsummary_all <- function(models, filename, output_dir = "output/tables", ...) {
  save_modelsummary(models, filename, output_dir, formats = c("html", "latex"), ...)
}

#' Save a flextable in multiple formats (for balance tables, etc.)
#' 
#' @param ft A flextable object
#' @param filename Base filename without extension
#' @param output_dir Directory for output
#' @param formats Vector of formats
save_flextable <- function(ft,
                           filename,
                           output_dir = "output/tables",
                           formats = c("html", "docx", "pdf")) {
  
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  saved_paths <- list()
  
  for (fmt in formats) {
    filepath <- file.path(output_dir, paste0(filename, ".", fmt))
    
    tryCatch({
      if (fmt == "html") {
        flextable::save_as_html(ft, path = filepath)
      } else if (fmt == "docx") {
        flextable::save_as_docx(ft, path = filepath)
      } else if (fmt == "pdf") {
        # Requires webshot2 and Chrome/Chromium
        temp_html <- tempfile(fileext = ".html")
        flextable::save_as_html(ft, path = temp_html)
        webshot2::webshot(temp_html, filepath, selector = "table")
        file.remove(temp_html)
      }
      saved_paths[[fmt]] <- filepath
      cat(sprintf("✓ Saved: %s\n", filepath))
    }, error = function(e) {
      cat(sprintf("✗ Failed to save %s: %s\n", filepath, e$message))
    })
  }
  
  invisible(saved_paths)
}

# ============================================================================
# MODEL OUTPUT FUNCTIONS
# ============================================================================

#' Save model object with metadata
#' 
#' @param model Model object (lm, glm, feols, etc.)
#' @param filename Base filename without extension
#' @param output_dir Directory for output (default: "output/models")
#' @param metadata Optional list of metadata to attach
#' @return Invisible path to saved file
save_model <- function(model, 
                       filename, 
                       output_dir = "output/models",
                       metadata = NULL) {
  
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  filepath <- file.path(output_dir, paste0(filename, ".rds"))
  
  # Wrap model with metadata
  model_obj <- list(
    model = model,
    metadata = c(
      list(
        saved_at = Sys.time(),
        r_version = R.version.string,
        model_class = class(model)
      ),
      metadata
    )
  )
  
  saveRDS(model_obj, filepath)
  cat(sprintf("✓ Saved model: %s\n", filepath))
  
  invisible(filepath)
}

# ============================================================================
# DIRECTORY SETUP HELPER
# ============================================================================

#' Ensure all output directories exist
#' 
#' @param base_dir Project root directory
setup_output_dirs <- function(base_dir = ".") {
  dirs <- c(
    file.path(base_dir, "output/figures"),
    file.path(base_dir, "output/tables"),
    file.path(base_dir, "output/models"),
    file.path(base_dir, "data/processed"),
    file.path(base_dir, "data/external/padep"),
    file.path(base_dir, "data/external/efacts")
  )
  
  for (d in dirs) {
    if (!dir.exists(d)) {
      dir.create(d, recursive = TRUE)
      cat(sprintf("Created: %s\n", d))
    }
  }
  
  invisible(dirs)
}

# ============================================================================
# COMBINED OUTPUT WRAPPER
# ============================================================================

#' Standard output wrapper - saves figure in PNG and PDF
save_plot <- function(plot, name, ...) {
  save_figure(plot, name, formats = c("png", "pdf"), ...)
}

#' Standard output wrapper - saves table in HTML, LaTeX, PDF
save_table <- function(table, name, ...) {
  if (inherits(table, "gt_tbl")) {
    save_gt_table(table, name, formats = c("html", "latex", "pdf"), ...)
  } else if (inherits(table, "flextable")) {
    save_flextable(table, name, formats = c("html", "docx", "pdf"), ...)
  } else {
    warning("Unknown table type. Attempting gt export.")
    save_gt_table(table, name, formats = c("html", "latex", "pdf"), ...)
  }
}
