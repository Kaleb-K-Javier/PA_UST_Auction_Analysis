# R/functions/coef_plot.R
# ============================================================================
# Helper function to create coefficient plots
# ============================================================================

#' Create a coefficient plot from a regression model
#' 
#' @param model A model object (lm, glm, fixest, etc.)
#' @param terms Optional character vector of terms to include (NULL = all)
#' @param exclude Optional character vector of terms to exclude
#' @param labels Optional named vector for term labels
#' @param title Plot title
#' @param subtitle Plot subtitle
#' @param xlab X-axis label
#' @param reference_line Y-value for reference line (default 0)
#' @param conf_level Confidence level for intervals (default 0.95)
#' @param color Point/line color
#' @return A ggplot object
coef_plot <- function(model,
                      terms = NULL,
                      exclude = c("(Intercept)"),
                      labels = NULL,
                      title = NULL,
                      subtitle = NULL,
                      xlab = "Coefficient Estimate",
                      reference_line = 0,
                      conf_level = 0.95,
                      color = "#1f77b4") {
  
  library(ggplot2)
  library(broom)
  library(dplyr)
  
  # Extract coefficients with confidence intervals
  coef_df <- tidy(model, conf.int = TRUE, conf.level = conf_level)
  
  # Filter terms
  if (!is.null(terms)) {
    coef_df <- coef_df %>%
      filter(term %in% terms)
  }
  
  if (!is.null(exclude)) {
    coef_df <- coef_df %>%
      filter(!term %in% exclude)
  }
  
  # Apply labels if provided
  if (!is.null(labels)) {
    coef_df <- coef_df %>%
      mutate(term_label = recode(term, !!!labels, .default = term))
  } else {
    coef_df <- coef_df %>%
      mutate(term_label = term)
  }
  
  # Order by estimate magnitude
  coef_df <- coef_df %>%
    mutate(term_label = reorder(term_label, estimate))
  
  # Create plot
  p <- ggplot(coef_df, aes(x = term_label, y = estimate)) +
    geom_hline(yintercept = reference_line, linetype = "dashed", color = "gray50") +
    geom_errorbar(aes(ymin = conf.low, ymax = conf.high), 
                  width = 0.2, color = color, linewidth = 0.8) +
    geom_point(size = 3, color = color) +
    coord_flip() +
    labs(
      title = title,
      subtitle = subtitle,
      x = NULL,
      y = xlab,
      caption = paste0(round(conf_level * 100), "% confidence intervals shown")
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold"),
      plot.caption = element_text(color = "gray50", size = 8)
    )
  
  return(p)
}

#' Create a forest plot for multiple specifications
#' 
#' @param models Named list of model objects
#' @param coef_name Name of coefficient to compare across models
#' @param labels Optional named vector for model labels
#' @param title Plot title
#' @return A ggplot object
forest_plot <- function(models,
                        coef_name,
                        labels = NULL,
                        title = "Coefficient Estimates Across Specifications") {
  
  library(ggplot2)
  library(broom)
  library(dplyr)
  library(purrr)
  
  # Extract coefficient from each model
  coef_df <- imap_dfr(models, function(mod, mod_name) {
    tidy(mod, conf.int = TRUE) %>%
      filter(term == coef_name) %>%
      mutate(model = mod_name)
  })
  
  # Apply labels
  if (!is.null(labels)) {
    coef_df <- coef_df %>%
      mutate(model = recode(model, !!!labels))
  }
  
  # Order models
  coef_df <- coef_df %>%
    mutate(model = factor(model, levels = rev(unique(model))))
  
  # Create plot
  ggplot(coef_df, aes(x = model, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
    geom_errorbar(aes(ymin = conf.low, ymax = conf.high), 
                  width = 0.2, color = "#1f77b4", linewidth = 0.8) +
    geom_point(size = 3, color = "#1f77b4") +
    coord_flip() +
    labs(
      title = title,
      subtitle = paste("Coefficient:", coef_name),
      x = NULL,
      y = "Estimate (95% CI)"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold")
    )
}
