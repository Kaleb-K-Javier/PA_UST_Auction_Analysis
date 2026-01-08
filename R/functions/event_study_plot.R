# R/functions/event_study_plot.R
# ============================================================================
# Helper function to create event study-style coefficient plots
# ============================================================================

#' Create an event study style plot from regression coefficients
#' 
#' @param model A fixest model object with time-relative coefficients
#' @param ref_period The reference period (omitted category)
#' @param title Plot title
#' @param subtitle Plot subtitle
#' @param xlab X-axis label
#' @param ylab Y-axis label
#' @return A ggplot object
event_study_plot <- function(model, 
                             ref_period = -1,
                             title = "Event Study Plot",
                             subtitle = NULL,
                             xlab = "Time Relative to Event",
                             ylab = "Estimate") {
  
  library(ggplot2)
  library(broom)
  library(dplyr)
  library(stringr)
  
  # Extract coefficients
  coef_df <- tidy(model, conf.int = TRUE) %>%
    filter(str_detect(term, "time|period|year")) %>%
    mutate(
      # Try to extract numeric time indicator
      time = as.numeric(str_extract(term, "-?\\d+")),
      # Mark significance
      significant = p.value < 0.05
    ) %>%
    filter(!is.na(time))
  
  # Add reference period
  ref_row <- tibble(
    time = ref_period,
    estimate = 0,
    conf.low = 0,
    conf.high = 0,
    significant = NA
  )
  
  coef_df <- bind_rows(coef_df, ref_row) %>%
    arrange(time)
  
  # Create plot
  p <- ggplot(coef_df, aes(x = time, y = estimate)) +
    # Zero line
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
    # Vertical line at treatment
    geom_vline(xintercept = 0, linetype = "dashed", color = "red", alpha = 0.5) +
    # Confidence intervals
    geom_errorbar(aes(ymin = conf.low, ymax = conf.high), 
                  width = 0.2, color = "#1f77b4", alpha = 0.8) +
    # Point estimates
    geom_point(aes(shape = significant), size = 3, color = "#1f77b4") +
    scale_shape_manual(values = c(`FALSE` = 1, `TRUE` = 16), 
                       labels = c("Not Significant", "Significant (p<0.05)"),
                       na.value = 4) +
    labs(
      title = title,
      subtitle = subtitle,
      x = xlab,
      y = ylab,
      shape = NULL
    ) +
    theme_minimal() +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold")
    )
  
  return(p)
}

#' Create a coefficient comparison plot
#' 
#' @param models Named list of fixest models
#' @param coef_names Character vector of coefficient names to plot
#' @param labels Optional named vector for coefficient labels
#' @return A ggplot object
coefficient_comparison_plot <- function(models, coef_names, labels = NULL) {
  
  library(ggplot2)
  library(broom)
  library(dplyr)
  library(purrr)
  
  # Extract coefficients from all models
  coef_df <- imap_dfr(models, function(mod, mod_name) {
    tidy(mod, conf.int = TRUE) %>%
      filter(term %in% coef_names) %>%
      mutate(model = mod_name)
  })
  
  # Apply labels if provided
  if (!is.null(labels)) {
    coef_df <- coef_df %>%
      mutate(term = recode(term, !!!labels))
  }
  
  # Create plot
  ggplot(coef_df, aes(x = term, y = estimate, color = model)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
    geom_point(position = position_dodge(width = 0.5), size = 3) +
    geom_errorbar(aes(ymin = conf.low, ymax = conf.high),
                  position = position_dodge(width = 0.5), width = 0.2) +
    coord_flip() +
    labs(
      x = NULL,
      y = "Coefficient Estimate (95% CI)",
      color = "Model"
    ) +
    theme_minimal() +
    theme(
      legend.position = "bottom",
      plot.title = element_text(face = "bold")
    )
}
