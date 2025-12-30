# R/functions/summary_stats_table.R
# ============================================================================
# Helper function to generate summary statistics tables
# ============================================================================

#' Create a summary statistics table for numeric variables
#' 
#' @param data Data frame
#' @param vars Character vector of variable names
#' @param by Optional grouping variable name (character)
#' @param digits Number of decimal places (default 2)
#' @return gt table object
summary_stats_table <- function(data, vars, by = NULL, digits = 2) {
  
  library(gt)
  library(tidyverse)
  
  # Helper to calculate stats for one variable
  calc_stats <- function(x, var_name) {
    tibble(
      Variable = var_name,
      N = sum(!is.na(x)),
      Mean = mean(x, na.rm = TRUE),
      SD = sd(x, na.rm = TRUE),
      Min = min(x, na.rm = TRUE),
      P25 = quantile(x, 0.25, na.rm = TRUE),
      Median = median(x, na.rm = TRUE),
      P75 = quantile(x, 0.75, na.rm = TRUE),
      Max = max(x, na.rm = TRUE)
    )
  }
  
  if (is.null(by)) {
    # Overall summary
    stats_df <- map_dfr(vars, ~calc_stats(data[[.x]], .x))
  } else {
    # Grouped summary
    stats_df <- data %>%
      group_by(!!sym(by)) %>%
      group_modify(~map_dfr(vars, function(v) calc_stats(.x[[v]], v))) %>%
      ungroup()
  }
  
  # Format as gt table
  tbl <- stats_df %>%
    gt() %>%
    fmt_number(columns = c(Mean, SD, Min, P25, Median, P75, Max), decimals = digits) %>%
    fmt_integer(columns = N) %>%
    tab_options(
      table.font.size = 10,
      column_labels.font.weight = "bold"
    )
  
  return(tbl)
}

#' Create a balance table comparing groups
#' 
#' @param data Data frame
#' @param group_var Name of grouping variable
#' @param vars Character vector of variables to compare
#' @return gt table with group means and differences
balance_table <- function(data, group_var, vars) {
  
  library(gt)
  library(tidyverse)
  
  # Get group levels
  groups <- unique(data[[group_var]])
  if (length(groups) != 2) {
    stop("Balance table requires exactly 2 groups")
  }
  
  # Calculate means by group
  means <- data %>%
    group_by(!!sym(group_var)) %>%
    summarise(
      across(all_of(vars), list(mean = ~mean(.x, na.rm = TRUE), n = ~sum(!is.na(.x)))),
      .groups = "drop"
    ) %>%
    pivot_longer(-!!sym(group_var), names_to = c("variable", "stat"), names_sep = "_") %>%
    pivot_wider(names_from = c(!!sym(group_var), stat), values_from = value)
  
  # Create table
  means %>%
    gt() %>%
    tab_options(
      table.font.size = 10,
      column_labels.font.weight = "bold"
    )
}
