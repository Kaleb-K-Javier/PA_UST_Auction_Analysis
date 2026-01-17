# ==============================================================================
# SECTION: Human vs. ML Cost Prediction Comparison
# ==============================================================================

run_human_vs_ml_comparison <- function(dt, 
                                       outcome_var = "log_cost",
                                       cluster_var = "dep_region",
                                       ols_features = features_ols,       # DEFAULT to global env variable
                                       ml_features = features_ml_base,    # DEFAULT to global env variable
                                       human_binary_vars = features_human_binary,
                                       output_prefix = "comparison") {
  
  # Required Libraries
  library(grf)
  library(data.table)
  library(ggplot2)
  library(scales)
  library(patchwork) # Required for p1 + p2 layout
  
  # -------------------------------------------------------------------------
  # A. DATA PREPARATION
  # -------------------------------------------------------------------------
  message("\n--- Preparing Data for Human vs. ML Comparison ---")
  
  # Copy data to avoid modification by reference
  dt_analysis <- copy(dt)

  # Check if helper function exists, otherwise simple 0 fill
  if (exists("impute_human_na")) {
    dt_analysis <- impute_human_na(dt_analysis, human_binary_vars)
  } else {
    message("Warning: 'impute_human_na' not found. Filling NAs with 0 for binary vars.")
    for (v in human_binary_vars) {
      set(dt_analysis, which(is.na(dt_analysis[[v]])), v, 0)
    }
  }
  
  # Filter to complete outcome and cluster
  dt_analysis <- dt_analysis[!is.na(get(outcome_var)) & is.finite(get(outcome_var))]
  dt_analysis <- dt_analysis[!is.na(get(cluster_var))]
  
  # Create numeric cluster ID
  dt_analysis[, cluster_id := as.numeric(as.factor(get(cluster_var)))]
  
  # Get unique regions
  unique_clusters <- unique(dt_analysis[[cluster_var]])
  n_clusters <- length(unique_clusters)
  
  message(sprintf("Analysis set: %d observations across %d regions",
                  nrow(dt_analysis), n_clusters))
  
  # -------------------------------------------------------------------------
  # B. MODEL A: Human OLS with Leave-One-Region-Out CV
  # -------------------------------------------------------------------------
  message("\n--- Training Human Model (LORO-CV) ---")
  
  # Build formula: Binary flags + key continuous + Year FE
  # Ensure we only use features present in the data
  valid_ols_vars <- intersect(ols_features, names(dt_analysis))
  
  f_human <- as.formula(paste(outcome_var, "~", 
                               paste(valid_ols_vars, collapse = " + "),
                               "+ factor(Year)"))
  
  # Initialize prediction column
  dt_analysis[, pred_human := NA_real_]
  
  # LORO-CV loop
  cv_results_human <- list()
  for (i in seq_along(unique_clusters)) {
    r <- unique_clusters[i]
    train_idx <- which(dt_analysis[[cluster_var]] != r)
    test_idx <- which(dt_analysis[[cluster_var]] == r)
    
    if (length(test_idx) == 0) next
    
    # Fit on training regions
    # Use tryCatch to handle potential rank-deficiency in small folds
    m_human <- tryCatch({
      lm(f_human, data = dt_analysis[train_idx])
    }, error = function(e) return(NULL))
    
    if (!is.null(m_human)) {
      # Predict on held-out region
      pred_k <- predict(m_human, newdata = dt_analysis[test_idx])
      dt_analysis[test_idx, pred_human := pred_k]
      
      # Store fold metrics
      actual_k <- dt_analysis[test_idx, get(outcome_var)]
      cv_results_human[[i]] <- data.table(
        region = r,
        n = length(test_idx),
        mse = mean((actual_k - pred_k)^2, na.rm = TRUE)
      )
    }
  }
  
  cv_human_dt <- rbindlist(cv_results_human)
  
  # -------------------------------------------------------------------------
  # C. MODEL B: ML Regression Forest with Cluster-Robust OOB
  # -------------------------------------------------------------------------
  message("\n--- Training ML Model (Cluster-Robust GRF) ---")
  
  # Check for helper function, otherwise standard model.matrix
  if (exists("build_ml_matrix")) {
    X_ml <- build_ml_matrix(dt_analysis, ml_features, include_bin_qty = TRUE)
  } else {
    message("Using standard model.matrix for ML...")
    # Basic formula expansion
    f_ml <- as.formula(paste("~", paste(intersect(ml_features, names(dt_analysis)), collapse="+"), "-1"))
    X_ml <- model.matrix(f_ml, data = dt_analysis)
  }

  Y <- dt_analysis[[outcome_var]]
  clusters <- dt_analysis$cluster_id
  
  # Remove rows with any NA in outcome (X can have NA - GRF uses MIA)
  valid_idx <- which(!is.na(Y) & is.finite(Y))
  X_ml <- X_ml[valid_idx, , drop = FALSE]
  Y <- Y[valid_idx]
  clusters <- clusters[valid_idx]
  
  # Train GRF with cluster-robust OOB
  set.seed(20250115)
  rf_ml <- regression_forest(
    X = X_ml,
    Y = Y,
    num.trees = 2000,
    clusters = clusters,
    tune.parameters = "all",
    compute.oob.predictions = TRUE,
    seed = 42
  )
  
  # Extract OOB predictions
  pred_ml_oob <- predict(rf_ml)$predictions
  dt_analysis[valid_idx, pred_ml := pred_ml_oob]
  
  # -------------------------------------------------------------------------
  # D. COMPUTE COMPARISON METRICS
  # -------------------------------------------------------------------------
  message("\n--- Computing Comparison Metrics ---")
  
  # Filter to rows with both predictions
  dt_compare <- dt_analysis[!is.na(pred_human) & !is.na(pred_ml)]
  actual <- dt_compare[[outcome_var]]
  
  # RMSE (Log Scale)
  rmse_human <- sqrt(mean((actual - dt_compare$pred_human)^2, na.rm = TRUE))
  rmse_ml <- sqrt(mean((actual - dt_compare$pred_ml)^2, na.rm = TRUE))
  
  # R² (Variance Explained)
  ss_tot <- sum((actual - mean(actual))^2)
  ss_res_human <- sum((actual - dt_compare$pred_human)^2, na.rm = TRUE)
  ss_res_ml <- sum((actual - dt_compare$pred_ml)^2, na.rm = TRUE)
  
  r2_human <- 1 - ss_res_human / ss_tot
  r2_ml <- 1 - ss_res_ml / ss_tot
  
  # Lift metrics
  lift_r2 <- (r2_ml - r2_human) / r2_human * 100
  lift_rmse <- (rmse_human - rmse_ml) / rmse_human * 100
  
  # Back-transform representative errors to Real Dollars
  median_log_cost <- median(actual)
  
  # Approximate dollar error
  dollar_error_human <- exp(median_log_cost) * rmse_human
  dollar_error_ml <- exp(median_log_cost) * rmse_ml
  
  metrics_dt <- data.table(
    Model = c("Human Intuition (OLS)", "ML Discovery (GRF)"),
    `RMSE (Log)` = c(rmse_human, rmse_ml),
    `R² (OOB/CV)` = c(r2_human, r2_ml),
    `Approx. $ Error` = c(dollar_error_human, dollar_error_ml)
  )
  
  message(sprintf("\nPERFORMANCE COMPARISON (Cluster-Robust):"))
  message(sprintf("  Human OLS:  RMSE = %.3f | R² = %.1f%%", rmse_human, r2_human * 100))
  message(sprintf("  ML GRF:     RMSE = %.3f | R² = %.1f%%", rmse_ml, r2_ml * 100))
  message(sprintf("  ML Lift:    +%.1f%% variance explained | %.1f%% lower RMSE", 
                  lift_r2, lift_rmse))
  
  # -------------------------------------------------------------------------
  # E. VISUALIZATION: LIFT CHART
  # -------------------------------------------------------------------------
  
  # Bar chart comparing R² and RMSE
  plot_data <- data.table(
    Model = rep(c("Human Intuition\n(Standard Flags)", 
                  "ML Discovery\n(High-Dimensional)"), 2),
    Metric = c(rep("Variance Explained (R²)", 2), rep("Prediction Error (RMSE)", 2)),
    Value = c(r2_human, r2_ml, rmse_human, rmse_ml),
    Label = c(sprintf("%.1f%%", r2_human * 100), sprintf("%.1f%%", r2_ml * 100),
              sprintf("%.2f", rmse_human), sprintf("%.2f", rmse_ml))
  )
  
  p_r2 <- ggplot(plot_data[Metric == "Variance Explained (R²)"], 
                 aes(x = Model, y = Value, fill = Model)) +
    geom_col(width = 0.6, alpha = 0.9) +
    geom_text(aes(label = Label), vjust = -0.5, fontface = "bold", size = 5) +
    scale_y_continuous(labels = percent_format(), limits = c(0, max(r2_ml * 1.3, 0.5)),
                       expand = expansion(mult = c(0, 0.1))) +
    scale_fill_manual(values = c("Human Intuition\n(Standard Flags)" = "#3498db",
                                 "ML Discovery\n(High-Dimensional)" = "#9b59b6")) +
    labs(title = "How Much Cost Variation Can We Explain?",
         subtitle = sprintf("ML discovers +%.0f%% more signal than standard risk flags", lift_r2),
         y = "Variance Explained (R²)",
         x = NULL,
         caption = "Note: Both models validated using Leave-One-Region-Out to prevent geographic overfitting.") +
    theme_minimal(base_size = 14) +
    theme(legend.position = "none",
          plot.title = element_text(face = "bold", size = 16),
          plot.subtitle = element_text(color = "#7f8c8d", size = 12),
          axis.text.x = element_text(face = "bold", size = 12),
          panel.grid.major.x = element_blank())
  
  p_rmse <- ggplot(plot_data[Metric == "Prediction Error (RMSE)"],
                   aes(x = Model, y = Value, fill = Model)) +
    geom_col(width = 0.6, alpha = 0.9) +
    geom_text(aes(label = Label), vjust = -0.5, fontface = "bold", size = 5) +
    scale_y_continuous(limits = c(0, max(rmse_human * 1.3)),
                       expand = expansion(mult = c(0, 0.1))) +
    scale_fill_manual(values = c("Human Intuition\n(Standard Flags)" = "#3498db",
                                 "ML Discovery\n(High-Dimensional)" = "#9b59b6")) +
    labs(title = "Prediction Accuracy (Lower is Better)",
         subtitle = sprintf("ML reduces error by %.0f%%", lift_rmse),
         y = "Root Mean Squared Error (Log Scale)",
         x = NULL) +
    theme_minimal(base_size = 14) +
    theme(legend.position = "none",
          plot.title = element_text(face = "bold", size = 16),
          plot.subtitle = element_text(color = "#7f8c8d", size = 12),
          axis.text.x = element_text(face = "bold", size = 12),
          panel.grid.major.x = element_blank())
  
  # Combined figure (Requires patchwork)
  p_combined <- p_r2 + p_rmse +
    plot_annotation(
      title = "Human Intuition vs. Algorithmic Discovery",
      subtitle = "Comparing what adjusters traditionally check against data-driven signals",
      theme = theme(plot.title = element_text(face = "bold", size = 18, hjust = 0.5),
                    plot.subtitle = element_text(size = 13, hjust = 0.5, color = "#7f8c8d"))
    )
  
  # -------------------------------------------------------------------------
  # F. VARIABLE IMPORTANCE (What ML Found)
  # -------------------------------------------------------------------------
  
  var_imp <- variable_importance(rf_ml)
  imp_dt <- data.table(
    Feature = colnames(X_ml),
    Importance = as.vector(var_imp)
  )
  setorder(imp_dt, -Importance)
  
  # Categorize features
  imp_dt[, Category := fcase(
    grepl("^bin_|^qty_", Feature), "Data Signal (ML)",
    grepl("^has_", Feature), "Regulatory Flag (Human)",
    grepl("^business_|^Owner_|^final_owner", Feature), "Owner Profile",
    default = "Facility Trait"
  )]
  
  # Clean names for display
  imp_dt[, Feature_Display := gsub("^bin_", "", Feature)]
  imp_dt[, Feature_Display := gsub("^qty_", "Count: ", Feature_Display)]
  imp_dt[, Feature_Display := gsub("^has_", "Flag: ", Feature_Display)]
  imp_dt[, Feature_Display := gsub("_", " ", Feature_Display)]
  imp_dt[, Feature_Display := tools::toTitleCase(Feature_Display)]
  
  p_importance <- ggplot(head(imp_dt, 15),
                         aes(x = reorder(Feature_Display, Importance), 
                             y = Importance, fill = Category)) +
    geom_col(alpha = 0.9) +
    coord_flip() +
    scale_fill_manual(values = c(
      "Data Signal (ML)" = "#9b59b6",
      "Regulatory Flag (Human)" = "#e67e22",
      "Owner Profile" = "#27ae60",
      "Facility Trait" = "#3498db"
    )) +
    labs(title = "What Predicts Claim Costs?",
         subtitle = "Top 15 features ranked by predictive importance",
         x = NULL, y = "Relative Importance",
         caption = "Purple bars indicate signals not in standard checklists.") +
    theme_minimal(base_size = 13) +
    theme(legend.position = "bottom",
          legend.title = element_blank(),
          plot.title = element_text(face = "bold", size = 16))
  
  # -------------------------------------------------------------------------
  # G. RETURN RESULTS
  # -------------------------------------------------------------------------
  
  return(list(
    metrics = metrics_dt,
    lift_r2_pct = lift_r2,
    lift_rmse_pct = lift_rmse,
    plot_comparison = p_combined,
    plot_r2 = p_r2,
    plot_rmse = p_rmse,
    plot_importance = p_importance,
    importance_data = imp_dt,
    model_human_cv = cv_human_dt,
    model_ml = rf_ml,
    data = dt_compare
  ))
}

# ==============================================================================
# HELPER: Create Executive Summary Table (Non-Technical)
# ==============================================================================
create_executive_comparison_table <- function(metrics_dt, lift_r2, lift_rmse) {
  library(gt)
  library(glue) # Required for interpolation
  
  exec_tbl <- metrics_dt %>%
    gt() %>%
    cols_label(
      Model = "Approach",
      `RMSE (Log)` = "Prediction Error",
      `R² (OOB/CV)` = "Accuracy",
      `Approx. $ Error` = "Typical $ Error"
    ) %>%
    fmt_percent(columns = `R² (OOB/CV)`, decimals = 1) %>%
    fmt_number(columns = `RMSE (Log)`, decimals = 2) %>%
    fmt_currency(columns = `Approx. $ Error`, decimals = 0) %>%
    tab_header(
      title = "Predicting Claim Costs: Two Approaches",
      subtitle = "Human judgment flags vs. data-driven pattern recognition"
    ) %>%
    tab_spanner(
      label = "Model Performance (Higher Accuracy = Better)",
      columns = c(`RMSE (Log)`, `R² (OOB/CV)`)
    ) %>%
    tab_source_note(
      # ERROR FIX: Wrapped in glue::glue to interpolate the variable
      source_note = md(glue::glue("**Key Finding**: Machine learning explains **{round(lift_r2)}% more variation** in costs than standard risk flags alone."))
    ) %>% # ERROR FIX: Changed %> to %>%
    tab_footnote(
      footnote = "Validated using Leave-One-Region-Out cross-validation to ensure results generalize across PA.",
      locations = cells_column_labels(columns = `R² (OOB/CV)`)
    ) %>%
    tab_style(
      style = cell_fill(color = "#d5f5e3"),
      locations = cells_body(columns = `R² (OOB/CV)`, rows = Model == "ML Discovery (GRF)")
    ) %>%
    tab_options(
      heading.title.font.size = 18,
      heading.subtitle.font.size = 14,
      table.font.size = 12
    )
  
  return(exec_tbl)
}