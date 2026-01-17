# R/analysis/01_descriptive_stats.R
# ==============================================================================
# Pennsylvania UST Analysis - Descriptive Statistics
# ==============================================================================
# PURPOSE: Characterize the claim universe and market structure
#
# SECTIONS:
#   1.1) Correlates of Eligibility (Denied vs Withdrawn vs Eligible)
#   1.2) Denied Claims and ALAE ("Phantom Spend") [OPTIONAL - currently disabled]
#   1.3) Closed Claims: Cost Drivers, Duration Drivers
#   1.4) Remediation Trends (Costs & Durations Over Time)
#   1.5) Spatial/Temporal Visualization (Maps, Time Series)
#   3.1) Consultant Concentration (HHI by Region)
#   3.2) Adjuster & Auction Concentration
#
# OUTPUTS:
#   - Tables: output/tables/1XX_*.{html,tex}
#   - Figures: output/figures/1XX_*.{png,pdf}
# ==============================================================================

# ==============================================================================
# 0. SETUP & LIBRARIES
# ==============================================================================
suppressPackageStartupMessages({
  # Core data manipulation
  library(data.table)
  library(here)
  library(janitor)
  library(stringr)
  library(glue)
  
  # Modeling
  library(fixest)
  library(modelsummary)
  library(grf)
  
  # Trees & Visualization
  library(rpart)
  library(rpart.plot)
  library(ggparty)
  library(partykit)
  
  # Tables
  library(gt)
  library(kableExtra)
  
  # Plots
  library(ggplot2)
  library(scales)
  library(patchwork)
  

  # Spatial
  library(sf)
  library(maps)
})

# Source helper functions
source(here("R/functions/model_comparison.R"))
source(here("R/functions/style_guide.R"))
source(here("R/functions/save_utils.R"))
source(here("R/functions/variable_def.R"))

# Paths configuration
paths <- list(
  master    = here("data/processed/master_analysis_dataset.rds"),
  contracts = here("data/processed/contracts_with_real_values.rds"),
  feat_dict = here("data/processed/ml_feature_dictionary.rds"),
  tables    = here("output/tables"),
  figures   = here("output/figures")
)

dir.create(paths$tables, recursive = TRUE, showWarnings = FALSE)
dir.create(paths$figures, recursive = TRUE, showWarnings = FALSE)

# ==============================================================================
# 0.1 HELPER FUNCTIONS
# ==============================================================================

#' Manual hyperparameter tuning for GRF forests
#' 
#' @param X Feature matrix
#' @param Y Response vector (numeric for regression, factor for probability)
#' @param forest_type "regression" or "probability"
#' @param num_trials Number of tuning trials
#' @param num_trees_tune Trees for tuning (lower for speed)
#' @param num_trees_final Trees for final model
#' @param seed Random seed
#' @return Trained forest with optimal parameters
tune_grf_forest <- function(X, Y, 
                            forest_type = c("regression", "probability"),
                            num_trials = 15,
                            num_trees_tune = 500,
                            num_trees_final = 2000,
                            seed = 20250115) {
  
  forest_type <- match.arg(forest_type)
  set.seed(seed)
  
  message(sprintf("  Tuning %s forest (%d trials)...", forest_type, num_trials))
  
  # Build tuning grid
  tuning_grid <- data.frame(
    min.node.size     = sample(1:20, num_trials, replace = TRUE),
    sample.fraction   = runif(num_trials, 0.1, 0.45),
    mtry              = sample(ceiling(sqrt(ncol(X))):min(ncol(X), 50), num_trials, replace = TRUE),
    alpha             = runif(num_trials, 0.0, 0.2),
    honesty.fraction  = runif(num_trials, 0.5, 0.8),
    honesty.prune.leaves = sample(c(TRUE, FALSE), num_trials, replace = TRUE),
    error             = NA_real_
  )
  
  # Evaluate each configuration
  for (i in seq_len(nrow(tuning_grid))) {
    
    if (forest_type == "probability") {
      forest_tune <- grf::probability_forest(
        X = X, Y = Y, 
        num.trees = num_trees_tune,
        min.node.size     = tuning_grid$min.node.size[i],
        sample.fraction   = tuning_grid$sample.fraction[i],
        mtry              = tuning_grid$mtry[i],
        alpha             = tuning_grid$alpha[i],
        honesty           = TRUE,
        honesty.fraction  = tuning_grid$honesty.fraction[i],
        honesty.prune.leaves = tuning_grid$honesty.prune.leaves[i],
        compute.oob.predictions = TRUE,
        seed = seed
      )
      # MSE for probability predictions
      obs <- as.numeric(Y) - 1
      preds <- predict(forest_tune)$predictions[, 2]
      tuning_grid$error[i] <- mean((obs - preds)^2)
      
    } else {
      forest_tune <- grf::regression_forest(
        X = X, Y = Y,
        num.trees = num_trees_tune,
        min.node.size     = tuning_grid$min.node.size[i],
        sample.fraction   = tuning_grid$sample.fraction[i],
        mtry              = tuning_grid$mtry[i],
        alpha             = tuning_grid$alpha[i],
        honesty           = TRUE,
        honesty.fraction  = tuning_grid$honesty.fraction[i],
        honesty.prune.leaves = tuning_grid$honesty.prune.leaves[i],
        compute.oob.predictions = TRUE,
        seed = seed
      )
      # MSE for regression
      preds <- predict(forest_tune)$predictions
      tuning_grid$error[i] <- mean((Y - preds)^2)
    }
    
    if (i %% 5 == 0) message(sprintf("    Trial %d/%d completed...", i, num_trials))
  }
  
  # Select best parameters
  best_idx <- which.min(tuning_grid$error)
  best_params <- tuning_grid[best_idx, ]
  message("\n  Best parameters found:")
  print(best_params[, -which(names(best_params) == "error")])
  
  # Train final model
  message(sprintf("  Training final %s forest (%d trees)...", forest_type, num_trees_final))
  
  if (forest_type == "probability") {
    forest_final <- grf::probability_forest(
      X = X, Y = Y,
      num.trees = num_trees_final,
      min.node.size        = best_params$min.node.size,
      sample.fraction      = best_params$sample.fraction,
      mtry                 = best_params$mtry,
      alpha                = best_params$alpha,
      honesty              = TRUE,
      honesty.fraction     = best_params$honesty.fraction,
      honesty.prune.leaves = best_params$honesty.prune.leaves,
      seed = seed
    )
  } else {
    forest_final <- grf::regression_forest(
      X = X, Y = Y,
      num.trees = num_trees_final,
      min.node.size        = best_params$min.node.size,
      sample.fraction      = best_params$sample.fraction,
      mtry                 = best_params$mtry,
      alpha                = best_params$alpha,
      honesty              = TRUE,
      honesty.fraction     = best_params$honesty.fraction,
      honesty.prune.leaves = best_params$honesty.prune.leaves,
      seed = seed
    )
  }
  
  attr(forest_final, "best_params") <- best_params
  attr(forest_final, "tuning_grid") <- tuning_grid
  
  return(forest_final)
}


#' Create human-readable labels for ML features
#' @param feature_names Character vector of feature names
#' @param feature_dict Data.table with feature dictionary
#' @return Named vector: feature_name -> clean_label
create_display_labels <- function(feature_names, feature_dict = NULL) {
  
  labels <- sapply(feature_names, function(fn) {
    
    # Manual mappings for core variables
    manual_map <- c(
      # Facility continuous
      "avg_tank_age"           = "Avg Tank Age (Years)",
      "n_tanks_total"          = "Total Tanks at Facility",
      "n_tanks_active"         = "Active Tanks at Claim",
      "facility_age"           = "Facility Age (Years)",
      "total_capacity_gal"     = "Total Capacity (Gallons)",
      "avg_tank_capacity_gal"  = "Avg Tank Capacity (Gal)",
      "max_tank_capacity_gal"  = "Max Tank Capacity (Gal)",
      "facility_count"         = "Owner Portfolio Size",
      "n_closed_total"         = "Historical Tank Closures",
      "n_closed_0_30d"         = "Tanks Closed (0-30 Days)",
      "n_closed_31_90d"        = "Tanks Closed (31-90 Days)",
      "n_closed_1yr"           = "Tanks Closed (Past Year)",
      
      # Facility shares
      "share_bare_steel"       = "Share: Bare Steel Tanks",
      "share_single_wall"      = "Share: Single-Walled",
      "share_pressure_piping"  = "Share: Pressure Piping",
      "share_noncompliant"     = "Share: Non-Compliant",
      "share_data_unknown"     = "Share: Unknown Data",
      "share_tank_construction_unknown"      = "Share: Unknown Construction",
      "share_ug_piping_unknown"              = "Share: Unknown Piping",
      "share_tank_release_detection_unknown" = "Share: Unknown Detection",
      
      # Facility binary
      "has_bare_steel"             = "Has Bare Steel Tank",
      "has_single_walled"          = "Has Single-Walled System",
      "has_secondary_containment"  = "Has Secondary Containment",
      "has_pressure_piping"        = "Has Pressure Piping",
      "has_suction_piping"         = "Has Suction Piping",
      "has_galvanized_piping"      = "Has Galvanized Piping",
      "has_electronic_atg"         = "Has Electronic Tank Gauge",
      "has_manual_detection"       = "Has Manual Detection",
      "has_overfill_alarm"         = "Has Overfill Alarm",
      "has_gasoline"               = "Stores Gasoline",
      "has_diesel"                 = "Stores Diesel",
      "has_noncompliant"           = "Has Non-Compliant Tank",
      "has_legacy_grandfathered"   = "Has Grandfathered Tank",
      "has_unknown_data"           = "Has Missing Data",
      "has_recent_closure"         = "Recent Tank Closure (<90d)",
      
      # Claim variables
      "is_repeat_filer"        = "Repeat Filer",
      "reporting_lag_days"     = "Reporting Delay (Days)",
      "prior_claims_n"         = "Prior Claim Count",
      "days_since_prior_claim" = "Days Since Last Claim",
      "avg_prior_claim_cost"   = "Avg Prior Claim Cost ($)"
    )
    
    if (fn %in% names(manual_map)) return(manual_map[[fn]])
    
    # Dictionary lookup for bin_* and qty_*
    if (!is.null(feature_dict) && (grepl("^bin_", fn) || grepl("^qty_", fn))) {
      dict_match <- feature_dict[feature_name == fn]
      if (nrow(dict_match) == 1) {
        detail <- tools::toTitleCase(tolower(dict_match$detail_label))
        category <- tools::toTitleCase(tolower(dict_match$category_label))
        category_short <- gsub(" Construction & Corrosion Protection", "", category)
        category_short <- gsub(" Release Detection", " Detection", category_short)
        category_short <- gsub("Ust ", "Tank ", category_short)
        category_short <- gsub("Ug ", "Underground ", category_short)
        
        if (grepl("^bin_", fn)) return(paste0("Has ", detail, " (", category_short, ")"))
        return(paste0("Count: ", detail, " (", category_short, ")"))
      }
    }
    
    # Fallback: clean up variable name
    clean <- gsub("^(bin_ind_|qty_ind_|bin_|qty_)", "", fn)
    clean <- gsub("_", " ", clean)
    clean <- tools::toTitleCase(clean)
    if (grepl("^bin_", fn)) return(paste0("Has ", clean))
    if (grepl("^qty_", fn)) return(paste0("Count: ", clean))
    return(clean)
  }, USE.NAMES = TRUE)
  
  return(labels)
}


#' Filter regression results to significant coefficients
#' @param model fixest model object
#' @param threshold p-value threshold
#' @param label_map Named vector for labels
#' @return data.table of significant results
filter_to_significant <- function(model, threshold = 0.10, label_map = NULL) {
  
  coef_tbl <- as.data.table(coeftable(model))
  coef_tbl[, term := rownames(coeftable(model))]
  setnames(coef_tbl, c("Estimate", "Std_Error", "t_value", "p_value", "term"))
  
  sig_tbl <- coef_tbl[p_value < threshold & !grepl("^(Intercept)", term)]
  
  if (nrow(sig_tbl) == 0) {
    message("  No coefficients significant at p < ", threshold)
    return(NULL)
  }
  
  # Apply labels
  if (!is.null(label_map)) {
    sig_tbl[, Label := sapply(term, function(t) {
      if (grepl("Owner_Size_Class", t)) {
        clean_level <- gsub("Owner_Size_Class", "", t)
        return(paste0("Multi-Facility Owner: ", clean_level, " (vs. Single Tank)"))
      }
      base_term <- gsub("(business_category)(.+)", "Business Type: \\2", t)
      if (t %in% names(label_map)) return(label_map[[t]])
      return(base_term)
    })]
  } else {
    sig_tbl[, Label := term]
  }
  
  sig_tbl <- sig_tbl[order(-abs(Estimate))]
  
  sig_tbl[, Stars := fcase(
    p_value < 0.01, "***",
    p_value < 0.05, "**",
    p_value < 0.10, "*",
    default = ""
  )]
  sig_tbl[, Direction := ifelse(Estimate > 0, "Increase", "Decrease")]
  sig_tbl[, Coef_Display := sprintf("%.2f%% (%s)%s", Estimate * 100, Direction, Stars)]
  sig_tbl[, SE_Display := sprintf("(%.2f%%)", Std_Error * 100)]
  
  return(sig_tbl)
}


#' Build ML feature set from OLS variables + all bin_/qty_ indicators
#' @param dt Data.table
#' @param ols_vars Character vector of OLS variables
#' @return Character vector of ML features
build_ml_features <- function(dt, ols_vars) {
  ml_features <- c(
    ols_vars,
    "n_tanks_active", "facility_age",
    grep("^bin_", names(dt), value = TRUE),
    grep("^qty_", names(dt), value = TRUE)
  )
  intersect(unique(ml_features), names(dt))
}


#' Clean importance labels for plotting
#' @param dt_imp Importance data.table with Feature column
#' @param feature_dict Feature dictionary
#' @return data.table with Smart_Label and Plot_Label columns
clean_importance_labels <- function(dt_imp, feature_dict) {
  
  dt_imp[, Lookup_Name := gsub("^(bin_|qty_)", "", Feature)]
  dt_imp <- merge(dt_imp, feature_dict[, .(feature_name, clean_label)], 
                  by.x = "Lookup_Name", by.y = "feature_name", all.x = TRUE)
  
  to_title <- function(x) {
    s <- strsplit(tolower(x), " ")[[1]]
    paste(toupper(substring(s, 1, 1)), substring(s, 2), sep = "", collapse = " ")
  }
  
  dt_imp[, Smart_Label := {
    apply(.SD, 1, function(row) {
      feat <- row["Feature"]
      clean <- row["clean_label"]
      is_qty <- grepl("^qty_", feat)
      
      if (!is.na(clean)) {
        lbl <- gsub("\\s*\\[.*?\\]", "", clean)
        lbl <- gsub("\\s*\\([^)]*?[0-9].*?\\)$", "", lbl)
      } else {
        txt <- gsub("^(bin_|qty_|ind_)", "", feat)
        txt <- gsub("\\s*x[0-9]+[a-z]*", "", txt, ignore.case = TRUE)
        txt <- gsub("TRUE$", "", txt)
        txt <- gsub("_", " ", txt)
        lbl <- to_title(txt)
      }
      lbl <- trimws(lbl)
      if (is_qty && !grepl("^Number", lbl)) lbl <- paste("Number of", lbl)
      return(lbl)
    })
  }]
  
  # Overrides for common variables
  dt_imp[Feature == "avg_tank_age", Smart_Label := "Avg Tank Age (Years)"]
  dt_imp[Feature == "reporting_lag_days", Smart_Label := "Reporting Delay (Days)"]
  dt_imp[Feature == "n_tanks_total", Smart_Label := "Total Tanks"]
  dt_imp[Feature == "prior_claims_n", Smart_Label := "Prior Claims Count"]
  dt_imp[Feature == "is_repeat_filerTRUE", Smart_Label := "Repeat Filer"]
  dt_imp[Feature == "is_repeat_filerFALSE", Smart_Label := "First-Time Filer"]
  
  dt_imp[, Plot_Label := stringr::str_wrap(Smart_Label, width = 35)]
  
  return(dt_imp)
}


#' Run Human (OLS) vs ML (Probability Forest) Comparison for Binary Outcomes
#' 
#' Uses Leave-One-Region-Out CV for OLS and cluster-robust OOB for GRF
#' 
#' @param dt Data.table with outcome and features
#' @param outcome_var Name of binary outcome column (0/1)
#' @param outcome_label Human-readable label for plots
#' @param cluster_var Clustering variable
#' @param ols_vars Character vector of OLS predictor names
#' @param ml_features Character vector of ML feature names
#' @param feature_dict Feature dictionary for labeling
#' @param num_tuning_trials Number of manual tuning trials
#' @param seed Random seed
#' @return List with metrics, plots, and models
run_classification_comparison <- function(dt,
                                          outcome_var,
                                          outcome_label = "Outcome",
                                          cluster_var = "dep_region",
                                          ols_vars,
                                          ml_features,
                                          feature_dict = NULL,
                                          num_tuning_trials = 15,
                                          seed = 20250115) {
  
  message(sprintf("\n--- Classification Comparison: %s ---", outcome_label))
  
  # -------------------------------------------------------------------------
  # A. DATA PREPARATION
  # -------------------------------------------------------------------------
  dt_analysis <- copy(dt)
  dt_analysis <- dt_analysis[!is.na(get(outcome_var))]
  dt_analysis <- dt_analysis[!is.na(get(cluster_var))]
  dt_analysis[, cluster_id := as.numeric(as.factor(get(cluster_var)))]
  
  unique_clusters <- unique(dt_analysis[[cluster_var]])
  n_clusters <- length(unique_clusters)
  
  message(sprintf("  Data: %s observations across %d clusters",
                  format(nrow(dt_analysis), big.mark = ","), n_clusters))
  
  # -------------------------------------------------------------------------
  # B. MODEL A: Human OLS (Linear Probability Model) with LORO-CV
  # -------------------------------------------------------------------------
  message("  Training Human Model (LPM with LORO-CV)...")
  
  valid_ols_vars <- intersect(ols_vars, names(dt_analysis))
  f_human <- as.formula(paste(outcome_var, "~",
                              paste(valid_ols_vars, collapse = " + "),
                              "+ factor(Year)"))
  
  dt_analysis[, pred_human := NA_real_]
  
  for (i in seq_along(unique_clusters)) {
    r <- unique_clusters[i]
    train_idx <- which(dt_analysis[[cluster_var]] != r)
    test_idx <- which(dt_analysis[[cluster_var]] == r)
    
    if (length(test_idx) == 0) next
    
    m_human <- tryCatch({
      lm(f_human, data = dt_analysis[train_idx])
    }, error = function(e) NULL)
    
    if (!is.null(m_human)) {
      pred_k <- predict(m_human, newdata = dt_analysis[test_idx])
      # Clip to [0,1] for LPM
      pred_k <- pmax(0, pmin(1, pred_k))
      dt_analysis[test_idx, pred_human := pred_k]
    }
  }
  
  # -------------------------------------------------------------------------
  # C. MODEL B: ML Probability Forest with Manual Tuning
  # -------------------------------------------------------------------------
  message("  Training ML Model (Probability Forest with Manual Tuning)...")
  
  valid_ml_features <- intersect(ml_features, names(dt_analysis))
  f_ml <- as.formula(paste("~", paste(valid_ml_features, collapse = "+"), "- 1"))
  X_ml <- model.matrix(f_ml, data = dt_analysis)
  
  # model.matrix drops NA rows - align Y and clusters to matching rows
  used_rows <- as.integer(rownames(X_ml))
  Y <- as.factor(dt_analysis[[outcome_var]][used_rows])
  clusters <- dt_analysis$cluster_id[used_rows]
  
  # Manual tuning
  set.seed(seed)
  tuning_grid <- data.frame(
    min.node.size = sample(1:20, num_tuning_trials, replace = TRUE),
    sample.fraction = runif(num_tuning_trials, 0.1, 0.45),
    mtry = sample(ceiling(sqrt(ncol(X_ml))):min(ncol(X_ml), 50), num_tuning_trials, replace = TRUE),
    alpha = runif(num_tuning_trials, 0.0, 0.2),
    honesty.fraction = runif(num_tuning_trials, 0.5, 0.8),
    honesty.prune.leaves = sample(c(TRUE, FALSE), num_tuning_trials, replace = TRUE),
    error = NA_real_
  )
  
  for (i in seq_len(nrow(tuning_grid))) {
    pf_tune <- grf::probability_forest(
      X = X_ml, Y = Y, clusters = clusters,
      num.trees = 500,
      min.node.size = tuning_grid$min.node.size[i],
      sample.fraction = tuning_grid$sample.fraction[i],
      mtry = tuning_grid$mtry[i],
      alpha = tuning_grid$alpha[i],
      honesty = TRUE,
      honesty.fraction = tuning_grid$honesty.fraction[i],
      honesty.prune.leaves = tuning_grid$honesty.prune.leaves[i],
      compute.oob.predictions = TRUE,
      seed = seed
    )
    obs <- as.numeric(Y) - 1
    preds <- predict(pf_tune)$predictions[, 2]
    tuning_grid$error[i] <- mean((obs - preds)^2)  # Brier score
  }
  
  best_params <- tuning_grid[which.min(tuning_grid$error), ]
  
  # Final model
  pf_ml <- grf::probability_forest(
    X = X_ml, Y = Y, clusters = clusters,
    num.trees = 2000,
    min.node.size = best_params$min.node.size,
    sample.fraction = best_params$sample.fraction,
    mtry = best_params$mtry,
    alpha = best_params$alpha,
    honesty = TRUE,
    honesty.fraction = best_params$honesty.fraction,
    honesty.prune.leaves = best_params$honesty.prune.leaves,
    compute.oob.predictions = TRUE,
    seed = seed
  )
  
  pred_ml_oob <- predict(pf_ml)$predictions[, 2]
  dt_analysis[, pred_ml := NA_real_]
  dt_analysis[used_rows, pred_ml := pred_ml_oob]
  
  # -------------------------------------------------------------------------
  # D. COMPUTE COMPARISON METRICS
  # -------------------------------------------------------------------------
  dt_compare <- dt_analysis[!is.na(pred_human) & !is.na(pred_ml)]
  actual <- dt_compare[[outcome_var]]
  
  # Brier Score (MSE for probabilities - lower is better)
  brier_human <- mean((actual - dt_compare$pred_human)^2, na.rm = TRUE)
  brier_ml <- mean((actual - dt_compare$pred_ml)^2, na.rm = TRUE)
  
  # Pseudo-R² (1 - Brier/Brier_null)
  brier_null <- mean(actual) * (1 - mean(actual))  # Variance of Bernoulli
  pseudo_r2_human <- 1 - brier_human / brier_null
  pseudo_r2_ml <- 1 - brier_ml / brier_null
  
  # AUC (requires pROC or manual calculation)
  calc_auc <- function(pred, actual) {
    n1 <- sum(actual == 1)
    n0 <- sum(actual == 0)
    if (n1 == 0 || n0 == 0) return(NA)
    
    ranks <- rank(pred)
    auc <- (sum(ranks[actual == 1]) - n1 * (n1 + 1) / 2) / (n1 * n0)
    return(auc)
  }
  
  auc_human <- calc_auc(dt_compare$pred_human, actual)
  auc_ml <- calc_auc(dt_compare$pred_ml, actual)
  
  # Lift
  lift_r2 <- (pseudo_r2_ml - pseudo_r2_human) / abs(pseudo_r2_human) * 100
  lift_auc <- (auc_ml - auc_human) / (1 - auc_human) * 100  # % of remaining AUC captured
  
  metrics_dt <- data.table(
    Model = c("Human (LPM)", "ML (Prob Forest)"),
    Brier_Score = c(brier_human, brier_ml),
    Pseudo_R2 = c(pseudo_r2_human, pseudo_r2_ml),
    AUC = c(auc_human, auc_ml)
  )
  
  message(sprintf("  Human LPM:  Brier = %.4f | Pseudo-R² = %.1f%% | AUC = %.3f",
                  brier_human, pseudo_r2_human * 100, auc_human))
  message(sprintf("  ML PF:      Brier = %.4f | Pseudo-R² = %.1f%% | AUC = %.3f",
                  brier_ml, pseudo_r2_ml * 100, auc_ml))
  message(sprintf("  ML Lift:    +%.1f%% Pseudo-R² | +%.1f%% AUC improvement", lift_r2, lift_auc))
  
  # -------------------------------------------------------------------------
  # E. VISUALIZATIONS
  # -------------------------------------------------------------------------
  plot_data <- data.table(
    Model = factor(c("Human Intuition\n(LPM)", "ML Discovery\n(Prob Forest)"),
                   levels = c("Human Intuition\n(LPM)", "ML Discovery\n(Prob Forest)")),
    Pseudo_R2 = c(pseudo_r2_human, pseudo_r2_ml),
    AUC = c(auc_human, auc_ml)
  )
  
  p_r2 <- ggplot(plot_data, aes(x = Model, y = Pseudo_R2, fill = Model)) +
    geom_col(width = 0.6, alpha = 0.9) +
    geom_text(aes(label = sprintf("%.1f%%", Pseudo_R2 * 100)), vjust = -0.5, fontface = "bold", size = 5) +
    scale_y_continuous(labels = percent_format(),
                       limits = c(0, max(pseudo_r2_ml * 1.4, 0.3)),
                       expand = expansion(mult = c(0, 0.1))) +
    scale_fill_manual(values = c("Human Intuition\n(LPM)" = "#3498db",
                                 "ML Discovery\n(Prob Forest)" = "#9b59b6")) +
    labs(title = sprintf("Predicting %s: Explanatory Power", outcome_label),
         subtitle = sprintf("ML discovers +%.0f%% more signal", lift_r2),
         y = "Pseudo-R² (1 - Brier/Null)", x = NULL) +
    theme_minimal(base_size = 14) +
    theme(legend.position = "none",
          plot.title = element_text(face = "bold", size = 16),
          panel.grid.major.x = element_blank())
  
  p_auc <- ggplot(plot_data, aes(x = Model, y = AUC, fill = Model)) +
    geom_col(width = 0.6, alpha = 0.9) +
    geom_text(aes(label = sprintf("%.3f", AUC)), vjust = -0.5, fontface = "bold", size = 5) +
    geom_hline(yintercept = 0.5, linetype = "dashed", color = "red", alpha = 0.5) +
    scale_y_continuous(limits = c(0, 1), expand = expansion(mult = c(0, 0.1))) +
    scale_fill_manual(values = c("Human Intuition\n(LPM)" = "#3498db",
                                 "ML Discovery\n(Prob Forest)" = "#9b59b6")) +
    labs(title = "Discrimination (AUC)",
         subtitle = "Red line = random guessing (0.5)",
         y = "Area Under ROC Curve", x = NULL) +
    theme_minimal(base_size = 14) +
    theme(legend.position = "none",
          plot.title = element_text(face = "bold", size = 16),
          panel.grid.major.x = element_blank())
  
  p_combined <- p_r2 + p_auc +
    plot_annotation(
      title = sprintf("%s: Human vs ML Prediction", outcome_label),
      theme = theme(plot.title = element_text(face = "bold", size = 18, hjust = 0.5))
    )
  
  # Variable Importance
  var_imp <- grf::variable_importance(pf_ml)
  imp_dt <- data.table(Feature = colnames(X_ml), Importance = as.vector(var_imp))
  setorder(imp_dt, -Importance)
  
  imp_dt[, Category := fcase(
    grepl("^bin_|^qty_", Feature), "Data Signal (ML)",
    grepl("^has_", Feature), "Regulatory Flag (Human)",
    default = "Facility Trait"
  )]
  
  if (!is.null(feature_dict)) {
    imp_dt <- clean_importance_labels(imp_dt, feature_dict)
  } else {
    imp_dt[, Plot_Label := tools::toTitleCase(gsub("_", " ", Feature))]
  }
  
  p_importance <- ggplot(head(imp_dt, 15),
                         aes(x = reorder(Plot_Label, Importance), y = Importance, fill = Category)) +
    geom_col(alpha = 0.9) +
    coord_flip() +
    scale_fill_manual(values = c(
      "Data Signal (ML)" = "#9b59b6",
      "Regulatory Flag (Human)" = "#e67e22",
      "Facility Trait" = "#3498db"
    )) +
    labs(title = sprintf("What Predicts %s?", outcome_label),
         subtitle = "Top 15 features by importance",
         x = NULL, y = "Relative Importance") +
    theme_minimal(base_size = 13) +
    theme(legend.position = "bottom", legend.title = element_blank())
  
  return(list(
    metrics = metrics_dt,
    lift_r2 = lift_r2,
    lift_auc = lift_auc,
    pseudo_r2_human = pseudo_r2_human,
    pseudo_r2_ml = pseudo_r2_ml,
    auc_human = auc_human,
    auc_ml = auc_ml,
    brier_human = brier_human,
    brier_ml = brier_ml,
    plot_combined = p_combined,
    plot_r2 = p_r2,
    plot_auc = p_auc,
    plot_importance = p_importance,
    importance = imp_dt,
    model_ml = pf_ml,
    best_params = best_params
  ))
}


#' Create Executive Summary Table for Classification Comparison
create_classification_table <- function(comparison_result, outcome_label = "Outcome") {
  
  metrics_dt <- comparison_result$metrics
  lift_r2 <- comparison_result$lift_r2
  
  tbl <- metrics_dt %>%
    gt() %>%
    cols_label(
      Model = "Approach",
      Brier_Score = "Brier Score",
      Pseudo_R2 = "Pseudo-R²",
      AUC = "AUC"
    ) %>%
    fmt_number(columns = Brier_Score, decimals = 4) %>%
    fmt_percent(columns = Pseudo_R2, decimals = 1) %>%
    fmt_number(columns = AUC, decimals = 3) %>%
    tab_header(
      title = sprintf("Predicting %s: Model Comparison", outcome_label),
      subtitle = "Human judgment (LPM) vs ML (Probability Forest)"
    ) %>%
    tab_source_note(
      source_note = md(sprintf("**Key Finding**: ML explains **%.0f%% more variation** than standard flags.", lift_r2))
    ) %>%
    tab_style(
      style = cell_fill(color = "#d5f5e3"),
      locations = cells_body(columns = c(Pseudo_R2, AUC), rows = Model == "ML (Prob Forest)")
    ) %>%
    tab_style(style = cell_text(weight = "bold"), locations = cells_column_labels())
  
  return(tbl)
}


# ==============================================================================
# HUMAN vs ML COMPARISON (Integrated from model_comparison.R)
# ==============================================================================

#' Run Human (OLS) vs ML (GRF) Comparison with Proper Cross-Validation
#' 
#' Uses Leave-One-Region-Out CV for OLS and cluster-robust OOB for GRF
#' to ensure fair, generalizable comparison.
#'
#' @param dt Data.table with outcome and features
#' @param outcome_var Name of outcome column (should be log-transformed)
#' @param outcome_label Human-readable label for plots
#' @param cluster_var Clustering variable (e.g., "dep_region")
#' @param ols_vars Character vector of OLS predictor names
#' @param ml_features Character vector of ML feature names
#' @param feature_dict Feature dictionary for labeling
#' @param num_tuning_trials Number of manual tuning trials for GRF
#' @param seed Random seed
#' @return List with metrics, plots, importance, and models
run_human_vs_ml_comparison <- function(dt,
                                       outcome_var,
                                       outcome_label = "Outcome",
                                       cluster_var = "dep_region",
                                       ols_vars,
                                       ml_features,
                                       feature_dict = NULL,
                                       num_tuning_trials = 15,
                                       seed = 20250115) {
  
  message(sprintf("\n--- Human vs ML Comparison: %s ---", outcome_label))
  
  # -------------------------------------------------------------------------

  # A. DATA PREPARATION
  # -------------------------------------------------------------------------
  dt_analysis <- copy(dt)
  
 # Filter to complete cases
  dt_analysis <- dt_analysis[!is.na(get(outcome_var)) & is.finite(get(outcome_var))]
  dt_analysis <- dt_analysis[!is.na(get(cluster_var))]
  dt_analysis[, cluster_id := as.numeric(as.factor(get(cluster_var)))]
  
  unique_clusters <- unique(dt_analysis[[cluster_var]])
  n_clusters <- length(unique_clusters)
  
  message(sprintf("  Data: %s observations across %d clusters",
                  format(nrow(dt_analysis), big.mark = ","), n_clusters))
  
  # -------------------------------------------------------------------------
  # B. MODEL A: Human OLS with Leave-One-Region-Out CV
  # -------------------------------------------------------------------------
  message("  Training Human Model (LORO-CV)...")
  
  valid_ols_vars <- intersect(ols_vars, names(dt_analysis))
  f_human <- as.formula(paste(outcome_var, "~",
                              paste(valid_ols_vars, collapse = " + "),
                              "+ factor(Year)"))
  
  dt_analysis[, pred_human := NA_real_]
  cv_results_human <- list()
  
  for (i in seq_along(unique_clusters)) {
    r <- unique_clusters[i]
    train_idx <- which(dt_analysis[[cluster_var]] != r)
    test_idx <- which(dt_analysis[[cluster_var]] == r)
    
    if (length(test_idx) == 0) next
    
    m_human <- tryCatch({
      lm(f_human, data = dt_analysis[train_idx])
    }, error = function(e) NULL)
    
    if (!is.null(m_human)) {
      pred_k <- predict(m_human, newdata = dt_analysis[test_idx])
      dt_analysis[test_idx, pred_human := pred_k]
      
      actual_k <- dt_analysis[test_idx, get(outcome_var)]
      cv_results_human[[i]] <- data.table(
        cluster = r, n = length(test_idx),
        mse = mean((actual_k - pred_k)^2, na.rm = TRUE)
      )
    }
  }
  
  cv_human_dt <- rbindlist(cv_results_human)
  
  # -------------------------------------------------------------------------
  # C. MODEL B: ML Regression Forest with Manual Tuning + Cluster-Robust OOB
  # -------------------------------------------------------------------------
  message("  Training ML Model (Manual Tuning + Cluster-Robust)...")
  
  # Build feature matrix
  valid_ml_features <- intersect(ml_features, names(dt_analysis))
  f_ml <- as.formula(paste("~", paste(valid_ml_features, collapse = "+"), "- 1"))
  X_ml <- model.matrix(f_ml, data = dt_analysis)
  
  # model.matrix drops NA rows - align Y and clusters to matching rows
  used_rows <- as.integer(rownames(X_ml))
  Y <- dt_analysis[[outcome_var]][used_rows]
  clusters <- dt_analysis$cluster_id[used_rows]
  
  # Further filter to finite outcome values
  valid_idx <- which(!is.na(Y) & is.finite(Y))
  X_ml <- X_ml[valid_idx, , drop = FALSE]
  Y <- Y[valid_idx]
  clusters <- clusters[valid_idx]
  
  # Manual hyperparameter tuning
  set.seed(seed)
  tuning_grid <- data.frame(
    min.node.size = sample(1:20, num_tuning_trials, replace = TRUE),
    sample.fraction = runif(num_tuning_trials, 0.1, 0.45),
    mtry = sample(ceiling(sqrt(ncol(X_ml))):min(ncol(X_ml), 50), num_tuning_trials, replace = TRUE),
    alpha = runif(num_tuning_trials, 0.0, 0.2),
    honesty.fraction = runif(num_tuning_trials, 0.5, 0.8),
    honesty.prune.leaves = sample(c(TRUE, FALSE), num_tuning_trials, replace = TRUE),
    error = NA_real_
  )
  
  for (i in seq_len(nrow(tuning_grid))) {
    rf_tune <- grf::regression_forest(
      X = X_ml, Y = Y, clusters = clusters,
      num.trees = 500,
      min.node.size = tuning_grid$min.node.size[i],
      sample.fraction = tuning_grid$sample.fraction[i],
      mtry = tuning_grid$mtry[i],
      alpha = tuning_grid$alpha[i],
      honesty = TRUE,
      honesty.fraction = tuning_grid$honesty.fraction[i],
      honesty.prune.leaves = tuning_grid$honesty.prune.leaves[i],
      compute.oob.predictions = TRUE,
      seed = seed
    )
    preds <- predict(rf_tune)$predictions
    tuning_grid$error[i] <- mean((Y - preds)^2)
  }
  
  best_params <- tuning_grid[which.min(tuning_grid$error), ]
  
  # Train final model
  rf_ml <- grf::regression_forest(
    X = X_ml, Y = Y, clusters = clusters,
    num.trees = 2000,
    min.node.size = best_params$min.node.size,
    sample.fraction = best_params$sample.fraction,
    mtry = best_params$mtry,
    alpha = best_params$alpha,
    honesty = TRUE,
    honesty.fraction = best_params$honesty.fraction,
    honesty.prune.leaves = best_params$honesty.prune.leaves,
    compute.oob.predictions = TRUE,
    seed = seed
  )
  
  pred_ml_oob <- predict(rf_ml)$predictions
  dt_analysis[, pred_ml := NA_real_]
  # Map back to original dt_analysis rows: used_rows[valid_idx]
  final_rows <- used_rows[valid_idx]
  dt_analysis[final_rows, pred_ml := pred_ml_oob]
  
  # -------------------------------------------------------------------------
  # D. COMPUTE COMPARISON METRICS
  # -------------------------------------------------------------------------
  dt_compare <- dt_analysis[!is.na(pred_human) & !is.na(pred_ml)]
  actual <- dt_compare[[outcome_var]]
  
  # RMSE
  rmse_human <- sqrt(mean((actual - dt_compare$pred_human)^2, na.rm = TRUE))
  rmse_ml <- sqrt(mean((actual - dt_compare$pred_ml)^2, na.rm = TRUE))
  
  # R²
  ss_tot <- sum((actual - mean(actual))^2)
  ss_res_human <- sum((actual - dt_compare$pred_human)^2, na.rm = TRUE)
  ss_res_ml <- sum((actual - dt_compare$pred_ml)^2, na.rm = TRUE)
  
  r2_human <- 1 - ss_res_human / ss_tot
  r2_ml <- 1 - ss_res_ml / ss_tot
  
  # Lift
  lift_r2 <- (r2_ml - r2_human) / abs(r2_human) * 100
  lift_rmse <- (rmse_human - rmse_ml) / rmse_human * 100
  
  # Dollar approximation (for log outcomes)
  median_val <- median(actual)
  dollar_error_human <- exp(median_val) * rmse_human
  dollar_error_ml <- exp(median_val) * rmse_ml
  
  metrics_dt <- data.table(
    Model = c("Human (OLS)", "ML (GRF)"),
    RMSE = c(rmse_human, rmse_ml),
    R2 = c(r2_human, r2_ml),
    Dollar_Error = c(dollar_error_human, dollar_error_ml)
  )
  
  message(sprintf("  Human OLS:  RMSE = %.3f | R² = %.1f%%", rmse_human, r2_human * 100))
  message(sprintf("  ML GRF:     RMSE = %.3f | R² = %.1f%%", rmse_ml, r2_ml * 100))
  message(sprintf("  ML Lift:    +%.1f%% R² | -%.1f%% RMSE", lift_r2, lift_rmse))
  
  # -------------------------------------------------------------------------
  # E. VISUALIZATIONS
  # -------------------------------------------------------------------------
  
  # R² Comparison Bar Chart
  plot_data <- data.table(
    Model = factor(c("Human Intuition\n(OLS)", "ML Discovery\n(GRF)"),
                   levels = c("Human Intuition\n(OLS)", "ML Discovery\n(GRF)")),
    R2 = c(r2_human, r2_ml),
    RMSE = c(rmse_human, rmse_ml)
  )
  
  p_r2 <- ggplot(plot_data, aes(x = Model, y = R2, fill = Model)) +
    geom_col(width = 0.6, alpha = 0.9) +
    geom_text(aes(label = sprintf("%.1f%%", R2 * 100)), vjust = -0.5, fontface = "bold", size = 5) +
    scale_y_continuous(labels = percent_format(), 
                       limits = c(0, max(r2_ml * 1.4, 0.5)),
                       expand = expansion(mult = c(0, 0.1))) +
    scale_fill_manual(values = c("Human Intuition\n(OLS)" = "#3498db",
                                 "ML Discovery\n(GRF)" = "#9b59b6")) +
    labs(title = sprintf("Predicting %s: How Much Can We Explain?", outcome_label),
         subtitle = sprintf("ML discovers +%.0f%% more signal than standard risk flags", lift_r2),
         y = "Variance Explained (R²)", x = NULL,
         caption = "Validation: Leave-One-Region-Out CV (OLS) vs Cluster-Robust OOB (GRF)") +
    theme_minimal(base_size = 14) +
    theme(legend.position = "none",
          plot.title = element_text(face = "bold", size = 16),
          plot.subtitle = element_text(color = "#7f8c8d", size = 12),
          panel.grid.major.x = element_blank())
  
  p_rmse <- ggplot(plot_data, aes(x = Model, y = RMSE, fill = Model)) +
    geom_col(width = 0.6, alpha = 0.9) +
    geom_text(aes(label = sprintf("%.3f", RMSE)), vjust = -0.5, fontface = "bold", size = 5) +
    scale_y_continuous(limits = c(0, max(rmse_human * 1.3)),
                       expand = expansion(mult = c(0, 0.1))) +
    scale_fill_manual(values = c("Human Intuition\n(OLS)" = "#3498db",
                                 "ML Discovery\n(GRF)" = "#9b59b6")) +
    labs(title = "Prediction Error (Lower = Better)",
         subtitle = sprintf("ML reduces error by %.0f%%", lift_rmse),
         y = "RMSE (Log Scale)", x = NULL) +
    theme_minimal(base_size = 14) +
    theme(legend.position = "none",
          plot.title = element_text(face = "bold", size = 16),
          plot.subtitle = element_text(color = "#7f8c8d", size = 12),
          panel.grid.major.x = element_blank())
  
  # Combined plot
  p_combined <- p_r2 + p_rmse +
    plot_annotation(
      title = sprintf("%s: Human Intuition vs Algorithmic Discovery", outcome_label),
      subtitle = "Comparing standard risk flags against data-driven signals",
      theme = theme(plot.title = element_text(face = "bold", size = 18, hjust = 0.5),
                    plot.subtitle = element_text(size = 13, hjust = 0.5, color = "#7f8c8d"))
    )
  
  # -------------------------------------------------------------------------
  # F. VARIABLE IMPORTANCE
  # -------------------------------------------------------------------------
  var_imp <- grf::variable_importance(rf_ml)
  imp_dt <- data.table(Feature = colnames(X_ml), Importance = as.vector(var_imp))
  setorder(imp_dt, -Importance)
  
  # Categorize
  imp_dt[, Category := fcase(
    grepl("^bin_|^qty_", Feature), "Data Signal (ML)",
    grepl("^has_", Feature), "Regulatory Flag (Human)",
    grepl("^business_|^Owner_|^final_owner", Feature), "Owner Profile",
    default = "Facility Trait"
  )]
  
  # Clean labels
  if (!is.null(feature_dict)) {
    imp_dt <- clean_importance_labels(imp_dt, feature_dict)
  } else {
    imp_dt[, Plot_Label := gsub("_", " ", Feature)]
    imp_dt[, Plot_Label := tools::toTitleCase(Plot_Label)]
  }
  
  p_importance <- ggplot(head(imp_dt, 15),
                         aes(x = reorder(Plot_Label, Importance), y = Importance, fill = Category)) +
    geom_col(alpha = 0.9) +
    coord_flip() +
    scale_fill_manual(values = c(
      "Data Signal (ML)" = "#9b59b6",
      "Regulatory Flag (Human)" = "#e67e22",
      "Owner Profile" = "#27ae60",
      "Facility Trait" = "#3498db"
    )) +
    labs(title = sprintf("What Predicts %s?", outcome_label),
         subtitle = "Top 15 features by predictive importance",
         x = NULL, y = "Relative Importance",
         caption = "Purple = signals beyond standard checklists") +
    theme_minimal(base_size = 13) +
    theme(legend.position = "bottom", legend.title = element_blank(),
          plot.title = element_text(face = "bold", size = 16))
  
  # -------------------------------------------------------------------------
  # G. RETURN
  # -------------------------------------------------------------------------
  return(list(
    metrics = metrics_dt,
    lift_r2 = lift_r2,
    lift_rmse = lift_rmse,
    r2_human = r2_human,
    r2_ml = r2_ml,
    rmse_human = rmse_human,
    rmse_ml = rmse_ml,
    plot_combined = p_combined,
    plot_r2 = p_r2,
    plot_rmse = p_rmse,
    plot_importance = p_importance,
    importance = imp_dt,
    predictions = pred_ml_oob,
    X_matrix = X_ml,
    model_ml = rf_ml,
    cv_human = cv_human_dt,
    best_params = best_params
  ))
}


#' Create Executive Summary Table for Model Comparison
#' @param comparison_result Output from run_human_vs_ml_comparison
#' @param outcome_label Label for the outcome
#' @return gt table object
create_comparison_table <- function(comparison_result, outcome_label = "Outcome") {
  
  metrics_dt <- comparison_result$metrics
  lift_r2 <- comparison_result$lift_r2
  
  tbl <- metrics_dt %>%
    gt() %>%
    cols_label(
      Model = "Approach",
      RMSE = "Prediction Error (RMSE)",
      R2 = "Variance Explained (R²)",
      Dollar_Error = "Approx. $ Error"
    ) %>%
    fmt_percent(columns = R2, decimals = 1) %>%
    fmt_number(columns = RMSE, decimals = 3) %>%
    fmt_currency(columns = Dollar_Error, decimals = 0) %>%
    tab_header(
      title = sprintf("Predicting %s: Model Comparison", outcome_label),
      subtitle = "Human judgment (OLS) vs data-driven discovery (GRF)"
    ) %>%
    tab_source_note(
      source_note = md(sprintf("**Key Finding**: ML explains **%.0f%% more variation** than standard risk flags.", lift_r2))
    ) %>%
    tab_footnote(
      footnote = "Validated using Leave-One-Region-Out CV (OLS) and Cluster-Robust OOB (GRF)",
      locations = cells_column_labels(columns = R2)
    ) %>%
    tab_style(
      style = cell_fill(color = "#d5f5e3"),
      locations = cells_body(columns = R2, rows = Model == "ML (GRF)")
    ) %>%
    tab_style(style = cell_text(weight = "bold"), locations = cells_column_labels())
  
  return(tbl)
}


#' Create surrogate decision tree for ML model interpretation
#' @param predictions Vector of ML predictions
#' @param X_matrix Feature matrix used for training
#' @param imp_dt Importance data.table with Feature and Smart_Label
#' @param outcome_label Label for outcome (e.g., "Cost", "Duration")
#' @param outcome_unit Unit string (e.g., "$", "Months")
#' @param transform_fn Function to transform log predictions (e.g., exp)
#' @param filename_prefix Output filename prefix
#' @param n_features Number of top features to use
create_surrogate_tree <- function(predictions, X_matrix, imp_dt, feature_dict,
                                  outcome_label, outcome_unit, transform_fn,
                                  filename_prefix, n_features = 10) {
  
  tree_features <- head(imp_dt$Feature, n_features)
  X_subset <- as.data.frame(X_matrix[, tree_features, drop = FALSE])
  tree_data <- cbind(outcome_pred = predictions, X_subset)
  
  tree_model <- rpart(
    outcome_pred ~ ., data = tree_data, method = "anova",
    control = rpart.control(maxdepth = 3, cp = 0.01, minsplit = 50)
  )
  
  use_ggparty <- requireNamespace("ggparty", quietly = TRUE) && 
                 requireNamespace("partykit", quietly = TRUE)
  
  if (use_ggparty) {
    tree_party <- as.party(tree_model)
    node_ids <- predict(tree_party, type = "node")
    node_stats <- data.table(id = node_ids, val = tree_data$outcome_pred)[, .(
      avg_pred = mean(val), n_count = .N
    ), by = id]
    
    p <- ggparty(tree_party)
    p$data <- merge(p$data, node_stats, by = "id", all.x = TRUE)
    
    # Transform predictions back to original scale for display
    if (!is.null(transform_fn)) {
      p$data$term_label <- sprintf("%s: %s%s\nn = %s",
                                   outcome_label,
                                   outcome_unit,
                                   format(round(transform_fn(p$data$avg_pred)), big.mark = ","),
                                   format(p$data$n_count, big.mark = ","))
    } else {
      p$data$term_label <- sprintf("%s: %.1f %s\nn = %s",
                                   outcome_label,
                                   p$data$avg_pred,
                                   outcome_unit,
                                   format(p$data$n_count, big.mark = ","))
    }
    
    # Map technical names to labels
    tech_to_smart <- setNames(imp_dt$Smart_Label, imp_dt$Feature)
    if (!is.null(p$data$splitvar)) {
      p$data$splitvar_display <- tech_to_smart[as.character(p$data$splitvar)]
      p$data$splitvar_display[is.na(p$data$splitvar_display)] <- 
        as.character(p$data$splitvar[is.na(p$data$splitvar_display)])
    }
    
    p_tree <- p +
      geom_edge() +
      geom_edge_label(aes(label = breaks_label), parse = FALSE, size = 3, fill = "white") +
      geom_node_label(
        aes(label = stringr::str_wrap(splitvar_display, 20)),
        ids = "inner", size = 3.5, fontface = "bold", fill = "white"
      ) +
      geom_node_label(aes(label = term_label), ids = "terminal", size = 3, fill = "white") +
      labs(
        title = sprintf("Decision Rules for %s", outcome_label),
        subtitle = "Flowchart showing characteristics leading to higher/lower values"
      ) +
      theme_void() +
      theme(
        plot.title = element_text(face = "bold", size = 14, hjust = 0.5),
        plot.subtitle = element_text(size = 11, hjust = 0.5, color = "gray40")
      )
    
    ggsave(here(sprintf("output/figures/%s_surrogate_tree.png", filename_prefix)), 
           p_tree, width = 12.5, height = 8, bg = "white")
    ggsave(here(sprintf("output/figures/%s_surrogate_tree.pdf", filename_prefix)), 
           p_tree, width = 12.5, height = 8, bg = "white")
  } else {
    # Fallback to rpart.plot
    png(here(sprintf("output/figures/%s_surrogate_tree.png", filename_prefix)), 
        width = 1400, height = 900, res = 150)
    rpart.plot(tree_model, type = 4, extra = 101, under = TRUE,
               box.palette = "RdYlGn", nn = FALSE, roundint = FALSE,
               main = sprintf("Decision Rules for %s", outcome_label))
    dev.off()
  }
  
  message(sprintf("Saved: %s_surrogate_tree", filename_prefix))
}


# ==============================================================================
# 0.2 STANDARDIZED VARIABLE SETS
# ==============================================================================

# OLS Variables (Human Intuition - Used consistently across all regressions)
ols_vars_standard <- c(
  # Binary Facility Flags
  "has_bare_steel", "has_single_walled", "has_secondary_containment",
  "has_pressure_piping", "has_suction_piping", "has_galvanized_piping",
  "has_electronic_atg", "has_manual_detection", "has_overfill_alarm",
  "has_gasoline", "has_diesel", "has_legacy_grandfathered", "has_recent_closure",
  
  # Data Quality Flags
  "has_tank_construction_unknown", "has_ug_piping_unknown", 
  "has_tank_release_detection_unknown",
  
  # Continuous Facility Metrics
  "avg_tank_age", "n_tanks_total", "total_capacity_gal",
  "avg_tank_capacity_gal", "max_tank_capacity_gal",
  
  # Claim History Variables
  "reporting_lag_days", "prior_claims_n", "days_since_prior_claim", "is_repeat_filer"
)

# Variables to exclude from OLS
ols_vars_exclude <- c("has_noncompliant", "has_unknown_data", "Owner_Size_Class",
                      "business_category", "final_owner_sector")


# ==============================================================================
# 1. LOAD DATA
# ==============================================================================
message("\n", strrep("=", 70))
message("LOADING DATA")
message(strrep("=", 70))

master <- readRDS(paths$master)
setDT(master)

contracts <- readRDS(paths$contracts)
setDT(contracts)

feature_dict <- readRDS(paths$feat_dict)
setDT(feature_dict)

# Merge region to contracts
contracts <- merge(contracts, 
                   master[, .(claim_number, dep_region, county)], 
                   by = "claim_number", all.x = TRUE)

message(sprintf("Master: %s claims | Contracts: %s records",
                format(nrow(master), big.mark = ","),
                format(nrow(contracts), big.mark = ",")))


# ==============================================================================
# SECTION 1.1: CORRELATES OF ELIGIBILITY
# ==============================================================================
message("\n", strrep("=", 70))
message("SECTION 1.1: CORRELATES OF ELIGIBILITY")
message(strrep("=", 70))

# -----------------------------------------------------------------------------
# 1.1a Descriptive Comparison: Denied vs Eligible
# -----------------------------------------------------------------------------
message("\n--- 1.1a Descriptive Comparison ---")

var_map_headline <- c(
  "n_tanks_total"       = "Total Tanks on Site",
  "avg_tank_age"        = "Average Tank Age (Years)",
  "paid_alae_real"      = "ALAE Costs (2025$)",
  "reporting_lag_days"  = "Reporting Delay (Days)",
  "has_single_walled"   = "Risk: Single-Walled System",
  "has_unknown_data"    = "Risk: Missing Tank Data",
  "is_repeat_filer"     = "Repeat Filer",
  "prior_claims_n"      = "Prior Claim Count"
)

master[, eligibility_group := fcase(
  status_group == "Denied", "Denied",
  status_group %in% c("Eligible", "Post Remedial", "Open"), "Eligible/Active",
  default = NA_character_
)]

risk_summary <- master[
  !is.na(eligibility_group), 
  lapply(.SD, mean, na.rm = TRUE), 
  by = eligibility_group, 
  .SDcols = names(var_map_headline)
]

risk_long <- melt(risk_summary, id.vars = "eligibility_group", 
                  variable.name = "var_code", value.name = "mean_val")
risk_long[, Metric := var_map_headline[as.character(var_code)]]
risk_wide <- dcast(risk_long, Metric ~ eligibility_group, value.var = "mean_val")
risk_wide[, Diff_Pct := (`Denied` - `Eligible/Active`) / `Eligible/Active`]

human_risk_tbl <- risk_wide %>%
  gt() %>%
  cols_label(Metric = "Characteristic", Denied = "Denied Claims",
             `Eligible/Active` = "Eligible Claims", Diff_Pct = "Difference (%)") %>%
  fmt_number(columns = c("Denied", "Eligible/Active"), rows = !grepl("\\$", Metric), decimals = 2) %>%
  fmt_currency(columns = c("Denied", "Eligible/Active"), rows = grepl("\\$", Metric), decimals = 0) %>%
  fmt_percent(columns = "Diff_Pct", decimals = 1) %>%
  tab_header(title = "Profile of Denied vs. Eligible Claims",
             subtitle = "Simple comparison of key facility and behavioral characteristics") %>%
  tab_style(style = cell_text(weight = "bold"), locations = cells_column_labels()) %>%
  tab_style(style = list(cell_text(color = "#c0392b", weight = "bold")),
            locations = cells_body(columns = Diff_Pct, rows = abs(Diff_Pct) > 0.5)) %>%
  tab_source_note("Source: PA USTIF Claims Database (1994-2024)")

save_table(human_risk_tbl, "101_eligibility_profile")
saveRDS(human_risk_tbl, here("output/tables/101_eligibility_profile.rds"))
message("Saved: 101_eligibility_profile")

# -----------------------------------------------------------------------------
# 1.1b Balance Test (T-Tests)
# -----------------------------------------------------------------------------
message("\n--- 1.1b Balance Test (T-Tests) ---")

analysis_vars <- names(var_map_headline)
bal_data <- master[eligibility_group %in% c("Denied", "Eligible/Active"), 
                   c("eligibility_group", analysis_vars), with = FALSE]

balance_results <- lapply(analysis_vars, function(v) {
  denied <- bal_data[eligibility_group == "Denied", get(v)]
  eligible <- bal_data[eligibility_group == "Eligible/Active", get(v)]
  
  if (all(is.na(denied)) || all(is.na(eligible))) return(NULL)
  if (uniqueN(na.omit(denied)) < 2 && uniqueN(na.omit(eligible)) < 2) return(NULL)
  
  tryCatch({
    tt <- t.test(denied, eligible)
    data.table(
      Variable = v, Metric = var_map_headline[v],
      Mean_Denied = mean(denied, na.rm = TRUE),
      Mean_Eligible = mean(eligible, na.rm = TRUE),
      Difference = mean(denied, na.rm = TRUE) - mean(eligible, na.rm = TRUE),
      P_Value = tt$p.value
    )
  }, error = function(e) NULL)
})

balance_dt <- rbindlist(balance_results)

bal_tbl <- balance_dt[, .(Metric, Mean_Denied, Mean_Eligible, Difference, P_Value)] %>%
  gt() %>%
  cols_label(Metric = "Characteristic", Mean_Denied = "Denied (Mean)",
             Mean_Eligible = "Eligible (Mean)", Difference = "Difference", P_Value = "P-Value") %>%
  fmt_number(columns = c("Mean_Denied", "Mean_Eligible", "Difference"),
             rows = !grepl("\\$", Metric), decimals = 2) %>%
  fmt_currency(columns = c("Mean_Denied", "Mean_Eligible", "Difference"),
               rows = grepl("\\$", Metric), decimals = 0) %>%
  fmt_number(columns = "P_Value", decimals = 4) %>%
  tab_style(style = list(cell_text(color = "#c0392b", weight = "bold")),
            locations = cells_body(columns = P_Value, rows = P_Value < 0.05)) %>%
  tab_header(title = "Statistical Balance: Denied vs. Eligible Claims",
             subtitle = "Two-sample t-tests comparing group means") %>%
  tab_source_note("Source: PA USTIF Claims Database")

save_table(bal_tbl, "102_eligibility_balance_test")
saveRDS(bal_tbl, here("output/tables/102_eligibility_balance_test.rds"))
message("Saved: 102_eligibility_balance_test")

# -----------------------------------------------------------------------------
# 1.1c OLS Regression: Drivers of Denial
# -----------------------------------------------------------------------------
message("\n--- 1.1c OLS: Denial Drivers ---")

denial_data <- master[eligibility_group %in% c("Denied", "Eligible/Active")]
denial_data[, is_denied := as.integer(eligibility_group == "Denied")]

# Impute NA -> 0 for binary flags
denial_data <- impute_human_na(denial_data, facility_vars_binary)
denial_data <- impute_human_na(denial_data, claim_vars_binary)

# Build formula (exclude problematic variables)
ols_continuous <- c("avg_tank_age", "n_tanks_total", "reporting_lag_days", 
                    "prior_claims_n", "days_since_prior_claim",
                    "total_capacity_gal", "avg_tank_capacity_gal", "max_tank_capacity_gal")

vars_to_exclude <- c("has_noncompliant", "has_unknown_data", "has_non_compliant_tank", 
                     "Owner_Size_Class", "business_category")
facility_vars_clean <- setdiff(facility_vars_binary, vars_to_exclude)

ols_formula_terms <- c(
  facility_vars_clean, ols_continuous, "is_repeat_filer",
  "has_tank_construction_unknown", "has_ug_piping_unknown", 
  "has_tank_release_detection_unknown"
)
ols_formula_terms <- intersect(ols_formula_terms, names(denial_data))

f_denial <- as.formula(paste(
  "is_denied ~", paste(ols_formula_terms, collapse = " + "), "| region_cluster + Year"
))

denial_model <- feols(f_denial, data = denial_data, vcov = "hetero")

ols_label_map <- create_display_labels(ols_formula_terms, feature_dict)
sig_results <- filter_to_significant(denial_model, threshold = 0.10, label_map = ols_label_map)

if (!is.null(sig_results) && nrow(sig_results) > 0) {
  denial_sig_tbl <- sig_results[, .(Label, Coef_Display, SE_Display, p_value)] %>%
    gt() %>%
    cols_label(Label = "Predictor", Coef_Display = "Effect on Denial Risk",
               SE_Display = "Std. Error", p_value = "P-Value") %>%
    fmt_number(columns = "p_value", decimals = 4) %>%
    tab_header(title = "What Predicts Claim Denial?",
               subtitle = "Linear Probability Model (Significant Marginal Effects)") %>%
    tab_style(style = cell_text(weight = "bold"), locations = cells_column_labels()) %>%
    tab_style(style = list(cell_fill(color = "#fadbd8")),
              locations = cells_body(columns = Coef_Display, rows = sig_results$Estimate > 0)) %>%
    tab_style(style = list(cell_fill(color = "#d5f5e3")),
              locations = cells_body(columns = Coef_Display, rows = sig_results$Estimate < 0)) %>%
    tab_footnote("Values represent percentage point change in denial probability.",
                 locations = cells_column_labels(columns = Coef_Display)) %>%
    tab_footnote("Controls: DEP Region and Year FE. Robust SEs.", locations = cells_title()) %>%
    tab_source_note(paste0("Showing ", nrow(sig_results), " predictors with p < 0.10. ",
                           "* p<0.10, ** p<0.05, *** p<0.01"))
  
  save_table(denial_sig_tbl, "103_denial_drivers_significant")
  saveRDS(denial_sig_tbl, here("output/tables/103_denial_drivers_significant.rds"))
  message(sprintf("Saved: 103_denial_drivers_significant (%d significant predictors)", nrow(sig_results)))
}

# -----------------------------------------------------------------------------
# 1.1d ML: Variable Importance (Probability Forest with Manual Tuning)
# -----------------------------------------------------------------------------
message("\n--- 1.1d ML: Probability Forest (Manual Tuning) ---")

ml_features_denial <- c(
  "avg_tank_age", "n_tanks_total", "is_repeat_filer", "reporting_lag_days",
  "prior_claims_n", "facility_age", "n_tanks_active", 
  "total_capacity_gal", "avg_tank_capacity_gal", "max_tank_capacity_gal",
  grep("^bin_", names(denial_data), value = TRUE),
  grep("^qty_", names(denial_data), value = TRUE)
)
ml_features_denial <- intersect(ml_features_denial, names(denial_data))

ml_df_denial <- denial_data[complete.cases(denial_data[, ..ml_features_denial])]
X_denial <- model.matrix(
  as.formula(paste("~", paste(ml_features_denial, collapse = "+"), "- 1")), 
  data = ml_df_denial
)
Y_denial <- as.factor(ml_df_denial$is_denied)

message(sprintf("  Training on %s observations with %d features",
                format(nrow(X_denial), big.mark = ","), ncol(X_denial)))

# Train with manual tuning
pf_denial <- tune_grf_forest(
  X = X_denial, Y = Y_denial,
  forest_type = "probability",
  num_trials = 15,
  num_trees_tune = 500,
  num_trees_final = 2000,
  seed = 20250115
)

# Extract and clean importance
imp_denial <- grf::variable_importance(pf_denial)
dt_imp_denial <- data.table(Feature = colnames(X_denial), Importance = as.vector(imp_denial))
dt_imp_denial <- clean_importance_labels(dt_imp_denial, feature_dict)
setorder(dt_imp_denial, -Importance)

top_10_denial <- head(dt_imp_denial, 10)

p_importance_denial <- ggplot(top_10_denial, aes(x = reorder(Plot_Label, Importance), y = Importance)) +
  geom_col(fill = "#8e44ad", alpha = 0.85, width = 0.7) +
  coord_flip() +
  scale_y_continuous(expand = expansion(mult = c(0, 0.1))) +
  labs(title = "What Characteristics Best Predict Denial?",
       subtitle = "Top 10 features from probability forest",
       x = NULL, y = "Predictive Importance",
       caption = "Method: Probability Forest (2,000 trees, manually tuned)") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", size = 14),
        panel.grid.major.y = element_blank())

save_figure(p_importance_denial, "104_ml_denial_importance")
message("Saved: 104_ml_denial_importance")

# -----------------------------------------------------------------------------
# 1.1e Policy Tree: Interpretable Rules
# -----------------------------------------------------------------------------
message("\n--- 1.1e Policy Tree ---")

prob_denied_vec <- predict(pf_denial)$predictions[, 2]

create_surrogate_tree(
  predictions = prob_denied_vec,
  X_matrix = X_denial,
  imp_dt = dt_imp_denial,
  feature_dict = feature_dict,
  outcome_label = "Pr(Denied)",
  outcome_unit = "",
  transform_fn = function(x) round(x * 100, 1),  # Convert to percentage
  filename_prefix = "104_denial"
)

# -----------------------------------------------------------------------------
# 1.1 Summary
# -----------------------------------------------------------------------------

# Human vs ML Comparison for Denial (Classification)
message("\n  Running Human vs ML Comparison for Denial...")
denial_comparison <- run_classification_comparison(
  dt = denial_data,
  outcome_var = "is_denied",
  outcome_label = "Claim Denial",
  cluster_var = "region_cluster",
  ols_vars = ols_formula_terms,
  ml_features = ml_features_denial,
  feature_dict = feature_dict,
  num_tuning_trials = 15,
  seed = 20250115
)

# Save comparison outputs
save_figure(denial_comparison$plot_combined, "104b_denial_human_vs_ml", width = 12, height = 6)
save_figure(denial_comparison$plot_importance, "104c_denial_importance_by_category", width = 9, height = 7)

denial_comparison_tbl <- create_classification_table(denial_comparison, "Claim Denial")
save_table(denial_comparison_tbl, "104_denial_model_comparison")
saveRDS(denial_comparison_tbl, here("output/tables/104_denial_model_comparison.rds"))

message("\n", strrep("-", 60))
message("SECTION 1.1 COMPLETE")
message(strrep("-", 60))
message(sprintf("  Denied claims: %s (%.1f%%)",
                format(sum(denial_data$is_denied), big.mark = ","),
                100 * mean(denial_data$is_denied)))
message(sprintf("  DENIAL PREDICTION:"))
message(sprintf("    Human LPM: Pseudo-R² = %.1f%% | AUC = %.3f",
                denial_comparison$pseudo_r2_human * 100, denial_comparison$auc_human))
message(sprintf("    ML PF:     Pseudo-R² = %.1f%% | AUC = %.3f",
                denial_comparison$pseudo_r2_ml * 100, denial_comparison$auc_ml))
message(sprintf("    ML Lift:   +%.1f%% Pseudo-R²", denial_comparison$lift_r2))
message(sprintf("  Significant OLS predictors (p<0.10): %d",
                ifelse(is.null(sig_results), 0, nrow(sig_results))))
message(sprintf("  Top ML predictor: %s (Importance: %.3f)",
                top_10_denial$Smart_Label[1], top_10_denial$Importance[1]))


# ==============================================================================
# SECTION 1.3: CLOSED CLAIMS ANALYSIS (COSTS & DURATION)
# ==============================================================================
message("\n", strrep("=", 70))
message("SECTION 1.3: CLOSED CLAIMS ANALYSIS")
message(strrep("=", 70))

# Filter to closed, eligible claims with positive values
closed_claims <- master[status_group %in% c("Eligible", "Post Remedial") & 
                          total_paid_real > 1000 & 
                          claim_duration_days > 0]

ols_vars_for_regression <- intersect(ols_vars_standard, names(closed_claims))
ml_features_full <- build_ml_features(closed_claims, ols_vars_standard)

message(sprintf("  OLS Variables: %d | ML Variables: %d",
                length(ols_vars_for_regression), length(ml_features_full)))

# ==============================================================================
# 1.3a: COST DRIVERS
# ==============================================================================
message("\n--- 1.3a Cost Drivers ---")

# Distribution
p_cost_dist <- ggplot(closed_claims, aes(x = total_paid_real)) +
  geom_histogram(bins = 50, fill = "#2c3e50", alpha = 0.9) +
  scale_x_log10(labels = scales::dollar_format()) +
  labs(title = "Total Claim Cost Distribution",
       subtitle = sprintf("N = %s closed eligible claims", format(nrow(closed_claims), big.mark = ",")),
       x = "Total Cost (Real $, Log Scale)", y = "Count") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

save_figure(p_cost_dist, "105_cost_distribution")

# OLS Model
f_cost <- as.formula(paste(
  "log(total_paid_real) ~",
  paste(ols_vars_for_regression, collapse = " + "),
  "| dep_region + Year"
))

cost_ols_model <- feols(f_cost, data = closed_claims, vcov = "hetero")
ols_label_map_cost <- create_display_labels(ols_vars_for_regression, feature_dict)
sig_cost_ols <- filter_to_significant(cost_ols_model, threshold = 0.10, label_map = ols_label_map_cost)

if (!is.null(sig_cost_ols)) {
  cost_ols_tbl <- sig_cost_ols[, .(Label, Coef_Display, SE_Display, p_value)] %>%
    gt() %>%
    tab_header(title = "OLS: What Predicts Claim Cost?",
               subtitle = "Dependent Variable: Log(Total Paid Real $)") %>%
    cols_label(Label = "Predictor", Coef_Display = "Effect (Log Scale)",
               SE_Display = "Std. Error", p_value = "P-Value") %>%
    fmt_number(columns = "p_value", decimals = 4) %>%
    tab_style(style = cell_text(weight = "bold"), locations = cells_column_labels()) %>%
    tab_style(style = list(cell_fill(color = "#fadbd8")),
              locations = cells_body(columns = Coef_Display, rows = sig_cost_ols$Estimate > 0)) %>%
    tab_style(style = list(cell_fill(color = "#d5f5e3")),
              locations = cells_body(columns = Coef_Display, rows = sig_cost_ols$Estimate < 0)) %>%
    tab_source_note("Controls: DEP Region & Year FE. Robust SEs. Showing p < 0.10 only.")
  
  save_table(cost_ols_tbl, "106_cost_ols_significant")
}

# ML: Regression Forest with Manual Tuning
message("\n  Training Cost Regression Forest...")

closed_claims[, log_cost := log(total_paid_real)]

ml_df_cost <- closed_claims[complete.cases(closed_claims[, ..ml_features_full])]
X_cost <- model.matrix(
  as.formula(paste("~", paste(ml_features_full, collapse = "+"), "- 1")),
  data = ml_df_cost
)
Y_cost <- ml_df_cost$log_cost

rf_cost <- tune_grf_forest(
  X = X_cost, Y = Y_cost,
  forest_type = "regression",
  num_trials = 15,
  num_trees_tune = 500,
  num_trees_final = 2000,
  seed = 20250115
)

# Importance
imp_cost <- grf::variable_importance(rf_cost)
dt_imp_cost <- data.table(Feature = colnames(X_cost), Importance = as.vector(imp_cost))
dt_imp_cost <- clean_importance_labels(dt_imp_cost, feature_dict)
setorder(dt_imp_cost, -Importance)

top_10_cost <- head(dt_imp_cost, 10)

p_imp_cost <- ggplot(top_10_cost, aes(x = reorder(Plot_Label, Importance), y = Importance)) +
  geom_col(fill = "#2980b9", alpha = 0.85, width = 0.7) +
  coord_flip() +
  scale_y_continuous(expand = expansion(mult = c(0, 0.1))) +
  labs(title = "ML: Top Predictors of Claim Cost",
       subtitle = "Top 10 features ranked by importance",
       x = NULL, y = "Predictive Importance",
       caption = "Method: Regression Forest (2,000 trees, manually tuned)") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", size = 14),
        panel.grid.major.y = element_blank())

save_figure(p_imp_cost, "107_cost_ml_importance", width = 8, height = 5)

# Surrogate Tree
create_surrogate_tree(
  predictions = predict(rf_cost)$predictions,
  X_matrix = X_cost,
  imp_dt = dt_imp_cost,
  feature_dict = feature_dict,
  outcome_label = "Cost",
  outcome_unit = "$",
  transform_fn = exp,
  filename_prefix = "108_cost"
)

# Human vs ML Comparison (with proper cross-validation)
message("\n  Running Human vs ML Comparison for Cost...")
cost_comparison <- run_human_vs_ml_comparison(
  dt = closed_claims,
  outcome_var = "log_cost",
  outcome_label = "Claim Cost",
  cluster_var = "dep_region",
  ols_vars = ols_vars_for_regression,
  ml_features = ml_features_full,
  feature_dict = feature_dict,
  num_tuning_trials = 15,
  seed = 20250115
)

# Save comparison outputs
save_figure(cost_comparison$plot_combined, "108b_cost_human_vs_ml", width = 12, height = 6)
save_figure(cost_comparison$plot_importance, "108c_cost_importance_by_category", width = 9, height = 7)

cost_comparison_tbl <- create_comparison_table(cost_comparison, "Claim Cost")
save_table(cost_comparison_tbl, "108_cost_model_comparison")
saveRDS(cost_comparison_tbl, here("output/tables/108_cost_model_comparison.rds"))

# ==============================================================================
# 1.3b: DURATION DRIVERS
# ==============================================================================
message("\n--- 1.3b Duration Drivers ---")

# Distribution
p_dur_dist <- ggplot(closed_claims, aes(x = claim_duration_days / 30)) +
  geom_histogram(bins = 50, fill = "#27ae60", alpha = 0.9) +
  labs(title = "Claim Duration Distribution",
       subtitle = sprintf("N = %s closed eligible claims", format(nrow(closed_claims), big.mark = ",")),
       x = "Duration (Months)", y = "Count") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"))

save_figure(p_dur_dist, "109_duration_distribution")

# OLS Model
f_dur <- as.formula(paste(
  "log(claim_duration_days / 30) ~",
  paste(ols_vars_for_regression, collapse = " + "),
  "| dep_region + Year"
))

dur_ols_model <- feols(f_dur, data = closed_claims, vcov = "hetero")
sig_dur_ols <- filter_to_significant(dur_ols_model, threshold = 0.10, label_map = ols_label_map_cost)

if (!is.null(sig_dur_ols)) {
  dur_ols_tbl <- sig_dur_ols[, .(Label, Coef_Display, SE_Display, p_value)] %>%
    gt() %>%
    tab_header(title = "OLS: What Predicts Claim Duration?",
               subtitle = "Dependent Variable: Log(Duration in Months)") %>%
    cols_label(Label = "Predictor", Coef_Display = "Effect (Log Scale)",
               SE_Display = "Std. Error", p_value = "P-Value") %>%
    fmt_number(columns = "p_value", decimals = 4) %>%
    tab_style(style = cell_text(weight = "bold"), locations = cells_column_labels()) %>%
    tab_style(style = list(cell_fill(color = "#fadbd8")),
              locations = cells_body(columns = Coef_Display, rows = sig_dur_ols$Estimate > 0)) %>%
    tab_style(style = list(cell_fill(color = "#d5f5e3")),
              locations = cells_body(columns = Coef_Display, rows = sig_dur_ols$Estimate < 0)) %>%
    tab_source_note("Controls: DEP Region & Year FE. Robust SEs. Showing p < 0.10 only.")
  
  save_table(dur_ols_tbl, "110_duration_ols_significant")
}

# ML: Regression Forest with Manual Tuning
message("\n  Training Duration Regression Forest...")

closed_claims[, log_duration_months := log(claim_duration_days / 30)]

ml_df_dur <- closed_claims[complete.cases(closed_claims[, ..ml_features_full])]
X_dur <- model.matrix(
  as.formula(paste("~", paste(ml_features_full, collapse = "+"), "- 1")),
  data = ml_df_dur
)
Y_dur <- ml_df_dur$log_duration_months

rf_duration <- tune_grf_forest(
  X = X_dur, Y = Y_dur,
  forest_type = "regression",
  num_trials = 15,
  num_trees_tune = 500,
  num_trees_final = 2000,
  seed = 20250115
)

# Importance
imp_dur <- grf::variable_importance(rf_duration)
dt_imp_dur <- data.table(Feature = colnames(X_dur), Importance = as.vector(imp_dur))
dt_imp_dur <- clean_importance_labels(dt_imp_dur, feature_dict)
setorder(dt_imp_dur, -Importance)

top_10_dur <- head(dt_imp_dur, 10)

p_imp_dur <- ggplot(top_10_dur, aes(x = reorder(Plot_Label, Importance), y = Importance)) +
  geom_col(fill = "#27ae60", alpha = 0.85, width = 0.7) +
  coord_flip() +
  scale_y_continuous(expand = expansion(mult = c(0, 0.1))) +
  labs(title = "ML: Top Predictors of Claim Duration",
       subtitle = "Top 10 features ranked by importance",
       x = NULL, y = "Predictive Importance",
       caption = "Method: Regression Forest (2,000 trees, manually tuned)") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", size = 14),
        panel.grid.major.y = element_blank())

save_figure(p_imp_dur, "111_duration_ml_importance", width = 8, height = 5)

# Surrogate Tree
create_surrogate_tree(
  predictions = predict(rf_duration)$predictions,
  X_matrix = X_dur,
  imp_dt = dt_imp_dur,
  feature_dict = feature_dict,
  outcome_label = "Duration",
  outcome_unit = "Months",
  transform_fn = exp,
  filename_prefix = "112_duration"
)

# Human vs ML Comparison (with proper cross-validation)
message("\n  Running Human vs ML Comparison for Duration...")
duration_comparison <- run_human_vs_ml_comparison(
  dt = closed_claims,
  outcome_var = "log_duration_months",
  outcome_label = "Claim Duration",
  cluster_var = "dep_region",
  ols_vars = ols_vars_for_regression,
  ml_features = ml_features_full,
  feature_dict = feature_dict,
  num_tuning_trials = 15,
  seed = 20250115
)

# Save comparison outputs
save_figure(duration_comparison$plot_combined, "112b_duration_human_vs_ml", width = 12, height = 6)
save_figure(duration_comparison$plot_importance, "112c_duration_importance_by_category", width = 9, height = 7)

duration_comparison_tbl <- create_comparison_table(duration_comparison, "Claim Duration")
save_table(duration_comparison_tbl, "112_duration_model_comparison")
saveRDS(duration_comparison_tbl, here("output/tables/112_duration_model_comparison.rds"))

# -----------------------------------------------------------------------------
# 1.3 Summary
# -----------------------------------------------------------------------------
message("\n", strrep("-", 60))
message("SECTION 1.3 COMPLETE")
message(strrep("-", 60))
message(sprintf("  Closed claims analyzed: %s", format(nrow(closed_claims), big.mark = ",")))
message("\n  COST PREDICTION:")
message(sprintf("    Human OLS R²: %.1f%% | ML GRF R²: %.1f%% | Lift: +%.1f%%",
                cost_comparison$r2_human * 100,
                cost_comparison$r2_ml * 100,
                cost_comparison$lift_r2))
message(sprintf("    Top ML predictor: %s", top_10_cost$Smart_Label[1]))
message("\n  DURATION PREDICTION:")
message(sprintf("    Human OLS R²: %.1f%% | ML GRF R²: %.1f%% | Lift: +%.1f%%",
                duration_comparison$r2_human * 100,
                duration_comparison$r2_ml * 100,
                duration_comparison$lift_r2))
message(sprintf("    Top ML predictor: %s", top_10_dur$Smart_Label[1]))


# ==============================================================================
# SECTION 1.4: REMEDIATION TRENDS
# ==============================================================================
message("\n", strrep("=", 70))
message("SECTION 1.4: REMEDIATION TRENDS")
message(strrep("=", 70))

annual_trends <- master[claim_year >= 1995 & claim_year <= 2024, .(
  N_Claims = .N,
  Mean_Cost_Real = mean(total_paid_real, na.rm = TRUE),
  Median_Cost_Real = median(total_paid_real, na.rm = TRUE),
  Mean_Duration_Years = mean(claim_duration_days / 365, na.rm = TRUE),
  Share_Denied = mean(status_group == "Denied", na.rm = TRUE)
), by = claim_year]

# Cost Trends
p_cost_trend <- ggplot(annual_trends, aes(x = claim_year)) +
  geom_line(aes(y = Mean_Cost_Real, color = "Mean"), linewidth = 1) +
  geom_line(aes(y = Median_Cost_Real, color = "Median"), linewidth = 1) +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_color_manual(values = c("Mean" = "#e74c3c", "Median" = "#3498db")) +
  labs(title = "Claim Costs Over Time (Real $)", x = "Year", y = "Cost", color = NULL) +
  theme_minimal()

save_figure(p_cost_trend, "113_trend_costs")

# Panel Overview
trends_long <- melt(annual_trends, id.vars = "claim_year", 
                    measure.vars = c("N_Claims", "Mean_Cost_Real", "Mean_Duration_Years"))

p_trends_panel <- ggplot(trends_long, aes(x = claim_year, y = value)) +
  geom_line(color = "#2c3e50", linewidth = 0.8) +
  geom_smooth(method = "loess", se = TRUE, alpha = 0.2, color = "#e74c3c") +
  facet_wrap(~ variable, scales = "free_y", 
             labeller = labeller(variable = c(
               "N_Claims" = "Claim Volume",
               "Mean_Cost_Real" = "Avg Cost (Real $)",
               "Mean_Duration_Years" = "Avg Duration (Years)"
             ))) +
  labs(title = "Remediation Market Trends: 1995-2024", x = "Year", y = NULL) +
  theme_minimal()

save_figure(p_trends_panel, "114_trend_panel", width = 12, height = 6)

message("Saved: 113_trend_costs, 114_trend_panel")


# ==============================================================================
# SECTION 1.5: SPATIAL VISUALIZATION
# ==============================================================================
message("\n", strrep("=", 70))
message("SECTION 1.5: SPATIAL VISUALIZATION")
message(strrep("=", 70))

# Theme for maps
theme_map_clean <- theme_void() +
  theme(
    legend.position = "bottom",
    legend.title = element_text(face = "bold", size = 10),
    legend.text = element_text(size = 9),
    legend.key.width = unit(1.5, "cm"),
    plot.title = element_text(face = "bold", hjust = 0.5, size = 14)
  )

# DEP Region Mapping
dep_regions_lut <- data.table(
  region = c(rep("Southeast", 5), rep("Northeast", 11), rep("Southcentral", 15), 
             rep("Northcentral", 14), rep("Southwest", 10), rep("Northwest", 12)),
  subregion = tolower(c(
    "Bucks", "Chester", "Delaware", "Montgomery", "Philadelphia",
    "Carbon", "Lackawanna", "Lehigh", "Luzerne", "Monroe", "Northampton", "Pike", 
    "Schuylkill", "Susquehanna", "Wayne", "Wyoming",
    "Adams", "Bedford", "Berks", "Blair", "Cumberland", "Dauphin", "Franklin", 
    "Fulton", "Huntingdon", "Juniata", "Lancaster", "Lebanon", "Mifflin", "Perry", "York",
    "Bradford", "Cameron", "Centre", "Clearfield", "Clinton", "Columbia", "Lycoming", 
    "Montour", "Northumberland", "Potter", "Snyder", "Sullivan", "Tioga", "Union",
    "Allegheny", "Armstrong", "Beaver", "Cambria", "Fayette", "Greene", "Indiana", 
    "Somerset", "Washington", "Westmoreland",
    "Butler", "Clarion", "Crawford", "Elk", "Erie", "Forest", "Jefferson", 
    "Lawrence", "McKean", "Mercer", "Venango", "Warren"
  ))
)

# Build SF objects
pa_counties_sf <- st_as_sf(map("county", "pennsylvania", plot = FALSE, fill = TRUE))
setDT(pa_counties_sf)
pa_counties_sf[, subregion := tstrsplit(ID, ",")[[2]]]
pa_counties_sf <- merge(pa_counties_sf, dep_regions_lut, by = "subregion", all.x = TRUE)

pa_regions_sf <- pa_counties_sf %>%
  st_make_valid() %>%
  group_by(region) %>%
  summarize(geometry = st_union(geom)) %>%
  ungroup()

# Regional Summary
regional_stats <- master[!is.na(dep_region) & total_paid_real > 0, .(
  N_Claims = .N,
  Total_Cost_Real = sum(total_paid_real, na.rm = TRUE),
  Mean_Cost_Real = mean(total_paid_real, na.rm = TRUE),
  Mean_Duration_Years = mean(claim_duration_days, na.rm = TRUE) / 365
), by = dep_region][order(-N_Claims)]

county_stats <- master[!is.na(county) & total_paid_real > 0, .(
  N_Claims = .N,
  Total_Cost = sum(total_paid_real, na.rm = TRUE)
), by = county][order(-N_Claims)]

# Regional Table
region_tbl <- kbl(regional_stats, digits = 0, format = "html", 
                  caption = "Claims Summary by DEP Region") %>% 
  kable_styling(full_width = FALSE)
save_table(region_tbl, "115_regional_summary")

# Regional Bar Chart
p_region_bar <- ggplot(regional_stats, aes(x = reorder(dep_region, N_Claims), y = N_Claims)) +
  geom_col(fill = "#3498db", alpha = 0.9, width = 0.7) +
  coord_flip() +
  theme_minimal() +
  labs(title = "Claims Volume by DEP Region", x = NULL, y = "Number of Claims")
save_figure(p_region_bar, "115_regional_claims_bar")

# County Bar Chart
p_county_bar <- ggplot(head(county_stats, 20), aes(x = reorder(county, N_Claims), y = N_Claims)) +
  geom_col(fill = "#e67e22", alpha = 0.9, width = 0.7) +
  coord_flip() +
  theme_minimal() +
  labs(title = "Top 20 Counties by Claims Volume", x = NULL, y = "Number of Claims")
save_figure(p_county_bar, "116_county_claims_bar")

# Claims Map
map_data_claims <- merge(pa_counties_sf, 
                         county_stats[, .(county_clean = tolower(county), N_Claims)],
                         by.x = "subregion", by.y = "county_clean", all.x = TRUE)

p_claims_map <- ggplot() +
  geom_sf(data = map_data_claims, aes(fill = N_Claims), color = "white", linewidth = 0.1) +
  scale_fill_distiller(name = "Total Claims", palette = "Blues", direction = 1, na.value = "#ecf0f1") +
  labs(title = "Total Claims Distribution by County") +
  theme_map_clean

save_figure(p_claims_map, "117_total_claims_map", width = 8, height = 5)

message("Saved: 115-117 spatial outputs")


# ==============================================================================
# SECTION 3.1: MARKET CONCENTRATION (HHI)
# ==============================================================================
message("\n", strrep("=", 70))
message("SECTION 3.1: MARKET CONCENTRATION (HHI)")
message(strrep("=", 70))

contracts_geo <- merge(contracts, master[, .(claim_number, county)], by = "claim_number")
contracts_geo[, county_clean := tolower(county)]

county_hhi_data <- contracts_geo[!is.na(consultant) & total_contract_value_real > 0, .(
  val = sum(total_contract_value_real)
), by = .(county_clean, consultant)]

county_hhi_data[, share := val / sum(val), by = county_clean]
county_hhi_final <- county_hhi_data[, .(HHI = sum(share^2) * 10000), by = county_clean]

map_data_hhi <- merge(pa_counties_sf, county_hhi_final, 
                      by.x = "subregion", by.y = "county_clean", all.x = TRUE)

p_hhi_map <- ggplot() +
  geom_sf(data = map_data_hhi, aes(fill = HHI), color = NA) +
  geom_sf(data = map_data_hhi, fill = NA, color = "white", linewidth = 0.1) +
  geom_sf(data = pa_regions_sf, aes(color = region), fill = NA, linewidth = 1.0) +
  scale_fill_viridis_c(name = "HHI Concentration", option = "magma", direction = -1, na.value = "#ecf0f1") +
  scale_color_brewer(name = "DEP Region", palette = "Set1") +
  labs(title = "Consultant Market Concentration (HHI) by County",
       subtitle = "Overlaid with PA DEP Administrative Regions") +
  theme_map_clean

save_figure(p_hhi_map, "301_hhi_county_map_regions", width = 10, height = 7)

# Regional HHI
hhi_by_region <- contracts[!is.na(consultant) & !is.na(dep_region) & total_contract_value_real > 0, .(
  val = sum(total_contract_value_real)
), by = .(dep_region, consultant)]

hhi_by_region[, share := val / sum(val), by = dep_region]
hhi_by_region <- hhi_by_region[, .(HHI = sum(share^2) * 10000), by = dep_region][order(-HHI)]

message(sprintf("  Highest HHI Region: %s (%.0f)", hhi_by_region$dep_region[1], hhi_by_region$HHI[1]))


# ==============================================================================
# SECTION 3.2: ADJUSTER ANALYSIS
# ==============================================================================
message("\n", strrep("=", 70))
message("SECTION 3.2: ADJUSTER ANALYSIS")
message(strrep("=", 70))

adjuster_stats <- contracts[!is.na(adjuster), .(
  N_Contracts = .N,
  N_Regions = uniqueN(dep_region),
  Total_Value = sum(total_contract_value_real, na.rm = TRUE),
  N_PFP = sum(auction_type == "Bid-to-Result", na.rm = TRUE)
), by = adjuster]

adjuster_stats[, Share_PFP := N_PFP / N_Contracts]

p_adjuster_scatter <- ggplot(adjuster_stats[N_Contracts >= 10], 
                             aes(x = N_Contracts, y = Share_PFP)) +
  geom_point(alpha = 0.7, size = 3, color = "#9b59b6") +
  scale_y_continuous(labels = percent_format()) +
  theme_minimal() +
  labs(title = "Adjuster Auction Propensity",
       subtitle = "Adjusters with 10+ contracts",
       x = "Contract Volume", y = "Share: Bid-to-Result")

save_figure(p_adjuster_scatter, "302_adjuster_auction_propensity")

multi_region_adjusters <- adjuster_stats[N_Regions > 1]
message(sprintf("  Multi-Region Adjusters: %d (%.1f%%)",
                nrow(multi_region_adjusters),
                100 * nrow(multi_region_adjusters) / nrow(adjuster_stats)))


# ==============================================================================
# FINAL SUMMARY
# ==============================================================================
message("\n", strrep("=", 70))
message("ANALYSIS 01 COMPLETE: Descriptive Statistics")
message(strrep("=", 70))

message("\n  DATA SCOPE:")
message(sprintf("    Total claims: %s", format(nrow(master), big.mark = ",")))
message(sprintf("    Denial rate: %.1f%%", 100 * mean(master$status_group == "Denied", na.rm = TRUE)))
message(sprintf("    Closed eligible claims: %s", format(nrow(closed_claims), big.mark = ",")))

message("\n  PREDICTABILITY FINDINGS (Human OLS vs ML GRF):")
message("    Claim Denial (Classification):")
message(sprintf("      - Human: Pseudo-R² = %.1f%%, AUC = %.3f",
                denial_comparison$pseudo_r2_human * 100, denial_comparison$auc_human))
message(sprintf("      - ML:    Pseudo-R² = %.1f%%, AUC = %.3f",
                denial_comparison$pseudo_r2_ml * 100, denial_comparison$auc_ml))
message(sprintf("      - ML improvement: +%.1f%% Pseudo-R²", denial_comparison$lift_r2))

message("    Claim Cost (Regression):")
message(sprintf("      - Human: R² = %.1f%%", cost_comparison$r2_human * 100))
message(sprintf("      - ML:    R² = %.1f%%", cost_comparison$r2_ml * 100))
message(sprintf("      - ML improvement: +%.1f%% variance explained", cost_comparison$lift_r2))

message("    Claim Duration (Regression):")
message(sprintf("      - Human: R² = %.1f%%", duration_comparison$r2_human * 100))
message(sprintf("      - ML:    R² = %.1f%%", duration_comparison$r2_ml * 100))
message(sprintf("      - ML improvement: +%.1f%% variance explained", duration_comparison$lift_r2))

message("\n  MARKET STRUCTURE:")
message(sprintf("    Highest HHI Region: %s (%.0f)", hhi_by_region$dep_region[1], hhi_by_region$HHI[1]))
message(sprintf("    Multi-Region Adjusters: %d", nrow(multi_region_adjusters)))

message("\n  OUTPUTS GENERATED:")
message("    Tables: 101-104, 106, 108, 110, 112, 115")
message("    Figures: 104-117, 301-302")
message("    Model comparison: 104b/c, 108b/c, 112b/c")