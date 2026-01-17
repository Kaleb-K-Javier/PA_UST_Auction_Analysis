# R/analysis/02_cost_correlates.R
# ==============================================================================
# Pennsylvania UST Analysis - Auction Mechanics & Procurement Design
# ==============================================================================
# PURPOSE: Evaluate the efficacy of the current auction system (Descriptive)
#
# SECTIONS (from 1/16 Policy Brief Goals):
#   2) AUCTION MECHANICS & PROCUREMENT
#      2.1) Intervention Timing: When does USTIF send claims to auction?
#      2.2) Costs of Work in Auctions: Contract vs ALAE vs Loss breakdown
#      2.3) Procurement Design Comparison: PFP vs SOW vs T&M
#      2.4) Contract Trends Over Time
#
# NOTE: This script is DESCRIPTIVE. It documents correlations and patterns
#       but does NOT establish causality. See 03_causal_inference.R for IV/DML.
#
# OUTPUTS:
#   - Tables: output/tables/2XX_*.{html,tex}
#   - Figures: output/figures/2XX_*.{png,pdf}
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
  feat_dict = here("data/processed/master_analysis_dictionary.rds"),
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
  Y <- as.factor(dt_analysis[[outcome_var]])
  clusters <- dt_analysis$cluster_id
  
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
  dt_analysis[, pred_ml := pred_ml_oob]
  
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
  Y <- dt_analysis[[outcome_var]]
  clusters <- dt_analysis$cluster_id
  
  # Filter valid rows
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
  dt_analysis[valid_idx, pred_ml := pred_ml_oob]
  
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



#' Estimate surrogate decision tree and return model object
#' @param predictions Vector of ML predictions
#' @param X_matrix Feature matrix used for training
#' @param imp_dt Importance data.table (for selecting top features)
#' @param n_features Number of top features to include
#' @return List containing: model (rpart), data (data.table), vars (character vector of used variables)
create_surrogate_tree <- function(predictions, X_matrix, imp_dt, n_features = 10) {
  
  # 1. PREPARE DATA & RECONSTRUCT FACTORS
  # Identify top features (include extra for dummy grouping)
  top_raw_features <- head(imp_dt$Feature, n_features * 2) 
  
  # Initialize DataFrame with predictions
  tree_dt <- data.table(outcome_pred = predictions)
  
  # --- LOGIC TO RECOVER FACTORS FROM DUMMIES ---
  # Define known factor groups (Fixed Effects)
  factor_groups <- c("region_cluster", "factor(Year)", "claim_year", "Year")
  processed_cols <- c()
  
  # A. Handle Factors (Region/Year)
  for (grp in factor_groups) {
    # Escape brackets for regex
    safe_grp <- gsub("\\(", "\\\\(", gsub("\\)", "\\\\)", grp))
    pattern <- paste0("^", safe_grp)
    
    dummy_cols <- grep(pattern, colnames(X_matrix), value = TRUE)
    
    if (length(dummy_cols) > 0) {
      mat <- X_matrix[, dummy_cols, drop = FALSE]
      has_level <- rowSums(mat) > 0
      
      fac_vec <- rep("Reference/Other", nrow(mat))
      if (any(has_level)) {
        level_names <- gsub(pattern, "", dummy_cols)
        active_indices <- max.col(mat[has_level, , drop=FALSE])
        fac_vec[has_level] <- level_names[active_indices]
      }
      
      clean_name <- if(grepl("region", grp, ignore.case=T)) "Region" else "Year"
      tree_dt[[clean_name]] <- as.factor(fac_vec)
      processed_cols <- c(processed_cols, dummy_cols)
    }
  }
  
  # B. Handle Remaining Continuous/Binary Features
  remaining_feats <- setdiff(top_raw_features, processed_cols)
  remaining_feats <- intersect(remaining_feats, colnames(X_matrix))
  remaining_feats <- head(remaining_feats, n_features)
  
  if (length(remaining_feats) > 0) {
    subset_df <- as.data.frame(X_matrix[, remaining_feats, drop = FALSE])
    tree_dt <- cbind(tree_dt, subset_df)
  }
  
  # 2. FIT TREE
  tree_model <- rpart(
    outcome_pred ~ ., data = tree_dt, method = "anova",
    control = rpart.control(maxdepth = 3, cp = 0.01, minsplit = 50)
  )
  
  # 3. IDENTIFY USED VARIABLES (For User Reference)
  used_vars <- unique(tree_model$frame$var)
  used_vars <- used_vars[used_vars != "<leaf>"]
  
  message("\n--- SURROGATE TREE ESTIMATED ---")
  message("Variables used in splits (Copy these for your map):")
  print(as.character(used_vars))
  
  return(list(
    model = tree_model,
    data = tree_dt,
    vars = as.character(used_vars)
  ))
}

#' Map labels to surrogate tree (Regex Supported)
#' @param tree_obj Output from create_surrogate_tree
#' @param node_map Named vector of variable mappings
#' @param value_map Named vector of value mappings
#' @param outcome_config List for terminal node formatting
#' @param file_config List for saving options
mapped_surrogate_tree <- function(tree_obj, 
                                  node_map = NULL, 
                                  value_map = NULL,
                                  outcome_config = list(label = "Outcome", unit = "", transform = NULL), 
                                  file_config = list(prefix = "tree_output", w = 12, h = 8, dpi = 300)) {
  
  require(ggparty)
  require(partykit)
  require(ggplot2)
  require(stringr)
  
  # 1. SETUP GGPLOT OBJECT
  tree_party <- as.party(tree_obj$model)
  p <- ggparty(tree_party)
  
  # 2. APPLY NODE MAP (Strict Match)
  p$data$split_label <- sapply(p$data$splitvar, function(x) {
    if(is.na(x)) return(NA)
    raw <- as.character(x)
    if(!is.null(node_map) && raw %in% names(node_map)) return(node_map[[raw]])
    # Fallback to cleaning function if no map match
    return(clean_variable_label(raw))
  })
  
  # 3. APPLY VALUE MAP (Robust Regex)
  # We iterate through the map and apply replacements sequentially
  if (!is.null(value_map)) {
    p$data$breaks_label <- sapply(p$data$breaks_label, function(x) {
      if (is.na(x)) return(NA)
      curr_label <- x
      
      for (pattern in names(value_map)) {
        replacement <- value_map[[pattern]]
        # Check if the pattern is meant to be a Regex or Literal
        # If it contains special regex chars, treat as regex. 
        # Otherwise, use fixed matching for safety.
        is_regex <- grepl("[\\^\\$\\[\\]\\*\\+\\?]", pattern)
        
        if (is_regex) {
          curr_label <- gsub(pattern, replacement, curr_label)
        } else {
          curr_label <- gsub(pattern, replacement, curr_label, fixed = TRUE)
        }
      }
      return(curr_label)
    })
  }
  
  # 4. TERMINAL NODES
  node_ids <- predict(tree_party, type = "node")
  node_stats <- data.table(id = node_ids, val = tree_obj$data$outcome_pred)[, .(
    avg_pred = mean(val), n_count = .N
  ), by = id]
  
  p$data <- merge(p$data, node_stats, by = "id", all.x = TRUE)
  
  p$data$term_label <- ifelse(p$data$kids == 0, {
    val <- p$data$avg_pred
    if (!is.null(outcome_config$transform)) val <- outcome_config$transform(val)
    
    val_fmt <- if(mean(val, na.rm=T) > 100) format(round(val), big.mark=",") else sprintf("%.1f", val)
    sprintf("%s: %s %s\nn = %s", outcome_config$label, val_fmt, outcome_config$unit, 
            format(p$data$n_count, big.mark = ","))
  }, NA)
  
  # 5. RENDER & SAVE
  p_final <- p +
    geom_edge() + 
    geom_edge_label(aes(label = breaks_label), size = 3) +
    geom_node_label(aes(label = stringr::str_wrap(split_label, 20)), 
                    ids = "inner", size = 3.5, fontface = "bold", fill = "white", color = "black") +
    geom_node_label(aes(label = term_label), ids = "terminal", size = 3, fill = "#f0f0f0") +
    labs(title = sprintf("Decision Rules for %s", outcome_config$label)) +
    theme_void() +
    theme(plot.title = element_text(face = "bold", size = 16, hjust = 0.5))
  
  # Explicitly extract dimensions to avoid errors
  w <- file_config$w; h <- file_config$h; dpi <- file_config$dpi
  
  dir.create(here("output/figures"), recursive = TRUE, showWarnings = FALSE)
  ggsave(here(paste0("output/figures/", file_config$prefix, ".png")), p_final, width=w, height=h, dpi=dpi, bg="white")
  ggsave(here(paste0("output/figures/", file_config$prefix, ".pdf")), p_final, width=w, height=h, device="pdf", bg="white")
  
  message(sprintf("Saved: %s (Size: %dx%d)", file_config$prefix, w, h))
}



#' Helper to print the EXACT raw strings inside the tree
#' @param tree_obj The output from create_surrogate_tree()
inspect_tree_labels <- function(tree_obj) {
  require(partykit)
  require(ggparty)
  
  # Convert to the format ggparty uses internally
  p_temp <- ggparty(as.party(tree_obj$model))
  
  # 1. Extract Variable Names (Nodes)
  raw_nodes <- unique(na.omit(as.character(p_temp$data$splitvar)))
  
  # 2. Extract Split Labels (Values)
  raw_values <- unique(na.omit(as.character(p_temp$data$breaks_label)))
  
  message("\n--- COPY & PASTE THIS BLOCK FOR YOUR NODE MAP ---")
  cat("my_node_map_denial <- c(\n")
  for(v in raw_nodes) {
    # Print with escaping to catch hidden newlines/quotes
    cat(sprintf("  %s = \"%s\",\n", dput(v), v)) 
  }
  cat(")\n")
  
  message("\n--- COPY & PASTE THIS BLOCK FOR YOUR VALUE MAP ---")
  cat("my_value_map_denial <- c(\n")
  for(v in raw_values) {
    # Pre-populate Yes/No guesses for binary splits
    guess <- "FIX_ME"
    if(grepl("0.5", v) && grepl("<", v)) guess <- "No"
    if(grepl("0.5", v) && (grepl(">", v) || grepl("≥", v))) guess <- "Yes"
    
    cat(sprintf("  %s = \"%s\",\n", dput(v), guess))
  }
  cat(")\n")
}

#' Generate clean labels for variables with override support
#' @param var_names Character vector of variable names to clean
#' @param custom_map Named vector of user overrides c("raw_name" = "New Label")
#' @return Character vector of cleaned labels
clean_variable_label <- function(var_names, custom_map = NULL) {
  
  # A. Define Standard Base Map (Common defaults)
  # You can override these by including them in your custom_map
  base_map <- c(
    "n_tanks_total"        = "Total Tanks",
    "avg_tank_age"         = "Avg Tank Age",
    "region_cluster"       = "DEP Region",
    "Region"               = "DEP Region",
    "claim_year"           = "Claim Year",
    "Year"                 = "Year",
    "reporting_lag_days"   = "Reporting Delay (Days)",
    "prior_claims_n"       = "Prior Claim Count",
    "total_capacity_gal"   = "Total Capacity (Gal)",
    "has_single_walled"    = "Single-Walled System",
    "has_bare_steel"       = "Bare Steel Tank",
    "is_repeat_filer"      = "Repeat Filer"
  )
  
  # B. Processing Function
  sapply(var_names, function(x) {
    if (is.na(x) || x == "") return(NA)
    
    # 1. User Custom Map (Highest Priority)
    if (!is.null(custom_map) && x %in% names(custom_map)) {
      return(custom_map[[x]])
    }
    
    # 2. Base Map (Standard Defaults)
    if (x %in% names(base_map)) {
      return(base_map[[x]])
    }
    
    # 3. Algorithmic Fallback (Heuristic Cleaning)
    # Remove technical prefixes
    lbl <- gsub("^(bin_|qty_|ind_|has_|is_)", "", x)
    # Remove suffixes like TRUE/FALSE or numbering
    lbl <- gsub("(TRUE|FALSE)$", "", lbl)
    # Replace underscores
    lbl <- gsub("_", " ", lbl)
    # Title Case
    lbl <- tools::toTitleCase(lbl)
    
    # Fix specific acronyms
    lbl <- gsub("Ust", "UST", lbl)
    lbl <- gsub("Ast", "AST", lbl)
    lbl <- gsub("Dep", "DEP", lbl)
    
    return(lbl)
  }, USE.NAMES = FALSE)
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
message("\n--- Loading Data ---")

master <- readRDS(paths$master)
setDT(master)

contracts <- readRDS(paths$contracts)
setDT(contracts)

feature_dict <- readRDS(paths$feat_dict)
setDT(feature_dict)


# Merge claim info to contracts
contracts <- merge(contracts, 
                   master[, .(claim_number, dep_region, county, claim_date, 
                              total_paid_real, paid_alae_real, claim_duration_days,
                              avg_tank_age, n_tanks_total, business_category)],
                   by = "claim_number", all.x = TRUE)

# Create auction type factor
master[, auction_type_factor := factor(contract_type, 
                                        levels = c("No Contract", "Other Contract", 
                                                   "Scope of Work", "Bid-to-Result"))]



# ==============================================================================
# 2.0 DATA IMPUTATION & PRE-PROCESSING (ROBUST CATCH-ALL)
# ==============================================================================
message("\n", strrep("=", 70))
message("SECTION 2.0: HANDLING MISSING DATA (ROBUST)")
message(strrep("=", 70))

# -----------------------------------------------------------------------------
# A. Group 1: Zero-Fill Logic (Counts, Flags, Aggregates)
# -----------------------------------------------------------------------------
# UPDATED LOGIC:
# 1. 'has_', 'bin_': Binary flags -> 0 (Not Present)
# 2. 'qty_', 'n_':   Count variables -> 0 (None)
# 3. 'days_since_prior_claim': -> 0 (No prior claim)

# 1. Define broad regex for zero-fill targets (Added "n_" and "total_closed")
vars_to_zerofill <- c(
  "days_since_prior_claim",
  "avg_prior_claim_cost",  # If no prior claim, avg cost is 0
  grep("^(has_|bin_|qty_|n_)", names(master), value = TRUE),
  "has_tank_construction_unknown", 
  "has_ug_piping_unknown", 
  "has_tank_release_detection_unknown"
)

# 2. Filter to variables actually in the dataset
vars_to_zerofill <- intersect(vars_to_zerofill, names(master))

# 3. Apply Zero-Fill
count_fixed <- 0
for (col in vars_to_zerofill) {
  if (any(is.na(master[[col]]))) {
    set(master, i = which(is.na(master[[col]])), j = col, value = 0)
    count_fixed <- count_fixed + 1
  }
}
message(sprintf("  Zero-filled NAs in %d variables (Flags & Counts including 'n_')", count_fixed))

# -----------------------------------------------------------------------------
# B. Group 2: Continuous Metrics (Impute Median + Flag)
# -----------------------------------------------------------------------------
# Logic: Primary physical metrics that cannot be 0.
continuous_vars_to_impute <- c(
  "avg_tank_age",           
  "total_capacity_gal",     
  "avg_tank_capacity_gal",  
  "max_tank_capacity_gal", 
  "facility_age",
  "reporting_lag_days"      
)

for (col in continuous_vars_to_impute) {
  if (col %in% names(master) && sum(is.na(master[[col]])) > 0) {
      
      # 1. Create Missing Flag
      flag_name <- paste0(col, "_is_missing")
      master[, (flag_name) := as.integer(is.na(get(col)))]
      
      # 2. Calculate Median
      median_val <- median(master[[col]], na.rm = TRUE)
      if (is.na(median_val)) median_val <- 0
      
      # 3. Fill NA
      set(master, i = which(is.na(master[[col]])), j = col, value = median_val)
      
      # 4. Add to OLS list
      if (!flag_name %in% ols_vars_standard) {
        ols_vars_standard <- c(ols_vars_standard, flag_name)
      }
      
      message(sprintf("  Imputed '%s': Median=%.1f | Flagged", col, median_val))
  }
}

# -----------------------------------------------------------------------------
# C. Group 3: The "Straggler" Catch-All
# -----------------------------------------------------------------------------
# Find ANY remaining ML feature with NAs and fix it to prevent crash.
# We default to Median imputation for these unknowns to be safe.

# Identify any column starting with typical ML prefixes that still has NAs
ml_prefixes <- "^(bin_|qty_|avg_|total_|max_|n_|facility_)"
potential_ml_vars <- grep(ml_prefixes, names(master), value = TRUE)
stragglers <- potential_ml_vars[sapply(master[, ..potential_ml_vars], function(x) any(is.na(x)))]

if (length(stragglers) > 0) {
  message(sprintf("  Found %d straggling variables with NAs: %s", length(stragglers), paste(stragglers, collapse=", ")))
  
  for (col in stragglers) {
     # Default to Median for safety
     median_val <- median(master[[col]], na.rm = TRUE)
     if (is.na(median_val)) median_val <- 0
     set(master, i = which(is.na(master[[col]])), j = col, value = median_val)
  }
  message("  Fixed stragglers with median imputation.")
}

# -----------------------------------------------------------------------------
# D. Final Validation
# -----------------------------------------------------------------------------
remaining_na_ols <- nrow(master) - nrow(na.omit(master[, ..ols_vars_standard]))
ml_check_vars <- grep("^(bin_|qty_|avg_|total_|max_|n_)", names(master), value = TRUE)
remaining_na_ml <- nrow(master) - nrow(na.omit(master[, ..ml_check_vars]))

if (remaining_na_ols == 0 && remaining_na_ml == 0) {
  message("\n  SUCCESS: All Analysis & ML variables are 100% complete.")
} else {
  message(sprintf("\n  WARNING: NAs remain. OLS Rows Lost: %d | ML Rows Lost: %d", 
                  remaining_na_ols, remaining_na_ml))
}


# ==============================================================================
# 2.1 UPDATE VARIABLE LISTS (POST-IMPUTATION)
# ==============================================================================
message("\n", strrep("-", 60))
message("UPDATING REGRESSION VARIABLE LISTS")
message(strrep("-", 60))

# 1. Base Variables (Original List)
ols_vars_standard <- c(
  # Binary Facility Flags (Now Zero-Filled)
  "has_bare_steel", "has_single_walled", "has_secondary_containment",
  "has_pressure_piping", "has_suction_piping", "has_galvanized_piping",
  "has_electronic_atg", "has_manual_detection", "has_overfill_alarm",
  "has_gasoline", "has_diesel", "has_legacy_grandfathered", "has_recent_closure",
  
  # Data Quality Flags (Now Zero-Filled)
  "has_tank_construction_unknown", "has_ug_piping_unknown", 
  "has_tank_release_detection_unknown",
  
  # Continuous Facility Metrics (Now Median-Imputed)
  "avg_tank_age", "n_tanks_total", "total_capacity_gal",
  "avg_tank_capacity_gal", "max_tank_capacity_gal",
  
  # Claim History Variables
  "reporting_lag_days", "prior_claims_n", "days_since_prior_claim", "is_repeat_filer"
)

# 2. Dynamically Add Missingness Flags
# If the imputation step created a flag (e.g., 'avg_tank_age_is_missing'), add it here.
missing_flags <- grep("_is_missing$", names(master), value = TRUE)

if (length(missing_flags) > 0) {
  ols_vars_standard <- unique(c(ols_vars_standard, missing_flags))
  message(sprintf("  Added %d missingness flags to OLS model: %s", 
                  length(missing_flags), paste(missing_flags, collapse = ", ")))
}

# 3. Variables to Explicitly Exclude (Multicollinearity/Leakage Risks)
ols_vars_exclude <- c(
  "has_noncompliant",       # Often redundant with specific compliance flags
  "has_unknown_data",       # Replaced by specific _is_missing flags
  "Owner_Size_Class",       # Factor, handled separately
  "business_category",      # Factor, handled separately
  "final_owner_sector"      # Factor, handled separately
)

message(sprintf("  Final OLS Variable Count: %d features", length(ols_vars_standard)))




# Filter to claims with contracts for auction analysis
auction_claims <- master[contract_type != "No Contract" & total_paid_real > 1000]

message(sprintf("Master: %d claims | With Contracts: %d | Contracts Table: %d",
                nrow(master), nrow(auction_claims), nrow(contracts)))

# ==============================================================================
# SECTION 2.1: INTERVENTION TIMING
# ==============================================================================
message("\n--- 2.1 Intervention Timing ---")

# When does USTIF intervene with an auction?
# intervention_lag_days = date_first_contract - claim_date

intervention_summary <- auction_claims[!is.na(intervention_lag_days) & 
                                         intervention_lag_days > 0, .(
  N = .N,
  Mean_Days = mean(intervention_lag_days),
  Median_Days = median(intervention_lag_days),
  SD_Days = sd(intervention_lag_days),
  P25_Days = quantile(intervention_lag_days, 0.25),
  P75_Days = quantile(intervention_lag_days, 0.75),
  Mean_Years = mean(intervention_lag_days) / 365,
  Median_Years = median(intervention_lag_days) / 365
), by = contract_type]

intervention_tbl <- kbl(intervention_summary, digits = 1, format = "html",
                        caption = "Time from Claim to First Contract (Days)") %>%
  kable_styling()

save_table(intervention_tbl, "201_intervention_timing_summary")

# Distribution plot
p_intervention <- ggplot(auction_claims[intervention_lag_days > 0 ], 
                          aes(x = intervention_lag_days / 365)) +
  geom_histogram(bins = 40, fill = "#3498db", alpha = 0.8, color = "white") +
  geom_vline(aes(xintercept = median(intervention_lag_days, na.rm = TRUE) / 365),
             linetype = "dashed", color = "#e74c3c", linewidth = 1) +
  labs(title = "Time to Auction Intervention",
       subtitle = sprintf("Median: %.1f years (dashed line)", 
                          median(auction_claims$intervention_lag_days, na.rm = TRUE) / 365),
       x = "Years from Claim to First Contract",
       y = "Number of Claims")

save_figure(p_intervention, "201_intervention_timing_histogram")

# By contract type
p_intervention_by_type <- ggplot(auction_claims[intervention_lag_days > 0 & 
                                                  intervention_lag_days < 5000],
                                  aes(x = intervention_lag_days / 365, 
                                      fill = contract_type)) +
  geom_density(alpha = 0.5) +
  geom_vline(aes(xintercept = median(intervention_lag_days, na.rm = TRUE) / 365),
             linetype = "dashed", color = "#e74c3c", linewidth = 1) +

  scale_fill_viridis_d(option = "mako", begin = 0.2, end = 0.8) +
  labs(title = "Intervention Timing by Contract Type",
       x = "Years from Claim to First Contract",
       y = "Density", fill = "Contract Type")

save_figure(p_intervention_by_type, "202_intervention_by_type")

# What predicts going to auction?
# CORRECTED: Use 'went_to_auction' to capture ALL competitive mechanisms
# (Bid-to-Result + Scope of Work + Competitive T&M)


# -----------------------------------------------------------------------------
# 1.x OLS Regression: Drivers of Competitive Auctions
# -----------------------------------------------------------------------------
message("\n--- 1.x OLS: Auction Drivers ---")

# 1. Define Data Scope (Paid Claims > $1k)
# Note: Auctions only happen on paid, large cleanup jobs.
auction_data <- master[total_paid_real > 1000]

# 2. Impute NA -> 0 for binary flags (Using logic from Denial code)
auction_data <- impute_human_na(auction_data, facility_vars_binary)
auction_data <- impute_human_na(auction_data, claim_vars_binary)

# 3. Build Formula (Using comprehensive variable list from Denial code)
ols_continuous <- c("avg_tank_age", "n_tanks_total", "reporting_lag_days", 
                    "prior_claims_n", "days_since_prior_claim",
                    "total_capacity_gal", "avg_tank_capacity_gal", "max_tank_capacity_gal")

vars_to_exclude <- c("has_noncompliant", "has_unknown_data", "has_non_compliant_tank", 
                     "Owner_Size_Class", "business_category", 'Region')
facility_vars_clean <- setdiff(facility_vars_binary, vars_to_exclude)

ols_formula_terms <- c(
  facility_vars_clean, ols_continuous, "is_repeat_filer",
  "has_tank_construction_unknown", "has_ug_piping_unknown", 
  "has_tank_release_detection_unknown"
)


# Intersect to ensure variables exist in the subset
ols_formula_terms <- intersect(ols_formula_terms, names(auction_data))

f_auction <- as.formula(paste(
  "went_to_auction ~", paste(ols_formula_terms, collapse = " + "), "| region_cluster + Year"
))

# 4. Estimate Model (Using Fixed Effects from formula + Cluster from Auction template)
auction_model <- feols(f_auction, data = auction_data, cluster = "dep_region")

# 5. Generate Table (Using filtering logic from Denial code, adapted labels)
ols_label_map <- create_display_labels(ols_formula_terms, feature_dict)
sig_results <- filter_to_significant(auction_model, threshold = 0.10, label_map = ols_label_map)

if (!is.null(sig_results) && nrow(sig_results) > 0) {
  auction_sig_tbl <- sig_results[, .(Label, Coef_Display, SE_Display, p_value)] %>%
    gt() %>%
    cols_label(Label = "Predictor", Coef_Display = "Effect on Auction Probability",
               SE_Display = "Std. Error", p_value = "P-Value") %>%
    fmt_number(columns = "p_value", decimals = 4) %>%
    tab_header(title = "What Predicts Competitive Auctions?",
               subtitle = "Linear Probability Model (Significant Marginal Effects)") %>%
    tab_style(style = cell_text(weight = "bold"), locations = cells_column_labels()) %>%
    tab_style(style = list(cell_fill(color = "#fadbd8")),
              locations = cells_body(columns = Coef_Display, rows = sig_results$Estimate > 0)) %>%
    tab_style(style = list(cell_fill(color = "#d5f5e3")),
              locations = cells_body(columns = Coef_Display, rows = sig_results$Estimate < 0)) %>%
    tab_footnote("Values represent percentage point change in auction probability.",
                 locations = cells_column_labels(columns = Coef_Display)) %>%
    tab_footnote("Controls: DEP Region and Year FE. Clustered SEs (DEP Region).", locations = cells_title()) %>%
    tab_source_note(paste0("Showing ", nrow(sig_results), " predictors with p < 0.10. ",
                           "* p<0.10, ** p<0.05, *** p<0.01"))
  
  save_table(auction_sig_tbl, "203_auction_predictors_lpm")
  saveRDS(auction_sig_tbl, here("output/tables/203_auction_predictors_lpm.rds"))
  message(sprintf("Saved: 203_auction_predictors_lpm (%d significant predictors)", nrow(sig_results)))
}


# ==============================================================================
# SECTION 2.2: COSTS OF WORK IN AUCTIONS
# ==============================================================================
message("\n--- 2.2 Cost Breakdown ---")

# How much of claim cost is contract work vs ALAE vs other loss?
cost_breakdown <- auction_claims[, .(
  claim_number,
  total_paid = total_paid_real,
  contract_value = total_contract_value_real,
  alae = paid_alae_real,
  loss = paid_loss_real
)]

cost_breakdown[, `:=`(
  contract_share = contract_value / total_paid,
  alae_share = alae / total_paid,
  loss_share = loss / total_paid,
  other = total_paid - contract_value - alae
)]

# Cap shares at 1 for display
cost_breakdown[contract_share > 1, contract_share := 1]
cost_breakdown[alae_share > 1, alae_share := 1]

cost_shares_summary <- cost_breakdown[, .(
  N = .N,
  Mean_Contract_Share = mean(contract_share, na.rm = TRUE),
  Median_Contract_Share = median(contract_share, na.rm = TRUE),
  Mean_ALAE_Share = mean(alae_share, na.rm = TRUE),
  Mean_Total_Paid = mean(total_paid, na.rm = TRUE),
  Mean_Contract_Value = mean(contract_value, na.rm = TRUE)
)]

cost_breakdown_tbl <- kbl(cost_shares_summary, digits = 2, format = "html",
                          caption = "Cost Component Breakdown (Auction Claims)") %>%
  kable_styling()

save_table(cost_breakdown_tbl, "204_cost_breakdown_summary")

# Scatter: Contract Value vs Total Paid
p_cost_scatter <- ggplot(auction_claims[total_contract_value_real > 0], 
                          aes(x = total_contract_value_real, y = total_paid_real)) +
  geom_point(alpha = 0.4, color = "#3498db") +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "#e74c3c") +
  scale_x_log10(labels = dollar_format()) +
  scale_y_log10(labels = dollar_format()) +
  labs(title = "Contract Value vs Total Claim Cost",
       subtitle = "Dashed line = 45° (Contract = Total Cost)",
       x = "Total Contract Value (Real $)",
       y = "Total Claim Cost (Real $)")

save_figure(p_cost_scatter, "204_contract_vs_total_scatter")

# ==============================================================================
# SECTION 2.3: PROCUREMENT DESIGN COMPARISON
# ==============================================================================
message("\n--- 2.3 Procurement Design Comparison ---")

# Descriptive comparison: PFP vs SOW vs Other
procurement_summary <- master[contract_type != "No Contract" & total_paid_real > 1000, .(
  N_Claims = .N,
  Mean_Cost_Real = mean(total_paid_real, na.rm = TRUE),
  Median_Cost_Real = median(total_paid_real, na.rm = TRUE),
  Mean_Duration_Days = mean(claim_duration_days, na.rm = TRUE),
  Median_Duration_Days = median(claim_duration_days, na.rm = TRUE),
  Mean_Intervention_Days = mean(intervention_lag_days, na.rm = TRUE),
  Mean_Tank_Age = mean(avg_tank_age, na.rm = TRUE)
), by = contract_type][order(-N_Claims)]

proc_tbl <- kbl(procurement_summary, digits = 0, format = "html",
                caption = "Procurement Design Comparison (Descriptive)") %>%
  kable_styling() %>%
  footnote(general = "WARNING: These are RAW comparisons. Selection bias likely present.")

save_table(proc_tbl, "205_procurement_comparison")

# Boxplot comparison
p_cost_by_type <- ggplot(master[ total_paid_real > 100],
                          aes(y = contract_type_detailed, x = total_paid_real, 
                              fill = auction_type_factor)) +
  geom_boxplot(outlier.alpha = 0.2) +
  scale_x_log10(labels = dollar_format()) +
scale_fill_viridis_d(option = "cividis", begin = 0.1, end = 0.9) +
  labs(title = "Total Cost by Contract Type",
       subtitle = "CAUTION: Selection bias - complex sites go to auction",
       y = NULL, x = "Total Cost (Real $, Log Scale)") +
  theme(legend.position = "none")

ggsave(filename = "205_cost_by_contract_type.png",
       plot = p_cost_by_type,
       width = 10,
       height = 5.625,
       units = "in",
       dpi = 300)

save_figure(p_cost_by_type, "205_cost_by_contract_type")

# Duration comparison
p_duration_by_type <- ggplot(master[
                                      claim_duration_days > 0 & 
                                      claim_duration_days < 10000],
                              aes(y = contract_type_detailed, x = claim_duration_days / 365,
                                  fill = auction_type_factor)) +
  geom_boxplot(outlier.alpha = 0.2) +
scale_fill_viridis_d(option = "cividis", begin = 0.1, end = 0.9) +
  labs(title = "Claim Duration by Contract Type",
       y = NULL, x = "Duration (Years)") +
  theme(legend.position = "none")

ggsave(filename = "206_duration_by_contract_type.png",
       plot = p_duration_by_type,
       width = 10,
       height = 5.625,
       units = "in",
       dpi = 300)


save_figure(p_duration_by_type, "206_duration_by_contract_type")

# -----------------------------------------------------------------------------
# 2.x OLS Regression: Naive Cost Drivers (Log Cost)
# -----------------------------------------------------------------------------
message("\n--- 2.x OLS: Naive Cost Drivers ---")

# 1. Define Data Scope (Paid Claims > $1k)
cost_data <- master[total_paid_real > 1000  ]

# 2. Impute NA -> 0 for binary flags
cost_data <- impute_human_na(cost_data, facility_vars_binary)
cost_data <- impute_human_na(cost_data, claim_vars_binary)
# Ensure variable is a factor first
cost_data[, contract_type_detailed := as.factor(contract_type_detailed)]

# Set Reference Level to "Sole Source (Time & Material)"
cost_data[, contract_type_detailed := relevel(contract_type_detailed, ref = "Sole Source (Time & Material)")]

# Verification: Check levels to confirm the first one is the reference
print(levels(cost_data$contract_type_detailed))
# 3. Build Formula
# Reuse standard continuous variables and clean binary flags
ols_continuous <- c("avg_tank_age", "n_tanks_total", "reporting_lag_days", 
                    "prior_claims_n", "days_since_prior_claim",
                    "total_capacity_gal", "avg_tank_capacity_gal", "max_tank_capacity_gal")

vars_to_exclude <- c("has_noncompliant", "has_unknown_data", "has_non_compliant_tank", 
                     "Owner_Size_Class", "business_category", 'Region')
facility_vars_clean <- setdiff(facility_vars_binary, vars_to_exclude)

# Add contract_type explicitly to the predictors
ols_formula_terms <- c(
  "contract_type_detailed", 
  facility_vars_clean, ols_continuous, "is_repeat_filer",
  "has_tank_construction_unknown", "has_ug_piping_unknown", 
  "has_tank_release_detection_unknown"
)

# Intersect to ensure variables exist
ols_formula_terms <- intersect(ols_formula_terms, names(cost_data))

# Formula: Log(Cost) ~ Predictors | Fixed Effects
f_cost <- as.formula(paste(
  "log(total_paid_real) ~", paste(ols_formula_terms, collapse = " + "), "| region_cluster + Year"
))

# 4. Estimate Model (Naive OLS)
# Note: Cluster by dep_region as per original snippet
cost_model <- feols(f_cost, data = cost_data, cluster = "dep_region")

# 5. Generate Table
ols_label_map <- create_display_labels(ols_formula_terms, feature_dict)

# Add specific labels for contract_type_detailed
# Note: These MUST match "variable_name" + "Level Text" exactly
ols_label_map <- rbind(ols_label_map, data.table(
  term = c(
    "contract_type_detailedScope of Work (Competitive Fixed Price)", 
    "contract_type_detailedBid-to-Result (Competitive Fixed Price)", 
    "contract_type_detailedOther/Legacy Contract",
    "contract_type_detailedSole Source (Fixed Price)",
    "contract_type_detailedSole Source (Pay for Performance)"
  ),
  label = c(
    "Contract: Scope of Work", 
    "Contract: Bid-to-Result", 
    "Contract: Other/Legacy",
    "Contract: Sole Source (Fixed)",
    "Contract: Pay for Performance"
  )
), fill = TRUE)

sig_results <- filter_to_significant(cost_model, threshold = 0.10, label_map = ols_label_map)

if (!is.null(sig_results) && nrow(sig_results) > 0) {
  cost_sig_tbl <- sig_results[, .(Label, Coef_Display, SE_Display, p_value)] %>%
    gt() %>%
    cols_label(Label = "Predictor", Coef_Display = "Effect on Log(Cost)",
               SE_Display = "Std. Error", p_value = "P-Value") %>%
    fmt_number(columns = "p_value", decimals = 4) %>%
    tab_header(title = "Correlates of Cleanup Cost (Naive OLS)",
               subtitle = "Dependent Variable: Log(Total Paid)") %>%
    tab_style(style = cell_text(weight = "bold"), locations = cells_column_labels()) %>%
    # Color coding: Red = Higher Cost, Green = Lower Cost
    tab_style(style = list(cell_fill(color = "#fadbd8")),
              locations = cells_body(columns = Coef_Display, rows = sig_results$Estimate > 0)) %>%
    tab_style(style = list(cell_fill(color = "#d5f5e3")),
              locations = cells_body(columns = Coef_Display, rows = sig_results$Estimate < 0)) %>%
    tab_footnote("CAUTION: Coefficients on contract type are NOT causal. Selection bias present.",
                 locations = cells_column_labels(columns = Coef_Display)) %>%
    tab_footnote("Controls: DEP Region and Year FE. Clustered SEs (DEP Region).", locations = cells_title()) %>%
    tab_source_note(paste0("Showing ", nrow(sig_results), " predictors with p < 0.10. ",
                           "* p<0.10, ** p<0.05, *** p<0.01"))
  
  save_table(cost_sig_tbl, "207_naive_cost_regression")
  saveRDS(cost_sig_tbl, here("output/tables/207_naive_cost_regression.rds"))
  message(sprintf("Saved: 207_naive_cost_regression (%d significant predictors)", nrow(sig_results)))
}

# ==============================================================================
# SECTION 2.4: CONTRACT TRENDS OVER TIME
# ==============================================================================
message("\n--- 2.4 Contract Trends ---")

# Contract type usage over time
contract_trends <- contracts[!is.na(Year) & Year >= 2000 & Year <= 2024, .(
  N_Contracts = .N,
  Total_Value = sum(total_contract_value_real, na.rm = TRUE)
), by = .(Year, auction_type)]

contract_trends_wide <- dcast(contract_trends, Year ~ auction_type, 
                               value.var = "N_Contracts", fill = 0)

# Stacked area plot
p_contract_trend <- ggplot(contract_trends[!is.na(auction_type)], 
                            aes(x = Year, y = N_Contracts, fill = auction_type)) +
  geom_area(alpha = 0.8) +
  scale_fill_viridis_d(option = "mako", begin = 0.2, end = 0.9) +
  labs(title = "Contract Types Over Time",
       x = "Year", y = "Number of Contracts", fill = "Auction Type")

save_figure(p_contract_trend, "208_contract_trends_stacked")

# Line plot
p_contract_lines <- ggplot(contract_trends[!is.na(auction_type) & auction_type != ""], 
                            aes(x = Year, y = N_Contracts, color = auction_type)) +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  scale_color_viridis_d(option = "mako", begin = 0.2, end = 0.9) +
  labs(title = "Contract Types Over Time",
       x = "Year", y = "Number of Contracts", color = "Auction Type")

save_figure(p_contract_lines, "209_contract_trends_lines")

# PFP share over time
pfp_share_trend <- contracts[Year >= 2000 & Year <= 2024, .(
  N_Total = .N,
  N_PFP = sum(auction_type == "Bid-to-Result", na.rm = TRUE),
  Share_PFP = mean(auction_type == "Bid-to-Result", na.rm = TRUE)
), by = Year]

p_pfp_share <- ggplot(pfp_share_trend, aes(x = Year, y = Share_PFP)) +
  geom_line(color = "#e74c3c", linewidth = 1) +
  geom_point(color = "#e74c3c", size = 2) +
  scale_y_continuous(labels = percent_format()) +
  labs(title = "Share of Bid-to-Result (PFP) Contracts Over Time",
       x = "Year", y = "Share PFP")

save_figure(p_pfp_share, "210_pfp_share_trend")

# ==============================================================================
# SELECTION BIAS DIAGNOSTIC
# ==============================================================================
message("\n--- Selection Bias Diagnostic ---")

# Compare characteristics: Auction vs Non-Auction claims
selection_comparison <- master[total_paid_real > 1000, .(
  N = .N,
  Mean_Cost = mean(total_paid_real, na.rm = TRUE),
  Mean_Age = mean(avg_tank_age, na.rm = TRUE),
  Mean_Tanks = mean(n_tanks_total, na.rm = TRUE),
  Share_BareSteel = mean(share_bare_steel, na.rm = TRUE),
  Mean_Duration = mean(claim_duration_days, na.rm = TRUE)
), by = .(Has_Auction = contract_type != "No Contract")]

selection_tbl <- kbl(selection_comparison, digits = 2, format = "html",
                     caption = "Selection Bias Evidence: Auction vs Non-Auction Claims") %>%
  kable_styling() %>%
  footnote(general = "Auction claims are systematically different - older tanks, higher costs")

save_table(selection_tbl, "211_selection_bias_evidence")


# ==============================================================================
# SECTION 2.5: HUMAN INTUITION VS. ML DISCOVERY (Cost Prediction)
# ==============================================================================
message("\n--- 2.5 Human vs. ML Cost Prediction ---")

# Filter to eligible claims with positive costs
cost_analysis_set <- master[
  status_group %in% c("Eligible", "Post Remedial") &
  total_paid_real > 1000 &
  !is.na(dep_region)
]

# Create log cost target
cost_analysis_set[, log_cost := log(total_paid_real + 1)]

# Run comparison
comparison_results <- run_human_vs_ml_comparison(
  dt = cost_analysis_set,
  outcome_var = "log_cost",
  cluster_var = "dep_region",
  output_prefix = "cost"
)

# Save outputs
save_figure(comparison_results$plot_comparison, "212_human_vs_ml_lift_chart")
save_figure(comparison_results$plot_importance, "213_ml_variable_importance")

# Executive summary table
exec_tbl <- create_executive_comparison_table(
  comparison_results$metrics,
  comparison_results$lift_r2_pct,
  comparison_results$lift_rmse_pct
)
save_table(exec_tbl, "214_executive_model_comparison")

# Print summary
message(sprintf("\n  ML LIFT: +%.1f%% variance explained over human intuition",
                comparison_results$lift_r2_pct))

# ==============================================================================
# SECTION 2.7: MARKET STRUCTURE & HHI ANALYSIS (UPDATED)
# ==============================================================================
message("\n", strrep("=", 70))
message("SECTION 2.7: MARKET CONCENTRATION (HHI) & TRENDS")
message(strrep("=", 70))


hhi_dt = merge.data.table(contracts,master[,.(claim_number,contract_type_detailed)])
# ------------------------------------------------------------------------------
# 1. PREPARE DATA & CLEAN NAMES
# ------------------------------------------------------------------------------
hhi_dt <- hhi_dt[!is.na(consultant) & consultant != "" & 
                    total_contract_value_real > 0 & 
                    !is.na(contract_type_detailed)]

# Robust Name Cleaning
hhi_dt[, consultant_clean := str_squish(str_remove_all(str_to_lower(consultant), "[.,]|\\b(llc|inc|pa|ltd|corp)\\b"))]

# ------------------------------------------------------------------------------
# 2. ENHANCED HHI SUMMARY TABLE
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# 2. REVISED HHI SUMMARY TABLE (COUNT-BASED)
# ------------------------------------------------------------------------------

# Step 1: Calculate win counts per firm per contract type
firm_wins_dt <- hhi_dt[, .(
  n_wins = .N,
  total_val = sum(total_contract_value_real),
  avg_dur = mean(claim_duration_days, na.rm = TRUE)
), by = .(contract_type_detailed, consultant_clean)]

# Step 2: Calculate HHI based on win-share (count share)
hhi_summary <- firm_wins_dt[, .(
  HHI = sum((n_wins / sum(n_wins))^2) * 10000,
  N_Firms = .N,
  N_Contracts = sum(n_wins),
  Avg_Contract_Cost = sum(total_val) / sum(n_wins),
  Avg_Claim_Duration = mean(avg_dur, na.rm = TRUE),
  Top_Firm_Win_Share = max(n_wins / sum(n_wins)),
  Total_Market_Vol = sum(total_val)
), by = contract_type_detailed][order(HHI)]

# Step 3: Apply Concentration Thresholds
hhi_summary[, Concentration := fcase(
  HHI < 1500, "Competitive",
  HHI < 2500, "Moderately Concentrated",
  default = "Highly Concentrated"
)]

# Table Output
hhi_tbl <- hhi_summary %>%
  gt() %>%
  cols_label(
    contract_type_detailed = "Contract Type",
    HHI = "HHI Score",
    N_Firms = "Active Firms",
    N_Contracts = "Total Contracts",
    Avg_Contract_Cost = "Avg Contract $",
    Avg_Claim_Duration = "Avg Duration (Days)",
    Concentration = "Concentration Status"
  ) %>%
  fmt_number(columns = HHI, decimals = 0) %>%
  fmt_currency(columns = c(Avg_Contract_Cost, Total_Market_Vol), decimals = 0) %>%
  fmt_number(columns = Avg_Claim_Duration, decimals = 1) %>%
  fmt_percent(columns = Top_Firm_Share, decimals = 1) %>%
  tab_header(title = "Market Structure & Efficacy Metrics by Contract Type") %>%
  tab_style(style = cell_text(weight = "bold"), locations = cells_body(columns = HHI)) %>%
  tab_style(style = cell_fill(color = "#fadbd8"), locations = cells_body(rows = HHI > 2500))

save_table(hhi_tbl, "212_enhanced_hhi_summary")

# ------------------------------------------------------------------------------
# 3. HHI CONCENTRATION OVER TIME (TREND ANALYSIS)
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# 1. REVISED AGGREGATION: HHI BY CONTRACT COUNT (OVER TIME)
# ------------------------------------------------------------------------------

# Step A: Aggregate to Firm-Year level to get win counts
firm_year_stats <- hhi_dt[Year >= 1994 & Year <= 2024, .(
  n_wins = .N,
  total_val = sum(total_contract_value_real),
  claim_dur_total = sum(claim_duration_days / 365, na.rm = TRUE)
), by = .(contract_type_detailed, Year, consultant_clean)]

# Step B: Calculate HHI and Market Metrics by Type and Year
hhi_time <- firm_year_stats[, .(
  HHI = sum((n_wins / sum(n_wins))^2) * 10000,
  N_Firms = .N,
  N_Contracts = sum(n_wins),
  Avg_Contract_Cost = sum(total_val) / sum(n_wins),
  Avg_Claim_Duration = sum(claim_dur_total) / sum(n_wins),
  Top_Firm_Share = max(n_wins / sum(n_wins)),
  Total_Market_Vol = sum(total_val)
), by = .(contract_type_detailed, Year)]

# Apply Concentration Thresholds based on Count-HHI
hhi_time[, Concentration := fcase(
  HHI < 1500, "Competitive",
  HHI < 2500, "Moderately Concentrated",
  default = "Highly Concentrated"
)]

# ------------------------------------------------------------------------------
# 2. YEARLY COMPETITION SUMMARY (MEAN/SD)
# ------------------------------------------------------------------------------

# Collapse the 'Year' dimension to find average performance over time
hhi_stats_msd <- hhi_time[, .(
  HHI_m = mean(HHI, na.rm = TRUE), HHI_sd = sd(HHI, na.rm = TRUE),
  Firms_m = mean(N_Firms, na.rm = TRUE), Firms_sd = sd(N_Firms, na.rm = TRUE),
  Contracts_m = mean(N_Contracts, na.rm = TRUE), Contracts_sd = sd(N_Contracts, na.rm = TRUE),
  Cost_m = mean(Avg_Contract_Cost, na.rm = TRUE), Cost_sd = sd(Avg_Contract_Cost, na.rm = TRUE),
  Duration_m = mean(Avg_Claim_Duration, na.rm = TRUE), Duration_sd = sd(Avg_Claim_Duration, na.rm = TRUE),
  Share_m = mean(Top_Firm_Share, na.rm = TRUE), Share_sd = sd(Top_Firm_Share, na.rm = TRUE)
), by = contract_type_detailed]

# Helper for Mean (SD) formatting
fmt_msd <- function(m, s, is_currency = FALSE, is_pct = FALSE) {
  s[is.na(s)] <- 0 
  if (is_currency) return(sprintf("$%s ($%s)", format(round(m), big.mark=","), format(round(s), big.mark=",")))
  if (is_pct) return(sprintf("%.1f%% (%.1f%%)", m * 100, s * 100))
  return(sprintf("%.1f (%.1f)", m, s))
}

# Create Display Columns
hhi_stats_msd[, `:=`(
  `HHI Score`          = sprintf("%.0f (%.0f)", HHI_m, HHI_sd),
  `Active Firms`       = fmt_msd(Firms_m, Firms_sd),
  `Yearly Contracts`   = fmt_msd(Contracts_m, Contracts_sd),
  `Avg Contract Cost`  = fmt_msd(Cost_m, Cost_sd, is_currency = TRUE),
  `Avg Duration (Years)`= fmt_msd(Duration_m, Duration_sd),
  `Top Firm Win Share` = fmt_msd(Share_m, Share_sd, is_pct = TRUE)
)]

# ------------------------------------------------------------------------------
# REVISED GT TABLE WITH COLOR MAPPING (DOJ/FTC/FCC STANDARDS)
# ------------------------------------------------------------------------------
# ==============================================================================
# SECTION 2.7.3: NON-TECHNICAL MARKET SUMMARY TABLE
# ==============================================================================

hhi_msd_summary_tbl <- hhi_stats_msd[, .(
  contract_type_detailed, `HHI Score`, `Active Firms`, 
  `Yearly Contracts`, `Avg Contract Cost`, `Avg Duration (Years)`, 
  `Top Firm Win Share`, HHI_m # Retained HHI_m for styling logic
)] %>%
  gt() %>%
  tab_header(
    title = "Market Competition Summary: Annual Averages (1994-2024)",
    subtitle = "Metrics based on Contract Counts (Win Frequency) rather than Revenue"
  ) %>%
  cols_label(contract_type_detailed = "Contract Type") %>%
  cols_hide(columns = HHI_m) %>% # Hide helper column from final view
  
  # Apply Risk Level Color Mapping
  tab_style(
    style = cell_fill(color = "#ffcccc"), # Red: Highly Concentrated
    locations = cells_body(columns = `HHI Score`, rows = HHI_m >= 2500)
  ) %>%
  tab_style(
    style = cell_fill(color = "#ffffcc"), # Yellow: Moderately Concentrated
    locations = cells_body(columns = `HHI Score`, rows = HHI_m >= 1500 & HHI_m < 2500)
  ) %>%
  tab_style(
    style = cell_fill(color = "#ccffcc"), # Green: Competitive
    locations = cells_body(columns = `HHI Score`, rows = HHI_m < 1500)
  ) %>%
  
  # Standard Formatting (Bold Headers)
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_labels()
  ) %>%
  
  # ENHANCED SOURCE NOTE FOR NON-TECHNICAL AUDIENCE
  tab_source_note(
    source_note = html("
      <div style='text-align: left;'>
      <strong>Guide to Metrics:</strong><br>
      • <strong>Format:</strong> Values are shown as <em>Average (Fluctuation/SD)</em>. Large numbers in parentheses indicate high year-to-year instability.<br>
      • <strong>HHI Score:</strong> Market Concentration (0 to 10,000). Higher scores = Less Competition.<br>
      • <strong>Active Firms:</strong> The average number of unique vendors winning at least one contract in a typical year.<br>
      • <strong>Yearly Contracts:</strong> Average annual volume. <em><strong>Note:</strong> In years with very low volume (e.g., 1-3 contracts), HHI scores will mechanically show 'High Concentration' regardless of actual market health.</em><br>
      • <strong>Top Firm Win Share:</strong> The percentage of contracts won by the single leading vendor in a typical year.<br>
      <br>
      <strong>Risk Thresholds:</strong> <span style='background-color:#ccffcc'>Green</span> < 1500 (Competitive); <span style='background-color:#ffffcc'>Yellow</span> 1500-2500 (Moderate); <span style='background-color:#ffcccc'>Red</span> > 2500 (Highly Concentrated).
      </div>
    ")
  ) %>%
  tab_options(table.font.size = px(14))

# Save Table
save_table(hhi_msd_summary_tbl, "215_yearly_competition_summary_msd")


# ==============================================================================
# SECTION 2.7.1: MARKET CONCENTRATION BUBBLE PLOT (COUNT-BASED)
# ==============================================================================
message("\n", strrep("=", 70))
message("SECTION 2.7.1: HHI CONCENTRATION BUBBLE ANALYSIS (BY WIN COUNT)")
message(strrep("=", 70))

# 1. Prepare Annual HHI Data based on win counts
# First, aggregate to firm-year to get individual counts per vendor
firm_year_counts <- hhi_dt[, .(n_wins = .N), by = .(contract_type_detailed, Year, consultant_clean)]

# Second, calculate HHI based on n_wins share
hhi_bubble_dt <- firm_year_counts[, .(
  HHI = sum((n_wins / sum(n_wins))^2) * 10000,
  Total_Contracts = sum(n_wins),
  N_Firms = .N
), by = .(contract_type_detailed, Year)]

# 2. Add Risk Categorization for Color (Using FCC/DOJ Thresholds)
hhi_bubble_dt[, Market_Risk := fcase(
  HHI < 1500, "Low (Competitive)",
  HHI < 2500, "Moderate",
  default = "High (Concentrated)"
)]


# ==============================================================================
# SECTION 2.7.2: REFACTORED BUBBLE MAP WITH INTERNAL LABELS
# ==============================================================================

p_hhi_bubble_final <- ggplot(hhi_bubble_dt, 
                             aes(x = Year, y = contract_type_detailed, 
                                 size = HHI, color = Market_Risk)) +
  geom_hline(aes(yintercept = contract_type_detailed), color = "gray90", linetype = "dotted") +
  # Primary bubble layer
  geom_point(alpha = 0.6) +
  # Add black text labels inside the dots
  geom_text(aes(label = Total_Contracts), 
            color = "black", 
            size = 3, 
            fontface = "bold",
            show.legend = FALSE) + 
  
  scale_size_continuous(range = c(5, 20), guide = "none") + 
  scale_color_manual(values = c("Low (Competitive)" = "#2ecc71", 
                                "Moderate" = "#f1c40f", 
                                "High (Concentrated)" = "#e74c3c")) +
  scale_x_continuous(breaks = seq(1994, 2024, 2)) +
  labs(title = "Market Concentration Map (HHI Bubble by Win Count)",
       subtitle = "Numbers inside bubbles indicate total contract count for that year/type.",
       x = "Contract Year", y = NULL, color = "FCC/DOJ Market Risk Status:") +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "bottom",
    legend.box = "horizontal",
    panel.grid.major.y = element_blank(),
    plot.title = element_text(face = "bold", size = 16),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

# Export updated visualization
ggsave(
  filename = here("output/figures/214_hhi_bubble_map_labeled.png"),
  plot = p_hhi_bubble_final,
  width = 14,
  height = 8,
  units = "in",
  dpi = 300,
  bg = "white"
)


message("Saved: 214_hhi_bubble_map_optimized.png (Count-based HHI, Legend at bottom)")


# ==============================================================================
# SECTION 2.7.4: ENHANCED ENTRENCHMENT (CONTEXTUAL WINNERS)
# ==============================================================================

# 1. Firm-level stats (Market Leader identification)
global_leaders <- hhi_dt[, .(
  Champ_Contracts = .N,
  Champ_Vol = sum(total_contract_value_real),
  Avg_Champ_Cost = mean(total_contract_value_real),
  Years_Active = uniqueN(Year)
), by = .(contract_type_detailed, Champ_Name = consultant_clean)]

# 2. Identify Champion by Win Count
market_champs <- global_leaders[order(-Champ_Contracts, -Champ_Vol), head(.SD, 1), by = contract_type_detailed]

# 3. Market-level totals for context
market_totals <- hhi_dt[, .(
  Total_Market_Contracts = .N,
  Total_Market_Vol = sum(total_contract_value_real),
  Avg_Market_Cost = mean(total_contract_value_real),
  Global_Active_Years = uniqueN(Year)
), by = contract_type_detailed]

# 4. Yearly Champ logic for Entrenchment Rate
yearly_champs <- hhi_dt[, .(Yearly_Wins = .N), by = .(contract_type_detailed, Year, consultant_clean)
                        ][order(-Yearly_Wins), head(.SD, 1), by = .(contract_type_detailed, Year)]

# 5. Merge and calculate metrics
persistence_dt <- merge(market_champs, market_totals, by = "contract_type_detailed")

persistence_dt[, Years_As_No1 := sapply(1:.N, function(i) {
  nrow(yearly_champs[contract_type_detailed == persistence_dt$contract_type_detailed[i] & 
                     consultant_clean == persistence_dt$Champ_Name[i]])
})]

persistence_dt[, `:=`(
  Entrenchment_Rate = Years_As_No1 / Global_Active_Years,
  Market_Presence   = Years_Active / Global_Active_Years,
  Win_Share         = Champ_Contracts / Total_Market_Contracts,
  Cost_Ratio        = Avg_Champ_Cost / Avg_Market_Cost
)]

# ==============================================================================
# SECTION 2.7.4: REFACTORED PERSISTENCE TABLE WITH DEFINITIONS
# ==============================================================================

persistence_tbl_v2 <- persistence_dt[, .(
  contract_type_detailed, Champ_Name, Total_Market_Contracts,
  Entrenchment_Rate, Market_Presence, Win_Share, Cost_Ratio
)] %>%
  gt() %>%
  tab_header(
    title = "Firm Entrenchment & Market Context",
    subtitle = "Contextualizing 'Winners' by Market Size and Cost Behavior (1994-2024)"
  ) %>%
  cols_label(
    contract_type_detailed = "Contract Type",
    Champ_Name = "Market Leader",
    Total_Market_Contracts = "Total Contracts (N)",
    Entrenchment_Rate = "Entrenchment Rate",
    Market_Presence = "Market Breadth",
    Win_Share = "Win Share",
    Cost_Ratio = "Relative Cost"
  ) %>%
  fmt_percent(columns = c(Entrenchment_Rate, Market_Presence, Win_Share), decimals = 1) %>%
  fmt_number(columns = Cost_Ratio, decimals = 2) %>%
  # Highlight high-entrenchment and high-cost leaders
  tab_style(
    style = cell_fill(color = "#fadbd8"),
    locations = cells_body(columns = Entrenchment_Rate, rows = Entrenchment_Rate > 0.50)
  ) %>%
  # ADDING CLEARER DESCRIPTIONS IN THE NOTE
  tab_source_note(
    source_note = html("
      <strong>Column Definitions:</strong><br>
      • <strong>Market Leader:</strong> The firm with the highest total number of contract awards (wins) within the category.<br>
      • <strong>Total Contracts (N):</strong> The absolute count of awards in that category; provides the scale for the HHI bubbles.<br>
      • <strong>Entrenchment Rate:</strong> The percentage of years where the Market Leader held the #1 spot for annual win count.<br>
      • <strong>Market Breadth:</strong> The percentage of years the firm was active (won at least one contract) out of all years the category existed.<br>
      • <strong>Win Share:</strong> The firm's lifetime percentage of total contracts awarded in that category.<br>
      • <strong>Relative Cost:</strong> (Winner's Avg Cost / Market Avg Cost). Values > 1.00 indicate the leader wins more expensive/complex sites than average.
    ")
  )

save_table(persistence_tbl_v2, "216_enhanced_entrenchment_final")


# ==============================================================================
# SETUP & LIBRARIES
# ==============================================================================
library(data.table)
library(ggplot2)
library(sf)
library(maps)
library(scales)

# Standardized Theme
theme_map_clean <- theme_void() +
  theme(
    legend.position = "bottom",
    legend.title = element_text(face = "bold", size = 10),
    legend.text = element_text(size = 9),
    legend.key.width = unit(1.5, "cm"),
    plot.title = element_text(face = "bold", hjust = 0.5, size = 14)
  )

# ==============================================================================
# DATA PREPARATION: SPATIAL (PA DEP REGIONS)
# ==============================================================================
# 1. Define DEP Region Mapping
dep_regions_lut <- data.table(
  region = c(rep("Southeast", 5), rep("Northeast", 11), rep("Southcentral", 15), 
             rep("Northcentral", 14), rep("Southwest", 10), rep("Northwest", 12)),
  subregion = tolower(c(
    "Bucks", "Chester", "Delaware", "Montgomery", "Philadelphia",
    "Carbon", "Lackawanna", "Lehigh", "Luzerne", "Monroe", "Northampton", "Pike", "Schuylkill", "Susquehanna", "Wayne", "Wyoming",
    "Adams", "Bedford", "Berks", "Blair", "Cumberland", "Dauphin", "Franklin", "Fulton", "Huntingdon", "Juniata", "Lancaster", "Lebanon", "Mifflin", "Perry", "York",
    "Bradford", "Cameron", "Centre", "Clearfield", "Clinton", "Columbia", "Lycoming", "Montour", "Northumberland", "Potter", "Snyder", "Sullivan", "Tioga", "Union",
    "Allegheny", "Armstrong", "Beaver", "Cambria", "Fayette", "Greene", "Indiana", "Somerset", "Washington", "Westmoreland",
    "Butler", "Clarion", "Crawford", "Elk", "Erie", "Forest", "Jefferson", "Lawrence", "McKean", "Mercer", "Venango", "Warren"
  ))
)

# 2. Construct SF Objects
pa_map_data <- map("county", "pennsylvania", plot = FALSE, fill = TRUE)
pa_counties_sf <- st_as_sf(pa_map_data)

# Extract county names (ID format is "pennsylvania,bucks")
pa_counties_sf$subregion <- sapply(strsplit(pa_counties_sf$ID, ","), function(x) x[2])

# Merge Region info
pa_counties_sf <- merge(pa_counties_sf, dep_regions_lut, by = "subregion", all.x = TRUE)

# 3. Dissolve Region Boundaries
pa_regions_sf <- aggregate(
  x = st_geometry(pa_counties_sf),
  by = list(region = pa_counties_sf$region), 
  FUN = st_union
)
pa_regions_sf <- st_sf(pa_regions_sf)

# ==============================================================================
# SECTION 2: HHI MAPPING (COUNT-BASED)
# ==============================================================================
message(">>> EXECUTING: Section 2 (Count-Based HHI Analysis)")

# 2.1 Clean & Prepare Data
# Create a lower-case county key for matching with map data
hhi_dt[, county_clean := tolower(county)]

# 2.2 Calculate HHI based on WIN COUNTS (.N)
# We aggregate by county and consultant to count how many contracts each firm won
county_hhi_data <- hhi_dt[
  !is.na(consultant_clean) & consultant_clean != "" & !is.na(county_clean), 
  .(n_wins = .N), # <--- CHANGED FROM sum(value) TO .N
  by = .(county_clean, consultant_clean)
]

# Calculate Market Shares (Share of Wins) and HHI
county_hhi_data[, share := n_wins / sum(n_wins), by = county_clean]

# Sum squared shares to get HHI per county
county_hhi_final <- county_hhi_data[, .(
  HHI = sum(share^2) * 10000,
  Total_Contracts = sum(n_wins)
), by = county_clean]

# 2.3 Merge to Spatial Data
map_data_hhi <- merge(pa_counties_sf, county_hhi_final, 
                      by.x = "subregion", by.y = "county_clean", 
                      all.x = TRUE)

# ==============================================================================
# VISUALIZATION: HHI MAP
# ==============================================================================
p_hhi_map <- ggplot() +
  # A. HHI Fill (White to Black Gradient)
  geom_sf(data = map_data_hhi, aes(fill = HHI), color = NA) +
  
  # B. County Borders (Light Gray)
  geom_sf(data = map_data_hhi, fill = NA, color = "gray90", linewidth = 0.1) +
  
  # C. Region Outline Overlay (Colored Borders)
  geom_sf(data = pa_regions_sf, aes(color = region), fill = NA, linewidth = 0.8) +
  
  # D. Scales & Labels
  scale_fill_gradient(
    name = "HHI (Win Count)", 
    low = "white", 
    high = "black", 
    na.value = "#ecf0f1",
    limits = c(0, 10000)
  ) +
  scale_color_brewer(name = "DEP Region", palette = "Set1") +
  labs(
    title = "Consultant Market Concentration (HHI by Win Count)",
    subtitle = "Darker counties indicate fewer firms winning the majority of contracts",
    caption = "HHI calculated using number of won contracts (Share of Access)"
  ) +
  theme_map_clean

# Print and Save
print(p_hhi_map)
 save_plot_pub(p_hhi_map, "301_hhi_county_map_counts", width = 10, height = 7)

# ------------------------------------------------------------------------------
# 4. EXPLANATORY TEXT FOR THE BUBBLE MAP
# ------------------------------------------------------------------------------
# "Visualizing the 'Pulse' of the Market"
# This chart reveals that while Bid-to-Result (PFP) auctions are often 
# 'Highly Concentrated' (Large Red Bubbles), this is frequently due to low 
# contract volume in specific years rather than systemic monopoly. 
# In contrast, SOW (Competitive) auctions show smaller, greener bubbles, 
# indicating a more robust and consistent competitive vendor base.

# ==============================================================================
# SUMMARY
# ==============================================================================
message("\n========================================")
message("ANALYSIS 02 COMPLETE: Auction Mechanics")
message("========================================")
message("KEY FINDINGS (DESCRIPTIVE ONLY):")
message(sprintf("  - Median Intervention Lag: %.1f years", 
                median(auction_claims$intervention_lag_days, na.rm = TRUE) / 365))
message(sprintf("  - PFP Claims: N=%d, Mean Cost=$%s",
                sum(master$contract_type == "Bid-to-Result"),
                format(mean(master[contract_type == "Bid-to-Result", total_paid_real], na.rm = TRUE), 
                       big.mark = ",", nsmall = 0)))
message("\nCAUTION: All comparisons subject to selection bias.")
message("See 03_causal_inference.R for IV/DML treatment effects.")
