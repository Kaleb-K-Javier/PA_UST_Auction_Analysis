# R/analysis/01_descriptive_stats.R
# ==============================================================================
# Pennsylvania UST Analysis - Descriptive Statistics
# ==============================================================================
# PURPOSE: Characterize the claim universe and market structure
#
# SECTIONS (from 1/16 Policy Brief Goals):
#   1) BROAD CLAIMS ANALYSIS
#      1.1) Correlates of Eligibility (Denied vs Withdrawn vs Eligible)
#      1.2) Denied Claims and ALAE ("Phantom Spend")
#      1.3) Closed Claims: Cost Drivers, Closing Time, Reporting Lag
#      1.4) Remediation Trends (Costs & Durations Over Time)
#      1.5) Spatial/Temporal Visualization (Maps, Time Series)
#
#   3) MARKET CONCENTRATION
#      3.1) Consultant Concentration (HHI by Region)
#      3.2) Adjuster & Auction Concentration
#
# OUTPUTS:
#   - Tables: output/tables/1XX_*.{html,tex}
#   - Figures: output/figures/1XX_*.{png,pdf}
# ==============================================================================



suppressPackageStartupMessages({
  library(data.table)
  library(fixest)
  library(modelsummary)
  library(kableExtra)
  library(janitor)
  library(stringr)
  library(ggplot2)
  library(scales)
  library(patchwork)
  library(here)
})

source(here("R/functions/style_guide.R"))
source(here("R/functions/save_utils.R"))

# ==============================================================================
# 0. SETUP
# ==============================================================================
paths <- list(
  master   = here("data/processed/master_analysis_dataset.rds"),
  contracts = here("data/processed/contracts_with_real_values.rds"),
  claims = here('data/processed/claims'),
  tables   = here("output/tables"),
  figures  = here("output/figures")
)

dir.create(paths$tables, recursive = TRUE, showWarnings = FALSE)
dir.create(paths$figures, recursive = TRUE, showWarnings = FALSE)

# Output helpers
save_table <- function(tbl, name) {
  writeLines(as.character(tbl), file.path(paths$tables, paste0(name, ".html")))
  message(sprintf("Saved: %s.html", name))
}

save_figure <- function(p, name, width = 9, height = 6) {
  p_clean <- p + 
    theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold", size = 14),
      axis.title = element_text(face = "bold"),
      legend.position = "bottom",
      panel.grid.minor = element_blank()
    )
  ggsave(file.path(paths$figures, paste0(name, ".png")), p_clean, width = width, height = height, bg = "white")
  ggsave(file.path(paths$figures, paste0(name, ".pdf")), p_clean, width = width, height = height, bg = "white")
  message(sprintf("Saved: %s.{png,pdf}", name))
}

# ==============================================================================
# 1. LOAD DATA
# ==============================================================================
message("\n--- Loading Data ---")

master <- readRDS(paths$master)
setDT(master)

contracts <- readRDS(paths$contracts)
setDT(contracts)



# Merge region to contracts for HHI
contracts <- merge(contracts, 
                   master[, .(claim_number, dep_region, county)], 
                   by = "claim_number", all.x = TRUE)

message(sprintf("Master: %d claims | Contracts: %d records", nrow(master), nrow(contracts)))

# ==============================================================================
# SECTION 1.1: CORRELATES OF ELIGIBILITY
# ==============================================================================
message("\n--- 1.1 Correlates of Eligibility ---")

## Check status counts
print(master[, .N, by = status_group])

# Create analysis groups
master[, eligibility_group := fcase(
  status_group == "Denied", "Denied",
  status_group == "Withdrawn", "Withdrawn",
  status_group %in% c("Eligible", "Post Remedial", "Open"), "Eligible/Active",
  default = "Other"
)]

# -------------------------------------------------------------------------
# 1.1a Comprehensive Profile (Facilities + Costs + Regulatory Flags)
# -------------------------------------------------------------------------

# Define variables to analyze
# 1. Facility Characteristics (The Physical Site)
fac_vars <- c("n_tanks_total", "avg_tank_age_at_claim", "paid_alae_real", "claim_open_days")

# 2. Human Regulatory Flags (The Checklist)
reg_vars <- c("has_bare_steel", "has_single_walled", "has_pressure_piping", 
              "has_noncompliant_pa", "has_unknown_material", "is_repeat_filer")

analysis_vars <- c(fac_vars, reg_vars)

# Summarize by group
risk_summary <- master[eligibility_group %in% c("Denied", "Eligible/Active"), 
                       lapply(.SD, mean, na.rm = TRUE), 
                       by = eligibility_group, 
                       .SDcols = analysis_vars]

# Transpose for better readability
risk_summary_long <- melt(risk_summary, id.vars = "eligibility_group", 
                          variable.name = "Metric", value.name = "Mean_Value")
risk_summary_wide <- dcast(risk_summary_long, Metric ~ eligibility_group, value.var = "Mean_Value")

# Calculate difference
risk_summary_wide[, Diff_Pct := (`Denied` - `Eligible/Active`) / `Eligible/Active`]

# Format the table
human_risk_tbl <- kbl(risk_summary_wide, digits = 3, format = "html",
                      caption = "Claim Characteristics: Denied vs. Eligible") %>%
  kable_styling(bootstrap_options = c("striped", "hover")) %>%
  pack_rows("Facility & Cost Metrics", 1, 4) %>%
  pack_rows("Regulatory Risk Flags", 5, 10)

save_table(human_risk_tbl, "101_eligibility_profile")

# -------------------------------------------------------------------------
# 1.1b Formal Balance Test (T-Tests)
# -------------------------------------------------------------------------
bal_data <- master[eligibility_group %in% c("Denied", "Eligible/Active"), 
                   c("eligibility_group", analysis_vars), with = FALSE]

balance_results <- lapply(analysis_vars, function(v) {
  denied <- bal_data[eligibility_group == "Denied", get(v)]
  eligible <- bal_data[eligibility_group == "Eligible/Active", get(v)]
  
  # Handle potential all-NA cases
  if(all(is.na(denied)) || all(is.na(eligible))) return(NULL)
  
  tt <- t.test(denied, eligible)
  data.table(
    Variable = v,
    Mean_Denied = mean(denied, na.rm = TRUE),
    Mean_Eligible = mean(eligible, na.rm = TRUE),
    Difference = mean(denied, na.rm = TRUE) - mean(eligible, na.rm = TRUE),
    P_Value = tt$p.value
  )
})

balance_dt <- rbindlist(balance_results)

bal_tbl <- kbl(balance_dt, digits = 3, format = "html",
               caption = "Balance Test: Denied vs Eligible Claims") %>%
  kable_styling() %>%
  column_spec(5, color = ifelse(balance_dt$P_Value < 0.05, "red", "black"), 
              bold = ifelse(balance_dt$P_Value < 0.05, TRUE, FALSE))

save_table(bal_tbl, "102_eligibility_balance_test")

# ==============================================================================
# SECTION 1.2: DENIED CLAIMS AND ALAE ("Phantom Spend")
# ==============================================================================
message("\n--- 1.2 Denied Claims and ALAE ---")

# Total "Phantom Spend" on denied claims
denied_claims <- master[status_group == "Denied"]

phantom_spend <- denied_claims[, .(
  N_Claims = .N,
  Total_ALAE_Real = sum(paid_alae_real, na.rm = TRUE),
  Mean_ALAE_Real = mean(paid_alae_real, na.rm = TRUE),
  Median_ALAE_Real = median(paid_alae_real, na.rm = TRUE),
  Total_Loss_Real = sum(paid_loss_real, na.rm = TRUE),
  Mean_Duration_Days = mean(claim_duration_days, na.rm = TRUE)
)]

message(sprintf("PHANTOM SPEND: $%s in ALAE on %d denied claims",
                format(phantom_spend$Total_ALAE_Real, big.mark = ",", nsmall = 0),
                phantom_spend$N_Claims))

# Breakdown by owner type
phantom_by_owner <- denied_claims[!is.na(business_category), .(
  N = .N,
  Total_ALAE = sum(paid_alae_real, na.rm = TRUE),
  Mean_ALAE = mean(paid_alae_real, na.rm = TRUE)
), by = business_category][order(-Total_ALAE)]

phantom_tbl <- kbl(phantom_by_owner, digits = 0, format = "html",
                   caption = "ALAE on Denied Claims by Business Type") %>%
  kable_styling()

save_table(phantom_tbl, "103_phantom_spend_by_owner")

# ALAE distribution for denied claims
p_alae_denied <- ggplot(denied_claims[paid_alae_real > 0], aes(x = paid_alae_real)) +
  geom_histogram(bins = 30, fill = "#e74c3c", alpha = 0.8) +
  scale_x_log10(labels = dollar_format()) +
  labs(title = "ALAE Distribution: Denied Claims",
       subtitle = sprintf("N = %d claims with ALAE > $0", sum(denied_claims$paid_alae_real > 0, na.rm = TRUE)),
       x = "ALAE (Real $, Log Scale)", y = "Count")

save_figure(p_alae_denied, "103_alae_denied_distribution")

# ==============================================================================
# SECTION 1.3: CLOSED CLAIMS - Cost Drivers
# ==============================================================================
message("\n--- 1.3 Closed Claims Analysis ---")

# Filter to closed, eligible claims with real costs
closed_claims <- master[status_group %in% c("Eligible", "Post Remedial") & 
                          total_paid_real > 1000]

# Cost distribution
p_cost_dist <- ggplot(closed_claims, aes(x = total_paid_real)) +
  geom_histogram(bins = 50, fill = "#2c3e50", alpha = 0.9) +
  scale_x_log10(labels = dollar_format()) +
  labs(title = "Total Claim Cost Distribution (Closed Eligible)",
       subtitle = sprintf("N = %s claims", format(nrow(closed_claims), big.mark = ",")),
       x = "Total Cost (Real $, Log Scale)", y = "Count")

save_figure(p_cost_dist, "104_cost_distribution")

# Cost drivers regression
cost_reg <- feols(
  log(total_paid_real) ~ avg_tank_age + n_tanks_total + 
    share_bare_steel + share_pressure_piping + 
    business_category | dep_region,
  data = closed_claims[!is.na(avg_tank_age)],
  cluster = "dep_region"
)

coef_map <- c(
  "avg_tank_age" = "Tank Age (Years)",
  "n_tanks_total" = "N Tanks at Facility",
  "share_bare_steel" = "Share: Bare Steel Tanks",
  "share_pressure_piping" = "Share: Pressure Piping"
)

cost_tbl <- modelsummary(
  list("Log(Total Cost)" = cost_reg),
  coef_map = coef_map,
  stars = c('*' = .1, '**' = .05, '***' = .01),
  gof_map = c("nobs", "r.squared"),
  output = "kableExtra"
) %>% kable_styling()

save_table(cost_tbl, "105_cost_drivers_regression")

# Duration analysis
p_duration <- ggplot(closed_claims[claim_duration_days > 0 & claim_duration_days < 10000], 
                     aes(x = claim_duration_days / 365)) +
  geom_histogram(bins = 40, fill = "#27ae60", alpha = 0.8) +
  labs(title = "Claim Duration Distribution",
       x = "Duration (Years)", y = "Count")

save_figure(p_duration, "106_duration_distribution")

# ==============================================================================
# SECTION 1.4: REMEDIATION TRENDS
# ==============================================================================
message("\n--- 1.4 Remediation Trends ---")

# Annual trends
annual_trends <- master[claim_year >= 1990 & claim_year <= 2024, .(
  N_Claims = .N,
  Mean_Cost_Real = mean(total_paid_real, na.rm = TRUE),
  Median_Cost_Real = median(total_paid_real, na.rm = TRUE),
  Mean_ALAE_Real = mean(paid_alae_real, na.rm = TRUE),
  Mean_Duration_Days = mean(claim_duration_days, na.rm = TRUE),
  Share_Denied = mean(status_group == "Denied", na.rm = TRUE)
), by = claim_year]

# Cost trends
p_cost_trend <- ggplot(annual_trends, aes(x = claim_year)) +
  geom_line(aes(y = Mean_Cost_Real, color = "Mean"), linewidth = 1) +
  geom_line(aes(y = Median_Cost_Real, color = "Median"), linewidth = 1) +
  scale_y_continuous(labels = dollar_format()) +
  scale_color_manual(values = c("Mean" = "#e74c3c", "Median" = "#3498db")) +
  labs(title = "Claim Costs Over Time (Real $)",
       x = "Year", y = "Cost", color = NULL)

save_figure(p_cost_trend, "107_cost_trends")

# Duration trends
p_duration_trend <- ggplot(annual_trends[claim_year >= 1995], 
                           aes(x = claim_year, y = Mean_Duration_Days / 365)) +
  geom_line(color = "#27ae60", linewidth = 1) +
  geom_smooth(method = "loess", se = TRUE, alpha = 0.2) +
  labs(title = "Average Claim Duration Over Time",
       x = "Year", y = "Duration (Years)")

save_figure(p_duration_trend, "108_duration_trends")

# Denial rate trends
p_denial_trend <- ggplot(annual_trends[claim_year >= 1995], 
                         aes(x = claim_year, y = Share_Denied)) +
  geom_line(color = "#9b59b6", linewidth = 1) +
  geom_smooth(method = "loess", se = TRUE, alpha = 0.2) +
  scale_y_continuous(labels = percent_format()) +
  labs(title = "Denial Rate Over Time",
       x = "Year", y = "Share Denied")

save_figure(p_denial_trend, "109_denial_rate_trends")

# Combined trends panel
trends_long <- melt(annual_trends[claim_year >= 1995], 
                    id.vars = "claim_year",
                    measure.vars = c("N_Claims", "Mean_Cost_Real", "Mean_Duration_Days"))

p_trends_panel <- ggplot(trends_long, aes(x = claim_year, y = value)) +
  geom_line(color = "#2c3e50", linewidth = 0.8) +
  geom_smooth(method = "loess", se = TRUE, alpha = 0.2, color = "#e74c3c") +
  facet_wrap(~ variable, scales = "free_y", 
             labeller = labeller(variable = c(
               "N_Claims" = "Number of Claims",
               "Mean_Cost_Real" = "Mean Cost (Real $)",
               "Mean_Duration_Days" = "Duration (Days)"
             ))) +
  labs(title = "Remediation Trends: 1995-2024", x = "Year", y = NULL)

save_figure(p_trends_panel, "110_trends_panel", width = 12, height = 6)

# ==============================================================================
# SECTION 1.5: SPATIAL VISUALIZATION
# ==============================================================================
message("\n--- 1.5 Spatial Analysis ---")

# Regional summary
regional_summary <- master[!is.na(dep_region) & total_paid_real > 0, .(
  N_Claims = .N,
  Total_Cost_Real = sum(total_paid_real, na.rm = TRUE),
  Mean_Cost_Real = mean(total_paid_real, na.rm = TRUE),
  Mean_Duration_Years = mean(claim_duration_days, na.rm = TRUE) / 365
), by = dep_region][order(-N_Claims)]

region_tbl <- kbl(regional_summary, digits = 0, format = "html",
                  caption = "Claims Summary by DEP Region") %>%
  kable_styling()

save_table(region_tbl, "111_regional_summary")

# Regional bar chart
p_region <- ggplot(regional_summary, aes(x = reorder(dep_region, N_Claims), y = N_Claims)) +
  geom_col(fill = "#3498db", alpha = 0.8) +
  coord_flip() +
  labs(title = "Claims by DEP Region",
       x = NULL, y = "Number of Claims")

save_figure(p_region, "111_regional_claims_bar")

# County concentration
county_summary <- master[!is.na(county) & total_paid_real > 0, .(
  N_Claims = .N,
  Total_Cost = sum(total_paid_real, na.rm = TRUE)
), by = county][order(-N_Claims)]

top_counties <- head(county_summary, 20)

p_counties <- ggplot(top_counties, aes(x = reorder(county, N_Claims), y = N_Claims)) +
  geom_col(fill = "#e67e22", alpha = 0.8) +
  coord_flip() +
  labs(title = "Top 20 Counties by Claims",
       x = NULL, y = "Number of Claims")

save_figure(p_counties, "112_county_claims_bar")

# ==============================================================================
# SECTION 3.1: CONSULTANT CONCENTRATION (HHI)
# ==============================================================================
message("\n--- 3.1 Consultant Concentration ---")

# HHI by region
consultant_shares <- contracts[!is.na(consultant) & !is.na(dep_region) & 
                                 total_contract_value_real > 0, .(
  contract_value = sum(total_contract_value_real, na.rm = TRUE)
), by = .(dep_region, consultant)]

consultant_shares[, market_share := contract_value / sum(contract_value), by = dep_region]
consultant_shares[, share_sq := market_share^2]

hhi_by_region <- consultant_shares[, .(
  HHI = sum(share_sq) * 10000,
  N_Consultants = uniqueN(consultant),
  Top_Consultant_Share = max(market_share)
), by = dep_region][order(-HHI)]

# Add interpretation
hhi_by_region[, Market_Structure := fcase(
  HHI < 1500, "Competitive",
  HHI < 2500, "Moderately Concentrated",
  default = "Highly Concentrated"
)]

hhi_tbl <- kbl(hhi_by_region, digits = 0, format = "html",
               caption = "Market Concentration (HHI) by Region") %>%
  kable_styling() %>%
  column_spec(5, background = ifelse(hhi_by_region$HHI > 2500, "#ffcccc", "white"))

save_table(hhi_tbl, "301_hhi_by_region")

# Top consultants overall
top_consultants <- contracts[!is.na(consultant) & total_contract_value_real > 0, .(
  N_Contracts = .N,
  Total_Value = sum(total_contract_value_real, na.rm = TRUE),
  Mean_Value = mean(total_contract_value_real, na.rm = TRUE)
), by = consultant][order(-Total_Value)][1:15]

top_consultants[, Market_Share := Total_Value / sum(Total_Value)]

consultant_tbl <- kbl(top_consultants, digits = 0, format = "html",
                      caption = "Top 15 Consultants by Contract Value") %>%
  kable_styling()

save_table(consultant_tbl, "302_top_consultants")

# ==============================================================================
# HHI ANALYSIS & MAPPING
# ==============================================================================

# 1. Define PA DEP Regions (County Lookup)
# This mapping ensures counties are grouped correctly for the outline
dep_regions <- data.table(
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

# 2. Calculate HHI by County
contracts_merged <- merge(contracts, master[, .(claim_number, county)], by = "claim_number")
contracts_merged[, county_clean := tolower(county)]

county_hhi <- contracts_merged[!is.na(consultant) & total_contract_value_real > 0, .(
  val = sum(total_contract_value_real)
), by = .(county_clean, consultant)]
county_hhi[, share := val / sum(val), by = county_clean]
county_hhi_final <- county_hhi[, .(HHI = sum(share^2) * 10000), by = county_clean]

# 3. Join with Map Data
pa_map <- map_data("county", "pennsylvania")
setDT(pa_map)
map_data_full <- merge(pa_map, dep_regions, by = "subregion", all.x = TRUE)
map_data_full <- merge(map_data_full, county_hhi_final, by = "subregion", all.x = TRUE)
setorder(map_data_full, group, order)

# 4. Generate the Map
p_hhi_map <- ggplot(map_data_full, aes(x = long, y = lat, group = group)) +
  # A. County Fill (The Heatmap)
  geom_polygon(aes(fill = HHI), color = NA) +
  # B. County Borders (Thin, White)
  geom_polygon(fill = NA, color = "white", linewidth = 0.1) +
  # C. Region Outlines (Thick, Dark) - trick: union polygons implicitly by grouping
  geom_polygon(data = map_data_full, aes(color = region), fill = NA, linewidth = 0.8) +
  scale_fill_ustif_c(name = "Market Concentration (HHI)", limits = c(0, 10000)) +
  scale_color_ustif(name = "DEP Region") +
  coord_fixed(1.3) +
  theme_void() + # Clean map theme
  theme(legend.position = "bottom", 
        legend.title = element_text(face="bold"),
        legend.key.width = unit(1.5, "cm"))

# 5. Save Artifact
save_plot_pub(p_hhi_map, "301_hhi_county_map_regions", width = 8, height = 5)

# ==============================================================================
# SECTION 3.2: ADJUSTER CONCENTRATION
# ==============================================================================
message("\n--- 3.2 Adjuster Concentration ---")

# Adjuster activity by region
adjuster_activity <- contracts[!is.na(adjuster) & !is.na(dep_region), .(
  N_Contracts = .N,
  N_Regions = uniqueN(dep_region),
  Total_Value = sum(total_contract_value_real, na.rm = TRUE)
), by = adjuster][order(-N_Contracts)]

# How many adjusters work across regions?
multi_region_adjusters <- adjuster_activity[N_Regions > 1]
message(sprintf("%d of %d adjusters work across multiple regions (%.1f%%)",
                nrow(multi_region_adjusters),
                nrow(adjuster_activity),
                100 * nrow(multi_region_adjusters) / nrow(adjuster_activity)))

# Auction propensity by adjuster
auction_by_adjuster <- contracts[!is.na(adjuster), .(
  N_Contracts = .N,
  N_PFP = sum(auction_type == "Bid-to-Result", na.rm = TRUE),
  Share_PFP = mean(auction_type == "Bid-to-Result", na.rm = TRUE)
), by = adjuster][N_Contracts >= 5][order(-Share_PFP)]

p_adjuster_pfp <- ggplot(auction_by_adjuster[N_Contracts >= 10], 
                          aes(x = N_Contracts, y = Share_PFP)) +
  geom_point(alpha = 0.6, size = 3, color = "#9b59b6") +
  scale_y_continuous(labels = percent_format()) +
  labs(title = "Adjuster Auction Propensity",
       subtitle = "Adjusters with 10+ contracts",
       x = "Number of Contracts", y = "Share: Bid-to-Result")

save_figure(p_adjuster_pfp, "303_adjuster_auction_propensity")

# ==============================================================================
# SUMMARY OUTPUT
# ==============================================================================
message("\n========================================")
message("ANALYSIS 01 COMPLETE: Descriptive Statistics")
message("========================================")
message(sprintf("Key Findings:"))
message(sprintf("  - Phantom Spend (ALAE on Denied): $%s", 
                format(phantom_spend$Total_ALAE_Real, big.mark = ",", nsmall = 0)))
message(sprintf("  - Highest HHI Region: %s (%.0f)", 
                hhi_by_region$dep_region[1], hhi_by_region$HHI[1]))
message(sprintf("  - Multi-Region Adjusters: %d (%.1f%%)", 
                nrow(multi_region_adjusters),
                100 * nrow(multi_region_adjusters) / nrow(adjuster_activity)))
