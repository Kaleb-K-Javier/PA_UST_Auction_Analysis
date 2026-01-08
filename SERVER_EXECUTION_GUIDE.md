# eFACTS Server Execution Guide

## Production Scrape: 38,545 PA UST Facilities

### Prerequisites

```bash
# Verify R installation
R --version  # Requires >= 4.0

# Verify required R packages
Rscript -e "
  required <- c('data.table', 'httr', 'rvest', 'janitor', 'stringr')
  missing <- required[!sapply(required, requireNamespace, quietly = TRUE)]
  if (length(missing) > 0) {
    install.packages(missing, repos = 'https://cloud.r-project.org')
  }
  cat('All packages ready.\n')
"
```

---

## Step 1: Pre-Flight Checks

```bash
# Navigate to project root
cd /path/to/PA_UST_Auction_Analysis

# Verify facility linkage table exists
ls -la data/external/padep/facility_linkage_table.csv

# Count facilities (should be ~38,546 including header)
wc -l data/external/padep/facility_linkage_table.csv

# Verify eFACTS ID coverage
Rscript -e "
  linkage <- read.csv('data/external/padep/facility_linkage_table.csv')
  n_total <- nrow(linkage)
  n_with_id <- sum(!is.na(linkage\$efacts_facility_id))
  cat(sprintf('Total facilities: %d\n', n_total))
  cat(sprintf('With eFACTS ID: %d (%.1f%%)\n', n_with_id, 100*n_with_id/n_total))
"
```

---

## Step 2: Clean Environment (CRITICAL)

```bash
# Remove stale data to prevent schema conflicts
rm -rf data/external/efacts/*.csv
rm -rf data/external/efacts/*.rds
rm -f data/external/efacts/scrape_checkpoint_v22.rds
rm -f data/external/efacts/scrape_log.txt

# Create fresh output directory
mkdir -p data/external/efacts
mkdir -p logs
```

---

## Step 3: Test Scraper (Required)

```bash
# Run test on 5 facilities
Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  test_ids <- c('575014', '575015', '575016', '575017', '575018')
  run_scraper(test_ids)
"

# Verify output schema
echo "=== Inspections Schema ==="
head -2 data/external/efacts/efacts_inspections.csv

echo "=== Violations Schema ==="
head -2 data/external/efacts/efacts_violations.csv

echo "=== Permits Tasks Schema ==="
head -2 data/external/efacts/efacts_permits_tasks.csv

# Clean test data before production
rm -rf data/external/efacts/*.csv data/external/efacts/*.rds
rm -f data/external/efacts/scrape_checkpoint_v22.rds
```

---

## Step 4: Launch Production Scrape

### Option A: Screen Session (Recommended)

```bash
# Start screen session
screen -S efacts_scrape

# Launch scraper
Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  
  # Load facility IDs
  linkage <- read.csv('data/external/padep/facility_linkage_table.csv')
  ids <- unique(linkage\$efacts_facility_id[!is.na(linkage\$efacts_facility_id)])
  
  cat(sprintf('Starting scrape of %d facilities\n', length(ids)))
  cat(sprintf('Estimated runtime: 16-24 hours\n'))
  
  # Run scraper
  run_scraper(ids)
"

# Detach: Ctrl+A, then D
# Reattach later: screen -r efacts_scrape
```

### Option B: Background with nohup

```bash
nohup Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  linkage <- read.csv('data/external/padep/facility_linkage_table.csv')
  ids <- unique(linkage\$efacts_facility_id[!is.na(linkage\$efacts_facility_id)])
  run_scraper(ids)
" > logs/efacts_scrape_$(date +%Y%m%d_%H%M%S).log 2>&1 &

# Get process ID
echo $! > logs/scrape.pid
cat logs/scrape.pid
```

### Option C: systemd Service (Production Servers)

```bash
# Create service file
sudo cat > /etc/systemd/system/efacts-scrape.service << 'EOF'
[Unit]
Description=eFACTS Web Scraper
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/PA_UST_Auction_Analysis
ExecStart=/usr/bin/Rscript -e "source('R/etl/02b_efacts_scrape.R'); linkage <- read.csv('data/external/padep/facility_linkage_table.csv'); ids <- unique(linkage$efacts_facility_id[!is.na(linkage$efacts_facility_id)]); run_scraper(ids)"
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
EOF

# Enable and start
sudo systemctl daemon-reload
sudo systemctl start efacts-scrape
sudo systemctl status efacts-scrape

# View logs
sudo journalctl -u efacts-scrape -f
```

---

## Step 5: Monitor Progress

### Real-Time Log

```bash
tail -f data/external/efacts/scrape_log.txt
```

### Progress Status

```bash
Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  get_scrape_status()
"
```

### Output File Sizes

```bash
watch -n 60 'ls -lh data/external/efacts/*.csv 2>/dev/null | tail -20'
```

### Record Counts

```bash
Rscript -e "
  files <- list.files('data/external/efacts', pattern = '\\.csv$', full.names = TRUE)
  for (f in files) {
    n <- length(readLines(f)) - 1
    cat(sprintf('%s: %d records\n', basename(f), n))
  }
"
```

---

## Step 6: Handle Interruptions

### Check if Process Running

```bash
ps aux | grep -i "[r]script.*efacts"
```

### Resume After Interruption

```bash
# Checkpoint system automatically resumes from last saved position
Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  
  # Check current status
  get_scrape_status()
  
  # Resume
  linkage <- read.csv('data/external/padep/facility_linkage_table.csv')
  ids <- unique(linkage\$efacts_facility_id[!is.na(linkage\$efacts_facility_id)])
  run_scraper(ids)
"
```

### Force Restart (Discard Progress)

```bash
# WARNING: This discards all progress
Rscript -e "
  source('R/etl/02b_efacts_scrape.R')
  reset_checkpoint()
"

# Then re-run from Step 4
```

---

## Step 7: Post-Scrape Validation

```bash
# Verify all tables have data
Rscript -e "
  files <- c(
    'efacts_facility_meta.csv',
    'efacts_tanks.csv',
    'efacts_inspections.csv',
    'efacts_violations.csv',
    'efacts_permits_detail.csv',
    'efacts_permits_tasks.csv',
    'efacts_remediation_summary.csv',
    'efacts_remediation_substances.csv',
    'efacts_remediation_milestones.csv',
    'efacts_facility_coverage.csv'
  )
  
  cat('=== OUTPUT VALIDATION ===\n')
  for (f in files) {
    path <- file.path('data/external/efacts', f)
    if (file.exists(path)) {
      dt <- data.table::fread(path, nrows = 1)
      n <- length(readLines(path)) - 1
      cat(sprintf('✓ %s: %d records, %d columns\n', f, n, ncol(dt)))
    } else {
      cat(sprintf('✗ %s: MISSING\n', f))
    }
  }
"

# Check for errors
grep -c "error" data/external/efacts/efacts_scrape_errors.csv 2>/dev/null || echo "No error file"

# Coverage summary
Rscript -e "
  cov <- data.table::fread('data/external/efacts/efacts_facility_coverage.csv')
  cat(sprintf('Total scraped: %d\n', nrow(cov)))
  cat(sprintf('Successful: %d (%.1f%%)\n', 
      sum(cov\$status == 'success'), 
      100*mean(cov\$status == 'success')))
  cat(sprintf('With violations: %d\n', sum(cov\$n_violations > 0)))
  cat(sprintf('With permits: %d\n', sum(cov\$n_permits > 0)))
"
```

---

## Configuration Tuning

### Adjust Rate Limiting

Edit `R/etl/02b_efacts_scrape.R`:

```r
CONFIG <- list(
  delay_facility = 2.0,    # Increase if getting HTTP 429
  delay_detail = 1.0,      # Increase for safer rate
  batch_size = 50,         # Decrease for more frequent saves
  timeout = 60,            # Increase for slow connections
  max_retries = 5          # Increase for flaky networks
)
```

### Selective Scraping

```r
# Skip detail pages for faster run (facility-level only)
CONFIG$scrape_violations <- FALSE
CONFIG$scrape_permit_details <- FALSE
CONFIG$scrape_remediation_details <- FALSE
```

---

## Runtime Reference

| Facilities | Delay | Est. Runtime |
|------------|-------|--------------|
| 38,545 | 1.0s | ~11 hours |
| 38,545 | 1.5s | ~16 hours |
| 38,545 | 2.0s | ~21 hours |
| 38,545 | 2.5s | ~27 hours |

*Note: Detail page scraping adds 50-100% to base time*

---

## Emergency Contacts

- **HTTP 429 (Too Many Requests)**: Increase delays, wait 1 hour, resume
- **Connection Timeout**: Check network, increase timeout setting
- **Disk Full**: Clear old logs, check `data/external/efacts/` size
- **Memory Error**: Reduce batch_size, restart R session

---

## Quick Reference Card

```bash
# Start scrape
screen -S efacts && Rscript -e "source('R/etl/02b_efacts_scrape.R'); linkage <- read.csv('data/external/padep/facility_linkage_table.csv'); ids <- unique(linkage\$efacts_facility_id[!is.na(linkage\$efacts_facility_id)]); run_scraper(ids)"

# Check progress
Rscript -e "source('R/etl/02b_efacts_scrape.R'); get_scrape_status()"

# Watch log
tail -f data/external/efacts/scrape_log.txt

# Resume after crash
# (Same command as start - checkpoint handles resume)

# Reset and start over
Rscript -e "source('R/etl/02b_efacts_scrape.R'); reset_checkpoint()"
rm -rf data/external/efacts/*.csv
```
