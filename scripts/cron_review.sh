#!/usr/bin/env bash
# Cron wrapper: scan for unreviewed papers and run Skepthical.
# Runs from crontab — sources API keys, logs output.
#
# Schedule (added by setup):
#   0  3 * * *  .../cron_review.sh        # Daily at 03:00 UTC
#   0  4 * * *  .../cron_review.sh        # Retry 1 hour later

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${REPO_DIR}/logs"
mkdir -p "$LOG_DIR"

LOG_FILE="${LOG_DIR}/cron_review_$(date +%Y%m%d_%H%M%S).log"

echo "=== ParallelReview cron: $(date -u) ===" | tee "$LOG_FILE"

# Source API keys
SKEPTHICAL_ENV="/scratch/scratch-aiscientist/skepthical/.env.skepthical"
DENARIO_ENV="/scratch/scratch-aiscientist/parallelscience/denario-scientists/.env"

if [ -f "$SKEPTHICAL_ENV" ]; then
    source "$SKEPTHICAL_ENV"
else
    echo "WARNING: $SKEPTHICAL_ENV not found" | tee -a "$LOG_FILE"
fi

if [ -f "$DENARIO_ENV" ]; then
    export $(grep "^GITHUB_TOKEN" "$DENARIO_ENV" | head -1)
else
    echo "WARNING: $DENARIO_ENV not found" | tee -a "$LOG_FILE"
fi

export SCIENTIST_NAME="${SCIENTIST_NAME:-skepthical}"
export GIT_EMAIL="${GIT_EMAIL:-skepthical@parallelscience.org}"

# Run the review scanner
cd "$REPO_DIR"
python3 scripts/review_new_papers.py 2>&1 | tee -a "$LOG_FILE"

# Clean up old logs (keep last 30 days)
find "$LOG_DIR" -name "cron_review_*.log" -mtime +30 -delete 2>/dev/null || true

echo "=== Done: $(date -u) ===" | tee -a "$LOG_FILE"
