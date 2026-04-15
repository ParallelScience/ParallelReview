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

# Single-instance lock: if a previous cron run is still going (long backlog,
# slow OpenAI API, etc.), exit immediately so we do not stack runs that fight
# over the same SQLite DB and re-attempt the same papers in parallel. The lock
# is automatically released when this script exits because flock is bound to
# fd 200 in the current shell.
LOCK_FILE="/tmp/parallel_review_cron.lock"
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    echo "[$(date -u)] cron_review: previous run still active, exiting" >&2
    exit 0
fi

LOG_FILE="${LOG_DIR}/cron_review_$(date +%Y%m%d_%H%M%S).log"

echo "=== ParallelReview cron: $(date -u) ===" | tee "$LOG_FILE"

# Skepthical and all its dependencies (Playwright/Chromium, AG2, OpenAI, etc.)
# live inside the parallel-review Docker container, not on the host. The
# container also has the env vars (OPENAI_API_KEY, MISTRAL_API_KEY, GITHUB_TOKEN,
# SCIENTIST_NAME, GIT_EMAIL) loaded from .env via docker-compose. Always run
# the review scanner inside the container.
CONTAINER="${PARALLEL_REVIEW_CONTAINER:-parallel-review}"

if ! docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true; then
    echo "ERROR: container '$CONTAINER' is not running" | tee -a "$LOG_FILE"
    exit 1
fi

docker exec "$CONTAINER" python3 /app/scripts/review_new_papers.py 2>&1 | tee -a "$LOG_FILE"

# Clean up old logs (keep last 30 days)
find "$LOG_DIR" -name "cron_review_*.log" -mtime +30 -delete 2>/dev/null || true

echo "=== Done: $(date -u) ===" | tee -a "$LOG_FILE"
