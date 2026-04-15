#!/usr/bin/env bash
set -uo pipefail

# WEBHOOK_SECRET must be provided via the environment, e.g.:
#   set -a; source /scratch/scratch-aiscientist/parallelscience/ParallelReview/.env; set +a
#   ./scripts/trigger_all_reviews.sh
# Never hardcode the secret here — this script lives in a public repo.
if [ -z "${WEBHOOK_SECRET:-}" ]; then
    echo "ERROR: WEBHOOK_SECRET env var not set. Source the .env file first." >&2
    exit 1
fi
SECRET="$WEBHOOK_SECRET"
LOCAL_URL="https://orion.taila855ba.ts.net:8444/webhook/github"
CLOUD_URL="https://parallel-review-689836870161.us-central1.run.app/webhook/github"
DB="/scratch/scratch-aiscientist/parallelscience/arxiv-browse/browse/data/papers.db"

echo "=== Trigger all reviews: $(date -u) ==="

# Get all paper repos
REPOS=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$DB')
conn.row_factory = sqlite3.Row
for r in conn.execute('SELECT repo FROM papers WHERE is_current = 1 ORDER BY px_id'):
    print(r['repo'])
conn.close()
")

TOTAL=$(echo "$REPOS" | wc -l)
echo "Found $TOTAL papers"

# Loop until all are reviewed
for round in 1 2 3 4 5 6 7 8 9 10; do
    echo ""
    echo "--- Round $round ($(date -u)) ---"
    PENDING=0
    
    for repo in $REPOS; do
        RESULT=$(python3 -c "
import hmac, hashlib, json, urllib.request
secret = '$SECRET'
payload = json.dumps({'build':{'status':'built'},'repository':{'name':'$repo'},'organization':{'login':'ParallelScience'}}).encode()
sig = 'sha256=' + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
req = urllib.request.Request('$LOCAL_URL', data=payload, method='POST', headers={'X-GitHub-Event':'page_build','X-Hub-Signature-256':sig,'Content-Type':'application/json'})
try:
    with urllib.request.urlopen(req) as resp:
        print(resp.read().decode())
except Exception as e:
    print(str(e))
" 2>&1)
        
        if echo "$RESULT" | grep -q "review_started"; then
            echo "  STARTED: $repo"
        elif echo "$RESULT" | grep -q "skipped"; then
            : # already done, silent
        elif echo "$RESULT" | grep -q "rejected\|503"; then
            PENDING=$((PENDING + 1))
        else
            echo "  OTHER: $repo -> $RESULT"
        fi
    done
    
    if [ "$PENDING" -eq 0 ]; then
        echo "All papers reviewed or in progress!"
        break
    fi
    
    echo "$PENDING papers still pending, waiting 20 minutes..."
    sleep 1200
done

# Index all review repos locally AND on Cloud Run
echo ""
echo "=== Indexing reviews ==="
REVIEW_REPOS=$(python3 -c "
import json, urllib.request, os
repos = []
page = 1
while True:
    url = f'https://api.github.com/orgs/ParallelScience/repos?per_page=100&page={page}'
    req = urllib.request.Request(url, headers={'Accept':'application/vnd.github+json'})
    token = os.environ.get('GITHUB_TOKEN','')
    if token: req.add_header('Authorization', f'Bearer {token}')
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    if not data: break
    for r in data:
        if r['name'].startswith('review-'): print(r['name'])
    page += 1
" 2>&1)

for repo in $REVIEW_REPOS; do
    for TARGET_URL in "$LOCAL_URL" "$CLOUD_URL"; do
        python3 -c "
import hmac, hashlib, json, urllib.request
secret = '$SECRET'
payload = json.dumps({'build':{'status':'built'},'repository':{'name':'$repo'},'organization':{'login':'ParallelScience'}}).encode()
sig = 'sha256=' + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
req = urllib.request.Request('$TARGET_URL', data=payload, method='POST', headers={'X-GitHub-Event':'page_build','X-Hub-Signature-256':sig,'Content-Type':'application/json'})
try:
    with urllib.request.urlopen(req) as resp:
        print(f'  {\"$repo\"} -> {\"$TARGET_URL\"[:40]}: {resp.read().decode()[:60]}')
except Exception as e:
    print(f'  {\"$repo\"} -> {\"$TARGET_URL\"[:40]}: {e}')
" 2>&1
    done
done

echo ""
echo "=== Done: $(date -u) ==="
