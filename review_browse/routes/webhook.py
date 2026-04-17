"""GitHub webhook endpoint for real-time review ingestion and generation.

Handles two types of page_build events:
1. Paper repos (not prefixed with 'review-') → run Skepthical, publish review
2. Review repos (prefixed with 'review-') → scrape and index the review
"""

import hashlib
import hmac
import logging
import os
import threading
import time as _time

from flask import Blueprint, Response, current_app, request

blueprint = Blueprint("webhook", __name__, url_prefix="/webhook")
log = logging.getLogger(__name__)

# --- Skepthical availability ---
# Cloud Run uses the lightweight image (`Dockerfile`, no Skepthical/Playwright)
# and only acts as the indexer. The review-generator role lives on the host
# image (`Dockerfile.reviewer`) which has Skepthical installed. Detect this at
# import time so we can fast-fail paper-repo webhooks on Cloud Run instead of
# spawning a background thread that immediately ImportErrors.
try:
    import skepthical  # type: ignore  # noqa: F401
    _SKEPTHICAL_AVAILABLE = True
except ImportError:
    _SKEPTHICAL_AVAILABLE = False

# --- Concurrency guard: prevent duplicate reviews for the same paper ---
# Mirrored to the active_reviews table in SQLite so the cap survives restarts.
_active_reviews: set[str] = set()
_active_reviews_lock = threading.Lock()
_MAX_CONCURRENT_REVIEWS = 3


def init_active_reviews_state() -> None:
    """Wipe stale active_reviews rows from prior PIDs and load survivors.

    Called once from the app factory after init_db. Any row whose pid is not
    the current process can't still be running (its owner is gone), so we
    clear it. Whatever's left is treated as in-flight by the in-memory guard.
    """
    from review_browse.services.database import (
        list_active_reviews, reset_active_reviews_for_pid,
    )
    cleared = reset_active_reviews_for_pid(os.getpid())
    if cleared:
        log.info("init_active_reviews_state: cleared %d stale rows from prior pid", cleared)
    survivors = list_active_reviews()
    with _active_reviews_lock:
        _active_reviews.clear()
        _active_reviews.update(r["repo"] for r in survivors)
    if survivors:
        log.info("init_active_reviews_state: loaded %d in-flight reviews", len(survivors))


def _verify_signature(payload: bytes, signature_header: str, secret: str) -> bool:
    if not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature_header)


@blueprint.route("/github", methods=["POST"])
def github_webhook() -> Response:
    """Handle GitHub page_build events.

    - Paper repos → kick off Skepthical review in background thread
    - Review repos → scrape and index immediately (like arxiv-browse)
    """
    secret = current_app.config.get("WEBHOOK_SECRET", "")

    # Always verify signature (reject if secret not configured)
    if not secret:
        log.error("WEBHOOK_SECRET not configured — rejecting all webhooks")
        return Response("server misconfigured", status=500)

    signature = request.headers.get("X-Hub-Signature-256", "")
    if not _verify_signature(request.data, signature, secret):
        log.warning("Invalid webhook signature")
        return Response("Forbidden", status=403)

    event = request.headers.get("X-GitHub-Event", "")
    if event == "ping":
        return Response("pong", status=200)
    if event != "page_build":
        return Response("ignored", status=204)

    payload = request.get_json(silent=True)
    if not payload:
        return Response("bad payload", status=400)

    build = payload.get("build", {})
    if build.get("status") != "built":
        return Response("not a successful build", status=204)

    repo_name = payload.get("repository", {}).get("name", "")
    org_name = payload.get("organization", {}).get("login", "")
    if not repo_name or not org_name:
        return Response("missing repo/org", status=400)

    log.info("page_build event for %s/%s", org_name, repo_name)

    if repo_name.startswith("review-"):
        return _handle_review_repo(org_name, repo_name)
    else:
        return _handle_paper_repo(org_name, repo_name)


def _handle_review_repo(org_name: str, repo_name: str) -> Response:
    """Schedule indexing of a review repo in a background thread.

    The full ingest path runs an OpenAI math-sanitization call that can take
    tens of seconds, sometimes more. GitHub gives webhook handlers only ~10
    seconds before declaring the delivery a failure (and won't retry on a 500
    fast enough either), so we acknowledge the webhook immediately with 202
    and do all the slow work asynchronously.
    """
    gcs_bucket = current_app.config.get("GCS_BUCKET", "parallel-review")
    app = current_app._get_current_object()  # capture for the thread

    thread = threading.Thread(
        target=_index_review_background,
        args=(app, org_name, repo_name, gcs_bucket),
        daemon=True,
        name=f"index-{repo_name[:30]}",
    )
    thread.start()

    return Response(
        f'{{"action":"indexing_started","repo":"{repo_name}"}}',
        status=202,
        mimetype="application/json",
    )


def _index_review_background(app, org_name: str, repo_name: str, gcs_bucket: str) -> None:
    """Background-thread review-repo ingest. Logs failures, never crashes."""
    from review_browse.services.database import get_db_standalone, sync_to_gcs
    from review_browse.services.scraper import scrape_single_repo, upsert_review

    log.info("[index-bg] Indexing %s/%s", org_name, repo_name)
    try:
        meta = scrape_single_repo(org_name, repo_name)
        if not meta:
            log.info("[index-bg] No review metadata for %s/%s", org_name, repo_name)
            return

        conn = get_db_standalone()
        try:
            rx_id, version, action = upsert_review(conn, meta, org_name, gcs_bucket=gcs_bucket)
        finally:
            conn.close()

        if action != "unchanged":
            if not sync_to_gcs():
                log.warning(
                    "[index-bg] GCS sync skipped/failed for %s — local DB has the row "
                    "but it will not propagate to other instances until they re-scrape",
                    rx_id,
                )
            log.info("[index-bg] Indexed %s as %s (action=%s, version=%d)",
                     repo_name, rx_id, action, version)
        else:
            log.info("[index-bg] %s unchanged", rx_id)
    except Exception as exc:
        log.error("[index-bg] FAILED to index %s/%s: %s", org_name, repo_name, exc, exc_info=True)


def _handle_paper_repo(org_name: str, repo_name: str) -> Response:
    """Fetch paper PDF and run Skepthical review in a background thread."""
    import re
    import urllib.request

    from review_browse.services.database import (
        register_active_review, unregister_active_review,
    )

    # --- Guard: this deployment cannot generate reviews ---
    # On Cloud Run (lightweight image) Skepthical isn't installed. Bail out
    # cleanly with 204 instead of spawning a background thread that will just
    # ImportError and silently leave the slot half-occupied.
    if not _SKEPTHICAL_AVAILABLE:
        log.info("Skepthical not installed; ignoring paper webhook for %s/%s",
                 org_name, repo_name)
        return Response(
            '{"action":"ignored","reason":"skepthical_not_installed"}',
            status=204, mimetype="application/json",
        )

    # --- Guard: already reviewing this paper? Claim slot atomically ---
    with _active_reviews_lock:
        if repo_name in _active_reviews:
            log.info("Already reviewing %s, skipping", repo_name)
            return Response(
                f'{{"action":"skipped","reason":"already in progress"}}',
                status=200, mimetype="application/json",
            )
        if len(_active_reviews) >= _MAX_CONCURRENT_REVIEWS:
            log.warning("Max concurrent reviews reached (%d), rejecting %s",
                        _MAX_CONCURRENT_REVIEWS, repo_name)
            return Response(
                f'{{"action":"rejected","reason":"max concurrent reviews"}}',
                status=503, mimetype="application/json",
            )
        # Claim the slot immediately to prevent races. Persist to DB so the
        # cap survives container restarts.
        _active_reviews.add(repo_name)
    try:
        register_active_review(repo_name)
    except Exception as exc:
        log.warning("register_active_review failed for %s: %s", repo_name, exc)

    # --- Guard: already has a review repo on GitHub? ---
    try:
        token = os.environ.get("GITHUB_TOKEN", "")
        check_url = f"https://api.github.com/repos/{org_name}/review-{repo_name}"
        req = urllib.request.Request(check_url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        })
        try:
            urllib.request.urlopen(req, timeout=10)
            log.info("Paper %s already has review repo review-%s, skipping", repo_name, repo_name)
            with _active_reviews_lock:
                _active_reviews.discard(repo_name)
            try:
                unregister_active_review(repo_name)
            except Exception:
                pass
            return Response(
                f'{{"action":"skipped","reason":"review repo exists"}}',
                status=200, mimetype="application/json",
            )
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
    except Exception as e:
        log.warning("Could not check for existing review: %s", e)

    log.info("Launching Skepthical review for %s/%s", org_name, repo_name)
    thread = threading.Thread(
        target=_run_review_background,
        args=(org_name, repo_name),
        daemon=True,
    )
    thread.start()

    return Response(
        f'{{"action":"review_started","repo":"{repo_name}"}}',
        status=202,
        mimetype="application/json",
    )


def _download_pdf_with_retry(url: str, dest: str, retries: int = 5, delay: float = 30) -> bool:
    """Download PDF with retries (GitHub Pages may not be ready immediately)."""
    import urllib.request
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                data = resp.read()
            if len(data) < 500:
                log.warning("[review-bg] PDF too small (%d bytes), attempt %d/%d",
                            len(data), attempt + 1, retries)
                _time.sleep(delay)
                continue
            with open(dest, "wb") as f:
                f.write(data)
            return True
        except Exception as e:
            log.warning("[review-bg] PDF download failed (attempt %d/%d): %s",
                        attempt + 1, retries, e)
            if attempt < retries - 1:
                _time.sleep(delay)
    return False


def _run_review_background(org_name: str, repo_name: str):
    """Background thread: fetch PDF, run Skepthical, publish review."""
    import glob
    import json
    import re
    import shutil
    import subprocess
    import sys
    import tempfile
    import urllib.error
    import urllib.request

    pages_url = f"https://{org_name.lower()}.github.io/{repo_name}/"
    pdf_url = f"{pages_url}paper.pdf"

    log.info("[review-bg] Starting review for %s", repo_name)

    work_dir = tempfile.mkdtemp(prefix=f"review-{repo_name[:30]}-")
    try:
        # 1. Download PDF with retry
        pdf_path = os.path.join(work_dir, "paper.pdf")
        if not _download_pdf_with_retry(pdf_url, pdf_path):
            log.error("[review-bg] Failed to download PDF for %s after retries", repo_name)
            return

        # 2. Scrape paper metadata
        title = repo_name
        author = "unknown"
        abstract = ""
        try:
            with urllib.request.urlopen(pages_url, timeout=15) as resp:
                html = resp.read().decode("utf-8", errors="replace")
            m = re.search(r'<h1[^>]*>(.+?)</h1>', html, re.DOTALL)
            if m:
                title = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            if title == repo_name:
                # <h1> failed or returned repo name, try <title>
                m = re.search(r'<title>([^<]+)</title>', html)
                if m and m.group(1).strip() != repo_name:
                    title = m.group(1).strip()
            m = re.search(r'Author:\s*</span>\s*(.+?)<', html)
            if not m:
                m = re.search(r'Author:\s*([^<]+)', html)
            if m:
                author = m.group(1).strip()
            m = re.search(r'<p>(.{50,}?)</p>', html, re.DOTALL)
            if m:
                abstract = re.sub(r'<[^>]+>', '', m.group(1)).strip()[:1000]
        except Exception as e:
            log.warning("[review-bg] Could not scrape metadata for %s: %s", repo_name, e)

        log.info("[review-bg] Reviewing: %s by %s", title[:60], author)

        # 3. Run Skepthical
        try:
            from skepthical import Skepthical
        except ImportError:
            log.error("[review-bg] skepthical not installed, cannot review")
            return

        temperature = 0.5
        temperature_gpt5 = 1

        params_skepthical = {
            "paper_pdf_file": pdf_path,
            "paper_title": title,
            "work_dir": work_dir,
            "max_chars": 30000,
            "models": {
                "engineer": "gpt-5",
                "group_manager": "gpt-5",
                "paper_verification_agent": "gpt-5",
                "figure_reviewer_agent": "gpt-5",
                "reference_extractor": "gpt-4.1",
                "latex_formatter": "gpt-4.1",
                "doc_type_filter_agent": "gpt-4.1-mini",
                "total_reviewer": "gpt-5.1",
                "statement_extractor": "gpt-5.1",
                "report_merger": "gpt-5.1",
                "report_merger_figures": "gpt-4.1",
                "unstructured_reviewer": "gpt-5.2-thinking",
                "unstructured_report_merger": "gpt-5.2-thinking",
                "maths_reviewer": "gpt-5.2-thinking",
                "numerics_extractor": "gpt-5.2",
                "numerics_coder": "gpt-5.2",
                "numerics_reporter": "gpt-5.2",
            },
            "max_rounds": 2,
            "temperature": {
                "engineer": temperature,
                "figure_reviewer_agent": temperature_gpt5,
                "paper_verification_agent": temperature_gpt5,
                "reference_extractor": temperature,
                "latex_formatter": temperature,
                "doc_type_filter_agent": 0.1,
                "total_reviewer": temperature_gpt5,
                "statement_extractor": 0.1,
                "report_merger": temperature_gpt5,
                "report_merger_figures": 0.1,
                "unstructured_reviewer": temperature_gpt5,
                "unstructured_report_merger": temperature_gpt5,
                "maths_reviewer": None,
                "numerics_extractor": None,
                "numerics_coder": None,
                "numerics_reporter": None,
            },
            "agents_ag2": [
                "reference_extractor", "latex_formatter", "doc_type_filter_agent",
                "total_reviewer", "statement_extractor",
                "report_merger", "report_merger_figures",
            ],
            "agents_direct_call": [
                "figure_reviewer_agent", "paper_verification_agent",
                "unstructured_reviewer", "unstructured_report_merger",
            ],
            "agents": [
                "reference_extractor", "latex_formatter", "doc_type_filter_agent",
                "total_reviewer", "statement_extractor",
                "report_merger", "report_merger_figures",
            ],
            "timeout": 21600,  # 6 hours max
            "n_key_statements": None,
            "force_parse_paper": True,
            "emails": [],
            "paper_summary_word_limit": 250,
            "strengths_word_limit": 120,
            "major_issues_word_limit": 500,
            "minor_issues_word_limit": 500,
            "very_minor_issues_word_limit": 200,
            "review_mode": "compact",
            "unstructured_report": True,
            "figures_review": True,
            "verify_statements": True,
            "review_maths": True,
            "review_numerics": True,
            "review_reproducibility": False,
            "max_checked_items_maths": 20,
            "max_checked_items_numerics": 25,
            "numerics_exec_timeout_s": 20,
            "n_total_review": 2,
            "total_reviewer_models": ["gpt-5", "gpt-5"],

            # Paper scoring (1-10 ratings)
            "get_score": True,
        }

        sk = Skepthical(params_skepthical=params_skepthical)
        report = sk.run()

        # Extract scores
        scores = None
        if isinstance(report, dict):
            scores = report.get("scores")

        # Extract total cost from Skepthical's final context
        total_cost = None
        try:
            cost_df = sk.final_context.get("cost_dataframe")
            if cost_df is not None:
                total_row = cost_df[cost_df["Agent"] == "Total"]
                if not total_row.empty:
                    total_cost = float(total_row["Cost ($)"].iloc[0])
                    log.info("[review-bg] Review cost for %s: $%.4f", repo_name, total_cost)
        except Exception as e:
            log.warning("[review-bg] Could not extract cost: %s", e)

        # Extract markdown
        review_md = ""
        if isinstance(report, str):
            review_md = report
        elif isinstance(report, dict):
            md = report.get("report_md")
            if isinstance(md, str):
                review_md = md
        if not review_md:
            log.error("[review-bg] Skepthical returned no markdown for %s", repo_name)
            review_md = "Review completed, but output format was unexpected."

        # Find review PDF
        review_pdf_path = ""
        pdf_files = sorted(glob.glob(os.path.join(work_dir, "reports_pdf", "Review_*.pdf")))
        if pdf_files:
            review_pdf_path = pdf_files[-1]

        log.info("[review-bg] Skepthical done for %s, publishing...", repo_name)

        # 4. Publish to a review-{repo_name} GitHub Pages repo
        publish_dir = os.path.join(work_dir, "publish")
        os.makedirs(publish_dir, exist_ok=True)

        shutil.copy2(pdf_path, os.path.join(publish_dir, "paper.pdf"))
        with open(os.path.join(publish_dir, "review.md"), "w") as f:
            f.write(review_md)
        if review_pdf_path and os.path.exists(review_pdf_path):
            shutil.copy2(review_pdf_path, os.path.join(publish_dir, "review.pdf"))
        if total_cost is not None:
            with open(os.path.join(publish_dir, "cost.json"), "w") as f:
                json.dump({"total_cost": total_cost}, f)
        if scores is not None:
            with open(os.path.join(publish_dir, "scores.json"), "w") as f:
                json.dump(scores, f)

        # Build the review page
        build_script = os.path.join(
            os.path.dirname(__file__), "..", "..", "scripts", "build_review_page.py"
        )

        # Use review-{original_repo_name} for exact 1:1 mapping
        review_repo_name = f"review-{repo_name}"

        token = os.environ.get("GITHUB_TOKEN", "")
        org = org_name

        def github_request(method, path, body=None, max_retries=4):
            """GitHub API call with retry on 5xx, 429, and network errors.

            GitHub occasionally returns 502/503 for a few seconds at a time.
            Without a retry the review is permanently dropped (the webhook
            handler already returned 202, so GitHub will not redeliver).
            """
            url = f"https://api.github.com{path}"
            data = json.dumps(body).encode() if body else None
            last_exc = None
            for attempt in range(max_retries):
                req = urllib.request.Request(url, data=data, method=method, headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "Content-Type": "application/json",
                })
                try:
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        return json.loads(resp.read()) if resp.length != 0 else {}
                except urllib.error.HTTPError as e:
                    if e.code in (429, 500, 502, 503, 504):
                        retry_after = e.headers.get("Retry-After") if hasattr(e, "headers") else None
                        try:
                            delay = float(retry_after) if retry_after else (2 ** attempt)
                        except ValueError:
                            delay = 2 ** attempt
                        log.warning("[review-bg] github %s %s -> HTTP %d, retry %d/%d in %.1fs",
                                    method, path, e.code, attempt + 1, max_retries, delay)
                        last_exc = e
                        _time.sleep(delay)
                        continue
                    raise
                except urllib.error.URLError as e:
                    log.warning("[review-bg] github %s %s -> network error: %s, retry %d/%d",
                                method, path, e, attempt + 1, max_retries)
                    last_exc = e
                    _time.sleep(2 ** attempt)
                    continue
            raise last_exc or RuntimeError(f"github_request: exhausted retries for {method} {path}")

        # Check if review repo already exists (shouldn't due to guard, but be safe)
        try:
            github_request("GET", f"/repos/{org}/{review_repo_name}")
            log.info("[review-bg] Review repo %s already exists, skipping publish", review_repo_name)
            return
        except urllib.error.HTTPError as e:
            if e.code != 404:
                log.error("[review-bg] GitHub API error: %s", e)
                return

        review_repo_url = f"https://github.com/{org}/{review_repo_name}"
        review_pages_url = f"https://{org.lower()}.github.io/{review_repo_name}/"

        # Create repo + populate it. Wrapped in a try/except so any failure
        # AFTER repo creation (build script crash, git push fail, etc.) deletes
        # the orphan repo from GitHub instead of leaving an empty/half-pushed
        # one that blocks future review attempts via the "review repo exists"
        # guard.
        repo_created = False
        try:
            github_request("POST", f"/orgs/{org}/repos", {
                "name": review_repo_name,
                "private": False,
                "description": f"Review: {title[:240]}",
            })
            repo_created = True

            _time.sleep(3)

            # Build page
            result = subprocess.run(
                [sys.executable, build_script, publish_dir,
                 "--repo-url", review_repo_url,
                 "--author", author,
                 "--title", title,
                 "--abstract", abstract,
                 "--paper-pages-url", pages_url],
                capture_output=True, text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"build_review_page.py failed: {result.stderr[:500]}"
                )

            # README
            with open(os.path.join(publish_dir, "README.md"), "w") as f:
                f.write(f"# Review: {title}\n\n**Reviewer:** Skepthical\n"
                        f"**Paper Author:** {author}\n\n"
                        f"**[View Review]({review_pages_url})**\n\n"
                        f"**[View Paper]({pages_url})**\n")

            with open(os.path.join(publish_dir, ".gitignore"), "w") as f:
                f.write("*.npz\n*.npy\n*.pkl\n*.h5\n*.hdf5\n*.csv\n__pycache__/\n")

            # Git push (mask token from any error output)
            git_name = os.environ.get("SCIENTIST_NAME", "skepthical")
            git_email = os.environ.get("GIT_EMAIL", f"{git_name}@parallelscience.org")
            url_with_token = f"https://{token}@github.com/{org}/{review_repo_name}.git"

            def git(*args):
                result = subprocess.run(
                    ["git"] + list(args), cwd=publish_dir,
                    capture_output=True, text=True,
                )
                if result.returncode != 0:
                    stderr = result.stderr.replace(token, "***") if token else result.stderr
                    raise RuntimeError(f"git {' '.join(args)}: {stderr}")

            git("init")
            git("config", "user.name", git_name)
            git("config", "user.email", git_email)
            git("add", "-A")
            git("commit", "-m", f"Review: {title[:120]}")
            git("branch", "-M", "main")
            git("remote", "add", "origin", url_with_token)
            git("-c", "credential.helper=", "push", "-u", "origin", "main")

            # Enable Pages — non-fatal if it fails, the repo is still useful.
            try:
                github_request("POST", f"/repos/{org}/{review_repo_name}/pages", {
                    "source": {"branch": "main", "path": "/docs"}
                })
            except Exception as exc:
                log.warning("[review-bg] enable Pages failed for %s: %s", review_repo_name, exc)

            log.info("[review-bg] Published review: %s -> %s", repo_name, review_repo_url)

        except Exception as publish_exc:
            log.error("[review-bg] Publish failed for %s: %s", repo_name, publish_exc)
            if repo_created:
                # Roll back the partial repo so the next webhook / cron run
                # can retry from a clean slate.
                try:
                    github_request("DELETE", f"/repos/{org}/{review_repo_name}")
                    log.info("[review-bg] Rolled back orphan repo %s", review_repo_name)
                except Exception as cleanup_exc:
                    log.error("[review-bg] FAILED to rollback orphan repo %s: %s — manual cleanup needed",
                              review_repo_name, cleanup_exc)
            raise

    except Exception as e:
        log.error("[review-bg] Review failed for %s: %s", repo_name, e)
        import traceback
        traceback.print_exc()
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        with _active_reviews_lock:
            _active_reviews.discard(repo_name)
        try:
            from review_browse.services.database import unregister_active_review
            unregister_active_review(repo_name)
        except Exception as exc:
            log.warning("[review-bg] unregister_active_review failed for %s: %s", repo_name, exc)


# ---------------------------------------------------------------------------
# Cron: safety-net full rescrape
# ---------------------------------------------------------------------------

@blueprint.route("/cron/rescrape", methods=["POST"])
def cron_rescrape() -> Response:
    """Re-scrape all review repos and sync DB to GCS.

    Intended to be called by Cloud Scheduler or similar cron. Authenticates
    with WEBHOOK_SECRET via either an `Authorization: Bearer <secret>` header
    or an `X-Cron-Secret: <secret>` header. Cloud Scheduler can be configured
    to send the bearer token; do NOT trust the User-Agent header for auth
    since it is attacker-controlled.
    """
    secret = current_app.config.get("WEBHOOK_SECRET", "")
    if not secret:
        log.error("WEBHOOK_SECRET not configured — rejecting cron rescrape")
        return Response("server misconfigured", status=500)

    auth_header = request.headers.get("Authorization", "")
    cron_secret_header = request.headers.get("X-Cron-Secret", "")

    expected_bearer = f"Bearer {secret}"
    is_bearer_match = hmac.compare_digest(auth_header, expected_bearer)
    is_header_match = hmac.compare_digest(cron_secret_header, secret)

    if not (is_bearer_match or is_header_match):
        log.warning("Cron rescrape: unauthorized request from %s", request.remote_addr)
        return Response("Unauthorized", status=403)

    from review_browse.services.database import get_db, sync_to_gcs
    from review_browse.services.scraper import scrape_all_repos

    org = current_app.config.get("GITHUB_ORG", "ParallelScience")
    gcs_bucket = current_app.config.get("GCS_BUCKET", "parallel-review")

    conn = get_db()
    counts = scrape_all_repos(conn, org=org, gcs_bucket=gcs_bucket)

    total_changes = counts.get("new", 0) + counts.get("updated", 0)
    if total_changes > 0:
        synced = sync_to_gcs()
        counts["gcs_synced"] = synced
        if not synced:
            log.error("GCS sync failed after cron rescrape")
    else:
        # Sync anyway as a heartbeat — ensures GCS has the latest DB
        sync_to_gcs()

    log.info("Cron rescrape complete: %s", counts)
    import json
    return Response(json.dumps(counts), status=200, mimetype="application/json")
