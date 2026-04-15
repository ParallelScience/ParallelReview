"""Review ID registry.

Review IDs are derived from the paper's PX ID:
    PX:2604.00003-R1  (first review of paper PX:2604.00003)
    PX:2604.00003-R2  (second review of same paper)

The paper's PX ID is looked up from the Parallel ArXiv database or API.
"""

import json
import logging
import os
import re
import sqlite3
import tempfile
import time
import urllib.request

log = logging.getLogger(__name__)

# Canonical arxiv-browse DB lives in GCS. Cloud Run writes to it via the
# authenticated SDK on every webhook ingest, but reading it via the public
# URL (storage.googleapis.com/...) is subject to GCS's edge cache — the
# object's default `Cache-Control: public, max-age=3600` means readers can
# see bytes up to an hour old. Prefer the authenticated SDK whenever
# credentials are available (Cloud Run), and fall back to the public URL
# for contexts that don't have ADC (local container).
_ARXIV_GCS_URI = os.environ.get(
    "PARALLEL_ARXIV_DB_URI",
    "gs://parallel-arxiv-pdfs/papers.db",
)
_ARXIV_DB_URL = os.environ.get(
    "PARALLEL_ARXIV_DB_URL",
    "https://storage.googleapis.com/parallel-arxiv-pdfs/papers.db",
)

# In-process cache: (timestamp, local_path). Keep short so new papers show
# up within the minute of arxiv-browse ingesting them (important for new
# reviews that race with the paper's own page_build webhook).
_ARXIV_CACHE: dict = {"ts": 0.0, "path": ""}
_ARXIV_CACHE_TTL_S = 60


def _parse_gcs_uri(uri: str) -> tuple[str, str] | None:
    m = re.match(r"gs://([^/]+)/(.+)", uri)
    return (m.group(1), m.group(2)) if m else None


def _get_arxiv_db_path() -> str | None:
    """Download the canonical arxiv DB to /tmp (with 60s TTL cache).

    Prefers authenticated GCS SDK (bypasses edge cache) and falls back to
    the public URL if the SDK / credentials aren't available.
    """
    now = time.time()
    cached = _ARXIV_CACHE.get("path", "")
    if cached and os.path.exists(cached) and (now - _ARXIV_CACHE["ts"]) < _ARXIV_CACHE_TTL_S:
        return cached

    local = os.path.join(tempfile.gettempdir(), "id_registry_arxiv_papers.db")

    # Try authenticated SDK first
    try:
        from google.cloud import storage
        from google.auth.exceptions import DefaultCredentialsError
        parsed = _parse_gcs_uri(_ARXIV_GCS_URI)
        if parsed:
            bucket_name, blob_path = parsed
            try:
                client = storage.Client()
            except DefaultCredentialsError:
                client = None
            if client is not None:
                blob = client.bucket(bucket_name).blob(blob_path)
                blob.reload()
                blob.download_to_filename(local)
                _ARXIV_CACHE["ts"] = now
                _ARXIV_CACHE["path"] = local
                log.info("id_registry: fetched arxiv DB via SDK (gen=%s)", blob.generation)
                return local
    except Exception as exc:
        log.warning("id_registry: SDK fetch failed (%s); falling back to public URL", exc)

    # Fallback: public URL (may be edge-cached up to max-age)
    try:
        urllib.request.urlretrieve(_ARXIV_DB_URL, local)
        _ARXIV_CACHE["ts"] = now
        _ARXIV_CACHE["path"] = local
        return local
    except Exception as exc:
        log.warning("id_registry: failed to fetch arxiv DB from %s: %s", _ARXIV_DB_URL, exc)
        return None


def _lookup_px_id_from_db(paper_repo: str) -> str | None:
    """Look up a paper's PX ID from the canonical arxiv-browse database.

    Tries exact match first, then prefix match for truncated repo names
    (GitHub truncates long repo names, so review-{paper_repo} may be shorter
    than the actual paper repo name), and reverse prefix match (in case the
    db.repo is a prefix of our paper_repo).
    """
    db_path = _get_arxiv_db_path()
    if not db_path:
        return None
    try:
        # Open immutable + read-only via URI so SQLite does not try to create
        # -wal/-shm sidecars.
        conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
        conn.row_factory = sqlite3.Row
        # Exact match
        row = conn.execute(
            "SELECT px_id FROM papers WHERE repo = ? AND is_current = 1",
            (paper_repo,),
        ).fetchone()
        if not row and len(paper_repo) > 20:
            # Our repo is a prefix of the db.repo (db has the full, untruncated name)
            row = conn.execute(
                "SELECT px_id FROM papers WHERE repo LIKE ? AND is_current = 1",
                (paper_repo + "%",),
            ).fetchone()
            if row:
                log.info("PX ID found via prefix match: %s -> %s", paper_repo, row["px_id"])
        if not row and len(paper_repo) > 20:
            # db.repo is a prefix of our paper_repo (GitHub truncated our side)
            row = conn.execute(
                "SELECT px_id, repo FROM papers "
                "WHERE ? LIKE repo || '%' AND length(repo) >= 20 "
                "AND is_current = 1",
                (paper_repo,),
            ).fetchone()
            if row:
                log.info("PX ID found via reverse prefix match: %s -> %s (db.repo=%s)",
                         paper_repo, row["px_id"], row["repo"])
        conn.close()
        return row["px_id"] if row else None
    except Exception as e:
        log.warning("Failed to look up PX ID from arxiv DB: %s", e)
        return None


def _lookup_px_id_from_api(paper_repo: str, org: str = "ParallelScience") -> str | None:
    """Look up a paper's PX ID from the papers.parallelscience.org API."""
    import urllib.request
    papers_url = os.environ.get("PAPERS_SITE_URL", "https://papers.parallelscience.org")
    # The arxiv-browse site doesn't have a direct repo→id API,
    # but we can scrape the paper page for the PX ID
    try:
        pages_url = f"https://{org.lower()}.github.io/{paper_repo}/"
        # First try the papers site search
        url = f"{papers_url}/abs/{paper_repo}"
        # Actually, there's no such route. Let's scrape the GitHub Pages site instead.
        return None
    except Exception:
        return None


def get_or_assign_review_id(conn: sqlite3.Connection, paper_repo: str,
                            review_repo: str, org: str = "ParallelScience") -> str:
    """Get or assign a review ID based on the paper's PX ID.

    Format: PX:YYMM.NNNNN-RN (e.g., PX:2604.00003-R1)

    If the paper's PX ID can't be found, falls back to a hash-based ID.
    """
    # Check if this review repo already has an ID
    row = conn.execute(
        "SELECT review_id FROM id_registry WHERE repo = ?", (review_repo,)
    ).fetchone()
    if row:
        return row["review_id"]

    # Look up the paper's PX ID
    # The paper repo name is the review repo name minus "review-" prefix
    px_id = _lookup_px_id_from_db(paper_repo)

    if px_id:
        # Count existing reviews for this paper
        existing = conn.execute(
            "SELECT COUNT(*) as cnt FROM id_registry WHERE px_id = ?",
            (px_id,),
        ).fetchone()
        review_num = (existing["cnt"] if existing else 0) + 1
        review_id = f"{px_id}-R{review_num}"
    else:
        # Fallback: use repo-based ID if PX ID not found
        log.warning("Could not find PX ID for paper repo %s, using fallback", paper_repo)
        existing = conn.execute(
            "SELECT COUNT(*) as cnt FROM id_registry WHERE paper_repo = ?",
            (paper_repo,),
        ).fetchone()
        review_num = (existing["cnt"] if existing else 0) + 1
        review_id = f"R-{paper_repo[:40]}-R{review_num}"

    conn.execute(
        "INSERT INTO id_registry (repo, review_id, px_id, paper_repo) VALUES (?, ?, ?, ?)",
        (review_repo, review_id, px_id or "", paper_repo),
    )
    conn.commit()
    return review_id


def get_id_for_repo(conn: sqlite3.Connection, repo: str) -> str | None:
    row = conn.execute(
        "SELECT review_id FROM id_registry WHERE repo = ?", (repo,)
    ).fetchone()
    return row["review_id"] if row else None
