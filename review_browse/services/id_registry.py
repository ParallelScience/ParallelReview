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

log = logging.getLogger(__name__)

# Path to arxiv-browse DB (local fallback for PX ID lookup)
_ARXIV_DB = os.environ.get(
    "PARALLEL_ARXIV_DB",
    os.path.join(os.path.dirname(__file__), "..", "..", "..",
                 "arxiv-browse", "browse", "data", "papers.db"),
)


def _lookup_px_id_from_db(paper_repo: str) -> str | None:
    """Look up a paper's PX ID from the local arxiv-browse database."""
    if not os.path.exists(_ARXIV_DB):
        return None
    try:
        conn = sqlite3.connect(_ARXIV_DB)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT px_id FROM papers WHERE repo = ? AND is_current = 1",
            (paper_repo,),
        ).fetchone()
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
