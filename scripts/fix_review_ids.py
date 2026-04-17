"""One-shot migration: re-assign broken `R-{repo-slug}-R1` review IDs to the
canonical `{px_id}-R{version}` scheme.

The bug that produced them: `id_registry._lookup_px_id_from_db` used to read
from a stale local file path inside the container. When that file didn't
exist, every review got a hash-fallback ID keyed off the (truncated) paper
repo name instead of the paper's real Parallel ArXiv PX ID.

This script:
  1. Downloads gs://parallel-review/reviews.db (the canonical review DB).
  2. Downloads gs://parallel-arxiv-pdfs/papers.db (the canonical arxiv DB).
  3. For every row in id_registry whose px_id is empty OR whose review_id
     starts with "R-" (the broken scheme), looks up the real px_id from
     arxiv-papers by paper_repo (exact + both-direction prefix matches).
  4. Rewrites the id_registry row and every matching reviews row to use
     "{px_id}-R{version}" as the canonical review_id.
  5. Uploads the fixed review DB back to gs://parallel-review/reviews.db.

Run from the host (requires gsutil auth) OR from inside the parallel-review
container (requires GCP credentials; local container has none, so run on host).
"""

import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import urllib.request

ARXIV_DB_URL = "https://storage.googleapis.com/parallel-arxiv-pdfs/papers.db"
REVIEW_GCS = "gs://parallel-review/reviews.db"


def _download(url: str, dest: str) -> None:
    print(f"  downloading {url} -> {dest}", flush=True)
    urllib.request.urlretrieve(url, dest)
    size = os.path.getsize(dest)
    print(f"  got {size} bytes")


def _gsutil_cp_down(src: str, dest: str) -> None:
    print(f"  gsutil cp {src} {dest}", flush=True)
    subprocess.run(["gsutil", "cp", src, dest], check=True)


def _gsutil_cp_up(src: str, dest: str) -> None:
    print(f"  gsutil cp {src} {dest}", flush=True)
    subprocess.run(["gsutil", "cp", src, dest], check=True)


def lookup_px_id(arxiv_conn: sqlite3.Connection, paper_repo: str, paper_title: str | None = None) -> str | None:
    # Exact match first
    row = arxiv_conn.execute(
        "SELECT px_id FROM papers WHERE repo = ? AND is_current = 1",
        (paper_repo,),
    ).fetchone()
    if row:
        return row["px_id"]
    # Forward prefix (our repo is short, db.repo is long)
    if len(paper_repo) > 15:
        row = arxiv_conn.execute(
            "SELECT px_id, repo FROM papers WHERE repo LIKE ? AND is_current = 1 LIMIT 1",
            (paper_repo + "%",),
        ).fetchone()
        if row:
            return row["px_id"]
    # Reverse prefix (db.repo is short, our repo is long — happens when the
    # old title-derived slug publisher stored a slugified title as paper_repo
    # but the canonical paper_repo in arxiv is something unrelated like
    # "preprint" — so this path rarely helps for those cases).
    if len(paper_repo) > 15:
        row = arxiv_conn.execute(
            "SELECT px_id, repo FROM papers "
            "WHERE ? LIKE repo || '%' AND length(repo) >= 15 AND is_current = 1 "
            "LIMIT 1",
            (paper_repo,),
        ).fetchone()
        if row:
            return row["px_id"]
    # Title match — rescues reviews whose stored `paper_repo` is actually a
    # slugified title (a legacy bug from the old publisher) instead of the
    # real paper repo. The reviews.paper_title column holds the canonical
    # title scraped from the review page, which should match arxiv.title.
    if paper_title:
        row = arxiv_conn.execute(
            "SELECT px_id, repo FROM papers WHERE title = ? AND is_current = 1 LIMIT 1",
            (paper_title,),
        ).fetchone()
        if row:
            return row["px_id"]
    return None


def migrate(dry_run: bool = True) -> None:
    tmp_reviews = os.path.join(tempfile.gettempdir(), "migrate_reviews.db")
    tmp_arxiv = os.path.join(tempfile.gettempdir(), "migrate_arxiv.db")

    print("=== 1. Download DBs ===")
    _gsutil_cp_down(REVIEW_GCS, tmp_reviews)
    _download(ARXIV_DB_URL, tmp_arxiv)

    rconn = sqlite3.connect(tmp_reviews)
    rconn.row_factory = sqlite3.Row
    rconn.execute("PRAGMA foreign_keys = OFF")  # we'll rewrite PKs

    aconn = sqlite3.connect(f"file:{tmp_arxiv}?mode=ro&immutable=1", uri=True)
    aconn.row_factory = sqlite3.Row

    print("\n=== 2. Scan id_registry for broken rows ===")
    rows = rconn.execute(
        "SELECT ir.repo, ir.review_id, ir.px_id, ir.paper_repo, r.paper_title "
        "FROM id_registry ir "
        "LEFT JOIN reviews r ON r.review_id = ir.review_id AND r.is_current = 1 "
        "WHERE ir.review_id LIKE 'R-%' OR ir.px_id = ''"
    ).fetchall()
    print(f"  found {len(rows)} candidate rows to migrate")

    migrations: list[dict] = []
    for r in rows:
        old_review_id = r["review_id"]
        paper_repo = r["paper_repo"] or (r["repo"][len("review-"):] if r["repo"].startswith("review-") else r["paper_repo"])
        paper_title = r["paper_title"]
        real_px_id = lookup_px_id(aconn, paper_repo, paper_title=paper_title)
        if not real_px_id:
            print(f"  SKIP {old_review_id}: no px_id found for paper_repo={paper_repo!r} title={paper_title!r}")
            continue

        # Compute the new review_id. If the same px_id has multiple reviews,
        # preserve the R-number from the old id (typically R1); otherwise
        # assign R1.
        m = re.search(r"-R(\d+)$", old_review_id)
        review_num = int(m.group(1)) if m else 1
        new_review_id = f"{real_px_id}-R{review_num}"

        # Check for collisions with existing px_id-based reviews
        existing = rconn.execute(
            "SELECT review_id FROM id_registry WHERE review_id = ? AND repo != ?",
            (new_review_id, r["repo"]),
        ).fetchone()
        if existing:
            # Collision — bump the review number
            n = 2
            while rconn.execute(
                "SELECT 1 FROM id_registry WHERE review_id = ? AND repo != ?",
                (f"{real_px_id}-R{n}", r["repo"]),
            ).fetchone():
                n += 1
            new_review_id = f"{real_px_id}-R{n}"

        migrations.append({
            "repo": r["repo"],
            "old_review_id": old_review_id,
            "new_review_id": new_review_id,
            "px_id": real_px_id,
            "paper_repo": paper_repo,
        })
        print(f"  MIGRATE {r['repo']}: {old_review_id} -> {new_review_id}")

    if not migrations:
        print("\n  nothing to do.")
        return

    if dry_run:
        print(f"\n=== DRY RUN: would migrate {len(migrations)} rows. Re-run with --apply to write. ===")
        return

    print(f"\n=== 3. Apply {len(migrations)} migrations ===")
    rconn.execute("BEGIN IMMEDIATE")
    try:
        for m in migrations:
            # Update reviews table first (references id_registry.repo via FK,
            # but we need new review_id in the rows)
            rconn.execute(
                "UPDATE reviews SET review_id = ?, px_id = ? WHERE review_id = ?",
                (m["new_review_id"], m["px_id"], m["old_review_id"]),
            )
            # Update id_registry row
            rconn.execute(
                "UPDATE id_registry SET review_id = ?, px_id = ?, paper_repo = ? WHERE repo = ?",
                (m["new_review_id"], m["px_id"], m["paper_repo"], m["repo"]),
            )
        rconn.commit()
        print("  commit OK")
    except Exception:
        rconn.rollback()
        raise

    # Post-migration sanity check
    remaining = rconn.execute(
        "SELECT count(*) FROM id_registry WHERE review_id LIKE 'R-%' OR px_id = ''"
    ).fetchone()[0]
    print(f"  remaining broken rows: {remaining}")

    # Integrity check before upload
    integrity = rconn.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"  integrity_check = {integrity}")
    if integrity != "ok":
        raise RuntimeError(f"integrity_check failed: {integrity}")

    # Checkpoint WAL into main file
    rconn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    rconn.close()
    aconn.close()

    print("\n=== 4. Upload fixed DB to GCS ===")
    _gsutil_cp_up(tmp_reviews, REVIEW_GCS)

    print("\n=== Done. Force a Cloud Run cold-start to pick up the new DB. ===")


if __name__ == "__main__":
    apply = "--apply" in sys.argv
    migrate(dry_run=not apply)
