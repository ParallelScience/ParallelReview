"""SQLite database for Parallel Review.

Same GCS sync pattern as arxiv-browse: DB stored in GCS,
downloaded to /tmp on Cloud Run startup, synced back after writes.
"""

import logging
import os
import re
import shutil
import sqlite3

from flask import Flask, g

log = logging.getLogger(__name__)

_DB_PATH: str = ""
_GCS_DB_URI: str = ""

SCHEMA_SQL = """
-- Review ID registry: maps review repo → review_id (PX:YYMM.NNNNN-RN)
CREATE TABLE IF NOT EXISTS id_registry (
    repo         TEXT PRIMARY KEY,
    review_id    TEXT NOT NULL UNIQUE,
    px_id        TEXT NOT NULL DEFAULT '',
    paper_repo   TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- Reviews table with version tracking
CREATE TABLE IF NOT EXISTS reviews (
    review_id           TEXT NOT NULL,
    version             INTEGER NOT NULL DEFAULT 1,
    paper_title         TEXT NOT NULL,
    paper_author        TEXT NOT NULL,
    review_date         TEXT NOT NULL,
    summary             TEXT NOT NULL DEFAULT '',
    strengths           TEXT NOT NULL DEFAULT '',
    major_issues        TEXT NOT NULL DEFAULT '',
    minor_issues        TEXT NOT NULL DEFAULT '',
    very_minor_issues   TEXT NOT NULL DEFAULT '',
    maths_audit         TEXT NOT NULL DEFAULT '',
    numerics_audit      TEXT NOT NULL DEFAULT '',
    reviewer            TEXT NOT NULL DEFAULT 'Skepthical',
    repo                TEXT NOT NULL,
    pages_url           TEXT NOT NULL,
    github_url          TEXT NOT NULL,
    review_pdf_url      TEXT NOT NULL DEFAULT '',
    paper_pdf_url       TEXT NOT NULL DEFAULT '',
    paper_pages_url     TEXT NOT NULL DEFAULT '',
    px_id               TEXT NOT NULL DEFAULT '',
    is_current          INTEGER NOT NULL DEFAULT 1,
    total_cost          REAL,
    score_overall       REAL,
    score_soundness     INTEGER,
    score_novelty       INTEGER,
    score_significance  INTEGER,
    score_clarity       INTEGER,
    score_evidence      INTEGER,
    score_justification TEXT NOT NULL DEFAULT '',
    scraped_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    content_hash        TEXT,
    PRIMARY KEY (review_id, version),
    FOREIGN KEY (repo) REFERENCES id_registry(repo)
);

CREATE INDEX IF NOT EXISTS idx_reviews_current ON reviews(is_current) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS idx_reviews_author ON reviews(paper_author) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS idx_reviews_repo ON reviews(repo);
CREATE INDEX IF NOT EXISTS idx_reviews_px_id ON reviews(px_id) WHERE is_current = 1;

-- In-flight review tracking. Persisted so the concurrency guard survives
-- container restarts (an in-memory set leaks slots when a daemon thread dies
-- mid-review). Cleaned up at startup; entries older than the staleness window
-- are also GC'd by gc_stale_active_reviews().
CREATE TABLE IF NOT EXISTS active_reviews (
    repo         TEXT PRIMARY KEY,
    started_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    pid          INTEGER NOT NULL DEFAULT 0,
    hostname     TEXT NOT NULL DEFAULT ''
);
"""


def get_db_path() -> str:
    return _DB_PATH


def get_db() -> sqlite3.Connection:
    if "rx_db" not in g:
        g.rx_db = _connect()
    return g.rx_db


def get_db_standalone() -> sqlite3.Connection:
    return _connect()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def close_db(e=None) -> None:
    db = g.pop("rx_db", None)
    if db is not None:
        db.close()


# ---------------------------------------------------------------------------
# GCS sync
# ---------------------------------------------------------------------------

def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    m = re.match(r"gs://([^/]+)/(.+)", uri)
    if not m:
        raise ValueError(f"Invalid GCS URI: {uri}")
    return m.group(1), m.group(2)


def _gcs_public_url() -> str:
    bucket_name, blob_path = _parse_gcs_uri(_GCS_DB_URI)
    return f"https://storage.googleapis.com/{bucket_name}/{blob_path}"


def _download_from_gcs() -> bool:
    """Download from GCS to _DB_PATH."""
    return _download_from_gcs_to(_DB_PATH)


def _download_from_gcs_to(dest: str) -> bool:
    """Download the DB from GCS to a specific path.

    Prefers the authenticated google-cloud-storage SDK because the public
    URL (`https://storage.googleapis.com/...`) is served through GCS's edge
    cache, which honors the object's `Cache-Control: public, max-age=3600`
    and can serve stale bytes for up to an hour after an upload. Using the
    SDK bypasses the edge cache entirely and always returns the current
    object generation, so a fresh cold-start sees a fresh DB.

    Falls back to the public URL if the SDK or credentials aren't available
    (e.g. local container without ADC) — the edge-cache staleness is
    tolerable there because the local container isn't user-facing.
    """
    if not _GCS_DB_URI:
        return False

    # Prefer authenticated SDK path
    try:
        from google.cloud import storage
        from google.auth.exceptions import DefaultCredentialsError
        try:
            client = storage.Client()
        except DefaultCredentialsError:
            client = None
        if client is not None:
            bucket_name, blob_path = _parse_gcs_uri(_GCS_DB_URI)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            blob.reload()  # fetches the current generation
            generation = blob.generation
            print(f"[RX] Downloading DB via SDK: gs://{bucket_name}/{blob_path} "
                  f"(gen={generation}) -> {dest}", flush=True)
            blob.download_to_filename(dest)
            size = os.path.getsize(dest)
            c = sqlite3.connect(dest)
            count = c.execute("SELECT count(*) FROM reviews").fetchone()[0]
            c.close()
            print(f"[RX] Downloaded DB: {size} bytes, {count} reviews "
                  f"(gen={generation})", flush=True)
            return True
    except Exception as exc:
        print(f"[RX] SDK download path failed ({exc}); falling back to public URL",
              flush=True)

    # Fallback: public URL (subject to edge cache)
    try:
        import urllib.request
        url = _gcs_public_url()
        print(f"[RX] Downloading DB from {url} -> {dest}  (public URL — may be cached)",
              flush=True)
        urllib.request.urlretrieve(url, dest)
        size = os.path.getsize(dest)
        c = sqlite3.connect(dest)
        count = c.execute("SELECT count(*) FROM reviews").fetchone()[0]
        c.close()
        print(f"[RX] Downloaded DB: {size} bytes, {count} reviews", flush=True)
        return True
    except Exception as exc:
        print(f"[RX] FAILED to download DB from GCS: {exc}", flush=True)
        return False


def sync_to_gcs() -> bool:
    """Atomically upload the local SQLite DB to GCS.

    A direct overwrite of the canonical blob is unsafe: a network failure
    mid-upload would leave a truncated/corrupt blob, and the next cold start
    would download that and the app would refuse to open it. We instead:

      1. Checkpoint WAL into the main DB file.
      2. Validate the local DB with `PRAGMA integrity_check`.
      3. Upload to a temporary blob `<path>.tmp.<pid>`.
      4. Server-side copy `tmp -> canonical` with `if_generation_match=0`
         deferred — we just rewrite to the canonical path which is atomic
         from the perspective of any concurrent reader.
      5. Delete the temp blob.

    Any failure short-circuits and the canonical blob remains untouched.
    """
    if not _GCS_DB_URI:
        log.warning("[RX] sync_to_gcs called but GCS_DB_URI is not set — reviews will NOT persist across deploys!")
        return False

    # 1. Checkpoint WAL so the main DB file contains everything.
    conn = _connect()
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()

    # 2. Validate the local DB before we ship it.
    try:
        conn = sqlite3.connect(_DB_PATH)
        try:
            row = conn.execute("PRAGMA integrity_check").fetchone()
        finally:
            conn.close()
        if not row or row[0] != "ok":
            log.error("[RX] sync_to_gcs aborted: integrity_check returned %r", row)
            return False
    except Exception as exc:
        log.error("[RX] sync_to_gcs aborted: integrity_check raised: %s", exc, exc_info=True)
        return False

    try:
        from google.cloud import storage
        from google.auth.exceptions import DefaultCredentialsError
    except ImportError as exc:
        log.warning("[RX] sync_to_gcs: google-cloud-storage not installed: %s", exc)
        return False

    try:
        bucket_name, blob_path = _parse_gcs_uri(_GCS_DB_URI)
        try:
            client = storage.Client()
        except DefaultCredentialsError as exc:
            # Local container has no service-account credentials. Cloud Run is
            # the canonical writer of the GCS DB; the local container only
            # needs its own SQLite copy. Return True so the caller does not
            # treat this as an error and emit 5xx (which would cause GitHub to
            # retry-storm us with deliveries we cannot satisfy).
            log.info("[RX] sync_to_gcs: no GCP credentials, skipping (this is normal "
                     "for the local container; Cloud Run handles the canonical sync)")
            return True
        bucket = client.bucket(bucket_name)
        tmp_path = f"{blob_path}.tmp.{os.getpid()}"
        tmp_blob = bucket.blob(tmp_path)

        # 3. Upload to a temporary blob first.
        tmp_blob.upload_from_filename(_DB_PATH)
        size = os.path.getsize(_DB_PATH)

        # 4. Server-side copy to the canonical name. GCS object writes are
        # atomic from a reader's perspective: a concurrent download will see
        # either the old version or the new one, never a partial/torn write.
        bucket.copy_blob(tmp_blob, bucket, new_name=blob_path)

        # 5. Best-effort cleanup of the temp blob; failure is non-fatal.
        try:
            tmp_blob.delete()
        except Exception as exc:
            log.warning("[RX] sync_to_gcs: failed to delete temp blob %s: %s", tmp_path, exc)

        print(f"[RX] Uploaded DB ({size} bytes) to gs://{bucket_name}/{blob_path}", flush=True)
        return True
    except Exception as exc:
        log.error("[RX] FAILED to upload DB to GCS: %s", exc, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

def init_db(app: Flask) -> None:
    """Initialize the database for the Flask app.

    Resolution order for the DB file:
    1. If RX_DATABASE_PATH is writable (local dev / Docker volume), use it directly
    2. If GCS_DB_URI is set and path is read-only (Cloud Run), download to /tmp
    3. Else copy baked-in DB to /tmp as last resort
    """
    global _DB_PATH, _GCS_DB_URI

    _GCS_DB_URI = app.config.get("GCS_DB_URI", "") or os.environ.get("GCS_DB_URI", "")
    source_path = app.config.get(
        "RX_DATABASE_PATH",
        os.path.join(os.path.dirname(__file__), "..", "data", "reviews.db"),
    )
    print(f"[RX] init_db: GCS_DB_URI={_GCS_DB_URI!r}, source={source_path}", flush=True)

    # Prefer the configured path if its parent directory is writable
    # (local dev or Docker volume mount at /app/review_browse/data)
    parent = os.path.dirname(source_path) or "."
    if os.access(parent, os.W_OK):
        _DB_PATH = source_path
        print(f"[RX] Using writable path: {_DB_PATH}", flush=True)
        # If the DB doesn't exist yet but GCS has one, seed from GCS
        if not os.path.exists(_DB_PATH) and _GCS_DB_URI:
            _download_from_gcs_to(_DB_PATH)
    elif _GCS_DB_URI:
        # Cloud Run: app dir is read-only, work in /tmp with GCS sync
        _DB_PATH = "/tmp/reviews.db"
        if not os.path.exists(_DB_PATH):
            if not _download_from_gcs():
                if os.path.exists(source_path):
                    shutil.copy2(source_path, _DB_PATH)
                    print(f"[RX] Seeded /tmp DB from baked-in {source_path}", flush=True)
                else:
                    print("[RX] No baked-in DB either, creating fresh", flush=True)
        else:
            print(f"[RX] /tmp/reviews.db already exists ({os.path.getsize(_DB_PATH)} bytes), reusing", flush=True)
    else:
        # Fallback: read-only source, no GCS
        _DB_PATH = "/tmp/reviews.db"
        if os.path.exists(source_path) and not os.path.exists(_DB_PATH):
            shutil.copy2(source_path, _DB_PATH)

    os.makedirs(os.path.dirname(_DB_PATH) or ".", exist_ok=True)

    conn = _connect()
    try:
        conn.executescript(SCHEMA_SQL)
        # Migrate: add columns if missing
        cols = {r[1] for r in conn.execute("PRAGMA table_info(reviews)").fetchall()}
        migrations = [
            ("total_cost", "ALTER TABLE reviews ADD COLUMN total_cost REAL"),
            ("score_overall", "ALTER TABLE reviews ADD COLUMN score_overall REAL"),
            ("score_soundness", "ALTER TABLE reviews ADD COLUMN score_soundness INTEGER"),
            ("score_novelty", "ALTER TABLE reviews ADD COLUMN score_novelty INTEGER"),
            ("score_significance", "ALTER TABLE reviews ADD COLUMN score_significance INTEGER"),
            ("score_clarity", "ALTER TABLE reviews ADD COLUMN score_clarity INTEGER"),
            ("score_evidence", "ALTER TABLE reviews ADD COLUMN score_evidence INTEGER"),
            ("score_justification", "ALTER TABLE reviews ADD COLUMN score_justification TEXT NOT NULL DEFAULT ''"),
        ]
        for col_name, sql in migrations:
            if col_name not in cols:
                conn.execute(sql)
                print(f"[RX] Migrated: added {col_name} column to reviews", flush=True)
        conn.commit()

        # Backfill: recompute score_overall as mean of the 5 dimension scores
        # (soundness, novelty, significance, clarity, evidence), rounded to 1
        # decimal. Matches the write-time logic in scraper.compute_overall so
        # existing rows get the same aggregation as new ones. Idempotent:
        # running with already-correct values is a no-op.
        cur = conn.execute(
            "UPDATE reviews "
            "SET score_overall = round("
            "  (score_soundness + score_novelty + score_significance "
            "   + score_clarity + score_evidence) / 5.0, 1) "
            "WHERE score_soundness IS NOT NULL "
            "  AND score_novelty IS NOT NULL "
            "  AND score_significance IS NOT NULL "
            "  AND score_clarity IS NOT NULL "
            "  AND score_evidence IS NOT NULL "
            "  AND (score_overall IS NULL OR score_overall != round("
            "       (score_soundness + score_novelty + score_significance "
            "        + score_clarity + score_evidence) / 5.0, 1))"
        )
        conn.commit()
        if cur.rowcount:
            print(f"[RX] Backfilled score_overall on {cur.rowcount} rows", flush=True)
    finally:
        conn.close()

    # Verify GCS sync capability at startup
    if _GCS_DB_URI:
        try:
            from google.cloud import storage  # noqa: F401
            print(f"[RX] GCS sync ENABLED: {_GCS_DB_URI}", flush=True)
        except ImportError:
            log.error("[RX] google-cloud-storage NOT installed — GCS sync DISABLED, reviews WILL be lost on redeploy!")

    app.teardown_appcontext(close_db)
    log.info("Initialized reviews database at %s (GCS: %s)", _DB_PATH, _GCS_DB_URI or "none")


# ---------------------------------------------------------------------------
# Active-review tracking (persistent concurrency guard)
# ---------------------------------------------------------------------------
#
# The webhook handler spawns a daemon thread per Skepthical review, and tracks
# in-flight reviews in an in-memory set so it can enforce a concurrency cap and
# de-duplicate webhook redeliveries. The set is lost on container restart, and
# daemon threads die ungracefully on SIGTERM, so without persistence:
#   * Slots can leak forever (the in-memory cap drifts)
#   * Restarted containers can re-launch reviews for papers that are mid-flight
#     elsewhere (rare with one container; impossible to detect anyway)
#
# We persist a row per in-flight review keyed by paper repo. On startup we wipe
# any rows belonging to a previous PID — they cannot still be running because
# their owning process is gone — and rebuild the in-memory state from what is
# left. This bounds the worst-case stale-slot lifetime to "until next restart".

def register_active_review(repo: str) -> None:
    conn = _connect()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO active_reviews (repo, started_at, pid, hostname) "
            "VALUES (?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), ?, ?)",
            (repo, os.getpid(), os.uname().nodename),
        )
        conn.commit()
    finally:
        conn.close()


def unregister_active_review(repo: str) -> None:
    conn = _connect()
    try:
        conn.execute("DELETE FROM active_reviews WHERE repo = ?", (repo,))
        conn.commit()
    finally:
        conn.close()


def reset_active_reviews_for_pid(current_pid: int) -> int:
    """Wipe any active_reviews rows that don't belong to the current process.

    Called once at startup. Returns the number of stale rows deleted.
    """
    conn = _connect()
    try:
        cur = conn.execute("DELETE FROM active_reviews WHERE pid != ?", (current_pid,))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def list_active_reviews() -> list[dict]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT repo, started_at, pid, hostname FROM active_reviews ORDER BY started_at"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def init_standalone(db_path: str) -> None:
    global _DB_PATH
    _DB_PATH = db_path
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = _connect()
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()
