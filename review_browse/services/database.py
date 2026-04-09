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
    scraped_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    content_hash        TEXT,
    PRIMARY KEY (review_id, version),
    FOREIGN KEY (repo) REFERENCES id_registry(repo)
);

CREATE INDEX IF NOT EXISTS idx_reviews_current ON reviews(is_current) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS idx_reviews_author ON reviews(paper_author) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS idx_reviews_repo ON reviews(repo);
CREATE INDEX IF NOT EXISTS idx_reviews_px_id ON reviews(px_id) WHERE is_current = 1;
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
    """Download the DB from GCS to a specific path via public URL."""
    if not _GCS_DB_URI:
        return False
    try:
        import urllib.request
        url = _gcs_public_url()
        print(f"[RX] Downloading DB from {url} -> {dest}", flush=True)
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
    if not _GCS_DB_URI:
        log.warning("[RX] sync_to_gcs called but GCS_DB_URI is not set — reviews will NOT persist across deploys!")
        return False
    conn = _connect()
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    try:
        from google.cloud import storage
        bucket_name, blob_path = _parse_gcs_uri(_GCS_DB_URI)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(_DB_PATH)
        size = os.path.getsize(_DB_PATH)
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
        # Migrate: add total_cost column if missing
        cols = {r[1] for r in conn.execute("PRAGMA table_info(reviews)").fetchall()}
        if "total_cost" not in cols:
            conn.execute("ALTER TABLE reviews ADD COLUMN total_cost REAL")
            print("[RX] Migrated: added total_cost column to reviews", flush=True)
        conn.commit()
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
