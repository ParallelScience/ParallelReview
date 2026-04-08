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
-- Persistent ID registry: append-only, one row per repo, never deleted
CREATE TABLE IF NOT EXISTS id_registry (
    repo        TEXT PRIMARY KEY,
    rx_id       TEXT NOT NULL UNIQUE,
    yymm        TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- Next sequence number per month
CREATE TABLE IF NOT EXISTS id_sequence (
    yymm    TEXT PRIMARY KEY,
    next_n  INTEGER NOT NULL DEFAULT 1
);

-- Reviews table with version tracking
CREATE TABLE IF NOT EXISTS reviews (
    rx_id               TEXT NOT NULL,
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
    is_current          INTEGER NOT NULL DEFAULT 1,
    scraped_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    content_hash        TEXT,
    PRIMARY KEY (rx_id, version),
    FOREIGN KEY (repo) REFERENCES id_registry(repo)
);

CREATE INDEX IF NOT EXISTS idx_reviews_current ON reviews(is_current) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS idx_reviews_author ON reviews(paper_author) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS idx_reviews_repo ON reviews(repo);
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
    if not _GCS_DB_URI:
        return False
    try:
        import urllib.request
        url = _gcs_public_url()
        print(f"[RX] Downloading DB from {url}", flush=True)
        urllib.request.urlretrieve(url, _DB_PATH)
        size = os.path.getsize(_DB_PATH)
        c = sqlite3.connect(_DB_PATH)
        count = c.execute("SELECT count(*) FROM reviews").fetchone()[0]
        c.close()
        print(f"[RX] Downloaded DB: {size} bytes, {count} reviews", flush=True)
        return True
    except Exception as exc:
        print(f"[RX] FAILED to download DB from GCS: {exc}", flush=True)
        return False


def sync_to_gcs() -> bool:
    if not _GCS_DB_URI:
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
        print(f"[RX] Uploaded DB to gs://{bucket_name}/{blob_path}", flush=True)
        return True
    except Exception as exc:
        print(f"[RX] FAILED to upload DB to GCS: {exc}", flush=True)
        return False


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

def init_db(app: Flask) -> None:
    global _DB_PATH, _GCS_DB_URI

    _GCS_DB_URI = app.config.get("GCS_DB_URI", "") or os.environ.get("GCS_DB_URI", "")
    source_path = app.config.get(
        "RX_DATABASE_PATH",
        os.path.join(os.path.dirname(__file__), "..", "data", "reviews.db"),
    )

    if _GCS_DB_URI:
        _DB_PATH = "/tmp/reviews.db"
        if not os.path.exists(_DB_PATH):
            if not _download_from_gcs():
                if os.path.exists(source_path):
                    shutil.copy2(source_path, _DB_PATH)
    else:
        parent = os.path.dirname(source_path) or "."
        if os.access(parent, os.W_OK):
            _DB_PATH = source_path
        else:
            _DB_PATH = "/tmp/reviews.db"
            if os.path.exists(source_path) and not os.path.exists(_DB_PATH):
                shutil.copy2(source_path, _DB_PATH)

    os.makedirs(os.path.dirname(_DB_PATH) or ".", exist_ok=True)

    conn = _connect()
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()

    app.teardown_appcontext(close_db)
    log.info("Initialized reviews database at %s", _DB_PATH)


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
