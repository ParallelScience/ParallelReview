"""Query layer for reviews stored in SQLite."""

import re
import sqlite3
from typing import Optional

from review_browse.services.database import get_db


def _row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row)


def get_all_current_reviews() -> list[dict]:
    db = get_db()
    rows = db.execute(
        "SELECT * FROM reviews WHERE is_current = 1 ORDER BY review_date DESC"
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_reviews_by_author(author: str) -> list[dict]:
    """Return all current reviews for papers by a given author."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM reviews WHERE is_current = 1 AND lower(paper_author) = lower(?) "
        "ORDER BY review_date DESC",
        (author,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_reviews_by_paper(px_id: str) -> list[dict]:
    """Return all current reviews for a given paper PX ID."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM reviews WHERE is_current = 1 AND px_id = ? "
        "ORDER BY review_date DESC",
        (px_id,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_review_by_id(review_id: str, version: int | None = None) -> Optional[dict]:
    db = get_db()
    if version is not None:
        row = db.execute(
            "SELECT * FROM reviews WHERE review_id = ? AND version = ?",
            (review_id, version),
        ).fetchone()
    else:
        row = db.execute(
            "SELECT * FROM reviews WHERE review_id = ? AND is_current = 1",
            (review_id,),
        ).fetchone()
    return _row_to_dict(row) if row else None


def get_review_versions(review_id: str) -> list[dict]:
    db = get_db()
    rows = db.execute(
        "SELECT * FROM reviews WHERE review_id = ? ORDER BY version",
        (review_id,),
    ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_review_count() -> int:
    db = get_db()
    row = db.execute("SELECT count(*) FROM reviews WHERE is_current = 1").fetchone()
    return row[0] if row else 0


def count_issues(review: dict) -> dict:
    """Count issues by severity from stored text fields."""
    def _count(text: str) -> int:
        if not text:
            return 0
        return len(re.findall(r"^\d+\.", text, re.MULTILINE))

    return {
        "major": _count(review.get("major_issues", "")),
        "minor": _count(review.get("minor_issues", "")),
        "very_minor": _count(review.get("very_minor_issues", "")),
    }
