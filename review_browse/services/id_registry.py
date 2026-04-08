"""Persistent RX ID registry.

IDs are assigned once per repo and never change. Format: YYMM.NNNNN
(e.g., 2604.00001 = first review of April 2026).
"""

import re
import sqlite3


def get_or_assign_id(conn: sqlite3.Connection, repo: str, date: str) -> str:
    """Look up or assign an RX ID for the given repo."""
    row = conn.execute(
        "SELECT rx_id FROM id_registry WHERE repo = ?", (repo,)
    ).fetchone()
    if row:
        return row["rx_id"]

    yymm = _date_to_yymm(date)

    conn.execute(
        "INSERT INTO id_sequence (yymm, next_n) VALUES (?, 1) "
        "ON CONFLICT(yymm) DO NOTHING",
        (yymm,),
    )
    cur = conn.execute(
        "UPDATE id_sequence SET next_n = next_n + 1 WHERE yymm = ? RETURNING next_n - 1",
        (yymm,),
    )
    seq = cur.fetchone()[0]
    rx_id = f"{yymm}.{seq:05d}"

    conn.execute(
        "INSERT INTO id_registry (repo, rx_id, yymm) VALUES (?, ?, ?)",
        (repo, rx_id, yymm),
    )
    conn.commit()
    return rx_id


def get_id_for_repo(conn: sqlite3.Connection, repo: str) -> str | None:
    row = conn.execute(
        "SELECT rx_id FROM id_registry WHERE repo = ?", (repo,)
    ).fetchone()
    return row["rx_id"] if row else None


def _date_to_yymm(date_str: str) -> str:
    match = re.match(r"(\d{4})-(\d{2})", date_str)
    if match:
        return match.group(1)[2:] + match.group(2)
    return "0000"
