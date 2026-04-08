#!/usr/bin/env python3
"""CLI scraper: scan all review repos in the ParallelScience org."""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from review_browse.services.database import init_standalone, get_db_standalone
from review_browse.services.scraper import scrape_all_repos


def main():
    parser = argparse.ArgumentParser(description="Scrape all review repos")
    parser.add_argument("--org", default="ParallelScience")
    parser.add_argument("--db", default="review_browse/data/reviews.db")
    parser.add_argument("--no-pdf", action="store_true")
    parser.add_argument("--gcs-bucket", default="parallel-review")
    args = parser.parse_args()

    init_standalone(args.db)
    conn = get_db_standalone()

    counts = scrape_all_repos(
        conn, org=args.org,
        skip_pdf=args.no_pdf,
        gcs_bucket=args.gcs_bucket if not args.no_pdf else None,
    )
    conn.close()

    print(f"\nDone: {counts}")
    total_changes = counts.get("new", 0) + counts.get("updated", 0)
    sys.exit(0 if total_changes >= 0 else 1)


if __name__ == "__main__":
    main()
