"""Configuration for Parallel Review."""

import os


class Settings:
    """App configuration loaded from environment variables."""

    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key")
    RX_DATABASE_PATH = os.environ.get(
        "RX_DATABASE_PATH",
        os.path.join(os.path.dirname(__file__), "data", "reviews.db"),
    )
    WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
    GCS_DB_URI = os.environ.get("GCS_DB_URI", "")  # e.g. gs://parallel-review/reviews.db
    GCS_BUCKET = os.environ.get("GCS_BUCKET", "parallel-review")
    GITHUB_ORG = os.environ.get("GITHUB_ORG", "ParallelScience")
    PAPERS_SITE_URL = os.environ.get("PAPERS_SITE_URL", "https://papers.parallelscience.org")
