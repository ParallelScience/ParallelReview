"""WSGI entry point for gunicorn."""
from review_browse.factory import create_app

app = create_app()
