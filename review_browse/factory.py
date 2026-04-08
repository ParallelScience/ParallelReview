"""Application factory for Parallel Review."""

import logging
from flask import Flask

from review_browse.config import Settings
from review_browse.routes import ui, webhook
from review_browse.services.database import init_db


def create_app(**kwargs) -> Flask:
    """Initialize the Parallel Review web application."""
    logging.basicConfig(level=logging.INFO)

    settings = Settings()

    app = Flask("review_browse",
                static_folder="static",
                template_folder="templates")
    app.config.from_object(settings)

    init_db(app)
    app.register_blueprint(ui.blueprint)
    app.register_blueprint(webhook.blueprint)

    app.jinja_env.trim_blocks = True
    app.jinja_env.lstrip_blocks = True

    return app
