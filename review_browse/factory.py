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

    # Template filter: parse issue text into structured items
    import re as _re

    def parse_issues(text: str) -> list[dict]:
        """Parse numbered issue text into [{title, recommendation}, ...].

        Input format (from Skepthical):
          1.  **Issue title text...**
              continuation lines...

              *Recommendation:* Fix the thing by doing X...
              continuation of recommendation...

          2.  **Next issue...**
        """
        items: list[dict] = []
        current_title_lines: list[str] = []
        current_rec_lines: list[str] = []
        in_rec = False

        def _flush():
            if current_title_lines:
                title = " ".join(current_title_lines).strip()
                # Strip bold markdown markers
                title = _re.sub(r'\*\*', '', title)
                rec = " ".join(current_rec_lines).strip()
                rec = _re.sub(r'\*\*', '', rec)
                # Clean leading "Recommendation:" from rec text
                rec = _re.sub(r'^\*?Recommendation:\*?\s*', '', rec)
                items.append({"title": title, "recommendation": rec})

        for line in text.split("\n"):
            stripped = line.strip()

            # New numbered item starts
            if _re.match(r'^\d+[\.\)]\s', stripped):
                _flush()
                current_title_lines = []
                current_rec_lines = []
                in_rec = False
                # Strip the leading number
                clean = _re.sub(r'^\d+[\.\)]\s*', '', stripped)
                current_title_lines.append(clean)

            # Recommendation line
            elif stripped.lower().startswith('*recommendation:') or stripped.lower().startswith('recommendation:'):
                in_rec = True
                current_rec_lines.append(stripped)

            # Continuation line
            elif stripped:
                if in_rec:
                    current_rec_lines.append(stripped)
                elif current_title_lines:
                    current_title_lines.append(stripped)

            # Blank line — doesn't end the current item, but if we haven't
            # hit a rec yet, it might precede one
            else:
                pass

        _flush()
        return items

    app.jinja_env.filters["parse_issues"] = parse_issues

    # Markdown rendering filter for audit blocks
    import markdown as _md
    from markupsafe import Markup

    def render_markdown(text: str) -> Markup:
        """Render markdown text to sanitized HTML for display in templates."""
        if not text:
            return Markup("")
        html = _md.markdown(
            text,
            extensions=["extra", "tables", "fenced_code", "sane_lists"],
            output_format="html5",
        )
        # Sanitize: strip dangerous tags/attributes (prevent XSS)
        import re as _sanitize_re
        html = _sanitize_re.sub(r'<script[^>]*>.*?</script>', '', html, flags=_sanitize_re.DOTALL | _sanitize_re.IGNORECASE)
        html = _sanitize_re.sub(r'<iframe[^>]*>.*?</iframe>', '', html, flags=_sanitize_re.DOTALL | _sanitize_re.IGNORECASE)
        html = _sanitize_re.sub(r'\bon\w+\s*=', '', html, flags=_sanitize_re.IGNORECASE)
        return Markup(html)

    app.jinja_env.filters["markdown"] = render_markdown

    return app
