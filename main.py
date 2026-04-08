#!/usr/bin/env python3
"""Run the Parallel Review app locally."""
import os

if __name__ == "__main__":
    os.environ.setdefault("TEMPLATES_AUTO_RELOAD", "1")
    from review_browse.factory import create_app
    app = create_app()
    app.run(debug=True, port=8090)
