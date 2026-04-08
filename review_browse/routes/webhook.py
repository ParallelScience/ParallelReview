"""GitHub webhook endpoint for real-time review ingestion."""

import hashlib
import hmac
import logging

from flask import Blueprint, Response, current_app, request

blueprint = Blueprint("webhook", __name__, url_prefix="/webhook")
log = logging.getLogger(__name__)


def _verify_signature(payload: bytes, signature_header: str, secret: str) -> bool:
    if not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature_header)


@blueprint.route("/github", methods=["POST"])
def github_webhook() -> Response:
    """Handle GitHub page_build events for review repos."""
    secret = current_app.config.get("WEBHOOK_SECRET", "")

    if secret:
        signature = request.headers.get("X-Hub-Signature-256", "")
        if not _verify_signature(request.data, signature, secret):
            return Response("Forbidden", status=403)

    event = request.headers.get("X-GitHub-Event", "")
    if event == "ping":
        return Response("pong", status=200)
    if event != "page_build":
        return Response("ignored", status=204)

    payload = request.get_json(silent=True)
    if not payload:
        return Response("bad payload", status=400)

    build = payload.get("build", {})
    if build.get("status") != "built":
        return Response("not a successful build", status=204)

    repo_name = payload.get("repository", {}).get("name", "")
    org_name = payload.get("organization", {}).get("login", "")
    if not repo_name or not org_name:
        return Response("missing repo/org", status=400)

    # Only process review repos
    if not repo_name.startswith("review-"):
        return Response("not a review repo", status=204)

    log.info("page_build event for %s/%s", org_name, repo_name)

    from review_browse.services.database import get_db, sync_to_gcs
    from review_browse.services.scraper import scrape_single_repo, upsert_review

    meta = scrape_single_repo(org_name, repo_name)
    if not meta:
        return Response("no review metadata", status=204)

    conn = get_db()
    gcs_bucket = current_app.config.get("GCS_BUCKET", "parallel-review")

    rx_id, version, action = upsert_review(conn, meta, org_name, gcs_bucket=gcs_bucket)

    if action != "unchanged":
        sync_to_gcs()

    return Response(
        f'{{"rx_id":"{rx_id}","version":{version},"action":"{action}"}}',
        status=200,
        mimetype="application/json",
    )
