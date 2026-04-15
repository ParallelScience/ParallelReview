"""Routes for Parallel Review.

URL patterns mirror OpenReview conventions:
  /                              — Home page
  /forum?id=<review_id>          — Review detail (forum view)
  /forum?id=<review_id>&v=<N>    — Specific version
  /notes?content.author=<a>      — Reviews by paper author
  /notes?content.px_id=<px_id>   — Reviews for a specific paper
  /pdf?id=<review_id>            — Review PDF
  /group?id=recent               — Recent reviews listing
"""

from datetime import datetime
from http import HTTPStatus as status

from flask import Blueprint, Response, render_template, request, redirect, url_for

blueprint = Blueprint("review_browse", __name__, url_prefix="/")


# ---------------------------------------------------------------------------
# Home
# ---------------------------------------------------------------------------

def _sort_reviews(reviews: list[dict], sort_key: str) -> list[dict]:
    """Sort reviews by the given key."""
    if sort_key == "score":
        return sorted(reviews, key=lambda r: (r.get("score_overall") is not None, r.get("score_overall") or 0), reverse=True)
    elif sort_key == "score_asc":
        return sorted(reviews, key=lambda r: (r.get("score_overall") is None, r.get("score_overall") or 0))
    elif sort_key == "date_asc":
        return sorted(reviews, key=lambda r: r.get("review_date", ""))
    else:  # default: date descending (newest first)
        return sorted(reviews, key=lambda r: r.get("review_date", ""), reverse=True)


@blueprint.route("index", methods=["GET"])
@blueprint.route("/", methods=["GET"])
def home() -> Response:
    from review_browse.services.reviews import get_all_current_reviews, count_issues
    reviews = get_all_current_reviews()
    for r in reviews:
        r["issues"] = count_issues(r)
        r["date_short"] = _format_date_short(r.get("review_date", ""))
    sort_by = request.args.get("sort", "date")
    reviews = _sort_reviews(reviews, sort_by)
    return render_template("home/home.html",
                           reviews=reviews,
                           review_count=len(reviews),
                           sort_by=sort_by,
                           now=datetime.now().strftime("%a, %d %b %Y")), status.OK, {}


# ---------------------------------------------------------------------------
# Forum — review detail (matches OpenReview /forum?id=...)
# ---------------------------------------------------------------------------

@blueprint.route("forum", methods=["GET"])
def forum() -> Response:
    """Review detail page. Query params: id (review ID), v (version, optional)."""
    from review_browse.services.reviews import get_review_by_id, get_review_versions, count_issues

    review_id = request.args.get("id", "")
    version = request.args.get("v", None, type=int)

    if not review_id:
        return redirect(url_for("review_browse.home"))

    review = get_review_by_id(review_id, version=version)
    if review is None:
        return render_template("detail/not_found.html", review_id=review_id), status.NOT_FOUND, {}

    review["issues"] = count_issues(review)
    review["date_formatted"] = _format_date_long(review.get("review_date", ""))
    review["date_short"] = _format_date_short(review.get("review_date", ""))
    review["versions"] = get_review_versions(review_id)
    return render_template("detail/review.html", review=review), status.OK, {}


# ---------------------------------------------------------------------------
# Notes — listing by author or paper
# ---------------------------------------------------------------------------

@blueprint.route("notes", methods=["GET"])
def notes() -> Response:
    """List reviews. Supports: content.author=<name>, content.px_id=<px_id>"""
    from review_browse.services.reviews import (
        get_all_current_reviews, get_reviews_by_author, get_reviews_by_paper, count_issues,
    )

    author = request.args.get("content.author", "")
    px_id = request.args.get("content.px_id", "")

    if author:
        reviews = get_reviews_by_author(author)
        context = f"Reviews for papers by {author}"
    elif px_id:
        reviews = get_reviews_by_paper(px_id)
        context = f"Reviews for paper PX:{px_id}"
    else:
        reviews = get_all_current_reviews()
        context = "All Reviews"

    for r in reviews:
        r["issues"] = count_issues(r)
        r["date_short"] = _format_date_short(r.get("review_date", ""))

    sort_by = request.args.get("sort", "date")
    reviews = _sort_reviews(reviews, sort_by)
    return render_template("list/review_list.html",
                           reviews=reviews,
                           context=context,
                           sort_by=sort_by,
                           now=datetime.now().strftime("%a, %d %b %Y")), status.OK, {}


# ---------------------------------------------------------------------------
# Group — matches OpenReview /group?id=...
# ---------------------------------------------------------------------------

@blueprint.route("group", methods=["GET"])
def group() -> Response:
    from review_browse.services.reviews import get_all_current_reviews, count_issues

    reviews = get_all_current_reviews()
    for r in reviews:
        r["issues"] = count_issues(r)
        r["date_short"] = _format_date_short(r.get("review_date", ""))

    sort_by = request.args.get("sort", "date")
    reviews = _sort_reviews(reviews, sort_by)
    return render_template("list/review_list.html",
                           reviews=reviews,
                           context="Recent Reviews",
                           sort_by=sort_by,
                           now=datetime.now().strftime("%a, %d %b %Y")), status.OK, {}


# ---------------------------------------------------------------------------
# PDF — matches OpenReview /pdf?id=...
# ---------------------------------------------------------------------------

@blueprint.route("pdf", methods=["GET"])
def pdf() -> Response:
    review_id = request.args.get("id", "")
    version = request.args.get("v", None, type=int)
    if not review_id:
        return "Missing id parameter", status.BAD_REQUEST, {}
    return _serve_pdf(review_id, version=version)


# ---------------------------------------------------------------------------
# Legacy / convenience aliases
# ---------------------------------------------------------------------------

@blueprint.route("review/<path:review_id>")
def review_redirect(review_id: str) -> Response:
    return redirect(url_for("review_browse.forum", id=review_id))


@blueprint.route("author/<author>")
def author_redirect(author: str) -> Response:
    return redirect(url_for("review_browse.notes", **{"content.author": author}))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serve_pdf(review_id: str, version: int | None) -> Response:
    """Stream the review PDF for a given review id.

    Always serve from the live GitHub Pages URL (`pages_url + "/review.pdf"`)
    rather than from the `review_pdf_url` column. The DB column points at a
    GCS-cached copy that was uploaded once at first ingest and is never
    refreshed unless `content_hash` changes — so any later regeneration of
    `review.pdf` (e.g. a Skepthical CSS fix) silently fails to propagate to
    the user-visible PDF. Reading the live Pages copy on every request makes
    the served PDF always reflect what is actually published in the review
    repo, with zero cache-invalidation logic to maintain.

    Falls back to `review_pdf_url` only if `pages_url` is somehow missing,
    so reviews that were ingested via a path that didn't capture pages_url
    (legacy rows) still resolve.
    """
    import urllib.request
    from review_browse.services.reviews import get_review_by_id
    review = get_review_by_id(review_id, version=version)
    if review is None:
        return "Review not found", status.NOT_FOUND, {}

    pages_url = (review.get("pages_url") or "").rstrip("/")
    if pages_url:
        url = pages_url + "/review.pdf"
    else:
        url = review.get("review_pdf_url", "")
    if not url:
        return "PDF not available", status.NOT_FOUND, {}

    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            pdf_data = resp.read()
    except Exception:
        return "PDF not available", status.NOT_FOUND, {}
    v = review.get("version", 1)
    safe_id = review_id.replace(":", "_")
    # `Cache-Control: no-store` prevents browser-side caching of the PDF.
    # Chromium in particular aggressively caches `application/pdf` responses;
    # without this header, users continue to see stale PDFs even after the
    # underlying review is regenerated and re-published to GitHub Pages.
    return Response(
        pdf_data,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"inline; filename=review_{safe_id}v{v}.pdf",
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


def _format_date_short(date_str: str) -> str:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%d %b %Y")
        except ValueError:
            continue
    return date_str


def _format_date_long(date_str: str) -> str:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%a, %d %b %Y %H:%M:%S") + " AOE"
        except ValueError:
            continue
    return date_str
