#!/usr/bin/env python3
"""Scan Parallel ArXiv for unreviewed papers, run Skepthical, publish to ParallelReview.

Usage:
    # Review all unreviewed papers
    python scripts/review_new_papers.py

    # Review a specific paper by PX ID
    python scripts/review_new_papers.py --paper 2604.00001

    # Dry run — just list unreviewed papers
    python scripts/review_new_papers.py --dry-run

Environment variables:
    OPENAI_API_KEY          Required by Skepthical
    MISTRAL_API_KEY         Required by Skepthical (PDF parsing)
    GITHUB_TOKEN            Required for publishing review repos
    GITHUB_ORG              GitHub org (default: ParallelScience)
    SCIENTIST_NAME          Git author name (default: skepthical)
    GIT_EMAIL               Git email (default: skepthical@parallelscience.org)
"""

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request

GITHUB_ORG = os.environ.get("GITHUB_ORG", "ParallelScience")
PARALLEL_ARXIV_DB = os.environ.get(
    "PARALLEL_ARXIV_DB",
    os.path.join(os.path.dirname(__file__), "..", "..",
                 "arxiv-browse", "browse", "data", "papers.db"),
)
PARALLEL_REVIEW_DB = os.environ.get(
    "PARALLEL_REVIEW_DB",
    os.path.join(os.path.dirname(__file__), "..",
                 "review_browse", "data", "reviews.db"),
)


def get_all_papers() -> list[dict]:
    """Fetch all current papers from the Parallel ArXiv database."""
    if not os.path.exists(PARALLEL_ARXIV_DB):
        # Fallback: scrape from the API
        print(f"ArXiv DB not found at {PARALLEL_ARXIV_DB}, scraping from GitHub...")
        return _scrape_papers_from_github()

    conn = sqlite3.connect(PARALLEL_ARXIV_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT px_id, title, author, repo, pages_url, pdf_url, date "
        "FROM papers WHERE is_current = 1 ORDER BY date DESC"
    ).fetchall()
    papers = [dict(r) for r in rows]
    conn.close()
    return papers


def _scrape_papers_from_github() -> list[dict]:
    """Fallback: list paper repos from GitHub and scrape their Pages sites."""
    repos = []
    page = 1
    while True:
        url = f"https://api.github.com/orgs/{GITHUB_ORG}/repos?per_page=100&page={page}"
        req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
        token = os.environ.get("GITHUB_TOKEN")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        if not data:
            break
        for r in data:
            if not r["name"].startswith("review-"):
                repos.append(r["name"])
        page += 1

    papers = []
    for repo in repos:
        try:
            url = f"https://{GITHUB_ORG.lower()}.github.io/{repo}/"
            with urllib.request.urlopen(url, timeout=10) as resp:
                html = resp.read().decode("utf-8", errors="replace")
            # Quick check it's a paper page (has abstract)
            if "abstract" not in html.lower():
                continue
            # Extract real title from <h1> or <title>
            title = repo
            m = re.search(r'<h1[^>]*>(.+?)</h1>', html, re.DOTALL)
            if m:
                title = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            elif (m := re.search(r'<title>([^<]+)</title>', html)):
                title = m.group(1).strip()
            # Extract author
            author = "unknown"
            m = re.search(r'Author:\s*</span>\s*(.+?)<', html)
            if not m:
                m = re.search(r'Author:\s*([^<]+)', html)
            if m:
                author = m.group(1).strip()
            papers.append({
                "px_id": repo,
                "title": title,
                "author": author,
                "repo": repo,
                "pages_url": url,
                "pdf_url": f"{url}paper.pdf",
                "date": "",
            })
        except Exception:
            continue

    return papers


def get_reviewed_repos() -> set[str]:
    """Get the set of paper repos that already have reviews."""
    reviewed = set()

    # Check local review DB
    if os.path.exists(PARALLEL_REVIEW_DB):
        conn = sqlite3.connect(PARALLEL_REVIEW_DB)
        conn.row_factory = sqlite3.Row
        # The review repo name is "review-{paper_slug}", extract original
        rows = conn.execute(
            "SELECT repo, paper_pages_url FROM reviews WHERE is_current = 1"
        ).fetchall()
        for r in rows:
            reviewed.add(r["repo"])
            # Also track by paper_pages_url
            if r["paper_pages_url"]:
                reviewed.add(r["paper_pages_url"])
        conn.close()

    # Also check GitHub for existing review-* repos
    try:
        page = 1
        while True:
            url = f"https://api.github.com/orgs/{GITHUB_ORG}/repos?per_page=100&page={page}"
            req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
            token = os.environ.get("GITHUB_TOKEN")
            if token:
                req.add_header("Authorization", f"Bearer {token}")
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
            if not data:
                break
            for r in data:
                if r["name"].startswith("review-"):
                    # "review-foo-bar" means "foo-bar" was reviewed
                    original = r["name"][len("review-"):]
                    reviewed.add(original)
            page += 1
    except Exception as e:
        print(f"Warning: could not list GitHub repos: {e}")

    return reviewed


def download_pdf(url: str, dest: str) -> bool:
    """Download a PDF to a local path."""
    try:
        urllib.request.urlretrieve(url, dest)
        size = os.path.getsize(dest)
        if size < 100:
            return False
        return True
    except Exception as e:
        print(f"  Failed to download PDF: {e}")
        return False


def run_skepthical_review(pdf_path: str, work_dir: str) -> dict:
    """Run Skepthical on a paper PDF. Returns {report_md, report_pdf}."""
    from skepthical import Skepthical

    temperature = 0.5
    temperature_gpt5 = 1

    params = {
        "paper_pdf_file": pdf_path,
        "work_dir": work_dir,
        "max_chars": 30000,

        "models": {
            "engineer": "gpt-5",
            "group_manager": "gpt-5",
            "paper_verification_agent": "gpt-5",
            "figure_reviewer_agent": "gpt-5",
            "reference_extractor": "gpt-4.1",
            "latex_formatter": "gpt-4.1",
            "doc_type_filter_agent": "gpt-4.1-mini",
            "total_reviewer": "gpt-5.1",
            "statement_extractor": "gpt-5.1",
            "report_merger": "gpt-5.1",
            "report_merger_figures": "gpt-4.1",
            "unstructured_reviewer": "gpt-5.2-thinking",
            "unstructured_report_merger": "gpt-5.2-thinking",
            "maths_reviewer": "gpt-5.2-thinking",
            "numerics_extractor": "gpt-5.2",
            "numerics_coder": "gpt-5.2",
            "numerics_reporter": "gpt-5.2",
        },

        "max_rounds": 2,

        "temperature": {
            "engineer": temperature,
            "figure_reviewer_agent": temperature_gpt5,
            "paper_verification_agent": temperature_gpt5,
            "reference_extractor": temperature,
            "latex_formatter": temperature,
            "doc_type_filter_agent": 0.1,
            "total_reviewer": temperature_gpt5,
            "statement_extractor": 0.1,
            "report_merger": temperature_gpt5,
            "report_merger_figures": 0.1,
            "unstructured_reviewer": temperature_gpt5,
            "unstructured_report_merger": temperature_gpt5,
            "maths_reviewer": None,
            "numerics_extractor": None,
            "numerics_coder": None,
            "numerics_reporter": None,
        },

        "agents_ag2": [
            "reference_extractor", "latex_formatter", "doc_type_filter_agent",
            "total_reviewer", "statement_extractor",
            "report_merger", "report_merger_figures",
        ],
        "agents_direct_call": [
            "figure_reviewer_agent", "paper_verification_agent",
            "unstructured_reviewer", "unstructured_report_merger",
        ],
        "agents": [
            "reference_extractor", "latex_formatter", "doc_type_filter_agent",
            "total_reviewer", "statement_extractor",
            "report_merger", "report_merger_figures",
        ],

        "timeout": 12000,
        "n_key_statements": None,
        "force_parse_paper": True,
        "emails": [],

        "paper_summary_word_limit": 250,
        "strengths_word_limit": 120,
        "major_issues_word_limit": 500,
        "minor_issues_word_limit": 500,
        "very_minor_issues_word_limit": 200,

        "review_mode": "compact",
        "unstructured_report": True,

        # High thoroughness, all boxes except reproducibility
        "figures_review": True,
        "verify_statements": True,
        "review_maths": True,
        "review_numerics": True,
        "review_reproducibility": False,

        "max_checked_items_maths": 20,
        "max_checked_items_numerics": 25,
        "numerics_exec_timeout_s": 20,

        # High thoroughness: 2 reviewer runs
        "n_total_review": 2,
        "total_reviewer_models": ["gpt-5", "gpt-5"],
    }

    sk = Skepthical(params_skepthical=params)
    report = sk.run()

    # Extract markdown
    review_md = ""
    if isinstance(report, str):
        review_md = report
    elif isinstance(report, dict):
        md = report.get("report_md")
        if isinstance(md, str):
            review_md = md
    if not review_md:
        review_md = "Review completed, but output format was unexpected."

    # Find PDF
    import glob
    review_pdf = ""
    pdf_files = sorted(glob.glob(os.path.join(work_dir, "reports_pdf", "Review_*.pdf")))
    if pdf_files:
        review_pdf = pdf_files[-1]

    return {"report_md": review_md, "report_pdf": review_pdf}


def publish_review(paper: dict, review_md: str, review_pdf: str, paper_pdf: str) -> str:
    """Publish a review to a GitHub Pages repo. Returns the pages URL."""
    from datetime import datetime, timezone, timedelta

    # Build publish dir
    publish_dir = tempfile.mkdtemp(prefix="parallel-review-")

    # Copy files
    shutil.copy2(paper_pdf, os.path.join(publish_dir, "paper.pdf"))
    with open(os.path.join(publish_dir, "review.md"), "w") as f:
        f.write(review_md)
    if review_pdf and os.path.exists(review_pdf):
        shutil.copy2(review_pdf, os.path.join(publish_dir, "review.pdf"))

    # Build the page
    build_script = os.path.join(os.path.dirname(__file__), "build_review_page.py")
    title = paper.get("title", "Untitled")
    author = paper.get("author", "unknown")
    paper_pages_url = paper.get("pages_url", "")

    # Extract abstract from paper pages if available
    abstract = ""
    if paper_pages_url:
        try:
            with urllib.request.urlopen(paper_pages_url, timeout=10) as resp:
                html = resp.read().decode("utf-8", errors="replace")
            m = re.search(r'<p>(.{50,}?)</p>', html, re.DOTALL)
            if m:
                abstract = re.sub(r'<[^>]+>', '', m.group(1)).strip()[:1000]
        except Exception:
            pass

    # Slugify repo name
    slug = re.sub(r'[^a-z0-9\s-]', '', title.lower())
    slug = re.sub(r'\s+', '-', slug.strip())
    slug = re.sub(r'-{2,}', '-', slug)[:60].rstrip('-') or "paper"
    repo_name = f"review-{slug}"

    # Check for collision
    token = os.environ.get("GITHUB_TOKEN", "")
    org = GITHUB_ORG

    def github_request(method, path, body=None):
        url = f"https://api.github.com{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, method=method, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        })
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read()) if resp.length != 0 else {}
        except urllib.error.HTTPError as e:
            body_text = e.read().decode(errors="replace")
            raise RuntimeError(f"GitHub API {method} {path}: {e.code}: {body_text}")

    try:
        github_request("GET", f"/repos/{org}/{repo_name}")
        # Exists — append timestamp
        ts = datetime.now(timezone(timedelta(hours=-12))).strftime("%Y%m%d%H%M")
        repo_name = f"{repo_name[:50]}-{ts}"
    except RuntimeError as e:
        if "404" not in str(e):
            raise

    repo_url = f"https://github.com/{org}/{repo_name}"
    pages_url = f"https://{org.lower()}.github.io/{repo_name}/"

    # Create repo
    print(f"  Creating repo {org}/{repo_name}...")
    github_request("POST", f"/orgs/{org}/repos", {
        "name": repo_name,
        "private": False,
        "description": f"Review: {title[:240]}",
    })
    time.sleep(3)

    # Build page
    subprocess.run(
        [sys.executable, build_script, publish_dir,
         "--repo-url", repo_url,
         "--author", author,
         "--title", title,
         "--abstract", abstract,
         "--paper-pages-url", paper_pages_url],
        check=True, capture_output=True, text=True,
    )

    # Generate README
    aoe = timezone(timedelta(hours=-12))
    date = datetime.now(aoe).strftime("%Y-%m-%d")
    readme = f"# Review: {title}\n\n**Reviewer:** Skepthical\n**Paper Author:** {author}\n**Date:** {date}\n\n**[View Review]({pages_url})**\n"
    if paper_pages_url:
        readme += f"\n**[View Paper on Parallel ArXiv]({paper_pages_url})**\n"
    with open(os.path.join(publish_dir, "README.md"), "w") as f:
        f.write(readme)

    # .gitignore
    with open(os.path.join(publish_dir, ".gitignore"), "w") as f:
        f.write("*.npz\n*.npy\n*.pkl\n*.h5\n*.hdf5\n*.csv\n__pycache__/\n")

    # Git push
    git_name = os.environ.get("SCIENTIST_NAME", "skepthical")
    git_email = os.environ.get("GIT_EMAIL", f"{git_name}@parallelscience.org")
    url_with_token = f"https://{token}@github.com/{org}/{repo_name}.git"

    def git(*args):
        result = subprocess.run(
            ["git"] + list(args), cwd=publish_dir,
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)}: {result.stderr}")

    git("init")
    git("config", "user.name", git_name)
    git("config", "user.email", git_email)
    git("add", "-A")
    git("commit", "-m", f"Review: {title[:120]}")
    git("branch", "-M", "main")
    git("remote", "add", "origin", url_with_token)
    git("-c", "credential.helper=", "push", "-u", "origin", "main")

    # Enable Pages
    try:
        github_request("POST", f"/repos/{org}/{repo_name}/pages", {
            "source": {"branch": "main", "path": "/docs"}
        })
    except RuntimeError as e:
        print(f"  Warning: could not enable Pages: {e}")

    print(f"  Published: {repo_url}")
    print(f"  Pages: {pages_url}")

    # Cleanup
    shutil.rmtree(publish_dir, ignore_errors=True)
    return pages_url


def review_paper(paper: dict) -> bool:
    """Download, review, and publish a review for a single paper."""
    px_id = paper["px_id"]
    title = paper["title"]
    pdf_url = paper.get("pdf_url", "")

    if not pdf_url:
        pdf_url = paper.get("pages_url", "").rstrip("/") + "/paper.pdf"

    print(f"\n{'='*60}")
    print(f"Reviewing: {title[:70]}")
    print(f"  PX ID: {px_id}")
    print(f"  PDF:   {pdf_url}")

    # Create temp work dir
    work_dir = tempfile.mkdtemp(prefix=f"skepthical-{px_id}-")
    pdf_path = os.path.join(work_dir, "paper.pdf")

    try:
        # Download PDF
        print(f"  Downloading PDF...")
        if not download_pdf(pdf_url, pdf_path):
            print(f"  SKIP: could not download PDF")
            return False

        # Run Skepthical
        print(f"  Running Skepthical (high thoroughness, all audits)...")
        result = run_skepthical_review(pdf_path, work_dir)

        # Publish
        print(f"  Publishing review...")
        pages_url = publish_review(
            paper, result["report_md"], result["report_pdf"], pdf_path,
        )

        print(f"  DONE: {pages_url}")
        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(
        description="Review unreviewed papers from Parallel ArXiv")
    parser.add_argument("--paper", help="Review a specific paper by PX ID")
    parser.add_argument("--dry-run", action="store_true",
                        help="Just list unreviewed papers, don't review")
    parser.add_argument("--force", action="store_true",
                        help="Review even if already reviewed")
    args = parser.parse_args()

    papers = get_all_papers()
    print(f"Found {len(papers)} papers in Parallel ArXiv")

    if args.paper:
        # Review a specific paper
        paper = next((p for p in papers if p["px_id"] == args.paper), None)
        if not paper:
            paper = next((p for p in papers if args.paper in p.get("repo", "")), None)
        if not paper:
            print(f"Paper {args.paper} not found")
            sys.exit(1)
        if not args.dry_run:
            review_paper(paper)
        else:
            print(f"Would review: {paper['title'][:70]}")
        return

    # Find unreviewed papers
    if args.force:
        unreviewed = papers
    else:
        reviewed = get_reviewed_repos()
        print(f"Found {len(reviewed)} existing reviews")
        unreviewed = []
        for p in papers:
            repo = p.get("repo", "")
            pages_url = p.get("pages_url", "")
            # Check by repo slug match
            slug = re.sub(r'[^a-z0-9-]', '', repo.lower())
            if slug in reviewed or repo in reviewed or pages_url in reviewed:
                print(f"  Already reviewed: {p['title'][:60]}")
            else:
                unreviewed.append(p)

    print(f"\n{len(unreviewed)} papers to review")

    if args.dry_run:
        for p in unreviewed:
            print(f"  Would review: {p['px_id']}  {p['title'][:60]}")
        return

    # Review each
    success = 0
    failed = 0
    for p in unreviewed:
        if review_paper(p):
            success += 1
        else:
            failed += 1

    print(f"\n{'='*60}")
    print(f"Done: {success} reviewed, {failed} failed, {len(papers) - len(unreviewed)} skipped")


if __name__ == "__main__":
    main()
