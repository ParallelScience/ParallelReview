#!/usr/bin/env python3
"""Build an OpenReview-style GitHub Pages site for a Skepthical paper review.

Reads review.md (structured Skepthical output) and paper metadata,
copies assets into docs/, and generates index.html from the template.

Usage:
    python build_review_page.py <publish_dir> --repo-url <url> --title <title> \\
        --author <name> --abstract <text> [--paper-pages-url <url>]

Expects in <publish_dir>:
    review.md, review.pdf (optional), paper.pdf (optional)
"""

import argparse
import json
import os
import re
import shutil
import sys
from datetime import datetime, timezone, timedelta


# ---------------------------------------------------------------------------
# Markdown review parser
# ---------------------------------------------------------------------------

def parse_review_md(md_text: str) -> dict:
    """Parse a structured Skepthical markdown review into sections.

    Returns a dict with keys:
        summary, strengths, major_issues, minor_issues, very_minor_issues,
        key_statements, maths_audit, numerics_audit, unstructured_review
    Each value is the raw markdown content of that section.
    """
    sections = {}
    current_key = None
    current_lines = []

    # Map heading text → dict key
    heading_map = {
        "paper summary":                   "summary",
        "summary":                         "summary",
        "strengths":                       "strengths",
        "major issues":                    "major_issues",
        "minor issues":                    "minor_issues",
        "very minor issues":               "very_minor_issues",
        "key statements and references":   "key_statements",
        "key statements":                  "key_statements",
        "mathematical consistency audit":  "maths_audit",
        "mathematics audit":               "maths_audit",
        "numerical results audit":         "numerics_audit",
        "numerics audit":                  "numerics_audit",
        "unstructured review":             "unstructured_review",
        "figure review":                   "figure_review",
        "figures review":                  "figure_review",
        "reproducibility audit":           "reproducibility_audit",
    }

    for line in md_text.split("\n"):
        # Match ## headings
        m = re.match(r"^#{1,3}\s+(.+)$", line)
        if m:
            heading_text = m.group(1).strip().lower()
            # Remove leading "review:" or trailing punctuation
            heading_text = re.sub(r"^review:\s*", "", heading_text)
            heading_text = heading_text.rstrip(":")

            matched_key = heading_map.get(heading_text)
            if matched_key:
                # Save previous section
                if current_key:
                    sections[current_key] = "\n".join(current_lines).strip()
                current_key = matched_key
                current_lines = []
                continue

        if current_key is not None:
            current_lines.append(line)

    # Save last section
    if current_key:
        sections[current_key] = "\n".join(current_lines).strip()

    return sections


def _md_to_html_simple(md: str) -> str:
    """Minimal markdown-to-HTML conversion for review content."""
    if not md:
        return ""

    lines = md.split("\n")
    html_parts = []
    in_list = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_list:
                html_parts.append("</ul>")
                in_list = False
            html_parts.append("")
            continue

        # Numbered list items: "1. Issue: ..." or "1. **Issue:**"
        m_numbered = re.match(r"^\d+\.\s+(.+)$", stripped)
        # Bullet items
        m_bullet = re.match(r"^[-*]\s+(.+)$", stripped)

        if m_numbered or m_bullet:
            content = (m_numbered or m_bullet).group(1)
            if not in_list:
                html_parts.append("<ul>")
                in_list = True
            # Bold handling
            content = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", content)
            content = re.sub(r"__(.+?)__", r"<strong>\1</strong>", content)
            html_parts.append(f"<li>{content}</li>")
        else:
            if in_list:
                html_parts.append("</ul>")
                in_list = False
            # Bold handling
            stripped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", stripped)
            stripped = re.sub(r"__(.+?)__", r"<strong>\1</strong>", stripped)
            html_parts.append(f"<p>{stripped}</p>")

    if in_list:
        html_parts.append("</ul>")

    return "\n".join(html_parts)


def _parse_issues(md: str) -> list[dict]:
    """Parse numbered issues with optional recommendations.

    Expected format:
        1. Issue: <description>
           Recommendation: <fix>
        2. **Issue:** ...
    """
    issues = []
    current_issue = None

    for line in md.split("\n"):
        stripped = line.strip()

        # Start of a new issue
        m = re.match(r"^\d+\.\s+(.+)$", stripped)
        if m:
            if current_issue:
                issues.append(current_issue)
            text = m.group(1)
            # Remove "Issue:" or "**Issue:**" prefix
            text = re.sub(r"^\*?\*?Issue:?\*?\*?\s*", "", text, flags=re.IGNORECASE)
            current_issue = {"issue": text, "recommendation": ""}
            continue

        # Recommendation line
        if current_issue and stripped.lower().startswith("recommendation:"):
            rec = re.sub(r"^[Rr]ecommendation:\s*", "", stripped)
            current_issue["recommendation"] = rec
            continue

        # Continuation of current issue
        if current_issue and stripped:
            if current_issue["recommendation"]:
                current_issue["recommendation"] += " " + stripped
            else:
                current_issue["issue"] += " " + stripped

    if current_issue:
        issues.append(current_issue)

    return issues


def _parse_bullet_list(md: str) -> list[str]:
    """Parse a bullet/numbered list into string items."""
    items = []
    current = ""
    for line in md.split("\n"):
        stripped = line.strip()
        m = re.match(r"^[-*]\s+(.+)$", stripped) or re.match(r"^\d+\.\s+(.+)$", stripped)
        if m:
            if current:
                items.append(current)
            current = m.group(1)
        elif stripped and current:
            current += " " + stripped
    if current:
        items.append(current)
    return items


# ---------------------------------------------------------------------------
# HTML card builders
# ---------------------------------------------------------------------------

def _build_main_review_card(sections: dict, date: str, time_str: str) -> str:
    """Build the main review card HTML."""
    parts = []
    parts.append('<div class="reply-card">')
    parts.append('  <div class="reply-header">')
    parts.append('    <span class="reply-title">Official Review</span>')
    parts.append('    <span class="badge badge-review">Official Review</span>')
    parts.append(f'    <span class="reply-meta">by Skepthical &middot; {date}</span>')
    parts.append('  </div>')
    parts.append('  <div class="reply-body">')

    # Summary
    summary = sections.get("summary", "")
    if summary:
        parts.append('    <button class="section-toggle" onclick="toggleSection(this)">')
        parts.append('      <span class="chevron">&#9660;</span> <span class="field-label">Paper Summary</span>')
        parts.append('    </button>')
        parts.append('    <div class="section-content">')
        parts.append(f'      <div class="summary-text">{_md_to_html_simple(summary)}</div>')
        parts.append('    </div>')

    # Strengths
    strengths_md = sections.get("strengths", "")
    if strengths_md:
        strengths = _parse_bullet_list(strengths_md)
        parts.append('    <button class="section-toggle" onclick="toggleSection(this)">')
        parts.append(f'      <span class="chevron">&#9660;</span> <span class="field-label">Strengths ({len(strengths)})</span>')
        parts.append('    </button>')
        parts.append('    <div class="section-content">')
        for s in strengths:
            s_escaped = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            parts.append(f'      <div class="strength-item">{s_escaped}</div>')
        parts.append('    </div>')

    # Major Issues
    major_md = sections.get("major_issues", "")
    if major_md:
        major = _parse_issues(major_md)
        parts.append('    <button class="section-toggle" onclick="toggleSection(this)">')
        parts.append(f'      <span class="chevron">&#9660;</span> <span class="field-label">Major Issues ({len(major)})</span>')
        parts.append('    </button>')
        parts.append('    <div class="section-content">')
        parts.append('      <ul class="issue-list">')
        for item in major:
            parts.append('        <li class="issue-item issue-major">')
            issue_text = item["issue"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            parts.append(f'          <div class="issue-title">{issue_text}</div>')
            if item.get("recommendation"):
                rec = item["recommendation"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                parts.append(f'          <div class="issue-rec">Recommendation: {rec}</div>')
            parts.append('        </li>')
        parts.append('      </ul>')
        parts.append('    </div>')

    # Minor Issues
    minor_md = sections.get("minor_issues", "")
    if minor_md:
        minor = _parse_issues(minor_md)
        parts.append('    <button class="section-toggle" onclick="toggleSection(this)">')
        parts.append(f'      <span class="chevron">&#9660;</span> <span class="field-label">Minor Issues ({len(minor)})</span>')
        parts.append('    </button>')
        parts.append('    <div class="section-content">')
        parts.append('      <ul class="issue-list">')
        for item in minor:
            parts.append('        <li class="issue-item issue-minor">')
            issue_text = item["issue"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            parts.append(f'          <div class="issue-title">{issue_text}</div>')
            if item.get("recommendation"):
                rec = item["recommendation"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                parts.append(f'          <div class="issue-rec">Recommendation: {rec}</div>')
            parts.append('        </li>')
        parts.append('      </ul>')
        parts.append('    </div>')

    # Very Minor Issues
    vminor_md = sections.get("very_minor_issues", "")
    if vminor_md:
        vminor = _parse_issues(vminor_md)
        parts.append('    <button class="section-toggle" onclick="toggleSection(this)">')
        parts.append(f'      <span class="chevron">&#9660;</span> <span class="field-label">Very Minor Issues ({len(vminor)})</span>')
        parts.append('    </button>')
        parts.append('    <div class="section-content">')
        parts.append('      <ul class="issue-list">')
        for item in vminor:
            parts.append('        <li class="issue-item issue-vminor">')
            issue_text = item["issue"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            parts.append(f'          <div class="issue-title">{issue_text}</div>')
            if item.get("recommendation"):
                rec = item["recommendation"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                parts.append(f'          <div class="issue-rec">Recommendation: {rec}</div>')
            parts.append('        </li>')
        parts.append('      </ul>')
        parts.append('    </div>')

    parts.append('  </div>')
    parts.append('</div>')
    return "\n".join(parts)


def _build_audit_card(title: str, badge_class: str, badge_text: str,
                      content_md: str, date: str) -> str:
    """Build an audit section card (maths, numerics, figures, etc.)."""
    if not content_md:
        return ""

    parts = []
    parts.append('<div class="reply-card">')
    parts.append('  <div class="reply-header">')
    parts.append(f'    <span class="reply-title">{title}</span>')
    parts.append(f'    <span class="badge {badge_class}">{badge_text}</span>')
    parts.append(f'    <span class="reply-meta">by Skepthical &middot; {date}</span>')
    parts.append('  </div>')
    parts.append('  <div class="reply-body">')
    parts.append('    <button class="section-toggle" onclick="toggleSection(this)">')
    parts.append('      <span class="chevron">&#9660;</span> <span class="field-label">Details</span>')
    parts.append('    </button>')
    parts.append('    <div class="section-content">')
    parts.append(f'      <div class="summary-text">{_md_to_html_simple(content_md)}</div>')
    parts.append('    </div>')
    parts.append('  </div>')
    parts.append('</div>')
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

def build(publish_dir: str, repo_url: str, author: str, title: str,
          abstract: str, paper_pages_url: str):
    # Template lives in the ParallelReview repo's templates directory
    template_path = os.path.join(
        os.path.dirname(__file__), "..", "review_browse", "templates", "page_template.html"
    )
    review_md_path = os.path.join(publish_dir, "review.md")
    review_pdf_path = os.path.join(publish_dir, "review.pdf")
    paper_pdf_path = os.path.join(publish_dir, "paper.pdf")
    docs_dir = os.path.join(publish_dir, "docs")

    if not os.path.exists(template_path):
        print(f"Error: review_page_template.html not found at {template_path}", file=sys.stderr)
        sys.exit(1)

    # Read review markdown
    review_md = ""
    if os.path.exists(review_md_path):
        with open(review_md_path) as f:
            review_md = f.read()

    # Parse sections
    sections = parse_review_md(review_md)

    # AOE = UTC-12
    aoe = timezone(timedelta(hours=-12))
    now = datetime.now(aoe)
    date = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S") + " AOE"

    # Create docs/ and copy assets
    os.makedirs(docs_dir, exist_ok=True)

    if os.path.exists(review_pdf_path):
        shutil.copy2(review_pdf_path, docs_dir)
    if os.path.exists(paper_pdf_path):
        shutil.copy2(paper_pdf_path, docs_dir)
    if os.path.exists(review_md_path):
        shutil.copy2(review_md_path, docs_dir)

    # Also copy scores.json and cost.json if present so they are reachable at
    # the Pages URL (`https://<org>.github.io/<repo>/scores.json`). The
    # ParallelReview indexer fetches these via the public Pages site, not via
    # the GitHub Contents API, so they MUST live under docs/ — they are not
    # automatically included from the publish dir root.
    for asset in ("scores.json", "cost.json"):
        src = os.path.join(publish_dir, asset)
        if os.path.exists(src):
            shutil.copy2(src, docs_dir)

    # Build review cards
    cards = []

    # Main review card
    main_card = _build_main_review_card(sections, date, time_str)
    if main_card:
        cards.append(main_card)

    # Key statements card
    if sections.get("key_statements"):
        cards.append(_build_audit_card(
            "Key Statements & References", "badge-statements",
            "Statement Verification", sections["key_statements"], date))

    # Maths audit card
    if sections.get("maths_audit"):
        cards.append(_build_audit_card(
            "Mathematical Consistency Audit", "badge-maths",
            "Mathematics Audit", sections["maths_audit"], date))

    # Numerics audit card
    if sections.get("numerics_audit"):
        cards.append(_build_audit_card(
            "Numerical Results Audit", "badge-numerics",
            "Numerics Audit", sections["numerics_audit"], date))

    # Figure review card
    if sections.get("figure_review"):
        cards.append(_build_audit_card(
            "Figure Review", "badge-figures",
            "Figure Review", sections["figure_review"], date))

    # Reproducibility card
    if sections.get("reproducibility_audit"):
        cards.append(_build_audit_card(
            "Reproducibility Audit", "badge-maths",
            "Reproducibility", sections["reproducibility_audit"], date))

    # Unstructured review card
    if sections.get("unstructured_review"):
        cards.append(_build_audit_card(
            "Unstructured Review", "badge-review",
            "Unstructured Review", sections["unstructured_review"], date))

    review_count = len(cards)
    review_cards_html = "\n\n".join(cards) if cards else '<p style="color:var(--text-secondary);font-style:italic;">No review sections found.</p>'

    # Read template and replace placeholders
    with open(template_path) as f:
        html = f.read()

    html = html.replace("{{TITLE}}", title)
    html = html.replace("{{AUTHOR}}", author)
    html = html.replace("{{DATE}}", date)
    html = html.replace("{{TIME}}", time_str)
    html = html.replace("{{ABSTRACT}}", abstract)
    html = html.replace("{{GITHUB_URL}}", repo_url)
    html = html.replace("{{PAPER_PAGES_URL}}", paper_pages_url or "#")
    html = html.replace("{{REVIEW_COUNT}}", str(review_count))
    html = html.replace("{{REVIEW_CARDS}}", review_cards_html)

    # Write index.html
    index_path = os.path.join(docs_dir, "index.html")
    with open(index_path, "w") as f:
        f.write(html)

    print(f"Built OpenReview-style GitHub Pages site in {docs_dir}")
    print(f"  Title: {title}")
    print(f"  Author: {author}")
    print(f"  Review sections: {review_count}")
    print(f"  Files: {os.listdir(docs_dir)}")

    # Validate
    errors = validate_page(docs_dir)
    if errors:
        print(f"\n  WARNINGS:")
        for e in errors:
            print(f"    - {e}")


REQUIRED_FIELDS = {
    "{{TITLE}}": "title",
    "{{AUTHOR}}": "author",
    "{{DATE}}": "date",
    "{{TIME}}": "time",
    "{{ABSTRACT}}": "abstract",
    "{{GITHUB_URL}}": "GitHub URL",
}


def validate_page(docs_dir: str) -> list[str]:
    """Check that the generated page has all required fields populated."""
    errors = []
    index_path = os.path.join(docs_dir, "index.html")

    if not os.path.exists(index_path):
        return ["index.html not found"]

    with open(index_path) as f:
        html = f.read()

    for placeholder, name in REQUIRED_FIELDS.items():
        if placeholder in html:
            errors.append(f"{name} not set (placeholder {placeholder} still in HTML)")

    return errors


def main():
    parser = argparse.ArgumentParser(
        description="Build OpenReview-style GitHub Pages site for a Skepthical review")
    parser.add_argument("publish_dir", help="Path to the publish directory")
    parser.add_argument("--repo-url", required=True, help="GitHub repo URL")
    parser.add_argument("--author", default=os.environ.get("SCIENTIST_NAME", "denario"),
                        help="Paper author name")
    parser.add_argument("--title", default="Untitled", help="Paper title")
    parser.add_argument("--abstract", default="", help="Paper abstract")
    parser.add_argument("--paper-pages-url", default="",
                        help="URL to the parallelArXiv page for the paper")
    args = parser.parse_args()

    build(args.publish_dir, args.repo_url, args.author, args.title,
          args.abstract, args.paper_pages_url)


if __name__ == "__main__":
    main()
