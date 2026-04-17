"""Probe how KaTeX math elements compute style inside <strong>."""
import json
import pathlib
import re
import sys

import markdown
from playwright.sync_api import sync_playwright

md = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
html_body = markdown.markdown(
    md,
    extensions=["extra", "tables", "fenced_code", "toc", "sane_lists"],
)

# Slice content containing \mathbf which is KaTeX's bold-math command
match = re.search(r"<li>.*?\\mathbf\{.*?</li>", html_body, re.DOTALL)
slice_html = match.group(0) if match else "<p>no match</p>"
print(f"slice length: {len(slice_html)}")

doc = (
    "<!DOCTYPE html><html><head>"
    '<link rel="stylesheet" href="file:///app/output/assets/katex/katex.min.css">'
    "<style>body{font-family:serif} strong{font-weight:700}"
    ".katex,.katex *,.katex-display,.katex-display *{"
    "font-weight:normal !important;"
    "font-synthesis:none !important;"
    "-webkit-font-synthesis:none !important;}"
    "</style></head><body><ol>"
    + slice_html
    + "</ol>"
    '<script src="file:///app/output/assets/katex/katex.min.js"></script>'
    '<script src="file:///app/output/assets/katex/contrib/auto-render.min.js"></script>'
    "<script>renderMathInElement(document.body,{delimiters:["
    '{left:"$$",right:"$$",display:true},'
    '{left:"$",right:"$",display:false}]});</script>'
    "</body></html>"
)
pathlib.Path("/tmp/pdf-test/probe.html").write_text(doc)

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.goto("file:///tmp/pdf-test/probe.html")
    page.wait_for_timeout(1500)
    info = page.evaluate(
        """
() => {
  const out = [];
  // Look at every .mathbf element on the page (these are \mathbf{} renderings)
  const bfs = document.querySelectorAll('.mathbf, .mathbb, .mathfrak, .boldsymbol');
  for (const b of bfs) {
    const cs = getComputedStyle(b);
    out.push({
      cls: b.className,
      text: b.textContent,
      fw: cs.fontWeight,
      ff: cs.fontFamily,
      synth: cs.fontSynthesis,
    });
    if (out.length >= 5) break;
  }
  return out;
}
"""
    )
    print(json.dumps(info, indent=2))
    browser.close()
