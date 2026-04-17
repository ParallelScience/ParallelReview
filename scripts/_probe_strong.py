"""Render \\mathbf{\\Theta} inside vs outside <strong> with the current CSS."""
import json
import pathlib

from playwright.sync_api import sync_playwright

doc = """<!DOCTYPE html><html><head>
<link rel="stylesheet" href="file:///app/output/assets/katex/katex.min.css">
<style>
  body { font-family: serif; font-size: 24pt; line-height: 2; padding: 30px; }
  strong { font-weight: 700; }
  .label { color: #888; font-size: 16pt; }
  /* The CURRENT fix from latex.py */
  .katex, .katex *,
  .katex-display, .katex-display * {
    font-weight: normal !important;
    font-synthesis: none !important;
    -webkit-font-synthesis: none !important;
  }
</style></head><body>
<p><span class="label">non-bold ctx, mathbf Theta:</span> $\\mathbf{\\Theta}$</p>
<p><span class="label">bold ctx, mathbf Theta:</span> <strong>BOLD TEXT $\\mathbf{\\Theta}$ MORE BOLD</strong></p>
<p><span class="label">non-bold ctx, plain Theta:</span> $\\Theta$</p>
<p><span class="label">bold ctx, plain Theta:</span> <strong>BOLD TEXT $\\Theta$ MORE BOLD</strong></p>
<script src="file:///app/output/assets/katex/katex.min.js"></script>
<script src="file:///app/output/assets/katex/contrib/auto-render.min.js"></script>
<script>
renderMathInElement(document.body, {delimiters: [
  {left:"$$",right:"$$",display:true},
  {left:"$",right:"$",display:false}
]});
</script></body></html>"""
pathlib.Path("/tmp/probe-strong.html").write_text(doc)

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.goto("file:///tmp/probe-strong.html")
    page.wait_for_timeout(1500)
    page.screenshot(path="/tmp/probe-strong.png", full_page=True)

    info = page.evaluate(
        """
() => {
  const out = [];
  // Find both Theta instances
  const all = document.querySelectorAll('.katex .mord');
  for (const m of all) {
    if (!m.textContent.includes('\u0398')) continue;
    const cs = getComputedStyle(m);
    // Walk up to find if inside <strong>
    let inStrong = false;
    let n = m;
    while (n) {
      if (n.tagName === 'STRONG') { inStrong = true; break; }
      n = n.parentElement;
    }
    out.push({
      cls: m.className,
      text: m.textContent,
      inStrong,
      fw: cs.fontWeight,
      ff: cs.fontFamily.split(',')[0],
      synth: cs.fontSynthesis,
    });
  }
  return out;
}
"""
    )
    print(json.dumps(info, indent=2))
    browser.close()
