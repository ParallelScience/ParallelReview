"""Render `\\mathbf{\\Theta}` in isolation and inspect."""
import json
import pathlib
import sys

from playwright.sync_api import sync_playwright

doc = """<!DOCTYPE html><html><head>
<link rel="stylesheet" href="file:///app/output/assets/katex/katex.min.css">
<style>
  body { font-family: serif; font-size: 16pt; line-height: 2; }
  .label { color: #888; }
  .katex, .katex *,
  .katex-display, .katex-display * {
    font-weight: normal !important;
    font-synthesis: none !important;
    -webkit-font-synthesis: none !important;
  }
</style></head><body>
<p><span class="label">plain Theta:</span> $\\Theta$</p>
<p><span class="label">mathbf Theta:</span> $\\mathbf{\\Theta}$</p>
<p><span class="label">mathbf v:</span> $\\mathbf{v}$</p>
<p><span class="label">boldsymbol Theta:</span> $\\boldsymbol{\\Theta}$</p>
<p><span class="label">expression:</span> $\\partial_t u_k \\approx \\mathbf{\\Theta} \\xi_k$</p>
<script src="file:///app/output/assets/katex/katex.min.js"></script>
<script src="file:///app/output/assets/katex/contrib/auto-render.min.js"></script>
<script>
renderMathInElement(document.body, {delimiters: [
  {left:"$$",right:"$$",display:true},
  {left:"$",right:"$",display:false}
]});
</script></body></html>"""
pathlib.Path("/tmp/probe-theta.html").write_text(doc)

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.goto("file:///tmp/probe-theta.html")
    page.wait_for_timeout(1500)
    info = page.evaluate(
        """
() => {
  const out = [];
  // Walk every span inside .katex elements and capture computed style of
  // every "mord" (math ordinal — actual character glyph)
  const morder = document.querySelectorAll('.katex .mord');
  for (const m of morder) {
    const cs = getComputedStyle(m);
    out.push({
      cls: m.className,
      text: m.textContent,
      fw: cs.fontWeight,
      ff: cs.fontFamily.split(',')[0],
      fs: cs.fontStyle,
      synth: cs.fontSynthesis,
    });
  }
  return out;
}
"""
    )
    print(json.dumps(info, indent=2))
    # Also print the document fonts that were actually loaded
    fonts = page.evaluate(
        """
async () => {
  const out = [];
  for (const f of document.fonts) {
    out.push({
      family: f.family,
      style: f.style,
      weight: f.weight,
      status: f.status,
    });
  }
  return out;
}
"""
    )
    print("\nLOADED FONTS:")
    for f in fonts:
        if f["status"] == "loaded" and "KaTeX_Main" in f["family"]:
            print(f"  {f}")
    # Take a screenshot
    page.screenshot(path="/tmp/probe-theta.png", full_page=True)
    browser.close()
