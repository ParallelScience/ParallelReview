"""Measure the exact ink density of \\mathbf{\\Theta} inside vs outside <strong>."""
import pathlib
from playwright.sync_api import sync_playwright
from PIL import Image

doc = """<!DOCTYPE html><html><head>
<link rel="stylesheet" href="file:///app/output/assets/katex/katex.min.css">
<style>
  body { font-family: serif; font-size: 32pt; padding: 50px; background: white; }
  strong { font-weight: 700; }
  .katex, .katex *,
  .katex-display, .katex-display * {
    font-weight: normal !important;
    font-synthesis: none !important;
    -webkit-font-synthesis: none !important;
  }
</style></head><body>
<div id="non-bold">$\\mathbf{\\Theta}$</div>
<div id="bold"><strong>$\\mathbf{\\Theta}$</strong></div>
<script src="file:///app/output/assets/katex/katex.min.js"></script>
<script src="file:///app/output/assets/katex/contrib/auto-render.min.js"></script>
<script>
renderMathInElement(document.body, {delimiters: [{left:"$",right:"$",display:false}]});
</script></body></html>"""
pathlib.Path("/tmp/measure-theta.html").write_text(doc)

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 800, "height": 400}, device_scale_factor=3)
    page.goto("file:///tmp/measure-theta.html")
    page.wait_for_timeout(1500)

    # Get exact bounding box of the .mord element (the actual Θ glyph) in each
    for div_id in ("non-bold", "bold"):
        box = page.evaluate(f"""
() => {{
  const el = document.querySelector('#{div_id} .mord.mathbf');
  if (!el) return null;
  const r = el.getBoundingClientRect();
  return {{x: r.x, y: r.y, w: r.width, h: r.height}};
}}
""")
        if box is None:
            print(f"{div_id}: NO .mord.mathbf found")
            continue
        print(f"{div_id}: bbox = x={box['x']:.1f} y={box['y']:.1f} w={box['w']:.1f} h={box['h']:.1f}")
        # Screenshot just that element
        page.screenshot(
            path=f"/tmp/theta-{div_id}.png",
            clip={"x": box["x"], "y": box["y"], "width": box["w"], "height": box["h"]},
        )

    browser.close()

print("\n--- pixel counts ---")
for name in ("non-bold", "bold"):
    im = Image.open(f"/tmp/theta-{name}.png").convert("RGB")
    dark = sum(1 for r,g,b in im.getdata() if r < 100)
    total = im.width * im.height
    print(f"{name}: size={im.size} dark={dark} ({100*dark/total:.1f}% of {total})")
