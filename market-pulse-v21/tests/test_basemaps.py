"""Every map tile URL in the templates, checked for the three silent failures.

Run:  python tests/test_basemaps.py      (exit 0 = all pass)

Pure — reads the templates off disk, no network. Which is the point: the
sandbox cannot reach a tile server, so this cannot check that tiles
RENDER. What it can check is the three ways a tile URL goes wrong
without anybody noticing, all of which are visible in the string:

  1. A PROVIDER THAT NOW WANTS A KEY. CARTO moved their basemaps behind
     registration and started stamping unauthenticated tiles with an
     "API KEY REQUIRED" watermark. Nothing errored, nothing 404'd, the
     map just quietly went from clean to defaced and stayed that way
     until somebody looked at it.

  2. TRANSPOSED X AND Y. Esri's ArcGIS tile paths are {z}/{y}/{x} while
     every other provider in this repo is {z}/{x}/{y}. Swap them and the
     server returns real tiles of the wrong location — no error, no
     blank, just a map that is confidently somewhere else.

  3. ZOOMING PAST THE DEEPEST TILE THE SERVICE HAS. Esri's Light Gray
     Canvas stops at z16; both maps allow 19 and the ZIP and
     neighborhood layers are exactly what people zoom that far for.
     Without maxNativeZoom, Leaflet requests z17-19, gets 404s, and
     paints blank grey at the moment somebody drilled into the detail
     they came for.

None of the three raises. All three are silent, visual, and get found by
a person on a demo — which is how this file came to exist.
"""
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TEMPLATES = Path(__file__).resolve().parent.parent / "templates"

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# Every L.tileLayer(...) call, with the URL and the options block that
# follows it, so a per-layer option can be asserted against its own URL
# rather than against the file as a whole.
LAYER = re.compile(
    r"L\.tileLayer\(\s*(?P<url>(?:'[^']*'|\"[^\"]*\"|[A-Z_]+\s*\+\s*'[^']*'))"
    r"\s*,\s*\{(?P<opts>[^}]*)\}",
    re.S)

# Providers that used to be free and are not any more. A URL matching
# this does not fail — it serves a watermarked tile — so nothing but a
# check like this one will tell you.
NEEDS_KEY_NOW = {
    "basemaps.cartocdn.com": (
        "CARTO moved their basemaps behind registration; unauthenticated "
        "tiles come back stamped 'API KEY REQUIRED'. Either add a key or "
        "use a provider that does not want one."),
}

found_layers = []
for path in sorted(TEMPLATES.rglob("*.html")):
    body = path.read_text()
    for m in LAYER.finditer(body):
        line = body[:m.start()].count("\n") + 1
        found_layers.append({
            "file": path.name, "line": line,
            "url": m.group("url"), "opts": m.group("opts"),
        })

check(len(found_layers) >= 4,
      f"the scanner finds the tile layers at all — /map has two and "
      f"/fair-value has two (found {len(found_layers)})")


# ── 1. no provider that now demands a key ──
for layer in found_layers:
    for host, why in NEEDS_KEY_NOW.items():
        check(host not in layer["url"],
              f"{layer['file']}:{layer['line']} still points at {host}. {why}")


# ── 2. Esri paths are {z}/{y}/{x}, everyone else is {z}/{x}/{y} ──
esri = [l for l in found_layers if "arcgisonline.com" in l["url"]
        or "ESRI_CANVAS" in l["url"]]
check(len(esri) >= 4,
      f"the Esri layers are present on both maps (found {len(esri)})")

for layer in esri:
    check("{z}/{y}/{x}" in layer["url"],
          f"{layer['file']}:{layer['line']} — AN ESRI TILE PATH MUST BE "
          f"{{z}}/{{y}}/{{x}}, Y BEFORE X. Transposed, the server returns "
          f"real tiles of the wrong place: no error, no blank, just a map "
          f"that is confidently somewhere else. Got: {layer['url']}")
    check("{z}/{x}/{y}" not in layer["url"],
          f"{layer['file']}:{layer['line']} uses the {{z}}/{{x}}/{{y}} "
          f"order every other provider wants, which is wrong here")

others = [l for l in found_layers if l not in esri]
for layer in others:
    check("{z}/{y}/{x}" not in layer["url"],
          f"{layer['file']}:{layer['line']} is not an Esri layer but uses "
          f"Esri's y/x order — the same silent wrong-place failure, in the "
          f"other direction")


# ── 3. a zoom ceiling the service can actually serve ──
def opt(layer, name):
    m = re.search(rf"\b{name}\s*:\s*(\d+)", layer["opts"])
    return int(m.group(1)) if m else None


for layer in esri:
    native = opt(layer, "maxNativeZoom")
    check(native is not None,
          f"{layer['file']}:{layer['line']} — AN ESRI CANVAS LAYER NEEDS "
          f"maxNativeZoom. The service has no tiles past z16; without it "
          f"Leaflet requests z17-19, gets 404s, and paints blank grey "
          f"exactly when somebody has zoomed into a ZIP or neighborhood")
    check(native is None or native <= 16,
          f"{layer['file']}:{layer['line']} — maxNativeZoom {native} is "
          f"deeper than the z16 this service actually has")
    ceiling = opt(layer, "maxZoom")
    check(ceiling is None or native is None or ceiling >= native,
          f"{layer['file']}:{layer['line']} — maxZoom {ceiling} below "
          f"maxNativeZoom {native} means the layer stops drawing before it "
          f"stops having tiles, which is the ceiling backwards")


# ── the checks have to be able to fail ──
# Written as literals rather than by editing a template, so this is
# honest about what the regexes actually catch.
def scan(src):
    return [{"url": m.group("url"), "opts": m.group("opts")}
            for m in LAYER.finditer(src)]


_bad_order = scan(
    "L.tileLayer(ESRI + '/World_Light_Gray_Base/MapServer/tile/{z}/{x}/{y}', "
    "{ maxZoom: 19, maxNativeZoom: 16 })")
check(_bad_order and "{z}/{y}/{x}" not in _bad_order[0]["url"],
      "the y/x detector notices a transposed Esri path")

_no_native = scan(
    "L.tileLayer(ESRI + '/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}', "
    "{ maxZoom: 19 })")
check(_no_native and not re.search(r"maxNativeZoom", _no_native[0]["opts"]),
      "and the zoom detector notices a missing maxNativeZoom")

_carto = scan(
    "L.tileLayer('https://{s}.basemaps.cartocdn.com/light_nolabels/"
    "{z}/{x}/{y}{r}.png', { maxZoom: 19 })")
check(_carto and "basemaps.cartocdn.com" in _carto[0]["url"],
      "and the provider detector notices a CARTO URL coming back")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} basemap checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} basemap checks passed.")
print(f"   {len(found_layers)} tile layers across the templates: no "
      f"key-gated provider,\n   Esri paths in y/x order, and a zoom "
      f"ceiling every service can serve.")
print("   NOT checked: that tiles actually render. No network here — "
      "that one is yours.")
sys.exit(0)
