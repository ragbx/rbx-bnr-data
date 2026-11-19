"""Découpe le plan 1889 (VRT L93) en extraits et superpose les rues actuelles numérotées."""
import csv, subprocess, sys, json
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import box
from shapely.ops import transform

S = Path(sys.argv[1]); VRT = S / "m1889.vrt"; OUT = S / "tiles"; OUT.mkdir(exist_ok=True)
x0, y0, x1, y1 = map(float, sys.argv[2:6])
TILE, STEP, RES = 500.0, 450.0, 0.5
tr = Transformer.from_crs(4326, 2154, always_xy=True).transform
rues = []
for r in csv.DictReader(open("data/geo/referentiel_lieux_roubaix.csv")):
    if r["thesaurus"] != "rues" or not r["coord_wkt"]:
        continue
    rues.append((r["lieu_id"], r["libelle"], transform(tr, wkt.loads(r["coord_wkt"]))))
font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 15)
legend = {}
tx = 0; x = x0
while x < x1:
    ty = 0; y = y1
    while y > y0:
        name = f"t{ty}_{tx}"
        png = OUT / f"{name}_raw.png"
        subprocess.run(["gdal_translate", "-q", "-of", "PNG", "-tr", str(RES), str(RES),
                        "-projwin", str(x), str(y), str(x + TILE), str(y - TILE), str(VRT), str(png)], check=True)
        CX0, CY0, CX1, CY1 = 711700, 7065300, 712950, 7066650
        if x >= CX0 and x + TILE <= CX1 + 400 and y - TILE >= CY0 - 50 and y <= CY1 + 50:
            png.unlink(); y -= STEP; ty += 1; continue
        imL = Image.open(png).convert("L"); h = imL.histogram()
        if sum(h[240:]) + sum(h[:15]) > 0.85 * sum(h):
            png.unlink(); y -= STEP; ty += 1; continue
        im = Image.open(png).convert("RGB")
        ov = Image.new("RGBA", im.size, (0, 0, 0, 0)); d = ImageDraw.Draw(ov)
        bb = box(x, y - TILE, x + TILE, y)
        px = lambda X, Y: ((X - x) / RES, (y - Y) / RES)
        leg = []
        for i, (lid, lib, g) in enumerate(rues):
            if not g.intersects(bb):
                continue
            gi = g.intersection(bb)
            parts = getattr(gi, "geoms", [gi])
            for p in parts:
                if p.geom_type == "LineString":
                    d.line([px(*c) for c in p.coords], fill=(220, 0, 0, 110), width=3)
            n = len(leg) + 1
            m = gi.interpolate(0.5, normalized=True) if gi.geom_type != "Point" else gi
            mx, my = px(m.x, m.y)
            d.rectangle([mx - 2, my - 2, mx + 22, my + 18], fill=(255, 255, 255, 170))
            d.text((mx, my), str(n), fill=(200, 0, 0, 255), font=font)
            leg.append((n, lid, lib))
        Image.alpha_composite(im.convert("RGBA"), ov).convert("RGB").save(OUT / f"{name}.jpg", quality=88)
        legend[name] = {"bbox_l93": [x, y - TILE, x + TILE, y], "rues": leg}
        y -= STEP; ty += 1
    x += STEP; tx += 1
json.dump(legend, open(OUT / "legend.json", "w"), ensure_ascii=False, indent=1)
print(len(legend), "extraits")
