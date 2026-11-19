"""Pour chaque nom lu sur 1889 : extrait centré sur l'étiquette + les 4 rues actuelles les plus proches en couleurs."""
import csv, json, subprocess, sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import Point, box
from shapely.ops import transform

S = Path(sys.argv[1]); T = S / "tiles"; OUT = S / "check"; OUT.mkdir(exist_ok=True)
leg = json.load(open(T / "legend.json"))
tr = Transformer.from_crs(4326, 2154, always_xy=True).transform
rues = [(r["libelle"], transform(tr, wkt.loads(r["coord_wkt"])))
        for r in csv.DictReader(open("data/geo/referentiel_lieux_roubaix.csv"))
        if r["thesaurus"] == "rues" and r["coord_wkt"]]
CAND = json.load(open(S / "candidats.json"))
font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 15)
COL = [(0, 90, 255), (0, 160, 60), (190, 0, 190), (230, 120, 0)]
H, RES = 130.0, 0.4
PX = float(__import__("os").environ.get("PX", "0.4"))
res = {}
for nom, tile, px, py in CAND:
    x0, y0, x1, y1 = leg[tile]["bbox_l93"]
    X, Y = x0 + px * PX, y1 - py * PX
    p = Point(X, Y)
    near = sorted(((g.distance(p), lib, g) for lib, g in rues), key=lambda t: t[0])[:4]
    png = OUT / f"{nom}_raw.png"
    subprocess.run(["gdal_translate", "-q", "-of", "PNG", "-tr", str(RES), str(RES), "-projwin",
                    str(X - H), str(Y + H), str(X + H), str(Y - H), str(S / "m1889.vrt"), str(png)], check=True)
    im = Image.open(png).convert("RGBA"); ov = Image.new("RGBA", im.size); d = ImageDraw.Draw(ov)
    f = lambda a, b: ((a - X + H) / RES, (Y + H - b) / RES)
    bb = box(X - H, Y - H, X + H, Y + H)
    for i, (dist, lib, g) in enumerate(near):
        gi = g.intersection(bb)
        for part in getattr(gi, "geoms", [gi]):
            if part.geom_type == "LineString":
                d.line([f(*c) for c in part.coords], fill=COL[i] + (150,), width=5)
        d.rectangle([4, 4 + 20 * i, 330, 22 + 20 * i], fill=(255, 255, 255, 210))
        d.text((8, 5 + 20 * i), f"{lib} ({dist:.0f} m)", fill=COL[i] + (255,), font=font)
    c = f(X, Y); d.ellipse([c[0] - 6, c[1] - 6, c[0] + 6, c[1] + 6], outline=(0, 0, 0, 255), width=2)
    Image.alpha_composite(im, ov).convert("RGB").save(OUT / f"{nom}.jpg", quality=88)
    res[nom] = {"x": X, "y": Y, "proches": [(lib, round(dist)) for dist, lib, g in near]}
json.dump(res, open(OUT / "res.json", "w"), ensure_ascii=False, indent=1)
for k, v in res.items():
    print(k, v["proches"])
