"""Vignettes des plans anciens pour la carte carte-bnr : un carré pris au milieu de chaque plan géoréférencé (2026-11-16).

    python3 scripts/ead/vignettes_plans_anciens.py [taille_px] [part]      (Python avec GDAL et Pillow ; 96 et 0.35 par défaut)

Lit les GeoTIFF de data/geo/plans_anciens (masque ou bande alpha compris), prend au centre de l'emprise utile
du plan un carré de côté part × (plus petit côté de l'emprise), le réduit à taille_px et écrit
data/geo/plans_anciens/leaflet_1899/vignettes/<année>.webp, à copier dans carte-bnr/plans/<année>/vignette.webp.
La fiche de la carte les affiche à 44 px (96 px = écrans haute densité).
"""
import sys
from os import makedirs
from os.path import abspath, dirname, join

import numpy as np
from osgeo import gdal
from PIL import Image

gdal.UseExceptions()

RACINE = dirname(dirname(dirname(abspath(__file__))))
DOSSIER = join(RACINE, "data", "geo", "plans_anciens")
SORTIE = join(DOSSIER, "leaflet_1899", "vignettes")
PLANS = {
    "1811": "plan_roubaix_1811_verscheure_georef.tif",
    "1855": "plan_roubaix_1855_barbotin_georef.tif",
    "1889": "plan_roubaix_1889_georef.tif",
    "1899": "plan_roubaix_1899_georef.tif",
}
TAILLE = int(sys.argv[1]) if len(sys.argv) > 1 else 96
PART = float(sys.argv[2]) if len(sys.argv) > 2 else 0.35


def vignette(chemin, taille, part):
    ds = gdal.Open(chemin)
    w, h = ds.RasterXSize, ds.RasterYSize
    # emprise utile du plan d'après le masque (bande alpha ou masque PER_DATASET), lu en réduit
    k = max(w, h) / 1000
    masque = ds.GetRasterBand(1).GetMaskBand().ReadAsArray(buf_xsize=round(w / k), buf_ysize=round(h / k)) > 0
    lignes, colonnes = np.nonzero(masque)
    x0, x1, y0, y1 = colonnes.min() * k, (colonnes.max() + 1) * k, lignes.min() * k, (lignes.max() + 1) * k
    # carré centré sur le milieu de l'emprise
    cote = part * min(x1 - x0, y1 - y0)
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    xo, yo, n = int(cx - cote / 2), int(cy - cote / 2), int(cote)
    lecture = 4 * taille   # lecture GDAL moyennée à 4x la taille finale, puis Pillow (Lanczos)
    lire = lambda bande: bande.ReadAsArray(xo, yo, n, n, buf_xsize=lecture, buf_ysize=lecture,
                                           resample_alg=gdal.GRIORA_Average).astype(np.float32)
    rgb = np.dstack([lire(ds.GetRasterBand(b)) for b in (1, 2, 3)])
    alpha = lire(ds.GetRasterBand(1).GetMaskBand())[..., None] / 255
    # trous éventuels (joints entre feuilles) : couleur du papier = médiane des pixels clairs du carré
    clair = (alpha[..., 0] > 0.99) & (rgb.mean(-1) > 180)
    papier = np.median(rgb[clair], 0) if clair.any() else np.array([235, 208, 164], np.float32)
    rgb = rgb * alpha + papier * (1 - alpha)
    return Image.fromarray(rgb.clip(0, 255).astype(np.uint8), "RGB").resize((taille, taille), Image.LANCZOS)


def main():
    makedirs(SORTIE, exist_ok=True)
    for annee, nom in PLANS.items():
        img = vignette(join(DOSSIER, nom), TAILLE, PART)
        dest = join(SORTIE, f"{annee}.webp")
        img.save(dest, "WEBP", quality=85, method=6)
        print(f"→ {dest}")


if __name__ == "__main__":
    main()
