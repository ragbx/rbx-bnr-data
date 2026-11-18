"""Projet QGIS : plans de Roubaix de 1855, 1889 et 1899 sous le Plan IGN en transparence (2026-11-15, 1889 ajouté le 16).

    python3 scripts/ead/qgis_plan_1899.py [opacite_ign]      (Python de QGIS ; opacité par défaut 0.5)

Écrit data/geo/plans_anciens/plan_roubaix_1899.qgz (Lambert 93, chemins relatifs) avec, de haut en bas :
  - Plan IGN v2 (Géoplateforme, tuiles WMTS) en transparence ;
  - Plan de Roubaix 1855 (plan_roubaix_1855_barbotin_georef.tif, E. Barbotin, BnF Ge C 3003 ; centre-ville) ;
  - Plan de Roubaix 1889 (plan_roubaix_1889_georef.tif, service de la voirie, 1/4000, mosaïque des 4 feuilles 6Fi022,
    calées par corrélation sur le plan de 1899 ; feuilles séparées : plan_roubaix_1889_feuilleN_georef.tif) ;
  - Plan de Roubaix 1899 (plan_roubaix_1899_georef.tif, géoréférencé depuis le scan 6Fi024).
L'opacité de l'IGN se règle ensuite dans QGIS : Propriétés de la couche › Transparence.
"""
import os
import sys
from os.path import abspath, dirname, exists, join

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from qgis.core import (QgsApplication, QgsCoordinateReferenceSystem, QgsProject, QgsRasterLayer,  # noqa: E402
                       QgsReferencedRectangle)

RACINE = dirname(dirname(dirname(abspath(__file__))))
DOSSIER = join(RACINE, "data", "geo", "plans_anciens")
PLAN = join(DOSSIER, "plan_roubaix_1899_georef.tif")
PLAN_1855 = join(DOSSIER, "plan_roubaix_1855_barbotin_georef.tif")
PLAN_1889 = join(DOSSIER, "plan_roubaix_1889_georef.tif")
PROJET = join(DOSSIER, "plan_roubaix_1899.qgz")
OPACITE_IGN = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5

# même source que les projets de validation des sites (data/geo/validation_sites_20261112.qgz)
PLAN_IGN = ("crs=EPSG:3857&format&type=xyz&url=https://data.geopf.fr/wmts?SERVICE%3DWMTS%26REQUEST%3DGetTile"
            "%26VERSION%3D1.0.0%26TILEMATRIXSET%3DPM%26TILEMATRIX%3D%7Bz%7D%26TILEROW%3D%7By%7D%26TILECOL%3D%7Bx%7D"
            "%26LAYER%3DGEOGRAPHICALGRIDSYSTEMS.PLANIGNV2%26STYLE%3Dnormal%26FORMAT%3Dimage/png&zmax=19&zmin=0")


def main():
    for f in (PLAN, PLAN_1855, PLAN_1889):
        if not exists(f):
            sys.exit(f"GeoTIFF absent : {f}")
    app = QgsApplication([], False)
    app.initQgis()
    p = QgsProject.instance()
    p.setCrs(QgsCoordinateReferenceSystem("EPSG:2154"))
    p.setTitle("Plans de Roubaix 1855, 1889 et 1899 sous le Plan IGN")
    p.writeEntryBool("Paths", "/Absolute", False)

    plan = QgsRasterLayer(PLAN, "Plan de Roubaix 1899 (P. Pachy, 6Fi024)", "gdal")
    plan_1855 = QgsRasterLayer(PLAN_1855, "Plan de Roubaix 1855 (E. Barbotin, BnF Ge C 3003)", "gdal")
    plan_1889 = QgsRasterLayer(PLAN_1889, "Plan de Roubaix 1889 (service de la voirie, 6Fi022)", "gdal")
    ign = QgsRasterLayer(PLAN_IGN, "Plan IGN v2 (transparence)", "wms")
    for couche in (plan, plan_1855, plan_1889, ign):
        if not couche.isValid():
            sys.exit(f"couche invalide : {couche.name()}")
    ign.renderer().setOpacity(OPACITE_IGN)

    # légende, de haut en bas : IGN, 1855 (centre-ville), 1889, 1899
    racine = p.layerTreeRoot()
    for couche in (ign, plan_1855, plan_1889, plan):
        p.addMapLayer(couche, False)
        racine.addLayer(couche)

    p.viewSettings().setDefaultViewExtent(QgsReferencedRectangle(plan.extent(), plan.crs()))
    if not p.write(PROJET):
        sys.exit(f"écriture impossible : {PROJET}")
    print(f"→ {PROJET} (IGN à {OPACITE_IGN:.0%} d'opacité au-dessus des plans de 1855, 1889 et 1899)")
    app.exitQgis()


if __name__ == "__main__":
    main()
