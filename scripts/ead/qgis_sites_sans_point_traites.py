"""Projet QGIS de relecture des sites sans point déjà traités (2026-11-14).

    python3 scripts/ead/qgis_sites_sans_point_traites.py [AAAAMMJJ]      (Python de QGIS)

Lit, en lecture seule, la couche sites_a_valider de data/geo/validation_sites_sans_point_<date>.gpkg
(saisies faites dans QGIS) et ne garde que les sites qui ont une décision. Écrit dans data/geo/ :

  - sites_sans_point_traites_<date>.gpkg :
      * sites_traites : un point par lieu décidé — les formes qui tombent au même endroit avec la même
        décision sont regroupées (libellé = forme la plus citée, « +n » autres formes) ; position = le point
        saisi dans lieu_manuel s'il y en a un, sinon le point de la couche ;
      * deplacements : trait du point proposé vers le point saisi (lieu_manuel) ;
  - sites_sans_point_traites_<date>.qgz : repris du projet de validation (fonds IGN, état-major, photos
    1950-1965, cadastre, OSM ; couches du référentiel), points colorés selon la décision.

Le GeoPackage de validation n'est pas modifié : relancer le script après de nouvelles saisies.
"""
import os
import re
import sqlite3
import sys
from collections import OrderedDict
from os.path import abspath, dirname, join

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from qgis.core import (Qgis, QgsApplication, QgsCategorizedSymbolRenderer, QgsCoordinateReferenceSystem,
                       QgsCoordinateTransformContext, QgsFeature, QgsField, QgsFields, QgsGeometry,
                       QgsLineSymbol, QgsMarkerSymbol, QgsPalLayerSettings, QgsPointXY, QgsProject,
                       QgsReferencedRectangle, QgsRendererCategory, QgsTextBufferSettings, QgsTextFormat,
                       QgsVectorFileWriter, QgsVectorLayer, QgsVectorLayerSimpleLabeling, QgsWkbTypes)
from qgis.PyQt.QtCore import QVariant
from qgis.PyQt.QtGui import QColor

RACINE = dirname(dirname(dirname(abspath(__file__))))
GEO = join(RACINE, "data", "geo")
DATE = sys.argv[1] if len(sys.argv) > 1 else "20261114"
SOURCE = join(GEO, f"validation_sites_sans_point_{DATE}.gpkg")
MODELE = join(GEO, f"validation_sites_sans_point_{DATE}.qgz")
GPKG = join(GEO, f"sites_sans_point_traites_{DATE}.gpkg")
PROJET = join(GEO, f"sites_sans_point_traites_{DATE}.qgz")

# palette BnR : décision → couleur
COULEURS = OrderedDict([("oui", "#2FAAAF"), ("autre point", "#9460A4"), ("à revoir", "#E9B400"),
                        ("non", "#ADA796")])
CHAMPS = ["decision", "libelle", "formes", "nb_formes", "nb_notices", "famille", "groupes", "lieu_manuel",
          "remarque", "a_placer", "exemple_1", "ark_1"]
POINT = re.compile(r"^\s*point\s*\(\s*([-\d.]+)\s*[\s,]\s*([-\d.]+)\s*\)\s*$", re.I)   # « POINT (lon lat) », virgule tolérée


def geom_gpkg(blob):
    """Géométrie d'un blob GeoPackage (en-tête GP + WKB)."""
    enveloppe = [0, 32, 48, 48, 64][(blob[3] >> 1) & 7]
    g = QgsGeometry()
    g.fromWkb(bytes(blob[8 + enveloppe:]))
    return g


def lire_traites():
    c = sqlite3.connect(f"file:{SOURCE}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    lignes = c.execute("select * from sites_a_valider where coalesce(decision, '') <> '' order by fid").fetchall()
    c.close()
    return lignes


def regrouper(lignes):
    lieux = OrderedDict()
    traits = []
    for l in lignes:
        origine = geom_gpkg(l["geom"]).asPoint()
        m = POINT.match(l["lieu_manuel"] or "")
        pos = QgsPointXY(float(m.group(1)), float(m.group(2))) if m else origine
        if m and (abs(pos.x() - origine.x()) > 1e-7 or abs(pos.y() - origine.y()) > 1e-7):
            traits.append((l["decision"], origine, pos))
        cle = (l["decision"], round(pos.x(), 6), round(pos.y(), 6))
        lieux.setdefault(cle, {"pos": pos, "lignes": []})["lignes"].append(l)
    # un trait par couple (origine, point saisi)
    traits = list(OrderedDict(((d, round(o.x(), 6), round(o.y(), 6), round(p.x(), 6), round(p.y(), 6)), (d, o, p))
                              for d, o, p in traits).values())
    return lieux, traits


def uniq(valeurs):
    return list(OrderedDict.fromkeys(v for v in valeurs if v))


def ecrire(lieux, traits):
    crs = QgsCoordinateReferenceSystem("EPSG:4326")
    ctx = QgsCoordinateTransformContext()
    champs = QgsFields()
    for c in CHAMPS:
        champs.append(QgsField(c, QVariant.Int if c.startswith("nb_") else QVariant.String))
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName, opts.layerName = "GPKG", "sites_traites"
    w = QgsVectorFileWriter.create(GPKG, champs, QgsWkbTypes.Point, crs, ctx, opts)
    for (decision, _, _), lieu in lieux.items():
        ls = sorted(lieu["lignes"], key=lambda l: -int(l["nb_notices"] or 0))
        formes = uniq(l["forme"] for l in ls)
        libelle = formes[0] + (f" (+{len(formes) - 1})" if len(formes) > 1 else "")
        f = QgsFeature(champs)
        f.setGeometry(QgsGeometry.fromPointXY(lieu["pos"]))
        f.setAttributes([decision, libelle, " | ".join(formes), len(formes),
                         sum(int(l["nb_notices"] or 0) for l in ls),
                         " | ".join(uniq(l["famille"] for l in ls)), " | ".join(uniq(l["groupe"] for l in ls)),
                         " | ".join(uniq(l["lieu_manuel"] for l in ls)), " | ".join(uniq(l["remarque"] for l in ls)),
                         "oui" if any(l["a_placer"] == "oui" for l in ls) else "",
                         ls[0]["exemple_1"], ls[0]["ark_1"]])
        w.addFeature(f)
    del w

    champs_t = QgsFields()
    champs_t.append(QgsField("decision", QVariant.String))
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName, opts.layerName = "GPKG", "deplacements"
    opts.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
    w = QgsVectorFileWriter.create(GPKG, champs_t, QgsWkbTypes.LineString, crs, ctx, opts)
    for decision, o, p in traits:
        f = QgsFeature(champs_t)
        f.setGeometry(QgsGeometry.fromPolylineXY([o, p]))
        f.setAttributes([decision])
        w.addFeature(f)
    del w


def styler_sites(couche, comptes):
    cats = []
    for valeur, couleur in COULEURS.items():
        s = QgsMarkerSymbol.createSimple({"name": "circle", "color": couleur, "size": "3.6",
                                         "outline_color": "#000000", "outline_width": "0.3"})
        cats.append(QgsRendererCategory(valeur, s, f"{valeur} ({comptes.get(valeur, 0)})"))
    couche.setRenderer(QgsCategorizedSymbolRenderer("decision", cats))
    lab = QgsPalLayerSettings()
    lab.fieldName = "libelle"
    lab.enabled = True
    fmt = QgsTextFormat()
    fmt.setSize(8)
    tampon = QgsTextBufferSettings()
    tampon.setEnabled(True)
    tampon.setSize(0.8)
    tampon.setColor(QColor("white"))
    fmt.setBuffer(tampon)
    lab.setFormat(fmt)
    couche.setLabeling(QgsVectorLayerSimpleLabeling(lab))
    couche.setLabelsEnabled(True)
    couche.setDisplayExpression('"libelle"')


def styler_traits(couche):
    s = QgsLineSymbol.createSimple({"line_color": COULEURS["autre point"], "line_width": "0.5",
                                    "line_style": "dash"})
    couche.renderer().setSymbol(s)


def main():
    lignes = lire_traites()
    lieux, traits = regrouper(lignes)
    app = QgsApplication([], False)
    app.initQgis()
    ecrire(lieux, traits)

    p = QgsProject.instance()
    if not p.read(MODELE):
        sys.exit(f"projet modèle illisible : {MODELE}")
    racine = p.layerTreeRoot()
    couches = {l.name(): l for l in p.mapLayers().values()}
    groupe = racine.findLayer(couches["Sites à valider"].id()).parent()
    for nom in ("Sites à valider", "Lieux OSM candidats"):
        if nom in couches:
            p.removeMapLayer(couches[nom].id())
    groupe.setName(f"Sites sans point traités ({DATE[:4]}-{DATE[4:6]}-{DATE[6:]})")

    sites = QgsVectorLayer(f"{GPKG}|layername=sites_traites", "Sites traités", "ogr")
    deplacements = QgsVectorLayer(f"{GPKG}|layername=deplacements", "Déplacements (proposé → saisi)", "ogr")
    comptes = {}
    for f in sites.getFeatures():
        comptes[f["decision"]] = comptes.get(f["decision"], 0) + 1
    styler_sites(sites, comptes)
    styler_traits(deplacements)
    p.addMapLayer(deplacements, False)
    p.addMapLayer(sites, False)
    groupe.insertLayer(0, deplacements)
    groupe.insertLayer(0, sites)

    p.setTitle(f"Sites sans point traités ({DATE})")
    p.setFilePathStorage(Qgis.FilePathType.Relative)
    emprise = sites.extent()
    emprise.combineExtentWith(couches["Quartiers"].extent())
    emprise.grow(0.003)
    p.viewSettings().setDefaultViewExtent(QgsReferencedRectangle(emprise, sites.crs()))
    if not p.write(PROJET):
        sys.exit("écriture du projet impossible")
    print(f"{len(lignes)} formes décidées → {sites.featureCount()} lieux, {deplacements.featureCount()} déplacements")
    print("par décision (lieux) : " + ", ".join(f"{k} {comptes.get(k, 0)}" for k in COULEURS))
    print(f"→ {GPKG}\n→ {PROJET}")
    app.exitQgis()


if __name__ == "__main__":
    main()
