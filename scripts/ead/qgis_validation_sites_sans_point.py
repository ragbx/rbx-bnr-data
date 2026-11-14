"""Projet QGIS de validation des sites sans point (2026-11-14). À lancer avec le Python de QGIS :

    "C:/Program Files/QGIS 3.40.6/bin/python-qgis-ltr.bat" scripts/ead/qgis_validation_sites_sans_point.py [AAAAMMJJ]

Part du projet de validation du 12/11 (data/geo/validation_sites_20261112.qgz : fonds IGN, état-major,
photographies 1950-1965, cadastre, OSM ; couches du référentiel) et écrit, dans data/geo/ :

  - validation_sites_sans_point_<date>.gpkg :
      * sites_a_valider : un point par site de propositions_sites_sans_point_<date>.csv
        (sites_sans_point.py), à coord_manuel sinon à la proposition ; les sites sans piste sont posés
        en grille sur une « aire d'attente » à l'ouest de la ville (a_placer = oui), à glisser à leur place ;
      * osm_candidats : lieux OSM rapprochés d'un site ;
  - validation_sites_sans_point_<date>.qgz : la couche « Sites à valider » reprend le formulaire du
    12/11 (decision en liste : oui / non / autre point / à revoir ; lieu_manuel ; remarque ; exemples de
    notices et liens ARK), symbolisée selon la piste (solide, approximative, à placer, décidé).

Saisie dans QGIS : décision, lieu_manuel, remarque ; déplacer le point si besoin. Puis
`python scripts/ead/sites_sans_point.py --reporter` reporte ces saisies (et la position, si le point a
bougé) dans le CSV.
"""
import csv
import sys
from os.path import abspath, dirname, join

from qgis.core import (Qgis, QgsApplication, QgsCategorizedSymbolRenderer, QgsCoordinateReferenceSystem,
                       QgsEditorWidgetSetup, QgsFeature, QgsField, QgsFields, QgsGeometry, QgsMarkerSymbol,
                       QgsPalLayerSettings, QgsPointXY, QgsProject, QgsRectangle, QgsReferencedRectangle, QgsRendererCategory,
                       QgsVectorFileWriter, QgsVectorLayer, QgsVectorLayerSimpleLabeling, QgsWkbTypes,
                       QgsCoordinateTransformContext)
from qgis.PyQt.QtCore import QVariant

RACINE = dirname(dirname(dirname(abspath(__file__))))
GEO = join(RACINE, "data", "geo")
DATE = sys.argv[1] if len(sys.argv) > 1 else "20261114"
CSV = join(GEO, f"propositions_sites_sans_point_{DATE}.csv")
PROPOSITIONS_GPKG = join(GEO, f"propositions_sites_sans_point_{DATE}.gpkg")
MODELE = join(GEO, "validation_sites_20261112.qgz")
GPKG = join(GEO, f"validation_sites_sans_point_{DATE}.gpkg")
PROJET = join(GEO, f"validation_sites_sans_point_{DATE}.qgz")

CHAMPS = ["groupe", "site_cle", "forme", "famille", "nb_notices", "nb_retenus", "piste", "proposition",
          "libelle_propose", "id_propose", "rue_coindexee", "autres_candidats", "alertes", "variantes",
          "dates_note", "exemple_1", "ark_1", "exemple_2", "ark_2", "exemple_3", "ark_3", "a_placer", "coord_origine",
          "decision", "lieu_manuel", "remarque"]
SAISIES = {"decision", "lieu_manuel", "remarque"}
CHOIX = [("oui", "oui"), ("non", "non"), ("autre point (lieu_manuel)", "autre point"), ("à revoir", "à revoir")]
# Aire d'attente des sites sans piste : grille à l'ouest de Roubaix (WGS84), 12 colonnes.
ATTENTE_LON, ATTENTE_LAT, PAS, COLONNES = 3.1180, 50.6780, 0.0012, 12


def piste(l):
    if l["decision"]:
        return "décidé"
    p = l["proposition"]
    if p == "aucune piste" and not l["coord_manuel"]:
        return "à placer"
    if p.startswith("milieu"):
        return "approximative"
    return "solide"


def ecrire_couches(lignes):
    crs = QgsCoordinateReferenceSystem("EPSG:4326")
    champs = QgsFields()
    for c in CHAMPS:
        champs.append(QgsField(c, QVariant.String))
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName, opts.layerName = "GPKG", "sites_a_valider"
    w = QgsVectorFileWriter.create(GPKG, champs, QgsWkbTypes.Point, crs, QgsCoordinateTransformContext(), opts)
    attente = 0
    for l in lignes:
        f = QgsFeature(champs)
        geom_wkt = l["coord_manuel"] or l["coord_wkt"]
        if geom_wkt:
            g = QgsGeometry.fromWkt(geom_wkt)
            l["a_placer"] = ""
        else:
            g = QgsGeometry.fromPointXY(QgsPointXY(ATTENTE_LON + PAS * (attente % COLONNES),
                                                   ATTENTE_LAT - PAS * (attente // COLONNES)))
            attente += 1
            l["a_placer"] = "oui"
        l["piste"] = piste(l)
        l["coord_origine"] = g.asWkt(6)  # position d'origine : le report repère un point déplacé
        f.setGeometry(g)
        f.setAttributes([l.get(c, "") for c in CHAMPS])
        w.addFeature(f)
    del w
    # lieux OSM candidats, repris du GeoPackage de sites_sans_point.py
    osm = QgsVectorLayer(f"{PROPOSITIONS_GPKG}|layername=osm_candidats", "osm", "ogr")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName, opts.layerName = "GPKG", "osm_candidats"
    opts.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
    QgsVectorFileWriter.writeAsVectorFormatV3(osm, GPKG, QgsCoordinateTransformContext(), opts)
    return attente


def configurer_sites(couche):
    """Formulaire : saisies modifiables, le reste en lecture seule ; ARK en lien ; symboles et étiquettes."""
    fc = couche.editFormConfig()
    for i, f in enumerate(couche.fields()):
        nom = f.name()
        if nom == "decision":
            couche.setEditorWidgetSetup(i, QgsEditorWidgetSetup("ValueMap", {"map": [{k: v} for k, v in CHOIX]}))
        elif nom.startswith("ark_"):
            couche.setEditorWidgetSetup(i, QgsEditorWidgetSetup("ExternalResource", {"UseLink": True, "FullUrl": True}))
        elif nom in ("autres_candidats", "alertes", "variantes", "remarque") or nom.startswith("exemple_"):
            couche.setEditorWidgetSetup(i, QgsEditorWidgetSetup("TextEdit", {"IsMultiline": True}))
        else:
            couche.setEditorWidgetSetup(i, QgsEditorWidgetSetup("TextEdit", {}))
        fc.setReadOnly(i, nom not in SAISIES)
    couche.setEditFormConfig(fc)
    couleurs = {"solide": "#2FAAAF", "approximative": "#E9B400", "à placer": "#C03730", "décidé": "#9460A4"}
    cats = []
    for valeur, couleur in couleurs.items():
        s = QgsMarkerSymbol.createSimple({"name": "circle", "color": couleur, "size": "3.2",
                                         "outline_color": "#ffffff", "outline_width": "0.4"})
        cats.append(QgsRendererCategory(valeur, s, valeur))
    couche.setRenderer(QgsCategorizedSymbolRenderer("piste", cats))
    lab = QgsPalLayerSettings()
    lab.fieldName = "forme"
    lab.enabled = True
    couche.setLabeling(QgsVectorLayerSimpleLabeling(lab))
    couche.setLabelsEnabled(True)
    couche.setDisplayExpression('"forme"')


def main():
    with open(CSV, encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))
    app = QgsApplication([], False)
    app.initQgis()
    a_placer = ecrire_couches(lignes)

    p = QgsProject.instance()
    if not p.read(MODELE):
        sys.exit(f"projet modèle illisible : {MODELE}")
    couches = {l.name(): l for l in p.mapLayers().values()}
    sites = couches["Sites à valider"]
    sites.setDataSource(f"{GPKG}|layername=sites_a_valider", "Sites à valider", "ogr")
    configurer_sites(sites)
    autres = couches["Autres points candidats"]
    autres.setDataSource(f"{GPKG}|layername=osm_candidats", "Lieux OSM candidats", "ogr")
    autres.setDisplayExpression('"nom"')
    p.removeMapLayer(couches["Adresses des notes (BAN)"].id())
    p.setTitle(f"Validation des sites sans point ({DATE})")
    p.setFilePathStorage(Qgis.FilePathType.Relative)  # chemins relatifs
    # emprise par défaut : Roubaix (quartiers) et l'aire d'attente, avec une marge
    emprise = couches["Quartiers"].extent()
    emprise.combineExtentWith(sites.extent())
    emprise.grow(0.003)
    p.viewSettings().setDefaultViewExtent(QgsReferencedRectangle(emprise, couches["Quartiers"].crs()))
    if not p.write(PROJET):
        sys.exit("écriture du projet impossible")
    print(f"{len(lignes)} sites ({a_placer} sur l'aire d'attente) → {GPKG}")
    print(f"projet → {PROJET}")
    app.exitQgis()


if __name__ == "__main__":
    main()
