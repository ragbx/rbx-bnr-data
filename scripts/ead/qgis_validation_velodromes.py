"""Projet QGIS de validation des vélodromes, document par document (2026-11-17).

    python3 scripts/ead/qgis_validation_velodromes.py              (Python de QGIS) : génère le projet
    python3 scripts/ead/qgis_validation_velodromes.py --reporter   (tout Python) : saisies → CSV

Les notices qui citent un vélodrome sans le nommer sont réparties par sites_candidats.py (velodrome()) :
vélodrome proposé par un indice, par la date seule, ou « à revoir ». Ce script écrit, dans data/geo/ :

  - validation_velodromes_notices_<date>.gpkg :
      * notices : un point par notice, posé sur le vélodrome proposé (« à revoir » : aire d'attente à l'ouest
        de la ville) ; vélodrome proposé, motif, date, titre, lien ARK, note, indexation ; saisies decision
        (oui = comme proposé / Barbieux / Pétrieux / Stab / Quatre Villes / aucun / à revoir) et remarque ;
      * velodromes : les quatre vélodromes (dont les Quatre Villes, à Wattrelos) ;
  - validation_velodromes_notices_<date>.qgz : copie du projet de validation du 12/11 (fonds IGN,
    état-major, photographies 1950-1965, cadastre, OSM ; référentiel) et plans de 1855, 1889 et 1899.

Validation : table attributaire de « Notices », vue formulaire, une notice après l'autre.

`--reporter` écrit les décisions dans velodromes_notices_manuel.csv (ir, c_id, velodrome, remarque,
groupe_propose ; sauvegarde avant écriture), lu par sites_candidats.py : la notice passe dans
`velodrome <nom> (choix)` (validé) ou `velodrome (aucun)` (exclu). Relancer ensuite
`sites_candidats.py --date 20261112`. La génération commence par un report si le GeoPackage existe déjà.
"""
import csv
import os
import shutil
import sqlite3
import sys
import time
from glob import glob
from os.path import abspath, dirname, exists, join

RACINE = dirname(dirname(dirname(abspath(__file__))))
GEO = join(RACINE, "data", "geo")
DATE = "20261117"
PROPOSITIONS = join(GEO, "propositions_sites_20261112.csv")
NOTICES = join(GEO, "propositions_sites_20261112_notices.csv")
MANUEL = join(GEO, "velodromes_notices_manuel.csv")
REFERENTIELS = [join(GEO, "referentiel_lieux_roubaix.csv"), join(GEO, "referentiel_lieux_roubaix_ecartes.csv")]
IR_GEO = join(RACINE, "results", "ead", "ead_cor", "bnr2mnesysgeo")
IR_ACTUELS = join(RACINE, "results", "ead", "ead_cor", "bnr2mnesys")  # liens ARK publication:previous
MODELE = join(GEO, "validation_sites_20261112.qgz")
PLANS = join(GEO, "plans_anciens")
GPKG = join(GEO, f"validation_velodromes_notices_{DATE}.gpkg")
PROJET = join(GEO, f"validation_velodromes_notices_{DATE}.qgz")

# Même liste que VELODROMES de sites_candidats.py : nom court → (lieu_id, libellé du formulaire, dates).
VELODROMES = {
    "barbieux": ("bnr:velodrome roubaisien", "Vélodrome roubaisien (Barbieux)", "1895-1924"),
    "petrieux": ("bdtopo:SURFACTI0000002478333140", "Vélodrome André Pétrieux", "depuis 1936"),
    "stab": ("bdtopo:SURFACTI0000000331790932", "Stab (vélodrome couvert)", "depuis 2012"),
    "quatre-villes": ("bnr:velodrome quatre villes", "Vélodrome des Quatre Villes (Wattrelos)", "vers 1908-1913"),
}
DECISIONS = [("", ""), ("oui, comme proposé", "oui"), ("Vélodrome roubaisien (Barbieux)", "barbieux"),
             ("Vélodrome André Pétrieux", "petrieux"), ("Stab", "stab"),
             ("Vélodrome des Quatre Villes (Wattrelos)", "quatre-villes"), ("aucun de ces vélodromes", "aucun"),
             ("à revoir", "à revoir")]
ATTENTE = (3.1180, 50.6780)  # aire d'attente des notices « à revoir » (comme les sites sans point)
# Ordre du formulaire : ce qu'il faut lire et saisir d'abord.
CHAMPS = ["propose", "motif", "unitdate", "unittitle", "ark", "decision", "alerte", "remarque", "note", "indexation",
          "attribution", "lien", "ir", "c_id", "groupe"]
SAISIES = {"decision", "remarque"}


def lire(f):
    with open(f, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def nom_propose(groupe):
    """barbieux / petrieux / stab, ou "" pour « à revoir »."""
    mots = groupe.split()
    return mots[1] if len(mots) > 1 and mots[1] in VELODROMES else ""


def reporter():
    """Décisions du GeoPackage → velodromes_notices_manuel.csv (les choix déjà reportés sont gardés)."""
    con = sqlite3.connect(GPKG)
    saisies = list(con.execute("SELECT ir, c_id, groupe, decision, remarque FROM notices "
                               "WHERE coalesce(decision, '') != '' OR coalesce(remarque, '') != ''"))
    con.close()
    anciens = {(r["ir"], r["c_id"]): r for r in lire(MANUEL)} if exists(MANUEL) else {}
    nouveaux = dict(anciens)
    for ir, c_id, groupe, decision, remarque in saisies:
        velodrome = nom_propose(groupe) if decision == "oui" else (decision or "")
        if decision == "oui" and not velodrome:
            velodrome = "à revoir"  # « oui » sur une notice sans proposition : rien à confirmer
        nouveaux[(ir, c_id)] = {"ir": ir, "c_id": c_id, "velodrome": velodrome, "remarque": remarque or "",
                                "groupe_propose": groupe}
    if nouveaux != anciens:
        if exists(MANUEL):
            shutil.copy(MANUEL, MANUEL.replace(".csv", time.strftime(".backup_%Y%m%d_%H%M%S_avant_report.csv")))
        with open(MANUEL, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["ir", "c_id", "velodrome", "remarque", "groupe_propose"])
            w.writeheader()
            w.writerows(sorted(nouveaux.values(), key=lambda r: (r["ir"], r["c_id"])))
    print(f"report : {len(saisies)} notice(s) saisie(s) → {MANUEL}")


def details_notices(cles):
    """{(ir, c_id): {unitdate, note, indexation, ark}} des notices demandées, lues dans les IR."""
    from lxml import etree

    def txt(e):
        return " ".join(" ".join(e.itertext()).split()) if e is not None else ""

    out = {k: {"unitdate": "", "note": "", "indexation": "", "ark": ""} for k in cles}
    for ir in sorted({ir for ir, _ in cles}):
        for dossier, ark in ((IR_GEO, False), (IR_ACTUELS, True)):
            for f in glob(join(dossier, f"*_{ir}.xml")):
                for c in etree.parse(f).iter("c"):
                    k = (ir, c.get("id"))
                    if k not in out:
                        continue
                    if ark:
                        for d in c.findall("dao") + c.findall("daogrp/daoloc"):
                            if d.get("role") == "publication:previous" and "ark:/" in (d.get("href") or ""):
                                out[k]["ark"] = d.get("href")
                                break
                        continue
                    out[k]["unitdate"] = txt(c.find("did/unitdate"))
                    out[k]["note"] = " ".join(txt(x) for x in c.findall("scopecontent"))
                    out[k]["indexation"] = " ; ".join(f"{x.tag} : {txt(x)}" for x in c.findall("controlaccess/*"))
    return out


def points_velodromes():
    """{nom court: WKT} depuis le référentiel (en service ou écarté)."""
    ids = {v[0]: nom for nom, v in VELODROMES.items()}
    out = {ids[r["lieu_id"]]: r.get("coord_manuel") or r["coord_wkt"]
           for f in REFERENTIELS for r in lire(f) if r["lieu_id"] in ids}
    if set(VELODROMES) - set(out):
        sys.exit(f"vélodrome(s) absent(s) du référentiel : {set(VELODROMES) - set(out)}")
    return out


def generer():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from qgis.core import (Qgis, QgsApplication, QgsCategorizedSymbolRenderer, QgsCoordinateReferenceSystem,
                           QgsCoordinateTransformContext, QgsEditorWidgetSetup, QgsFeature, QgsField, QgsFields,
                           QgsGeometry, QgsMarkerSymbol, QgsPalLayerSettings, QgsPointXY, QgsProject, QgsRasterLayer,
                           QgsReferencedRectangle, QgsRendererCategory, QgsVectorFileWriter, QgsVectorLayer,
                           QgsVectorLayerSimpleLabeling, QgsWkbTypes)
    from qgis.PyQt.QtCore import QVariant

    groupes = {p["site_cle"] for p in lire(PROPOSITIONS) if p["site_cle"].startswith("velodrome")
               and p["site_cle"].endswith(("(indice)", "(date)", "(a revoir)", "(choix)", "(aucun)"))}
    notices = [n for n in lire(NOTICES) if n["site_cle"] in groupes]
    manuel = {(r["ir"], r["c_id"]): r for r in lire(MANUEL)} if exists(MANUEL) else {}
    details = details_notices({(n["ir"], n["c_id"]) for n in notices})
    velos = points_velodromes()

    app = QgsApplication([], False)
    app.initQgis()
    crs = QgsCoordinateReferenceSystem("EPSG:4326")

    def couche(nom, champs, rangs, premiere):
        fields = QgsFields()
        for c in champs:
            fields.append(QgsField(c, QVariant.String))
        opts = QgsVectorFileWriter.SaveVectorOptions()
        opts.driverName, opts.layerName = "GPKG", nom
        if not premiere:
            opts.actionOnExistingFile = QgsVectorFileWriter.CreateOrOverwriteLayer
        w = QgsVectorFileWriter.create(GPKG, fields, QgsWkbTypes.Point, crs, QgsCoordinateTransformContext(), opts)
        for geom, valeurs in rangs:
            f = QgsFeature(fields)
            f.setGeometry(geom)
            f.setAttributes([str(valeurs.get(c, "")) for c in champs])
            w.addFeature(f)
        del w

    rangs = []
    for n in notices:
        auto = n.get("groupe_auto") or n["site_cle"]  # proposition automatique, avant le choix à la validation
        nom = nom_propose(auto)
        motif = auto.split("(")[1].rstrip(")").replace("a revoir", "à revoir")
        d = details[(n["ir"], n["c_id"])]
        m = manuel.get((n["ir"], n["c_id"]), {})
        decision = m.get("velodrome", "")
        if decision and decision == nom:
            decision = "oui"
        # Décision prise avant l'ajout des Quatre Villes (2026-11-17), que la règle actuelle rend possibles.
        alerte = ""
        if decision and m.get("velodrome") != "quatre-villes" and "quatre-villes" in n["attribution"]:
            alerte = "décision prise avant l'ajout du vélodrome des Quatre Villes, désormais possible : à revérifier"
        g = QgsGeometry.fromWkt(velos[nom]) if nom else QgsGeometry.fromPointXY(QgsPointXY(*ATTENTE))
        rangs.append((g, {"propose": VELODROMES[nom][1] if nom else "à revoir", "motif": motif, **d,
                          "unittitle": n["unittitle"], "attribution": n["attribution"], "lien": n["lien"],
                          "ir": n["ir"], "c_id": n["c_id"], "groupe": auto, "alerte": alerte,
                          "decision": decision, "remarque": m.get("remarque", "")}))
    # ordre de la navigation : vélodrome proposé, motif, date
    rangs.sort(key=lambda r: (list(VELODROMES).index(nom_propose(r[1]["groupe"])) if nom_propose(r[1]["groupe"]) else 9,
                              r[1]["motif"], r[1]["unitdate"], r[1]["ir"], r[1]["c_id"]))
    if exists(GPKG):
        os.remove(GPKG)
    couche("notices", CHAMPS, rangs, True)
    couche("velodromes", ["nom", "libelle", "dates", "lieu_id"],
           [(QgsGeometry.fromWkt(velos[nom]), {"nom": nom, "lieu_id": v[0], "libelle": v[1], "dates": v[2]})
            for nom, v in VELODROMES.items()], False)

    p = QgsProject.instance()
    if not p.read(MODELE):
        sys.exit(f"projet modèle illisible : {MODELE}")
    racine = p.layerTreeRoot()
    for nom in ("Sites à valider", "Adresses des notes (BAN)", "Autres points candidats"):
        for l in p.mapLayersByName(nom):
            p.removeMapLayer(l.id())
    ancien = racine.findGroup("Validation des sites (2026-11-12)")
    if ancien is not None:
        racine.removeChildNode(ancien)
    groupe = racine.insertGroup(0, f"Validation des vélodromes ({DATE})")
    l_notices = QgsVectorLayer(f"{GPKG}|layername=notices", "Notices", "ogr")
    l_velos = QgsVectorLayer(f"{GPKG}|layername=velodromes", "Vélodromes", "ogr")
    for l in (l_notices, l_velos):
        if not l.isValid():
            sys.exit(f"couche invalide : {l.name()}")
        p.addMapLayer(l, False)
        groupe.addLayer(l)

    fc = l_notices.editFormConfig()
    for i, f in enumerate(l_notices.fields()):
        nom = f.name()
        if nom == "decision":
            l_notices.setEditorWidgetSetup(i, QgsEditorWidgetSetup("ValueMap", {"map": [{k: v} for k, v in DECISIONS]}))
        elif nom == "ark":
            l_notices.setEditorWidgetSetup(i, QgsEditorWidgetSetup("ExternalResource", {"UseLink": True, "FullUrl": True}))
        elif nom in ("unittitle", "note", "indexation", "attribution", "remarque", "alerte"):
            l_notices.setEditorWidgetSetup(i, QgsEditorWidgetSetup("TextEdit", {"IsMultiline": True}))
        fc.setReadOnly(i, nom not in SAISIES)
    l_notices.setEditFormConfig(fc)
    l_notices.setDisplayExpression('"unitdate" || \' — \' || "unittitle"')
    # couleur : à valider (selon le vélodrome proposé) ou décidé
    expr = ("CASE WHEN coalesce(\"alerte\", '') != '' THEN 'à revérifier' "
            "WHEN coalesce(\"decision\", '') != '' THEN 'décidé' ELSE \"propose\" END")
    couleurs = {VELODROMES["barbieux"][1]: "#2FAAAF", VELODROMES["petrieux"][1]: "#E9B400",
                VELODROMES["stab"][1]: "#ADA796",
                VELODROMES["quatre-villes"][1]: "#000000", "à revoir": "#C03730", "décidé": "#9460A4",
                "à revérifier": "#FF7F00"}
    l_notices.setRenderer(QgsCategorizedSymbolRenderer(expr, [QgsRendererCategory(v, QgsMarkerSymbol.createSimple(
        {"name": "circle", "color": c, "size": "4", "outline_color": "#ffffff", "outline_width": "0.5"}), v)
        for v, c in couleurs.items()]))
    l_velos.renderer().setSymbol(QgsMarkerSymbol.createSimple(
        {"name": "star", "color": "#000000", "size": "6", "outline_color": "#ffffff", "outline_width": "0.5"}))
    lab = QgsPalLayerSettings()
    lab.fieldName, lab.isExpression, lab.enabled = '"libelle" || \' \' || "dates"', True, True
    l_velos.setLabeling(QgsVectorLayerSimpleLabeling(lab))
    l_velos.setLabelsEnabled(True)

    # Plans anciens (data/geo/plans_anciens, hors git) : le vélodrome roubaisien figure sur 1889 et 1899.
    fonds = racine.insertGroup(1, "Plans anciens")
    for fichier, titre in (("plan_roubaix_1899_georef.tif", "Plan de Roubaix 1899 (P. Pachy, 6Fi024)"),
                           ("plan_roubaix_1889_georef.tif", "Plan de Roubaix 1889 (service de la voirie, 6Fi022)"),
                           ("plan_roubaix_1855_barbotin_georef.tif", "Plan de Roubaix 1855 (E. Barbotin)")):
        chemin = join(PLANS, fichier)
        r = QgsRasterLayer(chemin, titre, "gdal") if exists(chemin) else None
        if r is None or not r.isValid():
            print(f"  plan absent ou illisible, ignoré : {chemin}")
            continue
        p.addMapLayer(r, False)
        fonds.addLayer(r).setItemVisibilityChecked(fichier.startswith("plan_roubaix_1899"))

    p.setTitle(f"Validation des vélodromes, notice par notice ({DATE})")
    p.setFilePathStorage(Qgis.FilePathType.Relative)
    emprise = l_velos.extent()
    emprise.combineExtentWith(l_notices.extent())
    emprise.grow(0.004)
    p.viewSettings().setDefaultViewExtent(QgsReferencedRectangle(emprise, l_velos.crs()))
    if not p.write(PROJET):
        sys.exit("écriture du projet impossible")
    print(f"{len(rangs)} notices, {len(VELODROMES)} vélodromes → {GPKG}")
    print(f"projet → {PROJET}")
    app.exitQgis()


if __name__ == "__main__":
    if "--reporter" in sys.argv:
        reporter()
    else:
        if exists(GPKG):
            reporter()  # ne pas perdre les saisies d'une validation en cours
        generer()
