"""Projet QGIS de validation des sites des Archives municipales (IR Mnesys), notice par notice (2026-11-19).

    python3 scripts/ead/qgis_validation_sites_mnesys.py [--top 37]   (Python de QGIS) : génère le projet
    python3 scripts/ead/qgis_validation_sites_mnesys.py --reporter   (tout Python) : saisies → CSV

Sites à valider : les --top premiers de results/ead/indexation/sites_mnesys_priorites_<date>.csv (sites repérés
dans les IR Mnesys par sites_candidats.py, hors sites déjà validés de la carte, classés par nombre de notices).
Notices : leurs liens dans data/geo/propositions_sites_mnesys_<date>_notices.csv (sujet et mention ; les liens
« exclu » sont laissés de côté). Une entité par notice × site, comme pour les vélodromes (validation document par
document, une seule couche à valider).

Sorties (data/geo/) :
  - validation_sites_mnesys_<date>.gpkg :
      * notices : un point par notice × site, posé sur le lieu proposé (point du référentiel), sinon sur l'adresse
        trouvée dans les notes, sinon dans l'aire d'attente à l'ouest de la ville ; rang du site, site, lieu proposé,
        motif, cote, date, titre, note, indexation ; saisies decision et lieu_manuel, remarque ;
      * lieux_proposes : les lieux proposés (un point par site qui en a un) ;
  - validation_sites_mnesys_<date>.qgz : copie du projet de validation du 12/11 (fonds) et plans anciens.

decision : « oui, comme proposé » ; « oui, autre lieu » (lieu_manuel = lieu_id du référentiel ou POINT (lon lat)) ;
« non : ne porte pas sur ce site » ; « à revoir ». Validation : table attributaire de « Notices », vue formulaire.

`--reporter` écrit les saisies dans data/geo/sites_mnesys_notices_manuel.csv (ir, c_id, site_cle, decision,
lieu_manuel, remarque ; sauvegarde avant écriture). La génération commence par un report si le GeoPackage existe.
"""
import argparse
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
DATE = "20261119"
PRIORITES = join(RACINE, "results", "ead", "indexation", "sites_mnesys_priorites_20261119.csv")
PROPOSITIONS = join(GEO, "propositions_sites_mnesys_20261119.csv")
NOTICES = join(GEO, "propositions_sites_mnesys_20261119_notices.csv")
MANUEL = join(GEO, "sites_mnesys_notices_manuel.csv")
REFERENTIELS = [join(GEO, "referentiel_lieux_roubaix.csv"), join(GEO, "referentiel_lieux_roubaix_ecartes.csv")]
IR_MNESYS = join(RACINE, "data", "ead", "mnesys")
MODELE = join(GEO, "validation_sites_20261112.qgz")
PLANS = join(GEO, "plans_anciens")
GPKG = join(GEO, f"validation_sites_mnesys_{DATE}.gpkg")
PROJET = join(GEO, f"validation_sites_mnesys_{DATE}.qgz")

DECISIONS = [("", ""), ("oui, comme proposé", "oui"), ("oui, autre lieu (lieu_manuel)", "autre lieu"),
             ("non : ne porte pas sur ce site", "non"), ("à revoir", "à revoir")]
ATTENTE = (3.1180, 50.6780)  # aire d'attente des notices sans lieu proposé (comme les sites sans point)
# Ordre du formulaire : ce qu'il faut lire et saisir d'abord.
CHAMPS = ["rang", "site", "propose", "unittitle", "cote", "unitdate", "decision", "lieu_manuel", "remarque",
          "motif", "lien", "note", "indexation", "position", "ir", "c_id", "site_cle", "lieu_id_propose"]
SAISIES = {"decision", "lieu_manuel", "remarque"}
COLONNES_MANUEL = ["ir", "c_id", "site_cle", "decision", "lieu_manuel", "remarque"]


def lire(f):
    with open(f, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def reporter():
    """Saisies du GeoPackage → sites_mnesys_notices_manuel.csv (les saisies déjà reportées sont gardées)."""
    con = sqlite3.connect(GPKG)
    saisies = list(con.execute("SELECT ir, c_id, site_cle, decision, lieu_manuel, remarque FROM notices WHERE "
                               "coalesce(decision, '') != '' OR coalesce(lieu_manuel, '') != '' "
                               "OR coalesce(remarque, '') != ''"))
    con.close()
    anciens = {(r["ir"], r["c_id"], r["site_cle"]): r for r in lire(MANUEL)} if exists(MANUEL) else {}
    nouveaux = dict(anciens)
    for ir, c_id, site_cle, decision, lieu_manuel, remarque in saisies:
        nouveaux[(ir, c_id, site_cle)] = {"ir": ir, "c_id": c_id, "site_cle": site_cle, "decision": decision or "",
                                          "lieu_manuel": lieu_manuel or "", "remarque": remarque or ""}
    if nouveaux != anciens:
        if exists(MANUEL):
            shutil.copy(MANUEL, MANUEL.replace(".csv", time.strftime(".backup_%Y%m%d_%H%M%S_avant_report.csv")))
        with open(MANUEL, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=COLONNES_MANUEL, lineterminator="\n")
            w.writeheader()
            w.writerows(sorted(nouveaux.values(), key=lambda r: (r["site_cle"], r["ir"], r["c_id"])))
    print(f"report : {len(saisies)} notice(s) saisie(s) → {MANUEL}")


def details_notices(cles):
    """{(ir, c_id): {cote, unitdate, normal, note, indexation}} lus dans les IR Mnesys d'origine."""
    from lxml import etree

    def txt(e):
        return " ".join(" ".join(e.itertext()).split()) if e is not None else ""

    out = {}
    for ir in sorted({ir for ir, _ in cles}):
        chemin = join(IR_MNESYS, f"FRAM59100_{ir}.xml")
        try:
            arbre = etree.parse(chemin)
        except (OSError, etree.XMLSyntaxError):
            print(f"  IR illisible, ignoré : {chemin}")
            continue
        for c in arbre.iter("c"):
            k = (ir, c.get("id"))
            if k in cles:
                ud = c.find("did/unitdate")
                out[k] = {"cote": txt(c.find("did/unitid")), "unitdate": txt(ud),
                          "normal": (ud.get("normal") or "") if ud is not None else "",
                          "note": " ".join(txt(x) for x in c.findall("scopecontent")),
                          "indexation": " ; ".join(f"{x.tag} : {txt(x)}" for x in c.findall("controlaccess/*"))}
    return out


def generer(top):
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from qgis.core import (Qgis, QgsApplication, QgsCategorizedSymbolRenderer, QgsCoordinateReferenceSystem,
                           QgsCoordinateTransformContext, QgsEditorWidgetSetup, QgsFeature, QgsField, QgsFields,
                           QgsGeometry, QgsMarkerSymbol, QgsPalLayerSettings, QgsPointXY, QgsProject, QgsRasterLayer,
                           QgsReferencedRectangle, QgsRendererCategory, QgsVectorFileWriter, QgsVectorLayer,
                           QgsVectorLayerSimpleLabeling, QgsWkbTypes)
    from qgis.PyQt.QtCore import QVariant

    sites = [r for r in lire(PRIORITES) if int(r["rang"]) <= top]
    rang_de = {s["site_cle"]: s for s in sites}
    props = {p["site_cle"]: p for p in lire(PROPOSITIONS)}
    notices = [n for n in lire(NOTICES) if n["site_cle"] in rang_de and n["lien"] != "exclu"]
    manuel = {(r["ir"], r["c_id"], r["site_cle"]): r for r in lire(MANUEL)} if exists(MANUEL) else {}
    details = details_notices({(n["ir"], n["c_id"]) for n in notices})
    lieux = {r["lieu_id"]: r for f in REFERENTIELS for r in lire(f)}

    def point_propose(site_cle):
        """(géométrie, provenance) du site : lieu proposé, sinon adresse des notes, sinon aire d'attente."""
        s, p = rang_de[site_cle], props.get(site_cle, {})
        l = lieux.get(s["lieu_id_propose"])
        if l and (l.get("coord_manuel") or l.get("coord_wkt")):
            g = QgsGeometry.fromWkt(l.get("coord_manuel") or l["coord_wkt"])
            return (g if g.type() == 0 else g.pointOnSurface()), "lieu proposé"
        if p.get("adresse_wkt"):
            return QgsGeometry.fromWkt(p["adresse_wkt"]), "adresse des notes"
        return QgsGeometry.fromPointXY(QgsPointXY(*ATTENTE)), "aire d'attente (pas de lieu proposé)"

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

    positions = {k: point_propose(k) for k in rang_de}
    rangs, manquantes = [], 0
    for n in notices:
        s, d = rang_de[n["site_cle"]], details.get((n["ir"], n["c_id"]))
        if d is None:
            manquantes += 1
            continue
        m = manuel.get((n["ir"], n["c_id"], n["site_cle"]), {})
        geom, position = positions[n["site_cle"]]
        motif = " ; ".join(x for x in (f"proposition : {s['proposition']}", s["etat"], s["alertes"]) if x)
        rangs.append((geom, {"rang": f"{int(s['rang']):03d}", "site": s["forme"],
                             "propose": s["libelle_propose"] or "(aucun lieu proposé)", "unittitle": n["unittitle"],
                             **d, "motif": motif, "lien": n["lien"], "position": position, "ir": n["ir"],
                             "c_id": n["c_id"], "site_cle": n["site_cle"], "lieu_id_propose": s["lieu_id_propose"],
                             "decision": m.get("decision", ""), "lieu_manuel": m.get("lieu_manuel", ""),
                             "remarque": m.get("remarque", "")}))
    # ordre de la navigation : rang du site, notices « sujet » d'abord, puis date
    rangs.sort(key=lambda r: (r[1]["rang"], r[1]["lien"] != "sujet", r[1]["normal"] or r[1]["unitdate"] or "9999",
                              r[1]["ir"], r[1]["c_id"]))
    if exists(GPKG):
        os.remove(GPKG)
    couche("notices", CHAMPS, rangs, True)
    couche("lieux_proposes", ["rang", "site", "propose", "lieu_id", "position"],
           [(geom, {"rang": f"{int(rang_de[k]['rang']):03d}", "site": rang_de[k]["forme"],
                    "propose": rang_de[k]["libelle_propose"], "lieu_id": rang_de[k]["lieu_id_propose"],
                    "position": pos}) for k, (geom, pos) in positions.items() if pos != "aire d'attente (pas de lieu proposé)"],
           False)

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
    groupe = racine.insertGroup(0, f"Validation des sites des Archives ({DATE})")
    l_notices = QgsVectorLayer(f"{GPKG}|layername=notices", "Notices", "ogr")
    l_lieux = QgsVectorLayer(f"{GPKG}|layername=lieux_proposes", "Lieux proposés", "ogr")
    for l in (l_notices, l_lieux):
        if not l.isValid():
            sys.exit(f"couche invalide : {l.name()}")
        p.addMapLayer(l, False)
        groupe.addLayer(l)

    fc = l_notices.editFormConfig()
    for i, f in enumerate(l_notices.fields()):
        nom = f.name()
        if nom == "decision":
            l_notices.setEditorWidgetSetup(i, QgsEditorWidgetSetup("ValueMap", {"map": [{k: v} for k, v in DECISIONS]}))
        elif nom in ("unittitle", "note", "indexation", "motif", "remarque"):
            l_notices.setEditorWidgetSetup(i, QgsEditorWidgetSetup("TextEdit", {"IsMultiline": True}))
        fc.setReadOnly(i, nom not in SAISIES)
    l_notices.setEditFormConfig(fc)
    l_notices.setDisplayExpression('"rang" || \' ' + "·" + ' \' || "site" || \' — \' || "cote" || \' — \' || "unittitle"')
    # couleur : à valider (sujet / mention) ou décidé
    expr = ("CASE WHEN coalesce(\"decision\", '') != '' THEN 'décidé' "
            "WHEN \"lien\" = 'sujet' THEN 'à valider : sujet' ELSE 'à valider : mention' END")
    couleurs = {"à valider : sujet": "#2FAAAF", "à valider : mention": "#E9B400", "décidé": "#9460A4"}
    l_notices.setRenderer(QgsCategorizedSymbolRenderer(expr, [QgsRendererCategory(v, QgsMarkerSymbol.createSimple(
        {"name": "circle", "color": c, "size": "4", "outline_color": "#ffffff", "outline_width": "0.5"}), v)
        for v, c in couleurs.items()]))
    l_lieux.renderer().setSymbol(QgsMarkerSymbol.createSimple(
        {"name": "star", "color": "#000000", "size": "6", "outline_color": "#ffffff", "outline_width": "0.5"}))
    lab = QgsPalLayerSettings()
    lab.fieldName, lab.isExpression, lab.enabled = '"rang" || \' \' || "propose"', True, True
    l_lieux.setLabeling(QgsVectorLayerSimpleLabeling(lab))
    l_lieux.setLabelsEnabled(True)

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
        fonds.addLayer(r).setItemVisibilityChecked(False)

    p.setTitle(f"Validation des sites des Archives municipales, notice par notice ({DATE})")
    p.setFilePathStorage(Qgis.FilePathType.Relative)
    emprise = l_notices.extent()
    emprise.grow(0.004)
    p.viewSettings().setDefaultViewExtent(QgsReferencedRectangle(emprise, l_notices.crs()))
    if not p.write(PROJET):
        sys.exit("écriture du projet impossible")
    print(f"{len(rangs)} notices × site pour {len(sites)} sites → {GPKG}"
          + (f" ({manquantes} notice(s) introuvable(s) dans les IR)" if manquantes else ""))
    print(f"positions : {dict((pos, sum(1 for _, v in rangs if v['position'] == pos)) for pos in {v['position'] for _, v in rangs})}")
    print(f"projet → {PROJET}")
    app.exitQgis()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--top", type=int, default=37, help="nombre de sites, par rang (défaut : 37)")
    ap.add_argument("--reporter", action="store_true", help="reporter les saisies dans le CSV manuel, sans générer")
    a = ap.parse_args()
    if a.reporter:
        reporter()
    else:
        if exists(GPKG):
            reporter()  # ne pas perdre les saisies d'une validation en cours
        generer(a.top)
