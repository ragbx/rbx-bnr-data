"""Projet QGIS de relecture des rues du plan de 1889 (6Fi022), rue par rue (2026-11-20).

    python3 scripts/ead/qgis_validation_plan_1889.py              (Python de QGIS) : génère le projet
    python3 scripts/ead/qgis_validation_plan_1889.py --reporter   (tout Python) : saisies → CSV

Entrée : results/ead/indexation/plan_1889/rues_1889_traces.csv (plan_1889_traces.py : une ligne par proposition,
tracé relevé sur 1889 ou tracé actuel du référentiel).

Sorties (results/ead/indexation/plan_1889/) :
  - validation_plan_1889.gpkg, couche « rues » : une entité (ligne) par proposition. Lecture seule : nom 1889,
    certitude, provenance et précision du tracé, remarque, extrait de contrôle (affiché dans le formulaire).
    Modifiables : nom_corrige (nom lu sur le plan, prérempli : corriger une erreur de lecture ; nom_1889 reste
    la clé, inchangée), constat (renommée / disparue / …), libelle_actuel (rue renommée : son nom actuel, liste des rues
    du référentiel), emprise_actuelle (rue disparue : la voie qui occupe aujourd'hui son emplacement, facultatif),
    decision, remarque_relecture. La géométrie aussi : corriger un tracé = déplacer ses sommets (outil de nœud),
    puis choisir « oui, tracé corrigé ». Couleur = constat (renommée turquoise, disparue violet, projetée jaune).
  - validation_plan_1889.qgz : copie du projet de validation du 12/11 (fonds), plans 1889 / 1899 / 1855.

`--reporter` écrit les saisies dans rues_1889_relecture.csv (une ligne par rue relue ou corrigée : constat,
libelle_actuel + son lieu_id dans le référentiel, emprise_actuelle, decision, remarque_relecture, et le tracé WKT
quand la décision est « oui, tracé corrigé » ; sauvegarde avant écriture). La génération commence par un
report si le GeoPackage existe, et réapplique les saisies déjà reportées (y compris les tracés corrigés).
"""
import argparse
import csv
import os
import shutil
import sqlite3
import sys
import time
from os.path import abspath, dirname, exists, join

RACINE = dirname(dirname(dirname(abspath(__file__))))
D = join(RACINE, "results", "ead", "indexation", "plan_1889")
TRACES = join(D, "rues_1889_traces.csv")
PROPOSITIONS = join(D, "rues_1889_propositions.csv")
MANUEL = join(D, "rues_1889_relecture.csv")
GPKG = join(D, "validation_plan_1889.gpkg")
PROJET = join(D, "validation_plan_1889.qgz")
REFERENTIEL = join(RACINE, "data", "geo", "referentiel_lieux_roubaix.csv")
MODELE = join(RACINE, "data", "geo", "validation_sites_20261112.qgz")
PLANS = join(RACINE, "data", "geo", "plans_anciens")

CORRIGE = "oui, tracé corrigé"
DECISIONS = [("", ""), ("oui, comme proposé", "oui"), (CORRIGE, CORRIGE),
             ("non : proposition fausse", "non"), ("à revoir", "à revoir")]
CONSTATS = ["renommée", "disparue", "disparue en partie", "projetée, absente", "conflit", "à vérifier"]
CHAMPS = ["rang", "nom_1889", "nom_corrige", "constat", "libelle_actuel", "emprise_actuelle", "certitude", "decision",
          "remarque_relecture", "source_trace", "precision", "remarque", "extrait", "secteur", "lieu_id_actuel"]
CORRIGEABLES = ["nom_corrige", "constat", "libelle_actuel", "emprise_actuelle"]
SAISIES = set(CORRIGEABLES) | {"decision", "remarque_relecture"}
COLONNES_MANUEL = ["nom_1889", *CORRIGEABLES, "lieu_id_actuel", "decision", "remarque_relecture", "trace_corrige_wkt"]


def lire(f):
    with open(f, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def reporter():
    """Saisies du GeoPackage → rues_1889_relecture.csv (les saisies déjà reportées sont gardées).
    Une rue est reportée si decision / remarque_relecture sont remplies ou si nom_corrige, constat, libelle_actuel
    ou emprise_actuelle diffèrent de rues_1889_traces.csv (nom_corrige d'origine = nom_1889, nom lu sur le plan)."""
    origine = {}
    for r in lire(TRACES):
        origine.setdefault(r["nom_1889"], {**r, "nom_corrige": r["nom_1889"]})
    n = len(CORRIGEABLES)
    ids = {}
    for r in lire(REFERENTIEL):
        if r["thesaurus"] == "rues":
            ids.setdefault(r["libelle"], []).append(r["lieu_id"])
    con = sqlite3.connect(GPKG)
    presentes = {r[1] for r in con.execute("PRAGMA table_info(rues)")}  # GeoPackage d'une version antérieure
    cols = [c if c in presentes else "NULL" for c in CORRIGEABLES]
    lignes = list(con.execute("SELECT nom_1889, " + ", ".join(cols) + ", decision, remarque_relecture FROM rues"))
    if len(cols) != len([c for c in cols if c != "NULL"]):  # colonnes absentes : valeurs d'origine
        lignes = [(nom, *[x if c != "NULL" else (origine.get(nom, {}).get(k) or "")
                          for x, c, k in zip(v[:n], cols, CORRIGEABLES)], *v[n:]) for nom, *v in lignes]
    con.close()
    saisies = []
    for nom, *v in lignes:
        v = [x or "" for x in v]
        corr = dict(zip(CORRIGEABLES, v[:n]))
        corr["nom_corrige"] = corr["nom_corrige"].strip() or nom
        o = origine.get(nom, {})
        if v[n] or v[n + 1] or any(corr[c] != (o.get(c) or "") for c in CORRIGEABLES):
            saisies.append((nom, corr, v[n], v[n + 1]))
    wkt = {}
    if any(d == CORRIGE for _, _, d, _ in saisies):  # géométrie : lue par OGR (blob GPKG)
        from osgeo import ogr
        ogr.UseExceptions()
        ds = ogr.Open(GPKG)
        for f in ds.GetLayerByName("rues"):
            if f.GetField("decision") == CORRIGE:
                wkt[f.GetField("nom_1889")] = f.GetGeometryRef().ExportToWkt()
    anciens = {r["nom_1889"]: r for r in lire(MANUEL)} if exists(MANUEL) else {}
    nouveaux = dict(anciens)
    inconnus = []
    for nom, corr, decision, remarque in saisies:
        lib = corr["libelle_actuel"]
        if lib == (origine.get(nom, {}).get("libelle_actuel") or ""):
            lid = origine.get(nom, {}).get("lieu_id_actuel") or ""
        else:
            lid = ";".join(ids.get(lib, []))
            if lib and not lid:
                inconnus.append(f"{nom} → {lib}")
        nouveaux[nom] = {"nom_1889": nom, **corr, "lieu_id_actuel": lid, "decision": decision,
                         "remarque_relecture": remarque, "trace_corrige_wkt": wkt.get(nom, "")}
    if nouveaux != anciens:
        if exists(MANUEL):
            shutil.copy(MANUEL, MANUEL.replace(".csv", time.strftime(".backup_%Y%m%d_%H%M%S_avant_report.csv")))
        with open(MANUEL, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=COLONNES_MANUEL, lineterminator="\n")
            w.writeheader()
            w.writerows(sorted(nouveaux.values(), key=lambda r: r["nom_1889"]))
    print(f"report : {len(saisies)} rue(s) saisie(s) → {MANUEL}")
    for x in inconnus:
        print(f"  libellé actuel absent du référentiel (lieu_id vide) : {x}")


def generer():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from qgis.core import (Qgis, QgsApplication, QgsCategorizedSymbolRenderer, QgsCoordinateReferenceSystem,
                           QgsCoordinateTransformContext, QgsEditorWidgetSetup, QgsFeature, QgsField, QgsFields,
                           QgsGeometry, QgsLineSymbol, QgsPalLayerSettings, QgsProject, QgsRasterLayer,
                           QgsReferencedRectangle, QgsRendererCategory, QgsVectorFileWriter, QgsVectorLayer,
                           QgsVectorLayerSimpleLabeling, QgsWkbTypes)
    from qgis.PyQt.QtCore import QVariant

    traces = lire(TRACES)
    props = {r["nom_1889"]: r for r in lire(PROPOSITIONS)}
    manuel = {r["nom_1889"]: r for r in lire(MANUEL)} if exists(MANUEL) else {}
    # ordre de navigation : centre puis hors centre ; relevés sur 1889 d'abord (les moins sûrs), puis renommées
    ordre = sorted(range(len(traces)), key=lambda i: (traces[i]["secteur"] != "centre",
                                                     traces[i]["source_trace"] != "relevé sur le plan de 1889", i))

    app = QgsApplication([], False)
    app.initQgis()
    fields = QgsFields()
    for c in CHAMPS:
        fields.append(QgsField(c, QVariant.String))
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName, opts.layerName = "GPKG", "rues"
    if exists(GPKG):
        os.remove(GPKG)
    w = QgsVectorFileWriter.create(GPKG, fields, QgsWkbTypes.MultiLineString, QgsCoordinateReferenceSystem("EPSG:4326"),
                                   QgsCoordinateTransformContext(), opts)
    vus = set()
    rang = 0
    for i in ordre:
        t = traces[i]
        p, m = props.get(t["nom_1889"], {}), manuel.get(t["nom_1889"], {})
        nom = t["nom_1889"]
        if nom in vus:  # doublon de proposition (Avenue Bossut) : une seule entité
            continue
        vus.add(nom)
        rang += 1
        g = QgsGeometry.fromWkt(m.get("trace_corrige_wkt") or t["coord_wkt"])
        g.convertToMultiType()
        f = QgsFeature(fields)
        f.setGeometry(g)
        v = {**t, "rang": f"{rang:02d}", "remarque": p.get("remarque", ""), "extrait": p.get("extrait", ""),
             "decision": m.get("decision", ""), "remarque_relecture": m.get("remarque_relecture", "")}
        v["nom_corrige"] = nom
        if m:  # corrections déjà reportées
            v.update({c: m.get(c, "") for c in CORRIGEABLES + ["lieu_id_actuel"]})
            v["nom_corrige"] = m.get("nom_corrige") or nom
        f.setAttributes([v.get(c, "") for c in CHAMPS])
        w.addFeature(f)
    del w

    p = QgsProject.instance()
    if not p.read(MODELE):
        sys.exit(f"projet modèle illisible : {MODELE}")
    racine = p.layerTreeRoot()
    for g in list(racine.findGroups()):
        if g.name().startswith("Validation"):
            for l in g.findLayers():
                p.removeMapLayer(l.layerId())
            racine.removeChildNode(g)
    for nom in ("Sites à valider", "Adresses des notes (BAN)", "Autres points candidats"):
        for l in p.mapLayersByName(nom):
            p.removeMapLayer(l.id())
    groupe = racine.insertGroup(0, "Relecture des rues du plan de 1889")
    l = QgsVectorLayer(f"{GPKG}|layername=rues", "Rues 1889", "ogr")
    if not l.isValid():
        sys.exit("couche invalide")
    p.addMapLayer(l, False)
    groupe.addLayer(l)

    rues_ref = p.mapLayersByName("Rues")  # référentiel (projet modèle) : liste des noms actuels
    fc = l.editFormConfig()
    for i, f in enumerate(l.fields()):
        nom = f.name()
        if nom == "constat":
            l.setEditorWidgetSetup(i, QgsEditorWidgetSetup("ValueMap", {"map": [{c: c} for c in CONSTATS]}))
        elif nom in ("libelle_actuel", "emprise_actuelle") and rues_ref:
            l.setEditorWidgetSetup(i, QgsEditorWidgetSetup("ValueRelation", {
                "Layer": rues_ref[0].id(), "LayerName": rues_ref[0].name(), "Key": "libelle", "Value": "libelle",
                "AllowNull": True, "OrderByValue": True, "UseCompleter": True, "AllowMulti": False}))
        elif nom == "decision":
            l.setEditorWidgetSetup(i, QgsEditorWidgetSetup("ValueMap", {"map": [{k: v} for k, v in DECISIONS]}))
        elif nom == "extrait":  # image de contrôle dans le formulaire (chemin relatif au projet)
            l.setEditorWidgetSetup(i, QgsEditorWidgetSetup("ExternalResource", {
                "DocumentViewer": 1, "DocumentViewerHeight": 450, "DocumentViewerWidth": 450,
                "RelativeStorage": 1, "FileWidget": False, "UseLink": False}))
        elif nom in ("remarque", "remarque_relecture", "precision"):
            l.setEditorWidgetSetup(i, QgsEditorWidgetSetup("TextEdit", {"IsMultiline": True}))
        fc.setReadOnly(i, nom not in SAISIES)
    l.setEditFormConfig(fc)
    l.setDisplayExpression('"rang" || \' · \' || "nom_corrige" || \' — \' || "constat" || '
                           'CASE WHEN "libelle_actuel" != \'\' THEN \' → \' || "libelle_actuel" ELSE \'\' END')

    # couleur = constat ; trait fin en tirets une fois la décision prise
    groupe_constat = ("CASE WHEN \"constat\" LIKE 'renomm%' THEN 'renommée' WHEN \"constat\" LIKE 'disparue%' "
                      "THEN 'disparue' WHEN \"constat\" LIKE 'projet%' THEN 'projetée, jamais ouverte' "
                      "ELSE 'conflit / à vérifier' END")
    expr = (f"({groupe_constat}) || CASE WHEN coalesce(\"decision\", '') != '' THEN ' (décidé)' ELSE '' END")
    couleurs = {"renommée": "#2FAAAF", "disparue": "#9460A4", "projetée, jamais ouverte": "#E9B400",
                "conflit / à vérifier": "#000000"}
    cats = []
    for v, c in couleurs.items():
        cats.append(QgsRendererCategory(v, QgsLineSymbol.createSimple(
            {"line_color": c, "line_width": "1.2", "capstyle": "round"}), v))
        cats.append(QgsRendererCategory(v + " (décidé)", QgsLineSymbol.createSimple(
            {"line_color": c, "line_width": "0.5", "line_style": "dash"}), v + " (décidé)"))
    l.setRenderer(QgsCategorizedSymbolRenderer(expr, cats))
    lab = QgsPalLayerSettings()
    lab.fieldName, lab.isExpression, lab.enabled = (
        "\"nom_corrige\" || CASE WHEN \"constat\" LIKE 'renomm%' THEN ' → ' || coalesce(\"libelle_actuel\", '?') "
        "WHEN \"constat\" LIKE 'disparue%' THEN ' (disparue)' WHEN \"constat\" LIKE 'projet%' THEN ' (projetée)' "
        "ELSE ' (' || \"constat\" || ')' END", True, True)
    lab.placement = Qgis.LabelPlacement.Line
    l.setLabeling(QgsVectorLayerSimpleLabeling(lab))
    l.setLabelsEnabled(True)

    fonds = racine.insertGroup(1, "Plans anciens")
    for fichier, titre, visible in (("plan_roubaix_1889_georef.tif", "Plan de Roubaix 1889 (service de la voirie, 6Fi022)", True),
                                    ("plan_roubaix_1899_georef.tif", "Plan de Roubaix 1899 (P. Pachy, 6Fi024)", False),
                                    ("plan_roubaix_1855_barbotin_georef.tif", "Plan de Roubaix 1855 (E. Barbotin)", False)):
        chemin = join(PLANS, fichier)
        r = QgsRasterLayer(chemin, titre, "gdal") if exists(chemin) else None
        if r is None or not r.isValid():
            print(f"  plan absent ou illisible, ignoré : {chemin}")
            continue
        p.addMapLayer(r, False)
        fonds.addLayer(r).setItemVisibilityChecked(visible)

    p.setTitle("Relecture des rues du plan de Roubaix de 1889 (6Fi022)")
    p.setFilePathStorage(Qgis.FilePathType.Relative)
    emprise = l.extent()
    emprise.grow(0.002)
    p.viewSettings().setDefaultViewExtent(QgsReferencedRectangle(emprise, l.crs()))
    if not p.write(PROJET):
        sys.exit("écriture du projet impossible")
    print(f"{l.featureCount()} rues → {GPKG}\nprojet → {PROJET}")
    app.exitQgis()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--reporter", action="store_true", help="reporter les saisies dans le CSV, sans générer")
    a = ap.parse_args()
    if a.reporter:
        reporter()
    else:
        if exists(GPKG):
            reporter()  # ne pas perdre une relecture en cours
        generer()
