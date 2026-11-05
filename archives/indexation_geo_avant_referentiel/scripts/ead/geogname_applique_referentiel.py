"""
Applique le référentiel géographique des lieux aux <geogname> des IR, à titre
de test : lit les IR de results/ead/ead_cor/bnr2mnesys/ (non modifiés) et écrit
les IR corrigés dans results/ead/ead_cor/bnr2mnesysgeo/.

Pour chaque <geogname> :

1. sa forme est retrouvée dans la concordance formes ↔ lieux
   (data/geo/concordance_geogname_lieux.csv), par l'état d'origine
   (results/ead/indexation/rbx-bnr_geogname.csv : IR, composant, texte et
   @source actuels → forme et @source d'origine), sinon par son texte
   normalisé ;
2. la forme donne son lieu dans le référentiel des lieux
   (data/geo/referentiel_geogname.csv, regroupement de
   geogname_referentiel_lieux.py) ;
3. le <geogname> est réécrit :
     - texte : libellé du lieu (lien avec le concept du thésaurus, Mnesys
       reliant par le libellé seul) ;
     - @source : thésaurus du lieu, thesaurus--SLASH--bnr_geo_rbx_quartiers.xml,
       bnr_geo_rbx_rues.xml, bnr_geo_rbx_points.xml ou bnr_geo_communes_pays.xml ;
     - @coord : coord_manuel du lieu, sinon coord ; si le lieu n'a pas de
       géométrie, le @coord existant est conservé.
   Les autres attributs (role, normal…) sont conservés. Les <geogname> vides,
   chronologiques ou introuvables sont laissés tels quels.

Contrôle spatial : dans un même <controlaccess>, une rue indexée à plus de
DISTANCE_MAX mètres d'un quartier indexé est signalée (statut « rue et quartier
éloignés »), ainsi que ce quartier.

Options : --ir pour ne traiter que certains IR ; --sans-rues-ponctuelles pour
retirer les <geogname> de rue représentés par un simple point (rue ancienne
laissée telle quelle, ou rue rattachée sans tracé) ; --sans-geogname-sans-source
pour retirer ceux restés sans @source ; --sans-non-appliques (avec
--sans-reserve) pour retirer tous ceux qui n'ont pas été réécrits vers un
thésaurus ; --sans-reserve pour ne
réécrire que les <geogname> sans problème (forme et lieu sans réserve, cf.
a_traiter(), et sans incohérence rue / quartier), les autres restant tels
qu'ils sont dans bnr2mnesys ; --sortie pour écrire ailleurs que dans
bnr2mnesysgeo (dossier recréé à chaque exécution).

Rapport : results/ead/indexation/<dossier de sortie>_rapport.csv (une ligne
par <geogname> : IR, composant, avant / après, lieu, statut). Statut « à traiter » :
la forme du <geogname> pose problème dans la concordance, ou le lieu a un
problème qui lui est propre dans le référentiel.

Aucune validation n'est exigée : c'est un test. Les lieux encore « à
traiter » dans le référentiel sont appliqués comme les autres et comptés.

À lancer depuis la racine du dépôt, après geogname_referentiel_lieux.py :
    python scripts/ead/geogname_applique_referentiel.py
    python scripts/ead/geogname_applique_referentiel.py --ir MED_PHO --sans-reserve \\
        --sans-rues-ponctuelles --sans-geogname-sans-source --sans-non-appliques \\
        --sortie results/ead/ead_cor/bnr2mnesysgeo_sans_reserve
"""
import argparse
import csv
import json
import math
import shutil
import sys
from collections import Counter
from glob import glob
from os import makedirs
from os.path import abspath, basename, dirname, exists, join

from lxml import etree
from shapely.geometry import LineString, MultiLineString, Point, Polygon

sys.path.insert(0, dirname(__file__))
from geogname_concordance import GEO  # noqa: E402
from geogname_referentiel_lieux import CONCORDANCE, SORTIE as REFERENTIEL, construire  # noqa: E402

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")
DESTINATION = join("results", "ead", "ead_cor", "bnr2mnesysgeo")
GEOGNAME_CSV = join("results", "ead", "indexation", "rbx-bnr_geogname.csv")
RAPPORT = join("results", "ead", "indexation", "bnr2mnesysgeo_rapport.csv")

# Distance au-delà de laquelle une rue et un quartier indexés dans un même composant
# sont jugés incohérents (mètres) ; conversion degrés -> mètres approchée à Roubaix.
DISTANCE_MAX = 150
METRES_PAR_DEGRE = 111_320
COS_LAT = math.cos(math.radians(50.69))

SOURCES = {
    "quartiers": "thesaurus--SLASH--bnr_geo_rbx_quartiers.xml",
    "rues": "thesaurus--SLASH--bnr_geo_rbx_rues.xml",
    "points": "thesaurus--SLASH--bnr_geo_rbx_points.xml",
    "communes_pays": "thesaurus--SLASH--bnr_geo_communes_pays.xml",
}


def normaliser(texte):
    return " ".join(texte.replace("\\'", "'").split())


def charger():
    """(forme, @source) d'origine -> (lieu, ligne de la concordance) ; index de l'état d'origine ;
    index des formes normalisées."""
    with open(CONCORDANCE, encoding="utf-8") as f:
        formes = list(csv.DictReader(f))
    with open(REFERENTIEL, encoding="utf-8") as f:
        referentiel = {l["lieu_id"]: l for l in csv.DictReader(f)}
    par_cle = {(f["forme_bnr"], f["source_bnr"]): f for f in formes}
    lieu_de = {}
    for lieu in construire(formes):
        ligne = referentiel[lieu["lieu_id"]]
        for cle_forme in lieu["cles_formes"]:
            lieu_de[cle_forme] = (ligne, par_cle[cle_forme])
    origine = {}
    with open(GEOGNAME_CSV, encoding="utf-8") as f:
        for l in csv.DictReader(f):
            origine.setdefault((l["eadid"], l["c_id"], l["geogname"], l["source"]),
                               Counter())[(l["geogname_origine"], l["source_origine"])] += 1
    par_texte = {}
    for f in formes:
        par_texte.setdefault((normaliser(f["forme_bnr"]), f["source_bnr"]), (f["forme_bnr"], f["source_bnr"]))
    return lieu_de, origine, par_texte


def trouver_lieu(eadid, c_id, texte, source, lieu_de, origine, par_texte):
    candidats = origine.get((eadid, c_id, texte, source))
    if candidats:
        cle_forme = candidats.most_common(1)[0][0]
        if cle_forme in lieu_de:
            return lieu_de[cle_forme]
    cle_forme = par_texte.get((normaliser(texte), source))
    return lieu_de.get(cle_forme) if cle_forme else None


def a_traiter(lieu, forme):
    """Vrai si la forme pose problème dans la concordance, ou si le lieu a un problème qui
    lui est propre (le renvoi « n forme(s) à traiter dans la concordance » ne compte pas :
    il vise les autres formes du lieu)."""
    propres = [m for m in lieu["a_traiter"].split(" ; ") if m and "à traiter dans la concordance" not in m]
    return bool(forme["a_traiter"] or propres)


def est_point(coord):
    try:
        v = json.loads(coord)
    except ValueError:
        return False
    return isinstance(v, list) and bool(v) and isinstance(v[0], (int, float))


def retirer(element):
    """Retire element de l'arbre en gardant la mise en forme (son tail passe au précédent)."""
    parent, precedent = element.getparent(), element.getprevious()
    if precedent is not None:
        precedent.tail = element.tail
    else:
        parent.text = element.tail
    parent.remove(element)


def geometrie(coord):
    """@coord ([lat, lon], tracé ou contour) -> géométrie shapely en mètres approchés autour de Roubaix."""
    def pt(p):
        return (p[1] * METRES_PAR_DEGRE * COS_LAT, p[0] * METRES_PAR_DEGRE)
    v = json.loads(coord)
    if isinstance(v[0], (int, float)):
        return Point(pt(v))
    if isinstance(v[0][0], (int, float)):
        return [pt(p) for p in v]
    return MultiLineString([[pt(p) for p in t] for t in v])


def incoherences(proposes):
    """<geogname> d'un même composant dont la rue est à plus de DISTANCE_MAX mètres du quartier.

    proposes : [(geogname, lieu)] du composant ; renvoie l'ensemble des geogname concernés."""
    quartiers = []
    for g, l in proposes:
        coord = l["coord_manuel"] or l["coord"]
        geo = geometrie(coord) if l["thesaurus"] == "quartiers" and coord else None
        if isinstance(geo, list):
            quartiers.append((g, Polygon(geo)))
    rues = []
    for g, l in proposes:
        coord = l["coord_manuel"] or l["coord"]
        if l["thesaurus"] == "rues" and coord:
            geo = geometrie(coord)
            rues.append((g, LineString(geo) if isinstance(geo, list) else geo))
    hors = set()
    for gq, poly in quartiers:
        for gr, rue in rues:
            if poly.distance(rue) > DISTANCE_MAX:
                hors |= {gq, gr}
    return hors


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    parser.add_argument("--ir", nargs="*", help="IR à traiter (ex. MED_PHO) ; par défaut tous")
    parser.add_argument("--sans-reserve", action="store_true",
                        help="ne réécrire que les <geogname> sans problème : lieu sans a_traiter et "
                             f"rue / quartier d'un même composant distants de moins de {DISTANCE_MAX} m")
    parser.add_argument("--sans-rues-ponctuelles", action="store_true",
                        help="retire les <geogname> de rue représentés par un simple point (rue ancienne non "
                             "rattachée, ou rue rattachée sans tracé) ; un <controlaccess> vidé est retiré aussi")
    parser.add_argument("--sans-geogname-sans-source", action="store_true",
                        help="retire les <geogname> restés sans @source ; un <controlaccess> vidé est retiré aussi")
    parser.add_argument("--sans-non-appliques", action="store_true",
                        help="avec --sans-reserve : retire aussi les <geogname> non appliqués (restés avec leur "
                             "ancienne @source) ; un <controlaccess> vidé est retiré aussi")
    parser.add_argument("--sortie", default=DESTINATION, help=f"dossier des IR corrigés (défaut : {DESTINATION})")
    args = parser.parse_args()
    if abspath(args.sortie) == abspath(EAD_FOLDER):
        sys.exit("La sortie ne peut pas être le dossier source bnr2mnesys.")
    rapport_csv = join(dirname(RAPPORT), f"{basename(args.sortie.rstrip('/'))}_rapport.csv")

    lieu_de, origine, par_texte = charger()
    if exists(args.sortie):
        shutil.rmtree(args.sortie)
    makedirs(args.sortie)

    chemins = sorted(glob(join(EAD_FOLDER, "*.xml")))
    if args.ir:
        chemins = [c for c in chemins if basename(c)[:-4].removeprefix("FR595126101_") in args.ir]
    stats, par_thesaurus, rapport = Counter(), Counter(), []
    for chemin in chemins:
        tree = etree.parse(chemin)
        root = tree.getroot()
        eadid = (root.findtext("eadheader/eadid") or "").strip()
        lieux, formes, lignes = {}, {}, {}
        for g in root.iter("geogname"):
            c = next(g.iterancestors("c"), None)
            c_id = c.get("id", "") if c is not None else ""
            texte, source = g.text or "", g.get("source", "")
            lignes[g] = {"eadid": eadid, "c_id": c_id, "texte_avant": texte, "source_avant": source,
                         "coord_avant": g.get("coord", ""), "lieu_id": "", "texte_apres": texte,
                         "source_apres": source, "statut": ""}
            trouve = None if not texte.strip() or source == "chrono" else trouver_lieu(
                eadid, c_id, texte, source, lieu_de, origine, par_texte)
            if trouve is None:
                lignes[g]["statut"] = "vide" if not texte.strip() else ("chrono" if source == "chrono" else "introuvable")
            else:
                lieux[g], formes[g] = trouve
                lignes[g]["lieu_id"] = trouve[0]["lieu_id"]

        hors = set()
        for controlaccess in root.iter("controlaccess"):
            hors |= incoherences([(g, lieux[g]) for g in controlaccess.findall("geogname") if g in lieux])

        for g, lieu in lieux.items():
            ligne = lignes[g]
            statut = "à traiter" if a_traiter(lieu, formes[g]) else "sans réserve"
            if g in hors:
                statut += " ; rue et quartier éloignés"
            ligne["statut"] = statut
            if args.sans_reserve and statut != "sans réserve":
                ligne["statut"] += " (non appliqué)"
                continue
            g.text = lieu["libelle"]
            g.set("source", SOURCES[lieu["thesaurus"]])
            coord = lieu["coord_manuel"] or lieu["coord"]
            if coord:
                g.set("coord", coord)
            ligne.update(texte_apres=g.text, source_apres=g.get("source"))
            par_thesaurus[lieu["thesaurus"]] += 1
        for g, ligne in lignes.items():
            if g.getparent() is None:
                continue
            rue = g.get("source", "") in ("rue", SOURCES["rues"])
            motif = ("rue ponctuelle" if args.sans_rues_ponctuelles and rue and est_point(g.get("coord", "")) else
                     "sans source" if args.sans_geogname_sans_source and not g.get("source") else
                     "non appliqué" if args.sans_non_appliques and "(non appliqué)" in ligne["statut"] else "")
            if motif:
                controlaccess = g.getparent()
                retirer(g)
                if g in lieux and ligne["source_apres"] == SOURCES["rues"]:
                    par_thesaurus["rues"] -= 1
                ligne.update(texte_apres="", source_apres="", statut=ligne["statut"] + f" ; retiré ({motif})")
                if len(controlaccess) == 0 and not (controlaccess.text or "").strip():
                    retirer(controlaccess)
        for ligne in lignes.values():
            stats[ligne["statut"]] += 1
            rapport.append(ligne)
        tree.write(join(args.sortie, basename(chemin)), encoding="UTF-8", xml_declaration=True,
                   pretty_print=True, with_comments=True)

    with open(rapport_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rapport[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rapport)

    changes = sum(1 for l in rapport if (l["texte_avant"], l["source_avant"]) != (l["texte_apres"], l["source_apres"]))
    print(f"{len(rapport)} geogname dans {len(chemins)} IR → {args.sortie}")
    print(f"statuts : {dict(stats)}")
    print(f"réécrits par thésaurus : {dict(par_thesaurus)}")
    print(f"texte ou @source modifiés : {changes} ; rapport → {rapport_csv}")


if __name__ == "__main__":
    main()
