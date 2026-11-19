"""
Fi : applique les alignements validés des <geogname> aux inventaires Mnesys des séries Fi
(data/ead/mnesys/FRAM59100_*Fi*.xml, non modifiés) et écrit les IR corrigés dans
results/ead/ead_cor/<série>_geo/ (fi_geo ou mnesys_geo ; dossier recréé à chaque exécution ; seuls les IR modifiés y sont écrits).

Lit les fichiers de propositions de results/ead/indexation/ — le plus récent de chaque famille, ou
--propositions : <série>_<famille>_propositions_<date>.csv, familles quartiers (mnesys_geo_quartiers.py), reprise
(mnesys_geo_reprise.py), voies (mnesys_geo_voies.py), sites (mnesys_geo_sites.py) et territoires
(mnesys_geo_territoires.py), séries fi et mnesys : les
décisions sont globales, appliquées à tous les IR de la série où la forme apparaît. Une forme ne doit figurer
avec un lieu_id que dans un fichier (une ligne sans lieu_id, comme une commune listée sans proposition par
un fichier « sites », cède la place à l'autre). Un lieu_id absent du référentiel (lieu écarté à réintégrer, lieu OSM à créer) laisse
la forme en attente : elle est signalée dans le rapport et à l'écran.
Table de décisions cumulative data/geo/concordance_mnesys_lieux.csv (mnesys_geo_verse.py) : lue en premier, elle
l'emporte sur les fichiers de propositions (méthode « table » dans le rapport) ; --sans-table pour l'ignorer.
Lignes retenues (il faut un lieu_id, qui peut être corrigé ou saisi à la main : un lieu du référentiel
des lieux ou du thésaurus des territoires ; un identifiant copié sans son préfixe, « EQ_RESEA… » pour
« bdtopo:EQ_RESEA… », est complété s'il est unique) :
  - decision « non »                   : jamais ;
  - decision « oui »                   : toujours (validation de l'utilisateur ; c'est ainsi que l'on
                                         applique une ligne « manuelle » ou « aucune » dont on a saisi le lieu_id) ;
  - decision vide, méthode automatique : oui ; méthode manuelle ou aucune : non.

Pour chaque <geogname> dont le texte est une forme retenue, même réécriture que
geogname_applique_concordance.py (convention BnR, décision du 2026-11-18) :
  - texte  : libellé du lieu ;
  - @source : thésaurus du lieu (thesaurus--SLASH--bnr_geo_rbx_quartiers.xml, …_voies.xml, …_sites.xml,
    bnr_geo_territoires.xml) ; remplace un @source Mnesys existant (ex. thesaurus/index_Lieux.xml,
    décision du 2026-11-18) ;
  - @coord : géométrie du lieu (coord_manuel sinon coord_wkt) en WKT, lon lat, 6 décimales ; absent si le
    lieu n'a pas de géométrie.
Les autres attributs sont conservés. Deux formes d'un même lieu dans un même <controlaccess>
donnent deux <geogname> identiques : ils sont gardés et signalés dans le rapport.

Le fichier est modifié au niveau du texte, <geogname> par <geogname> : le reste de l'IR (DOCTYPE, fins de
ligne, entités, indentation) est conservé à l'octet près, pour que le diff se lise. Les <geogname> sont
repérés par expression régulière puis recoupés avec l'arbre XML (même nombre, même texte) ; le résultat
est relu pour contrôle.

Rapport : results/ead/indexation/<série>_geo_rapport.csv, une ligne par <geogname> concerné par une proposition
(IR, composant, avant / après, lieu, action, doublon).

À lancer depuis la racine du dépôt (après validation des propositions) :
    python scripts/ead/mnesys_geo_applique.py [--serie fi|mnesys] [--propositions CSV ...] [--ir 3Fi_20210427_import 5Fi]
"""
import argparse
import csv
import glob
import re
import shutil
import sys
from collections import Counter
from html import unescape
from os import makedirs
from os.path import abspath, basename, dirname, exists, join
from xml.sax.saxutils import escape

from lxml import etree

sys.path.insert(0, dirname(__file__))
from mnesys_geo_commun import SERIES, TABLE, lire_table, propositions_appliquees  # noqa: E402
from geogname_applique_concordance import SOURCES, coord_ir  # noqa: E402
from geogname_concordance_lieux import COMMUNES_PAYS, REFERENTIEL, lire  # noqa: E402

SOURCES_BNR = set(SOURCES.values())

GEOGNAME = re.compile(r"<geogname(?P<attrs>[^>]*?)(?:/>|>(?P<texte>[^<]*)</geogname>)")  # y compris <geogname/>
ATTRIBUT = re.compile(r'([\w:.-]+)\s*=\s*"([^"]*)"')
COLONNES_RAPPORT = [
    "ir", "c_id", "texte_avant", "source_avant", "methode", "decision", "lieu_id", "thesaurus",
    "texte_apres", "source_apres", "coord_apres", "action", "doublon",
]


def retenue(p):
    """Une proposition est-elle à appliquer ? (voir l'en-tête)"""
    decision = p["decision"].strip().lower()
    if not p["lieu_id"] or decision == "non":
        return False
    return decision == "oui" or p["methode"] == "automatique"


def resoudre(lieu_id, lieux):
    """lieu_id exact, sinon identifiant copié sans son préfixe (« EQ_RESEA… » pour « bdtopo:EQ_RESEA… ») s'il est unique."""
    if lieu_id in lieux:
        return lieu_id
    complets = [i for i in lieux if i.endswith(":" + lieu_id)]
    return complets[0] if len(complets) == 1 else lieu_id


def action_si_laisse(p):
    if p["decision"].strip().lower() == "non":
        return "laissé (décision « non »)"
    if not p["lieu_id"]:
        return "laissé (aucune proposition)"
    return "laissé (à valider)"


def reecrire(attrs, libelle, source, coord):
    """Balise <geogname> réécrite : autres attributs gardés, puis @source et @coord du lieu."""
    autres = [f' {k}="{v}"' for k, v in ATTRIBUT.findall(attrs) if k not in ("source", "coord")]
    coord = f' coord="{coord}"' if coord else ""
    return f'<geogname{"".join(autres)} source="{source}"{coord}>{escape(libelle)}</geogname>'


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    parser.add_argument("--serie", choices=SERIES, default="fi", help="série d'IR à traiter (défaut : fi)")
    parser.add_argument("--propositions", nargs="*", help="CSV des propositions (défaut : le plus récent de chaque série et famille)")
    parser.add_argument("--ir", nargs="*", help="IR à traiter (ex. 5Fi) ; par défaut tous ceux de la série")
    parser.add_argument("--sans-table", action="store_true",
                        help=f"ignorer la table de décisions {TABLE} (fichiers de propositions seuls)")
    parser.add_argument("--sortie", help="dossier des IR corrigés (défaut : celui de la série, ex. results/ead/ead_cor/fi_geo)")
    args = parser.parse_args()
    args.sortie = args.sortie or SERIES[args.serie]["destination"]
    rapport_csv = SERIES[args.serie]["rapport"]
    if abspath(args.sortie).startswith(abspath("data") + "/"):
        sys.exit("La sortie ne peut pas être dans data/ (les IR Fi d'origine ne sont pas modifiés).")
    fichiers = args.propositions or propositions_appliquees()
    if not fichiers and (args.sans_table or not lire_table()):
        sys.exit("Aucun fichier de propositions : lancer mnesys_geo_quartiers.py ou mnesys_geo_reprise.py d'abord.")

    table = {} if args.sans_table else lire_table()
    propositions, origine = {}, {}
    for fichier in fichiers:
        with open(fichier, encoding="utf-8") as f:
            for p in csv.DictReader(f):
                if p["forme"] in table:  # décision versée : la table l'emporte
                    continue
                if p["forme"] in propositions:
                    deja = propositions[p["forme"]]
                    # ligne inerte (commune ou pays listée sans proposition par un fichier « sites ») : l'autre l'emporte
                    if deja["lieu_id"] and p["lieu_id"]:
                        sys.exit(f"« {p['forme']} » figure dans {origine[p['forme']]} et dans {fichier}.")
                    if deja["lieu_id"]:
                        continue
                propositions[p["forme"]], origine[p["forme"]] = p, fichier
    for forme, t in table.items():
        propositions[forme] = {"forme": forme, "methode": "table", "decision": t["decision"], "lieu_id": t["lieu_id"]}
        origine[forme] = TABLE
    lieux ={l["lieu_id"]: l for l in lire(REFERENTIEL) + lire(COMMUNES_PAYS)}
    nouveau, en_attente = {}, {}
    for forme, p in propositions.items():
        if retenue(p):
            p["lieu_id"] = resoudre(p["lieu_id"], lieux)
            if p["lieu_id"] not in lieux:  # lieu à réintégrer (écarté) ou à créer (OSM) : en attente
                en_attente[forme] = p["lieu_id"]
                continue
            lieu = lieux[p["lieu_id"]]
            geo = lieu["coord_manuel"] or lieu["coord_wkt"]
            nouveau[forme] = (lieu, *(coord_ir(geo) if geo else ("", "")))

    chemins = sorted(glob.glob(SERIES[args.serie]["ir"]))
    if args.ir:
        chemins = [c for c in chemins if basename(c)[:-4].removeprefix("FRAM59100_") in args.ir]
    if exists(args.sortie):
        shutil.rmtree(args.sortie)
    makedirs(args.sortie)

    rapport, actions, ecrits, illisibles = [], Counter(), {}, []
    for chemin in chemins:
        ir = basename(chemin)[:-4].removeprefix("FRAM59100_")
        with open(chemin, encoding="utf-8", newline="") as f:
            texte_ir = f.read()
        try:
            elements = list(etree.parse(chemin).iter("geogname"))
        except etree.XMLSyntaxError:  # esperluettes non échappées : aucun <geogname> dans ces IR
            illisibles.append(ir)
            continue
        trouves = list(GEOGNAME.finditer(texte_ir))
        if len(elements) != len(trouves):
            sys.exit(f"{ir} : {len(trouves)} <geogname> repérés dans le texte, {len(elements)} dans l'arbre.")
        remplacements, vus = [], Counter()
        for g, m in zip(elements, trouves):
            forme = "".join(g.itertext()).strip()
            if unescape(m.group("texte") or "").strip() != forme:
                sys.exit(f"{ir} : texte différent entre l'arbre et le fichier pour « {forme} ».")
            if forme not in propositions:
                continue
            p = propositions[forme]
            c = next(g.iterancestors("c"), None)
            ligne = {"ir": ir, "c_id": c.get("id", "") if c is not None else "", "texte_avant": forme,
                     "source_avant": g.get("source", ""), "methode": p["methode"], "decision": p["decision"],
                     "lieu_id": p["lieu_id"], "thesaurus": "", "texte_apres": forme,
                     "source_apres": g.get("source", ""), "coord_apres": "", "action": action_si_laisse(p),
                     "doublon": ""}
            if forme in en_attente:
                ligne["action"] = "laissé (lieu absent du référentiel : à créer ou réintégrer)"
            if forme in nouveau:
                lieu, coord, type_coord = nouveau[forme]
                source = SOURCES[lieu["thesaurus"]]
                remplacements.append((m.start(), m.end(), reecrire(m.group("attrs"), lieu["libelle"], source, coord)))
                ligne.update(thesaurus=lieu["thesaurus"], texte_apres=lieu["libelle"], source_apres=source,
                             coord_apres=type_coord, action="réécrit")
                vus[(g.getparent(), lieu["lieu_id"])] += 1
                if vus[(g.getparent(), lieu["lieu_id"])] > 1:
                    ligne["doublon"] = "oui"
            rapport.append(ligne)
            actions[ligne["action"]] += 1
        if remplacements:
            for debut, fin, nouvelle in reversed(remplacements):
                texte_ir = texte_ir[:debut] + nouvelle + texte_ir[fin:]
            sortie_ir = join(args.sortie, basename(chemin))
            with open(sortie_ir, "w", encoding="utf-8", newline="") as f:
                f.write(texte_ir)
            ecrits[sortie_ir] = (len(elements), len(remplacements))

    # relecture : même nombre de <geogname>, nombre de lieux réécrits attendu
    for sortie_ir, (nb, nb_reecrits) in ecrits.items():
        arbre = list(etree.parse(sortie_ir).iter("geogname"))
        reecrits = [g for g in arbre if g.get("source") in SOURCES_BNR]
        assert len(arbre) == nb and len(reecrits) == nb_reecrits, f"relecture de {sortie_ir} : écart"

    with open(rapport_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES_RAPPORT, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rapport)

    print("propositions :", ", ".join(fichiers))
    if table:
        print(f"table de décisions : {TABLE} ({len(table)} formes, prioritaires sur les propositions)")
    print(f"{len(rapport)} <geogname> concernés dans {len(chemins)} IR ; {len(ecrits)} IR modifiés → {args.sortie}")
    print(f"actions : {dict(actions.most_common())}")
    print(f"formes appliquées : {len(nouveau)} ; doublons dans un même controlaccess : "
          f"{sum(1 for l in rapport if l['doublon'])}")
    if en_attente:
        print(f"ATTENTION : {len(en_attente)} forme(s) validée(s) dont le lieu_id n'est pas au référentiel "
              f"(écarté à réintégrer ou lieu OSM à créer) : {en_attente}")
    par_thesaurus = Counter(l["thesaurus"] for l in rapport if l["action"] == "réécrit")
    print(f"réécrits par thésaurus : {dict(par_thesaurus.most_common())}")
    for sortie_ir, (nb, nb_reecrits) in ecrits.items():
        print(f"  {basename(sortie_ir)}: {nb_reecrits} réécrits sur {nb} geogname")
    if illisibles:
        print(f"IR illisibles (ignorés, sans <geogname>) : {len(illisibles)}")
    print(f"rapport → {rapport_csv}")


if __name__ == "__main__":
    main()
