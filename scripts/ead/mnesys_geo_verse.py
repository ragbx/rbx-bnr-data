"""
Verse les décisions saisies dans les fichiers de propositions (colonne decision = « oui » ou « non ») dans la table
de décisions cumulative data/geo/concordance_mnesys_lieux.csv (forme -> lieu), à lancer après chaque séance de
validation : la validation se fait en plusieurs fois.

Règles du versement :
  - seules les lignes décidées sont versées ; les lignes sans décision restent dans les fichiers de propositions
    (et, pour les « automatiques », continuent d'y être appliquées par mnesys_geo_applique.py) ;
  - « oui » exige un lieu_id (un identifiant copié sans son préfixe est complété s'il est unique) ; « non » peut
    s'en passer (forme laissée telle quelle dans les IR) ;
  - la table n'est jamais reconstruite : une forme nouvelle est ajoutée, une forme déjà versée avec la même
    décision et le même lieu est gardée (sa remarque est mise à jour si elle a changé) ;
  - une forme déjà versée avec une autre décision ou un autre lieu est un CONFLIT : signalé, la table garde
    l'ancienne valeur, sauf --remplacer ;
  - les fichiers de propositions ne sont pas modifiés.

La table l'emporte sur les fichiers de propositions dans mnesys_geo_applique.py, et ses formes sont exclues des
propositions régénérées (mnesys_geo_quartiers/reprise/voies/sites/territoires.py) : on peut régénérer des
propositions, après un réimport Mnesys par exemple, sans perdre une décision versée. Une décision se corrige
directement dans la table, ou par --remplacer.

Colonnes : forme, decision, lieu_id (vide pour « non »), libelle et thesaurus (indicatifs, d'après le référentiel ;
vides si le lieu est à créer), lieu_rejete (pour « non » : le lieu proposé et refusé), remarque, validation (« explicite »), origine (fichier de propositions), date_versement, date_maj.

À lancer depuis la racine du dépôt :
    python scripts/ead/mnesys_geo_verse.py [--propositions CSV ...] [--remplacer] [--essai]
"""
import argparse
import csv
import sys
from collections import Counter
from datetime import date
from os import replace
from os.path import basename, dirname

sys.path.insert(0, dirname(__file__))
from mnesys_geo_commun import COLONNES_TABLE, TABLE, lire_table, propositions_appliquees  # noqa: E402
from mnesys_geo_applique import resoudre  # noqa: E402
from geogname_concordance_lieux import COMMUNES_PAYS, REFERENTIEL, lire  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    parser.add_argument("--propositions", nargs="*",
                        help="CSV de propositions à verser (défaut : le plus récent de chaque série et famille)")
    parser.add_argument("--remplacer", action="store_true",
                        help="en cas de conflit, la décision du fichier de propositions remplace celle de la table")
    parser.add_argument("--essai", action="store_true", help="bilan seulement, la table n'est pas écrite")
    args = parser.parse_args()
    fichiers = args.propositions or propositions_appliquees()
    aujourdhui = date.today().isoformat()

    lieux = {l["lieu_id"]: l for l in lire(REFERENTIEL) + lire(COMMUNES_PAYS)}
    table = lire_table()
    bilan, conflits, sans_lieu, absents, vues = Counter(), [], [], set(), {}
    for fichier in fichiers:
        with open(fichier, encoding="utf-8") as f:
            for p in csv.DictReader(f):
                decision = p["decision"].strip().lower()
                if decision not in ("oui", "non"):
                    if decision:
                        print(f"ATTENTION : décision « {p['decision']} » non reconnue pour « {p['forme']} » ({basename(fichier)})")
                    bilan["non décidées (restent à valider)"] += 1
                    continue
                lieu_id = resoudre(p["lieu_id"].strip(), lieux) if p["lieu_id"].strip() else ""
                if decision == "oui" and not lieu_id:
                    sans_lieu.append((p["forme"], basename(fichier)))
                    continue
                forme = p["forme"]
                if forme in vues and (vues[forme][0], vues[forme][1]) != (decision, lieu_id):
                    sys.exit(f"« {forme} » est décidée différemment dans {vues[forme][2]} et dans {basename(fichier)}.")
                vues[forme] = (decision, lieu_id, basename(fichier))
                if decision == "oui" and lieu_id not in lieux:
                    absents.add((forme, lieu_id))
                rejete = lieu_id if decision == "non" else ""  # « non » : le lieu proposé est refusé
                lieu_id = "" if decision == "non" else lieu_id
                lieu = lieux.get(lieu_id, {})
                ligne = {"forme": forme, "decision": decision, "lieu_id": lieu_id,
                         "libelle": lieu.get("libelle", ""), "thesaurus": lieu.get("thesaurus", ""),
                         "lieu_rejete": rejete,
                         "remarque": p.get("remarque", "").strip(), "validation": "explicite",
                         "origine": basename(fichier), "date_versement": aujourdhui, "date_maj": aujourdhui}
                ancienne = table.get(forme)
                if ancienne is None:
                    table[forme] = ligne
                    bilan["ajoutées"] += 1
                elif (ancienne["decision"], ancienne["lieu_id"]) == (decision, lieu_id):
                    maj = {k: ligne[k] for k in ("libelle", "thesaurus", "lieu_rejete", "remarque") if ligne[k] != ancienne[k]}
                    if maj:
                        ancienne.update(maj, date_maj=aujourdhui)
                        bilan["déjà versées, mises à jour"] += 1
                    else:
                        bilan["déjà versées, inchangées"] += 1
                else:
                    conflits.append((forme, ancienne["decision"], ancienne["lieu_id"], decision, lieu_id, basename(fichier)))
                    if args.remplacer:
                        ligne["date_versement"] = ancienne["date_versement"]
                        table[forme] = ligne
                        bilan["conflits remplacés"] += 1
                    else:
                        bilan["conflits (table inchangée)"] += 1

    print("propositions :", ", ".join(fichiers))
    print(f"bilan : {dict(bilan)}")
    if sans_lieu:
        print(f"ATTENTION : {len(sans_lieu)} « oui » sans lieu_id, non versés : {sans_lieu}")
    for forme, d0, l0, d1, l1, fichier in conflits:
        print(f"CONFLIT « {forme} » : table {d0} {l0 or '(sans lieu)'} / {fichier} {d1} {l1 or '(sans lieu)'}"
              + (" → remplacé" if args.remplacer else " → table gardée"))
    if absents:
        print(f"{len(absents)} forme(s) versée(s) avec un lieu_id absent du référentiel (en attente à l'application) : "
              f"{sorted(absents)}")
    d = Counter(r["decision"] for r in table.values())
    print(f"table : {len(table)} formes ({d['oui']} oui, {d['non']} non)")
    if args.essai:
        print("essai : table non écrite")
        return
    temporaire = TABLE + ".tmp"
    with open(temporaire, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES_TABLE, lineterminator="\n")
        writer.writeheader()
        writer.writerows(sorted(table.values(), key=lambda r: r["forme"].casefold()))
    replace(temporaire, TABLE)
    print(f"→ {TABLE}")


if __name__ == "__main__":
    main()
