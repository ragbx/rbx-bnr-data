"""
Aligne les @id des IR de results/ead/ead_cor/bnr2mnesys/ sur ceux que Mnesys
leur a attribués, d'après un export des IR depuis Mnesys.

Mnesys ignore les @id qu'on lui fournit : à chaque import, il attribue à chaque
<archdesc> et <c> un id neuf ("a01" + horodatage de l'import + suffixe). Pour
que les IR de travail, la concordance des id et les liens ARK restent ceux de
Mnesys, ce script :

1. apparie, IR par IR, les composants de bnr2mnesys et de l'export Mnesys :
   <archdesc> entre eux, puis <c> par (unitid, unittitle), puis par unitid
   seul, puis — pour une même clé en double — dans l'ordre du document. Un IR
   dont un composant reste sans correspondant n'est pas modifié ;
2. remplace dans les IR les @id par ceux de Mnesys, et dans les liens ARK
   l'id d'origine par l'id Mnesys (https://www.bn-r.fr/ark:/20179/BNR<id>,
   dans les <dao>/<daoloc> et les <odd>), par remplacement de texte : le reste
   du fichier est inchangé ;
3. reporte les nouveaux id dans results/ead/ead_cor/concordance_id.csv (relue
   par ead_bnr2mnesys.py) et dans la colonne c_id de
   results/ead/indexation/rbx-bnr_geogname_origine.csv (dates des documents
   pour l'indexation géographique) ;
4. écrit la table de correspondance
   results/ead/ead_cor/correspondance_id_mnesys_<date>.csv (fichier, unitid,
   unittitle, id_avant, id_mnesys, appariement).

Avant d'écrire, sauvegarde les IR dans
results/ead/ead_cor/archives/bnr2mnesys_<date>_avant_id_mnesys/ et les deux CSV
en <nom>.backup_<date>_avant_id_mnesys.csv. Rejouable : sur des IR déjà
alignés, rien ne change. Après application, régénérer
results/ead/indexation/rbx-bnr_geogname.csv avec geogname2csv.py.

Sans option, le script ne modifie rien : il affiche par IR le nombre d'id à
changer. Avec --appliquer, les fichiers sont réécrits.

Utilisation, depuis la racine du dépôt :

    python scripts/ead/ead_ids_mnesys.py <dossier de l'export Mnesys>              # à blanc
    python scripts/ead/ead_ids_mnesys.py <dossier de l'export Mnesys> --appliquer
"""
import argparse
import csv
import re
import shutil
from collections import Counter, defaultdict
from datetime import date
from glob import glob
from os.path import basename, exists, join

import pandas as pd
from lxml import etree

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")
ARCHIVES = join("results", "ead", "ead_cor", "archives")
CONCORDANCE_ID = join("results", "ead", "ead_cor", "concordance_id.csv")
GEOGNAME_ORIGINE = join("results", "ead", "indexation", "rbx-bnr_geogname_origine.csv")
CORRESPONDANCE = join("results", "ead", "ead_cor", "correspondance_id_mnesys_{date}.csv")

COMPOSANT = re.compile(r"c(0[1-9]|1[0-2])?")


def composants(chemin):
    """<archdesc> et <c> dans l'ordre du document : id, unitid, unittitle normalisé."""
    sortie = []
    for el in etree.parse(chemin).iter():
        if not isinstance(el.tag, str):
            continue
        nom = etree.QName(el).localname
        if nom != "archdesc" and not COMPOSANT.fullmatch(nom):
            continue
        did = next((d for d in el if isinstance(d.tag, str) and etree.QName(d).localname == "did"), None)
        unitid = titre = ""
        for d in did if did is not None else []:
            if not isinstance(d.tag, str):
                continue
            if etree.QName(d).localname == "unitid" and not unitid:
                unitid = " ".join("".join(d.itertext()).split())
            if etree.QName(d).localname == "unittitle" and not titre:
                titre = re.sub(r"[’'`]", "", " ".join("".join(d.itertext()).split())).lower()
        sortie.append({"id": el.get("id"), "unitid": unitid, "titre": titre, "archdesc": nom == "archdesc"})
    return sortie


def apparier(avant, mnesys):
    """[(composant avant, composant Mnesys, mode d'appariement)], composants sans correspondant."""
    paires = [(a, m, "archdesc") for a in avant if a["archdesc"] for m in mnesys if m["archdesc"]]
    pris = {id(a) for a, _, _ in paires} | {id(m) for _, m, _ in paires}
    for cle, mode in ((lambda c: (c["unitid"], c["titre"]), "unitid+titre"), (lambda c: c["unitid"], "unitid seul")):
        groupes_a, groupes_m = defaultdict(list), defaultdict(list)
        for c in avant:
            if id(c) not in pris:
                groupes_a[cle(c)].append(c)
        for c in mnesys:
            if id(c) not in pris:
                groupes_m[cle(c)].append(c)
        for k, liste in groupes_a.items():
            if k and len(liste) == 1 and len(groupes_m.get(k, [])) == 1:
                paires.append((liste[0], groupes_m[k][0], mode))
                pris |= {id(liste[0]), id(groupes_m[k][0])}
    groupes_a, groupes_m = defaultdict(list), defaultdict(list)
    for c in avant:
        if id(c) not in pris:
            groupes_a[(c["unitid"], c["titre"])].append(c)
    for c in mnesys:
        if id(c) not in pris:
            groupes_m[(c["unitid"], c["titre"])].append(c)
    for k, liste in groupes_a.items():
        if len(liste) == len(groupes_m.get(k, [])):
            for a, m in zip(liste, groupes_m[k]):
                paires.append((a, m, "clé en double, rang"))
                pris |= {id(a), id(m)}
    restes = [c for c in avant + mnesys if id(c) not in pris]
    return paires, restes


def remplacer_ids(texte, table):
    """@id des <archdesc>/<c> et id des liens ARK BnR remplacés selon table (ancien -> nouveau)."""
    texte = re.sub(r'(\sid=")([^"]+)(")', lambda m: m.group(1) + table.get(m.group(2), m.group(2)) + m.group(3), texte)
    return re.sub(r"(ark:/20179/BNR)([0-9A-Za-z]+)", lambda m: m.group(1) + table.get(m.group(2), m.group(2)), texte)


def sauvegarder(chemin, jour):
    copie = chemin.replace(".csv", f".backup_{jour}_avant_id_mnesys.csv")
    if not exists(copie):
        shutil.copy2(chemin, copie)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    parser.add_argument("export", help="dossier des IR exportés depuis Mnesys (mêmes noms de fichiers)")
    parser.add_argument("--appliquer", action="store_true", help="réécrit les fichiers (sinon : à blanc)")
    args = parser.parse_args()
    jour = date.today().strftime("%Y%m%d")

    table, lignes, modes, bloques = {}, [], Counter(), []
    a_ecrire = {}
    for chemin in sorted(glob(join(EAD_FOLDER, "*.xml"))):
        fichier = basename(chemin)
        export = join(args.export, fichier)
        if not exists(export):
            print(f"{fichier} : absent de l'export, ignoré")
            continue
        paires, restes = apparier(composants(chemin), composants(export))
        if restes:
            bloques.append(fichier)
            print(f"{fichier} : {len(restes)} composant(s) sans correspondant, IR non modifié")
            continue
        changes = {a["id"]: m["id"] for a, m, _ in paires if a["id"] != m["id"]}
        for a, m, mode in paires:
            modes[mode] += 1
            lignes.append({"fichier": fichier, "unitid": a["unitid"], "unittitle": a["titre"],
                           "id_avant": a["id"], "id_mnesys": m["id"], "appariement": mode})
        if changes:
            print(f"{fichier} : {len(changes)} id à changer")
            table.update(changes)
            a_ecrire[chemin] = changes

    print(f"Appariements : {dict(modes)}")
    print(f"Total : {len(table)} id {'changés' if args.appliquer else 'à changer (à blanc)'}"
          + (f" ; IR bloqués : {', '.join(bloques)}" if bloques else ""))
    if not args.appliquer or not table:
        raise SystemExit

    archive = join(ARCHIVES, f"bnr2mnesys_{jour}_avant_id_mnesys")
    if not exists(archive):
        shutil.copytree(EAD_FOLDER, archive)
    for chemin, changes in a_ecrire.items():
        with open(chemin, encoding="utf-8") as f:
            texte = f.read()
        with open(chemin, "w", encoding="utf-8") as f:
            f.write(remplacer_ids(texte, changes))

    sauvegarder(CONCORDANCE_ID, jour)
    concordance = pd.read_csv(CONCORDANCE_ID, dtype=str, keep_default_na=False)
    concordance["id"] = concordance["id"].map(lambda i: table.get(i, i))
    concordance.to_csv(CONCORDANCE_ID, index=False)

    sauvegarder(GEOGNAME_ORIGINE, jour)
    with open(GEOGNAME_ORIGINE, newline="", encoding="utf-8") as f:
        fin_de_ligne = "\r\n" if "\r\n" in f.read() else "\n"
    with open(GEOGNAME_ORIGINE, newline="", encoding="utf-8") as f:
        origine = list(csv.DictReader(f))
    for l in origine:
        l["c_id"] = table.get(l["c_id"], l["c_id"])
    with open(GEOGNAME_ORIGINE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(origine[0]), lineterminator=fin_de_ligne)
        writer.writeheader()
        writer.writerows(origine)

    sortie = CORRESPONDANCE.format(date=jour)
    pd.DataFrame(lignes).to_csv(sortie, index=False)
    print(f"Écrits : {len(a_ecrire)} IR, {CONCORDANCE_ID}, {GEOGNAME_ORIGINE}, {sortie}")
    print("À relancer ensuite : python scripts/ead/geogname2csv.py")
