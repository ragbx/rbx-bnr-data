"""
Rend conformes à la DTD EAD 2002 les niveaux de description hors liste.

L'attribut level d'un <c> n'admet que onze valeurs (cf. NIVEAUX_EAD). Toute
autre valeur — dans les IR, uniquement "subfile" — est reportée dans
l'attribut otherlevel :

    <c level="subfile">  →  <c level="otherlevel" otherlevel="subfile">

Correctif appliqué directement aux IR de results/ead/ead_cor/bnr2mnesys/
(base de travail).

Sans option, le script ne modifie rien : il affiche le décompte par IR. Avec
--appliquer, les fichiers sont réécrits. Rejouable : un second passage ne
change rien.

Utilisation, depuis la racine du dépôt :

    python scripts/ead/ead_otherlevel.py              # à blanc
    python scripts/ead/ead_otherlevel.py --appliquer
"""
import argparse
from collections import Counter
from glob import glob
from os.path import basename, join

from lxml import etree

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")

NIVEAUX_EAD = {
    "class", "collection", "file", "fonds", "item", "otherlevel", "recordgrp",
    "series", "subfonds", "subgrp", "subseries",
}


def corrige_niveaux(root):
    """Reporte dans otherlevel les level hors liste. Retourne leur décompte.

    L'attribut otherlevel est placé juste après level.
    """
    niveaux = Counter()
    for el in root.iter("archdesc", "c"):
        niveau = el.get("level")
        if niveau is None or niveau in NIVEAUX_EAD:
            continue
        attributs = []
        for nom, valeur in el.attrib.items():
            if nom == "level":
                attributs += [("level", "otherlevel"), ("otherlevel", niveau)]
            elif nom != "otherlevel":
                attributs.append((nom, valeur))
        el.attrib.clear()
        el.attrib.update(attributs)
        niveaux[niveau] += 1
    return niveaux


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    parser.add_argument(
        "--appliquer", action="store_true", help="réécrit les IR (sinon : à blanc)"
    )
    args = parser.parse_args()

    total = Counter()
    for chemin in sorted(glob(join(EAD_FOLDER, "*.xml"))):
        tree = etree.parse(chemin)
        niveaux = corrige_niveaux(tree.getroot())
        total += niveaux
        if niveaux:
            detail = ", ".join(f"{n} {niveau}" for niveau, n in niveaux.most_common())
            print(f"{basename(chemin)} : {detail}")
            if args.appliquer:
                tree.write(
                    chemin,
                    encoding="UTF-8",
                    xml_declaration=True,
                    pretty_print=True,
                    with_comments=True,
                )
    mode = "corrigés" if args.appliquer else "à corriger (à blanc)"
    print(f"Total : {sum(total.values())} niveaux {mode} {dict(total) or ''}")
