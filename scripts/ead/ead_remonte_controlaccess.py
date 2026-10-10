"""
Remonte avant les <c> enfants les <controlaccess> placés après eux.

La DTD EAD 2002 impose que les blocs descriptifs d'un <c> précèdent ses
sous-composants. Le <controlaccess> fautif est déplacé juste avant le
premier <c> enfant, sans autre modification.

Correctif appliqué directement aux IR de results/ead/ead_cor/bnr2mnesys/
(base de travail).

Sans option, le script ne modifie rien : il liste les composants concernés.
Avec --appliquer, les fichiers sont réécrits. Rejouable : un second passage
ne change rien.

Utilisation, depuis la racine du dépôt :

    python scripts/ead/ead_remonte_controlaccess.py              # à blanc
    python scripts/ead/ead_remonte_controlaccess.py --appliquer
"""
import argparse
from glob import glob
from os.path import basename, join

from lxml import etree

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")


def remonte(root):
    """Déplace les <controlaccess> mal placés. Retourne les cotes concernées."""
    cotes = []
    for parent in list(root.iter("archdesc", "dsc", "c")):
        premier_c = parent.find("c")
        if premier_c is None:
            continue
        deplaces = [
            e
            for e in parent.findall("controlaccess")
            if parent.index(e) > parent.index(premier_c)
        ]
        for controlaccess in deplaces:
            # L'indentation (tail) reste attachée à la position
            precedent = controlaccess.getprevious()
            precedent.tail, controlaccess.tail = controlaccess.tail, precedent.tail
            parent.remove(controlaccess)
            parent.insert(parent.index(premier_c), controlaccess)
        if deplaces:
            cotes.append((parent.findtext("did/unitid") or "").strip())
    return cotes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    parser.add_argument(
        "--appliquer", action="store_true", help="réécrit les IR (sinon : à blanc)"
    )
    args = parser.parse_args()

    total = 0
    for chemin in sorted(glob(join(EAD_FOLDER, "*.xml"))):
        tree = etree.parse(chemin)
        cotes = remonte(tree.getroot())
        total += len(cotes)
        if cotes:
            print(f"{basename(chemin)} : {', '.join(cotes)}")
            if args.appliquer:
                tree.write(
                    chemin,
                    encoding="UTF-8",
                    xml_declaration=True,
                    pretty_print=True,
                    with_comments=True,
                )
    mode = "déplacés" if args.appliquer else "à déplacer (à blanc)"
    print(f"Total : {total} <controlaccess> {mode}")
