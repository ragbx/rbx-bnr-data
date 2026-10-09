"""
Trie, à chaque niveau des IR, les <c> frères dans l'ordre alphanumérique de
leur <did>/<unitid>.

Correctif appliqué directement aux IR de results/ead/ead_cor/bnr2mnesys/
(base de travail) : seul l'ordre des <c> change, leur contenu est intact.

Ordre retenu (cf. cle_tri) : tri naturel, insensible à la casse. Les suites
de chiffres sont comparées comme des nombres (D9 avant D10, S2 = S02), un
nombre passe avant une lettre à la même position, et les espaces valent un
tiret bas ("RAD S02" est classé comme "RAD_S02"). À cote égale, l'ordre
d'origine est conservé.

Sans option, le script ne modifie rien : il affiche par IR le nombre de
groupes de <c> à réordonner. Avec --appliquer, les fichiers sont réécrits.

Utilisation, depuis la racine du dépôt :

    python scripts/ead/ead_tri_unitid.py              # à blanc
    python scripts/ead/ead_tri_unitid.py --appliquer
"""
import argparse
import re
from bisect import bisect_right
from glob import glob
from os.path import basename, join

from lxml import etree

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")


def cle_tri(unitid):
    """Clé de tri naturel d'une cote : alternance de nombres et de textes."""
    cote = re.sub(r"\s+", "_", unitid.strip()).lower()
    return [
        (0, int(morceau), "") if morceau.isdigit() else (1, 0, morceau)
        for morceau in re.findall(r"\d+|\D+", cote)
    ]


def unitid(c):
    return c.findtext("./did/unitid") or ""


def nb_deplaces(rangs):
    """Nombre minimal d'éléments à déplacer pour ordonner la suite : sa
    longueur moins celle de sa plus longue sous-suite croissante."""
    queues = []
    for rang in rangs:
        i = bisect_right(queues, rang)
        if i == len(queues):
            queues.append(rang)
        else:
            queues[i] = rang
    return len(rangs) - len(queues)


def trier(root):
    """Trie les <c> enfants directs de chaque <dsc> et de chaque <c>.

    Retourne (groupes réordonnés, <c> déplacés).
    """
    groupes = deplaces = 0
    for parent in list(root.iter("dsc", "c")):
        enfants = [e for e in parent if e.tag == "c"]
        tries = sorted(enfants, key=lambda c: cle_tri(unitid(c)))
        if tries == enfants:
            continue
        positions = [parent.index(c) for c in enfants]
        if positions != list(range(positions[0], positions[0] + len(enfants))):
            raise ValueError(
                f"<c> non contigus sous {parent.tag} {parent.get('id') or ''}"
            )
        groupes += 1
        rang = {c: i for i, c in enumerate(tries)}
        deplaces += nb_deplaces([rang[c] for c in enfants])
        # L'indentation (tail) reste attachée à la position, pas à l'élément
        tails = [c.tail for c in enfants]
        for c in enfants:
            parent.remove(c)
        for i, (c, tail) in enumerate(zip(tries, tails)):
            c.tail = tail
            parent.insert(positions[0] + i, c)
    return groupes, deplaces


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--appliquer", action="store_true", help="réécrit les IR (sinon : à blanc)"
    )
    args = parser.parse_args()

    total_groupes = total_deplaces = 0
    for chemin in sorted(glob(join(EAD_FOLDER, "*.xml"))):
        tree = etree.parse(chemin)
        groupes, deplaces = trier(tree.getroot())
        total_groupes += groupes
        total_deplaces += deplaces
        if groupes:
            print(f"{basename(chemin)} : {groupes} groupes, {deplaces} <c> déplacés")
            if args.appliquer:
                tree.write(
                    chemin,
                    encoding="UTF-8",
                    xml_declaration=True,
                    pretty_print=True,
                    with_comments=True,
                )
    mode = "réordonnés" if args.appliquer else "à réordonner (à blanc)"
    print(f"Total : {total_groupes} groupes {mode}, {total_deplaces} <c> déplacés")
