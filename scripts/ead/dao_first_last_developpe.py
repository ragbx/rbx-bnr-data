"""
Développe les plages de liens DAO exprimées par un couple first/last.

Dans results/ead/ead_cor/bnr2mnesys, un <daogrp> peut décrire une suite de
fichiers par ses seules bornes : un lien *:first et un lien *:last de même
préfixe de role (ex. access:image:first / access:image:last). Les fichiers
intermédiaires ne sont jamais listés : ils sont implicites. Exemple, pour
  access:image:first = CSV/RBX_CSV_PAL_1858_01.jpg
  access:image:last  = CSV/RBX_CSV_PAL_1858_08.jpg
sont aussi concernés _02 à _07, soit huit fichiers.

Ce script reconstitue, pour chaque couple first/last, la liste de tous les
fichiers de la plage. La borne first et la borne last ne diffèrent en général
que par un unique segment numérique, sur lequel porte l'énumération ; le
préfixe de chemin, l'extension et le reste du nom sont conservés tels quels
(logique de développement partagée avec dao_ref_link.py : module dao_plage.py).

Dans un même groupe, les bornes access:<média> et preservation:<média>
décrivent les mêmes fichiers ; on ne développe l'access que si le preservation
correspondant est absent du groupe (la conservation prime).

À lancer depuis la racine du dépôt. Produit
results/ead/ead_cor/dao_first_last_developpe.csv : une ligne par fichier de
chaque plage (colonnes ir, id_composant, unitid, role, href, position,
taille_plage). Les couples dont les bornes diffèrent par autre chose qu'un
seul segment numérique (énumération ambiguë) sont écartés et listés dans
results/ead/ead_cor/dao_first_last_ambigus.csv. Affiche une synthèse.
"""

import csv
from glob import glob
from os.path import basename, join

from lxml import etree

from dao_plage import developpe

DOSSIER = join("results", "ead", "ead_cor", "bnr2mnesys")
SORTIE = join("results", "ead", "ead_cor", "dao_first_last_developpe.csv")
AMBIGUS = join("results", "ead", "ead_cor", "dao_first_last_ambigus.csv")


def composant_parent(element):
    """Composant <c> (ou <archdesc>) contenant l'élément, pour le contexte."""
    c = element.getparent()
    while c is not None and c.tag not in ("c", "archdesc"):
        c = c.getparent()
    return c


def contexte(element):
    """(id_composant, unitid) du composant portant l'élément."""
    c = composant_parent(element)
    if c is None:
        return "", ""
    cid = c.get("id", "")
    unitid_elem = c.find("did/unitid")
    unitid = (unitid_elem.text or "") if unitid_elem is not None else ""
    return cid, unitid


lignes = []
lignes_ambigus = []
for path in sorted(glob(join(DOSSIER, "*.xml"))):
    ir = basename(path)
    root = etree.parse(path).getroot()
    groupes = list(root.iter("daogrp"))
    groupes += [d for d in root.iter("dao") if d.getparent().tag != "daogrp"]

    for groupe in groupes:
        # bornes first/last regroupées par préfixe de role (access:image…)
        bornes = {}
        for el in groupe.iter("daoloc", "dao"):
            role = el.get("role", "")
            if role.endswith(":first") or role.endswith(":last"):
                prefixe, position = role.rsplit(":", 1)
                bornes.setdefault(prefixe, {})[position] = el

        for prefixe, paire in bornes.items():
            el_first, el_last = paire.get("first"), paire.get("last")
            if el_first is None or el_last is None:
                continue
            # access et preservation d'une même famille décrivent les mêmes
            # fichiers ; on ne retient l'access que si le preservation
            # correspondant est absent du groupe (la conservation prime).
            if prefixe.startswith("access:"):
                media = prefixe.split(":", 1)[1]
                pres = bornes.get("preservation:" + media, {})
                if pres.get("first") is not None and pres.get("last") is not None:
                    continue
            href_first = el_first.get("href", "")
            href_last = el_last.get("href", "")
            cid, unitid = contexte(el_first)
            hrefs = developpe(href_first, href_last)
            if hrefs is None:
                lignes_ambigus.append(
                    [ir, cid, unitid, prefixe, href_first, href_last]
                )
                continue
            for rang, href in enumerate(hrefs):
                position = (
                    "first"
                    if rang == 0
                    else "last"
                    if rang == len(hrefs) - 1
                    else "intermediaire"
                )
                lignes.append(
                    [ir, cid, unitid, prefixe, href, position, len(hrefs)]
                )


with open(SORTIE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(
        ["ir", "id_composant", "unitid", "role", "href", "position", "taille_plage"]
    )
    writer.writerows(lignes)

with open(AMBIGUS, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(
        ["ir", "id_composant", "unitid", "role", "href_first", "href_last"]
    )
    writer.writerows(lignes_ambigus)

nb_plages = (
    len({(l[0], l[1], l[3]) for l in lignes}) if lignes else 0
)
intermediaires = sum(1 for l in lignes if l[5] == "intermediaire")
print(f"{len(lignes)} fichiers développés")
print(f"  dont {intermediaires} intermédiaires (implicites, ni first ni last)")
print("\nrépartition par role :")
from collections import Counter

for role, n in sorted(Counter(l[3] for l in lignes).items()):
    print(f"  {n:7d}  {role}")
print(f"\n{len(lignes_ambigus)} couples ambigus écartés (détail : {AMBIGUS})")
print(f"détail : {SORTIE}")
