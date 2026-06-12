"""
Liste, pour chaque IR de results/ead/ead_cor/bnr2mnesys, les liens de diffusion
(role access:*) sans fichier de conservation (role preservation:*) apparié.

L'appariement reprend la logique de ead_bnr2mnesys.py : dans un même <daogrp>,
un lien access:* est apparié à un lien preservation:* de la même famille de
média (image, audio, video, pdf — déduite du role) et de même nom de base sans
extension, avec les mêmes normalisations (suffixes de variante audio
_96kHz24B/_TI retirés, renommage FLRS RBX_MED_FLRS_ → RBX_MED_ et + → _).
Les liens access:ocr, dérivés des liens image/pdf, sont ignorés.

À lancer depuis la racine du dépôt. Produit
results/ead/ead_cor/dao_sans_conservation.csv (colonnes : ir, id_composant,
unitid, role, href) et affiche un décompte par IR.
"""

import csv
import re
from collections import Counter
from glob import glob
from os.path import basename, join, splitext

from lxml import etree

DOSSIER = join("results", "ead", "ead_cor", "bnr2mnesys")
SORTIE = join("results", "ead", "ead_cor", "dao_sans_conservation.csv")

MOTIF_VARIANTE_AUDIO = re.compile(r"_(\d+kHz\d+B|TI)$", re.IGNORECASE)


def famille(role):
    """Famille de média (image, audio, video, pdf) déduite du role."""
    parts = role.split(":")
    return parts[1] if len(parts) > 1 else None


def cles(href):
    """Clés candidates d'appariement pour un href : basename sans extension,
    avec variante audio retirée et normalisation FLRS."""
    key = splitext(basename(href))[0]
    flrs = key.replace("RBX_MED_FLRS_", "RBX_MED_").replace("+", "_")
    out = {key, flrs}
    out |= {MOTIF_VARIANTE_AUDIO.sub("", k) for k in list(out)}
    return out


def composant_parent(element):
    """Composant <c> (ou <archdesc>) contenant l'élément, pour le contexte."""
    c = element.getparent()
    while c is not None and c.tag not in ("c", "archdesc"):
        c = c.getparent()
    return c


lignes = []
for path in sorted(glob(join(DOSSIER, "*.xml"))):
    ir = basename(path)
    root = etree.parse(path).getroot()

    # groupes : chaque <daogrp>, plus chaque <dao> isolé (hors <daogrp>)
    groupes = [list(g) for g in root.iter("daogrp")]
    groupes += [[d] for d in root.iter("dao") if d.getparent().tag != "daogrp"]

    for groupe in groupes:
        conservation = set()  # couples (famille, clé)
        for el in groupe:
            role = el.get("role", "")
            if role.startswith("preservation:"):
                for k in cles(el.get("href", "")):
                    conservation.add((famille(role), k))
        for el in groupe:
            role = el.get("role", "")
            if not role.startswith("access:") or role == "access:ocr":
                continue
            fam = famille(role)
            if not any((fam, k) in conservation for k in cles(el.get("href", ""))):
                c = composant_parent(el)
                cid = c.get("id", "") if c is not None else ""
                unitid_elem = c.find("did/unitid") if c is not None else None
                unitid = (
                    unitid_elem.text or "" if unitid_elem is not None else ""
                )
                lignes.append([ir, cid, unitid, role, el.get("href", "")])

with open(SORTIE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["ir", "id_composant", "unitid", "role", "href"])
    writer.writerows(lignes)

compte = Counter(ligne[0] for ligne in lignes)
for path in sorted(glob(join(DOSSIER, "*.xml"))):
    print(f"{compte.get(basename(path), 0):6d}  {basename(path)}")
print(f"{len(lignes):6d}  TOTAL (détail : {SORTIE})")
