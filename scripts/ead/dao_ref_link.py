"""Extrait, depuis les IR sources de data/ead, le lien dao -> fichier + unitid.

Objectif : savoir quels fichiers sont reliés à un lien de diffusion (dao) et donc
à un unitid, pour les instruments de recherche présents dans data/ead — SANS
retransformer les IR (contrairement à ead_bnr2mnesys.py).

Deux formats sources, deux vocabulaires de rôle (sur <dao> et <daoloc>) :

  data/ead/bnr (format bn-r natif)
    <dao role="image|mp3|pdf|mp4" href="RBX_....jpg"/>   fichier isolé
    <daoloc role="image:first"/> + <daoloc role="image:last"/>  plage (daogrp)

  data/ead/mnesys (format Mnesys)
    <dao|daoloc href="Dossier/RBX_....jpg"/>  (role absent)   fichier isolé
    <daoloc role="first_image"/> + <daoloc role="last_image"/>  plage (daogrp)

Le contexte (unitid) est celui du composant <c> le plus proche (did/unitid) ; le
finding_aid est l'IR (sous-dossier + eadid). Les plages first/last sont
développées (tous les fichiers intermédiaires implicites), en réutilisant la
logique de dao_first_last_developpe.py.

Stage A (ce script pour l'instant) : produit results/ead/ead_cor/dao_ref_link_brut.csv
    colonnes : source, ir, finding_aid, id_composant, unitid, role, href, href_base,
               position, taille_plage
href_base = nom de fichier de diffusion sans dossier (basename), pour l'appariement
au référentiel (stage B, à venir).

À lancer depuis la racine du dépôt.
"""

import csv
import re
from glob import glob
from os.path import basename, dirname, join, splitext

from lxml import etree

SOURCES = [("bnr", join("data", "ead", "bnr")),
           ("mnesys", join("data", "ead", "mnesys"))]
SORTIE = join("results", "ead", "ead_cor", "dao_ref_link_brut.csv")

# XML source parfois mal formé (& non échappés dans certains IR mnesys)
PARSER = etree.XMLParser(recover=True)

MOTIF_TOKENS = re.compile(r"\d+|\D+")


def composant_parent(element):
    """Composant <c> (ou <archdesc>) contenant l'élément."""
    c = element.getparent()
    while c is not None and c.tag not in ("c", "archdesc"):
        c = c.getparent()
    return c


def contexte(element):
    """(id_composant, unitid, profondeur) du composant <c> portant l'élément.

    profondeur = nombre de <c> ancêtres (archdesc = 0). Sert à départager les
    conflits hiérarchiques : pour un même fichier, le composant le plus profond
    porte l'unitid le plus spécifique (enfant)."""
    c = composant_parent(element)
    if c is None:
        return "", "", -1
    cid = c.get("id", "")
    unitid_elem = c.find("did/unitid")
    unitid = (unitid_elem.text or "") if unitid_elem is not None else ""
    profondeur = len(c.xpath("ancestor-or-self::c"))
    return cid, unitid, profondeur


def position_role(role):
    """Normalise un role de borne -> ('image', 'first'|'last'), sinon None.

    Gère les deux conventions : bnr « <media>:first / <media>:last » et mnesys
    « first_image / last_image »."""
    if role is None:
        return None
    if role.endswith(":first") or role.endswith(":last"):
        prefixe, position = role.rsplit(":", 1)
        return prefixe, position
    if role in ("first_image", "last_image"):
        return "image", role.split("_")[0]
    return None


def segment_variable(stem_first, stem_last):
    """Index de l'unique segment numérique qui diffère entre deux stems, ou None
    si ambigu (nombre de tokens différent, ou != un seul segment numérique)."""
    tf = MOTIF_TOKENS.findall(stem_first)
    tl = MOTIF_TOKENS.findall(stem_last)
    if len(tf) != len(tl):
        return None
    diffs = [i for i, (a, b) in enumerate(zip(tf, tl)) if a != b]
    if len(diffs) != 1:
        return None
    i = diffs[0]
    if not (tf[i].isdigit() and tl[i].isdigit()):
        return None
    return tf, tl, i


def developpe(href_first, href_last):
    """Liste des href de la plage [first, last] incluse, ou None si ambiguë.
    Conserve dossier et extension de first ; seul le segment numérique varie."""
    dir_f = dirname(href_first)
    base_f, ext = splitext(basename(href_first))
    base_l = splitext(basename(href_last))[0]
    seg = segment_variable(base_f, base_l)
    if seg is None:
        return None
    tf, tl, i = seg
    debut, fin = int(tf[i]), int(tl[i])
    if fin < debut:
        return None
    largeur = len(tf[i])
    hrefs = []
    for n in range(debut, fin + 1):
        tf[i] = str(n).zfill(largeur)
        nom = "".join(tf) + ext
        hrefs.append(join(dir_f, nom) if dir_f else nom)
    return hrefs


def extrait_ir(source, path):
    """Lignes (dict) extraites d'un IR : fichiers isolés + plages développées."""
    ir = basename(path)
    root = etree.parse(path, PARSER).getroot()
    # finding_aid basé sur le NOM DE FICHIER de l'IR, pas sur l'eadid : 249 IR
    # mnesys ont un eadid générique (FRAM59100_0000XX) voire un placeholder de
    # test (TEST_EADMOUL_01) ; l'ancien ref indexe par nom de fichier.
    finding_aid = f"{source}_{splitext(ir)[0]}.xml"

    lignes = []

    def ligne(el, href, position, taille, role):
        cid, unitid, profondeur = contexte(el)
        lignes.append({
            "source": source, "ir": ir, "finding_aid": finding_aid,
            "id_composant": cid, "unitid": unitid, "profondeur": profondeur,
            "role": role, "href": href,
            "href_base": basename(href) if href else "",
            "position": position, "taille_plage": taille,
        })

    # 1) plages first/last, regroupées par daogrp puis par préfixe de média
    groupes = list(root.iter("daogrp"))
    bornes_traitees = set()
    for groupe in groupes:
        bornes = {}
        for el in groupe.iter("daoloc", "dao"):
            pr = position_role(el.get("role"))
            if pr is not None:
                prefixe, pos = pr
                bornes.setdefault(prefixe, {})[pos] = el
        for prefixe, paire in bornes.items():
            el_first, el_last = paire.get("first"), paire.get("last")
            if el_first is None or el_last is None:
                continue
            bornes_traitees.add(id(el_first))
            bornes_traitees.add(id(el_last))
            hf, hl = el_first.get("href", ""), el_last.get("href", "")
            hrefs = developpe(hf, hl)
            role = f"{prefixe}:plage"
            if hrefs is None:  # ambiguë : on garde au moins les deux bornes
                ligne(el_first, hf, "first", "", role + ":ambigu")
                ligne(el_last, hl, "last", "", role + ":ambigu")
                continue
            for rang, href in enumerate(hrefs):
                pos = ("first" if rang == 0
                       else "last" if rang == len(hrefs) - 1
                       else "intermediaire")
                ligne(el_first, href, pos, len(hrefs), role)

    # 2) fichiers isolés : tout <dao>/<daoloc> avec href, hors bornes de plage
    for tag in ("dao", "daoloc"):
        for el in root.iter(tag):
            if id(el) in bornes_traitees:
                continue
            href = el.get("href")
            if not href:
                continue
            ligne(el, href, "isole", 1, el.get("role"))

    return lignes


def main():
    champs = ["source", "ir", "finding_aid", "id_composant", "unitid", "profondeur",
              "role", "href", "href_base", "position", "taille_plage"]
    total = []
    exclus = []
    for source, dossier in SOURCES:
        n_ir = 0
        n_lignes = 0
        for path in sorted(glob(join(dossier, "*.xml"))):
            # on écarte les IR de test (ex. « test 1.xml », eadid TEST_EADMOUL_01) :
            # cotes malformées, non représentatives du fonds.
            if "test" in basename(path).lower():
                exclus.append(basename(path))
                continue
            lignes = extrait_ir(source, path)
            total.extend(lignes)
            n_ir += 1
            n_lignes += len(lignes)
        print(f"{source:7} : {n_ir:4d} IR -> {n_lignes:8d} liens dao")

    with open(SORTIE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=champs)
        w.writeheader()
        w.writerows(total)

    if exclus:
        print(f"\nIR de test écartés ({len(exclus)}) : {', '.join(exclus)}")
    print(f"\ntotal : {len(total)} liens dao")
    sans_unitid = sum(1 for l in total if not l["unitid"])
    print(f"  sans unitid : {sans_unitid} ({sans_unitid / max(len(total),1) * 100:.1f}%)")
    from collections import Counter
    print("  par position :", dict(Counter(l["position"] for l in total)))
    print(f"détail : {SORTIE}")


if __name__ == "__main__":
    main()
