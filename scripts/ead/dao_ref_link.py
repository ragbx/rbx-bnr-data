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

S'y ajoute une troisième source, restreinte à l'AUDIO :

  results/ead/ead_cor/bnr2mnesys (IR transformés par ead_bnr2mnesys.py)
    <daoloc role="preservation:audio" href=".../RBX_MED_X_96kHz24B.wav"/>

  Les IR sources ne portent que le mp3 de diffusion (RBX_MED_FLRS_X.mp3) alors
  que la conservation est nommée RBX_MED_X_{96kHz24B,44kHz24B,TI}.{wav,mp3} :
  l'appariement par stem échoue. Les liens preservation:audio des IR transformés
  portent les vrais noms de conservation (appariement déjà fait par
  ead_bnr2mnesys.py contre le ref). Le finding_aid émis est celui de l'IR SOURCE
  (résolu via l'Excel de transfert Mnesys), pour rester cohérent avec la source
  bnr et l'ancien ref.

Le contexte (unitid) est celui du composant <c> le plus proche (did/unitid) ; le
finding_aid est l'IR (sous-dossier + eadid). Les plages first/last sont
développées (tous les fichiers intermédiaires implicites) via le module partagé
dao_plage.py (même logique que dao_first_last_developpe.py).

Stage A (ce script pour l'instant) : produit results/ead/ead_cor/dao_ref_link_brut.csv
    colonnes : source, ir, finding_aid, id_composant, unitid, role, href, href_base,
               position, taille_plage
href_base = nom de fichier de diffusion sans dossier (basename), pour l'appariement
au référentiel (stage B, à venir).

À lancer depuis la racine du dépôt.
"""

import csv
from glob import glob
from os.path import basename, join, splitext

from lxml import etree

from dao_plage import developpe

# (source, dossier, roles) : roles=None -> tous les liens ; sinon seuls les
# éléments dont le role figure dans l'ensemble sont extraits (les IR transformés
# ne fournissent que les noms de conservation audio, le reste est déjà couvert
# par les sources bnr/mnesys).
SOURCES = [("bnr", join("data", "ead", "bnr"), None),
           ("mnesys", join("data", "ead", "mnesys"), None),
           ("bnr2mnesys", join("results", "ead", "ead_cor", "bnr2mnesys"),
            {"preservation:audio"})]
SORTIE = join("results", "ead", "ead_cor", "dao_ref_link_brut.csv")

# XML source parfois mal formé (& non échappés dans certains IR mnesys)
PARSER = etree.XMLParser(recover=True)


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


def extrait_ir(source, path, roles=None, finding_aid=None):
    """Lignes (dict) extraites d'un IR : fichiers isolés + plages développées.

    roles : ensemble de roles à extraire (None = tous). finding_aid : valeur à
    émettre (None = dérivée du nom de fichier de l'IR)."""
    ir = basename(path)
    root = etree.parse(path, PARSER).getroot()
    # finding_aid basé sur le NOM DE FICHIER de l'IR, pas sur l'eadid : 249 IR
    # mnesys ont un eadid générique (FRAM59100_0000XX) voire un placeholder de
    # test (TEST_EADMOUL_01) ; l'ancien ref indexe par nom de fichier.
    if finding_aid is None:
        finding_aid = f"{source}_{splitext(ir)[0]}.xml"

    lignes = []

    def ligne(el, href, position, taille, role):
        """Ajoute une ligne de lien à `lignes` : résout le contexte (composant,
        unitid, profondeur) de l'élément `el` et fige les métadonnées du lien
        (href, href_base=basename, position dans la plage, taille, role)."""
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
    # On garde les ÉLÉMENTS bornes (pas leurs id()) : les proxies lxml sont créés
    # à la demande et recyclés dès qu'ils ne sont plus référencés — un id() de
    # proxy mort peut être réattribué à un autre nœud (exclusions/réémissions
    # aléatoires constatées). Le set maintient les proxies en vie, et lxml
    # garantit un proxy unique par nœud tant qu'il est référencé.
    bornes_traitees = set()
    for groupe in groupes:
        bornes = {}
        for el in groupe.iter("daoloc", "dao"):
            if roles is not None and el.get("role") not in roles:
                continue
            pr = position_role(el.get("role"))
            if pr is not None:
                prefixe, pos = pr
                bornes.setdefault(prefixe, {})[pos] = el
        for prefixe, paire in bornes.items():
            el_first, el_last = paire.get("first"), paire.get("last")
            if el_first is None or el_last is None:
                continue
            bornes_traitees.add(el_first)
            bornes_traitees.add(el_last)
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
            if el in bornes_traitees:
                continue
            if roles is not None and el.get("role") not in roles:
                continue
            href = el.get("href")
            if not href:
                continue
            ligne(el, href, "isole", 1, el.get("role"))

    return lignes


def concordance_bnr2mnesys():
    """nouveau_nom_fichier (IR transformé) -> finding_aid de l'IR SOURCE
    (bnr_<nom sans extension>.xml), d'après le dernier Excel de transfert Mnesys.
    Les IR transformés viennent tous de data/ead/bnr (ex. FR595126101_MED_FLRS.xml
    <- FR595129901_MED_15.xml) : on émet le finding_aid de l'IR source pour rester
    cohérent avec la source bnr et l'ancien ref."""
    cands = sorted(glob(join("results", "ir",
                             "liste_instruments_recherche_*_transfert_mnesys.xlsx")))
    if not cands:
        return {}
    import pandas as pd
    x = pd.read_excel(cands[-1], usecols=["nom_fichier", "nouveau_nom_fichier"])
    x = x.dropna().drop_duplicates("nouveau_nom_fichier")
    return {nouveau: f"bnr_{splitext(str(src))[0]}.xml"
            for nouveau, src in zip(x["nouveau_nom_fichier"], x["nom_fichier"])}


def main():
    """Parcourt les IR de data/ead/{bnr,mnesys} et les IR transformés de
    results/ead/ead_cor/bnr2mnesys (audio de conservation seulement), hors IR de
    test, et écrit results/ead/ead_cor/dao_ref_link_brut.csv (Stage A)."""
    champs = ["source", "ir", "finding_aid", "id_composant", "unitid", "profondeur",
              "role", "href", "href_base", "position", "taille_plage"]
    total = []
    exclus = []
    sans_concordance = []
    for source, dossier, roles in SOURCES:
        conc = concordance_bnr2mnesys() if source == "bnr2mnesys" else {}
        n_ir = 0
        n_lignes = 0
        for path in sorted(glob(join(dossier, "*.xml"))):
            # on écarte les IR de test (ex. « test 1.xml », eadid TEST_EADMOUL_01) :
            # cotes malformées, non représentatives du fonds.
            if "test" in basename(path).lower():
                exclus.append(basename(path))
                continue
            finding_aid = conc.get(basename(path))
            if source == "bnr2mnesys" and finding_aid is None:
                sans_concordance.append(basename(path))
            lignes = extrait_ir(source, path, roles=roles, finding_aid=finding_aid)
            total.extend(lignes)
            n_ir += 1
            n_lignes += len(lignes)
        print(f"{source:10} : {n_ir:4d} IR -> {n_lignes:8d} liens dao")
    if sans_concordance:
        print(f"IR transformés absents de l'Excel de transfert "
              f"({len(sans_concordance)}, finding_aid par défaut) : "
              f"{', '.join(sans_concordance)}")

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
