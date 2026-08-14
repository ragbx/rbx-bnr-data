"""Extrait, depuis les IR bn-r de data/ead/bnr, le lien dao -> fichier + unitid.

Objectif : savoir quels fichiers sont reliés à un lien de diffusion (dao) et donc
à un unitid, pour les instruments de recherche bn-r — SANS retransformer les IR
(contrairement à ead_bnr2mnesys.py).

Entrée : uniquement data/ead/bnr (format bn-r natif), deux vocabulaires de rôle
sur <dao> et <daoloc> :
    <dao role="image|mp3|pdf|mp4" href="RBX_....jpg"/>   fichier isolé
    <daoloc role="image:first"/> + <daoloc role="image:last"/>  plage (daogrp)

Exception : FR595129901_MED_15.xml est remplacé par sa version transformée
results/ead/ead_cor/bnr2mnesys/FR595126101_MED_FLRS.xml (produite par
ead_bnr2mnesys.py). L'IR bnr ne porte que le mp3 de diffusion (RBX_MED_FLRS_X.mp3)
alors que la conservation est nommée RBX_MED_X_{96kHz24B,44kHz24B,TI}.{wav,mp3} :
la version transformée porte ces vrais noms de conservation (appariés au ref par
ead_bnr2mnesys.py). Le finding_aid émis reste celui de l'IR SOURCE
(bnr_FR595129901_MED_15.xml, résolu via l'Excel de transfert Mnesys), pour rester
cohérent avec l'ancien ref.

Le contexte (unitid) est celui du composant <c> le plus proche (did/unitid) ; le
finding_aid est l'IR (sous-dossier + eadid). Les plages first/last sont
développées (tous les fichiers intermédiaires implicites) via le module partagé
dao_plage.py (même logique que dao_first_last_developpe.py).

Stage A (ce script pour l'instant) : produit results/ead/ead_cor/dao_ref_link_brut.csv
    colonnes : source, ir, finding_aid, id_composant, unitid, role, href, href_base,
               position, taille_plage
               + colonnes de compatibilité avec l'ancien dao_flat (superset) :
               dao, daoloc_first, daoloc_last, nom_fichier, nom_fichier_base
href_base = nom de fichier de diffusion sans dossier (basename), pour l'appariement
au référentiel (stage B, à venir). dao = href du fichier isolé ; daoloc_first /
daoloc_last = hrefs des bornes de la plage (rappelés sur chaque ligne développée) ;
nom_fichier = basename(href) (= href_base) ; nom_fichier_base = idem sans extension.

À lancer depuis la racine du dépôt.
"""

import csv
from glob import glob
from os.path import basename, join, splitext

from lxml import etree

from dao_plage import developpe

# Entrée : uniquement les IR bn-r natifs de data/ead/bnr, à une exception près.
DOSSIER_BNR = join("data", "ead", "bnr")

# FR595129901_MED_15.xml (bnr) est remplacé par sa version transformée
# results/ead/ead_cor/bnr2mnesys/FR595126101_MED_FLRS.xml : cette dernière porte
# les vrais noms de conservation audio (RBX_MED_X_{96kHz24B,44kHz24B,TI}.{wav,mp3}),
# appariés au ref par ead_bnr2mnesys.py, alors que l'IR bnr ne liste que le mp3 de
# diffusion. Le finding_aid émis reste celui de l'IR SOURCE (bnr_FR595129901_MED_15.xml,
# résolu via l'Excel de transfert Mnesys), pour rester cohérent avec l'ancien ref.
BNR_EXCLUS = "FR595129901_MED_15.xml"
REMPLACEMENT = join("results", "ead", "ead_cor", "bnr2mnesys",
                    "FR595126101_MED_FLRS.xml")

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

    def ligne(el, href, position, taille, role,
              dao="", daoloc_first="", daoloc_last=""):
        """Ajoute une ligne de lien à `lignes` : résout le contexte (composant,
        unitid, profondeur) de l'élément `el` et fige les métadonnées du lien
        (href, href_base=basename, position dans la plage, taille, role).

        Colonnes de compatibilité avec l'ancien dao_flat (superset) :
        dao = href du fichier isolé (vide pour une plage) ; daoloc_first /
        daoloc_last = hrefs des bornes de la plage (vides pour un isolé) ;
        nom_fichier = basename(href) ; nom_fichier_base = idem sans extension."""
        cid, unitid, profondeur = contexte(el)
        nom_fichier = basename(href) if href else ""
        lignes.append({
            "source": source, "ir": ir, "finding_aid": finding_aid,
            "id_composant": cid, "unitid": unitid, "profondeur": profondeur,
            "role": role, "href": href,
            "href_base": nom_fichier,
            "position": position, "taille_plage": taille,
            "dao": dao, "daoloc_first": daoloc_first, "daoloc_last": daoloc_last,
            "nom_fichier": nom_fichier, "nom_fichier_base": splitext(nom_fichier)[0],
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
                ligne(el_first, hf, "first", "", role + ":ambigu",
                      daoloc_first=hf, daoloc_last=hl)
                ligne(el_last, hl, "last", "", role + ":ambigu",
                      daoloc_first=hf, daoloc_last=hl)
                continue
            for rang, href in enumerate(hrefs):
                pos = ("first" if rang == 0
                       else "last" if rang == len(hrefs) - 1
                       else "intermediaire")
                ligne(el_first, href, pos, len(hrefs), role,
                      daoloc_first=hf, daoloc_last=hl)

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
            ligne(el, href, "isole", 1, el.get("role"), dao=href)

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
    """Parcourt les IR bn-r de data/ead/bnr (hors IR de test), en remplaçant
    FR595129901_MED_15.xml par sa version transformée FR595126101_MED_FLRS.xml
    (audio de conservation), et écrit results/ead/ead_cor/dao_ref_link_brut.csv
    (Stage A)."""
    champs = ["source", "ir", "finding_aid", "id_composant", "unitid", "profondeur",
              "role", "href", "href_base", "position", "taille_plage",
              "dao", "daoloc_first", "daoloc_last", "nom_fichier", "nom_fichier_base"]
    total = []
    exclus = []
    n_ir = 0
    for path in sorted(glob(join(DOSSIER_BNR, "*.xml"))):
        # on écarte les IR de test (ex. « test 1.xml », eadid TEST_EADMOUL_01) :
        # cotes malformées, non représentatives du fonds.
        if "test" in basename(path).lower():
            exclus.append(basename(path))
            continue
        # FR595129901_MED_15.xml : remplacé par sa version transformée (cf. plus bas)
        if basename(path) == BNR_EXCLUS:
            continue
        total.extend(extrait_ir("bnr", path))
        n_ir += 1
    print(f"bnr        : {n_ir:4d} IR")

    # remplacement : la version transformée de FR595129901_MED_15.xml, tous roles,
    # finding_aid résolu vers l'IR source bnr via l'Excel de transfert Mnesys.
    conc = concordance_bnr2mnesys()
    finding_aid = conc.get(basename(REMPLACEMENT))
    if finding_aid is None:
        print(f"ATTENTION : {basename(REMPLACEMENT)} absent de l'Excel de transfert, "
              f"finding_aid par défaut")
    lignes_repl = extrait_ir("bnr2mnesys", REMPLACEMENT, finding_aid=finding_aid)
    total.extend(lignes_repl)
    print(f"remplacement {basename(REMPLACEMENT)} (finding_aid={finding_aid}) : "
          f"{len(lignes_repl)} liens dao")

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
