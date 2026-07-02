"""Développement des plages DAO first/last : logique partagée.

Une plage n'est décrite que par ses deux bornes (href first et href last) ; les
fichiers intermédiaires sont implicites. Les bornes ne doivent différer que par
un unique segment numérique, sur lequel porte l'énumération ; le dossier,
l'extension et le reste du nom sont conservés tels quels. Exemple :
  first = CSV/RBX_CSV_PAL_1858_01.jpg
  last  = CSV/RBX_CSV_PAL_1858_08.jpg
développe les huit fichiers _01 à _08.

Module partagé par dao_first_last_developpe.py (chaîne de diagnostic des IR
transformés, cf. dao_appariement.md) et dao_ref_link.py (maillon DAO de
l'enrichissement du ref, cf. enrichissement_ref.md) : même logique, un seul code.
"""

import re
from os.path import basename, dirname, join, splitext

MOTIF_TOKENS = re.compile(r"\d+|\D+")


def segment_variable(stem_first, stem_last):
    """Tokens des deux noms de base et index de l'unique segment numérique qui
    diffère, sous la forme (tokens_first, tokens_last, index). Renvoie None si
    l'énumération est ambiguë : nombre de tokens différent, ou autre chose qu'un
    unique segment numérique qui change."""
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
