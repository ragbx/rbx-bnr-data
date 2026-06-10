"""Génération d'id uniques de type Mnesys pour les éléments <c> des EAD.

Méthode de création
-------------------
Le format a été établi par rétro-ingénierie des 76 742 id commençant par "a0"
extraits des instruments de recherche de data/ead/mnesys (cf.
scripts/ead/c_id_mnesys.py, vérification du 2026-06-10 : 100 % conformes,
100 % uniques). Un id se compose de trois parties, sur 19 caractères :

    a0  1461682068  RUeGyW
    │   │           └─ 6 caractères aléatoires pris dans ALPHABET
    │   └─ timestamp Unix en secondes (10 chiffres, observés de 2015 à 2026)
    └─ préfixe constant

ALPHABET reprend l'alphabet observé dans les id existants : alphanumérique
sans "K", "N" ni "k" (absents des 76 742 suffixes, exclusion vraisemblablement
volontaire du générateur Mnesys), soit 60 caractères.

L'unicité est garantie à trois niveaux :
- le timestamp isole chaque seconde de génération ;
- le suffixe aléatoire (60^6 ≈ 4,7 × 10^10 combinaisons) rend les collisions
  au sein d'une même seconde hautement improbables ;
- le contrôle contre l'ensemble `deja_pris` les exclut absolument.

Usage
-----
    from mnesys_id import ids_existants, nouvel_id

    deja_pris = ids_existants()      # id des EAD de data/ead/mnesys
    i = nouvel_id(deja_pris)         # ex. "a01781183706PyJAGP"
"""

import random
import re
import time
from glob import glob
from os.path import join

ALPHABET = "0123456789ABCDEFGHIJLMOPQRSTUVWXYZabcdefghijlmnopqrstuvwxyz"

MOTIF = re.compile(r"a0(\d{10})[" + ALPHABET + "]{6}")


def nouvel_id(deja_pris):
    """Nouvel id de type Mnesys, absent de l'ensemble deja_pris.

    deja_pris est mis à jour : l'id retourné y est ajouté, l'ensemble peut
    donc être réutilisé d'appel en appel pour générer une série d'id.
    """
    while True:
        i = f"a0{int(time.time())}{''.join(random.choices(ALPHABET, k=6))}"
        if i not in deja_pris:
            deja_pris.add(i)
            return i


def ids_existants(dossier=join("data", "ead", "mnesys")):
    """Ensemble des attributs id des éléments <c> des EAD d'un dossier.

    Lecture directe des fichiers (pas de l'extraction datée de results/ead),
    pour refléter l'état courant des instruments de recherche. Le parse est
    tolérant : certains fichiers contiennent des & non échappés.
    """
    from lxml import etree

    ids = set()
    for f in sorted(glob(join(dossier, "*.xml"))):
        arbre = etree.parse(f, etree.XMLParser(recover=True))
        ids.update(c.get("id") for c in arbre.iter("{*}c") if c.get("id"))
    return ids


if __name__ == "__main__":
    deja_pris = ids_existants()
    print(f"{len(deja_pris)} id existants chargés")
    for _ in range(5):
        print(nouvel_id(deja_pris))
