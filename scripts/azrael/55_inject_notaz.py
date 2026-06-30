"""Étape 5.5 — réinjection de la partie non-az du référentiel.

La chaîne de matching (10→50) ne traite que les fichiers azrael (`source2s3 == 'az'`).
La partie non-az du ref précédent, mise de côté par l'étape 1 (`ref_notaz`), doit être
réintégrée AVANT le maillon d'enrichissement s3/dao/oai pour reconstituer l'ensemble
complet (az + non-az).

Ce script est volontairement indépendant : simple concaténation, aucune logique de
matching.

Entrées (results/ref/tmp/) :
    - _az_ok_all  : tous les fichiers az résolus (sortie de l'étape 50)
    - ref_notaz   : partie non-az du ref précédent (sortie de l'étape 10)

Sortie (results/ref/tmp/) :
    - _az_notaz_ok_all : az + non-az  -> entrée du maillon s3/dao/oai

    len(_az_notaz_ok_all) == len(_az_ok_all) + len(ref_notaz)

NB : concaténation en union de colonnes. Les fichiers az n'ont pas encore les colonnes
d'enrichissement (extension, file_type, source2s3, mix_*, s3_*, ...) : elles seront
remplies par le maillon s3/dao/oai. Le non-az conserve ses valeurs existantes.
"""

import pandas as pd

from _pipeline import tmp_file

az_ok = pd.read_csv(tmp_file("_az_ok_all"))
ref_notaz = pd.read_csv(tmp_file("ref_notaz"))

az_notaz = pd.concat([az_ok, ref_notaz], ignore_index=True)

print("_az_ok_all  :", len(az_ok))
print("ref_notaz   :", len(ref_notaz))
print("_az_notaz_ok_all :", len(az_notaz), "==", len(az_ok) + len(ref_notaz), "?",
      len(az_notaz) == len(az_ok) + len(ref_notaz))

az_notaz.to_csv(tmp_file("_az_notaz_ok_all"), index=False)
