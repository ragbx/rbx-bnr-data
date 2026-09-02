#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Reclassifie les jpeg de diffusion `EN LIGNE - À TRANSFERER ?` qui ont déjà un
tif de même nom `TRANSFERT_S3_OK` : leur maître est déjà en sécurité sur S3,
transférer le jpeg séparément n'a pas de sens (règle utilisateur, 2026-08-22 —
cf. le diagnostic mené sur `results/corpus/stagemel/recap/med_ms_recap_draft_*.xlsx`,
qui a mis au jour le trou : `s3_stem_profil` (ref_stem_profil_20260711.py) ne
couvre que les lignes déjà keyées ou les À SUPPRIMER rattachées, jamais les
EN LIGNE - À TRANSFERER ?).

Appariement : nom de fichier sans extension, en MAJUSCULES, TOUS CORPUS
CONFONDUS (volontairement — voir remarque ci-dessous sur les corpus_code
erronés). Portée : extension .jpg uniquement (la règle donnée porte sur les
jpeg de diffusion ; les .xml appariés de la même façon, 329 lignes, ne sont
pas concernés par cette règle et restent inchangés).

Sur les 577 jpeg EN LIGNE - À TRANSFERER ? ayant un tif TRANSFERT_S3_OK de
même nom, 329 (tout AMR_GUE) ont DÉJÀ une s3_key réelle posée par une
campagne dédiée antérieure (gue_7h_s3_key_cible.py et suite, cf.
s3_key_cible.md) — signe d'une décision déjà prise de les transférer malgré
le tif existant, qu'il ne faut pas écraser sans investigation propre à
AMR_GUE. Seules les 248 lignes SANS s3_key (s3_key == "", donc aucune
destination jamais décidée) sont reclassifiées ici : exclusivement MED_MS.

Nouveau statut : `À SUPPRIMER (DIFFUSION - MASTER DÉJÀ SUR S3)`, DISTINCT de
`À SUPPRIMER (DIFFUSION)` existant : ce dernier est utilisé à 99,97% pour des
fichiers jamais publiés (`publication_statut = jamais`, 131793/131832) alors
que les 248 lignes traitées ici sont TOUTES publiées (`publication_statut =
oui`) — les fondre dans le même statut aurait effacé cette différence.
`publication_statut` n'est PAS modifié : la question de la republication
depuis le nouveau master est distincte de celle du transfert du jpeg lui-même.

Entrée/sortie : results/ref/_ref_files_20260630.csv.gz (en place).
Backup en _old<n> (premier libre) avant écriture (--apply, dry-run par défaut).
"""
import os
import shutil
import sys

import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
NOUVEAU_STATUT = "À SUPPRIMER (DIFFUSION - MASTER DÉJÀ SUR S3)"

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)

stem = ref["name"].str.rsplit(".", n=1).str[0].str.upper()

tif_ok = (ref["extension"].str.lower().isin([".tif", ".tiff"])) & (ref["conservation_statut"] == "TRANSFERT_S3_OK")
tif_stems = set(stem[tif_ok])
print("tifs TRANSFERT_S3_OK (tous corpus):", len(tif_stems))

cible = (
    (ref["extension"] == ".jpg")
    & (ref["conservation_statut"] == "EN LIGNE - À TRANSFERER ?")
    & stem.isin(tif_stems)
    & (ref["s3_key"] == "")  # exclut AMR_GUE : s3_key déjà posée par une campagne dédiée, décision à ne pas écraser ici
)
n = int(cible.sum())
print("jpeg reclassifiés:", n)
assert n == 248, f"effectif inattendu : {n} (248 attendu au diagnostic du 2026-08-22, MED_MS uniquement)"
assert (ref.loc[cible, "corpus_code"] == "MED_MS").all(), "corpus_code inattendu parmi les lignes ciblées"
assert (ref.loc[cible, "publication_statut"] == "oui").all(), "publication_statut attendu 'oui' pour toutes les lignes ciblées"

print("\nrépartition par corpus_code :")
print(ref.loc[cible, "corpus_code"].value_counts().to_string())

ref.loc[cible, "conservation_statut"] = NOUVEAU_STATUT

if "--apply" in sys.argv:
    n_bak = 4
    while os.path.exists(f"results/ref/_ref_files_20260630_old{n_bak}.csv.gz"):
        n_bak += 1
    bak = f"results/ref/_ref_files_20260630_old{n_bak}.csv.gz"
    shutil.copy2(REF, bak)
    print("backup:", bak)
    ref.to_csv(REF, index=False, compression="gzip")
    print("écrit:", REF, ref.shape)
    print(
        "\nrappel : relancer ref_stem_profil_20260711.py --apply pour que "
        "s3_stem_profil intègre ces nouvelles À SUPPRIMER (rattachement par "
        "(corpus_code, basename) — voir sa docstring)."
    )
else:
    print("\nDRY-RUN (--apply pour écrire)")
