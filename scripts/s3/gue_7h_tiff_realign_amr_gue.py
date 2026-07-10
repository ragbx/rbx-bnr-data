#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Réaligne le dédup.gz sur la DÉCISION 2026-07-09 : les 341 TIFF masters 7H
(corpus_code AMR_AFF, IR AMR_007) basculent leur s3_key_cible de
`AMR/AMR_AFF/<name>` vers `AMR/AMR_GUE/<name>` pour unifier tout le set 7H
dans un seul dossier. Cible identifiée par le préfixe de nom RBX_AMR_AFF_7H_
+ s3_key_cible actuelle AMR/AMR_AFF/. On ne touche que ces lignes.

Source .gz lue et réécrite en place : results/ref/_ref_files_20260630_dedup.csv.gz
"""
import sys
import pandas as pd

GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"

df = pd.read_csv(GZ, dtype=str, keep_default_na=False)
print("shape lue:", df.shape)

mask = (df["name"].str.startswith("RBX_AMR_AFF_7H_")
        & df["s3_key_cible"].str.startswith("AMR/AMR_AFF/"))
print("341 TIFF 7H à basculer AMR_AFF->AMR_GUE:", int(mask.sum()))
assert int(mask.sum()) == 341, f"attendu 341, obtenu {int(mask.sum())}"

new = "AMR/AMR_GUE/" + df.loc[mask, "name"]
# pas de collision avec l'existant (les 678 dérivés ont jpg/xml, ceux-ci tif)
assert not (set(new) & set(df.loc[~mask & (df["s3_key_cible"] != ""), "s3_key_cible"])), "collision"
df.loc[mask, "s3_key_cible"] = new.values

print("AMR_GUE prefixe cibles après:", int((df["s3_key_cible"].str.startswith("AMR/AMR_GUE/")).sum()))
print("AMR_AFF prefixe cibles après:", int((df["s3_key_cible"].str.startswith("AMR/AMR_AFF/")).sum()))

if "--apply" in sys.argv:
    df.to_csv(GZ, index=False, compression="gzip")
    df.to_pickle("results/ref/_ref_files_20260630_dedup.pkl")
    print("GZ + pickle réécrits:", df.shape)
else:
    print("DRY-RUN (ajouter --apply)")
