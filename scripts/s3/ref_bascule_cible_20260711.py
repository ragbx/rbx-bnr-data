#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Bascule s3_key_cible -> s3_key dans le ref 20260630 (demande utilisateur
2026-07-11, post-fusion + harmonisation).

Vérifications AVANT toute écriture :
  1. aucune ligne où s3_key non vide et s3_key_cible != s3_key
     (les seules lignes déjà keyées doivent être des reprises à l'identique) ;
  2. les clés nouvellement posées n'existent nulle part dans les s3_key
     actuelles (aucun écrasement/collision inter-lignes) ;
  3. les clés nouvellement posées sont uniques entre elles ;
  4. le nombre de doublons de s3_key n'augmente pas.
Les lignes dont s3_key change sont datées du jour (date_maj_ligne).
NB : s3_key cesse d'être « présent sur S3 » strict — les lignes
« À TRANSFERER APRES VALIDATION » portent leur clé avant upload
(s3_uploaded reste le témoin du transfert réel).
Backup en _old5 avant écriture (--apply).
"""
import datetime
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
BAK = "results/ref/_ref_files_20260630_old5.csv.gz"
TODAY = datetime.date.today().isoformat()

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)

cible = ref["s3_key_cible"] != ""
deja = cible & (ref["s3_key"] != "")
new = cible & (ref["s3_key"] == "")
print("cibles:", int(cible.sum()), "| déjà keyées:", int(deja.sum()), "| nouvelles:", int(new.sum()))

# 1. jamais d'écrasement : clé existante => cible identique
diverge = ref.loc[deja & (ref["s3_key"] != ref["s3_key_cible"])]
print("écrasements potentiels (s3_key != cible):", len(diverge))
assert len(diverge) == 0, "des cibles écraseraient des clés existantes !"

# 2-3. nouvelles clés : uniques entre elles et absentes du parc existant
newk = ref.loc[new, "s3_key_cible"]
assert newk.is_unique, "nouvelles clés non uniques entre elles"
exist = set(ref.loc[ref["s3_key"] != "", "s3_key"])
coll = set(newk) & exist
print("collisions nouvelles clés vs parc existant:", len(coll))
assert not coll, "collision avec des s3_key existantes !"

# 4. doublons s3_key avant/après
dup_avant = int(ref.loc[ref["s3_key"] != "", "s3_key"].duplicated().sum())
ref.loc[new, "s3_key"] = ref.loc[new, "s3_key_cible"]
ref.loc[new, "date_maj_ligne"] = TODAY
dup_apres = int(ref.loc[ref["s3_key"] != "", "s3_key"].duplicated().sum())
print("doublons s3_key avant:", dup_avant, "| après:", dup_apres)
assert dup_apres == dup_avant, "la bascule a créé des doublons de s3_key"
assert (ref.loc[cible, "s3_key"] == ref.loc[cible, "s3_key_cible"]).all()

print("\ns3_key non vides:", int((ref["s3_key"] != "").sum()),
      "| par statut (lignes basculées):",
      ref.loc[new, "conservation_statut"].value_counts().to_dict())

if "--apply" in sys.argv:
    assert not os.path.exists(BAK), f"{BAK} existe déjà"
    shutil.copy2(REF, BAK)
    print("backup:", BAK)
    ref.to_csv(REF, index=False, compression="gzip")
    print("écrit:", REF, ref.shape)
else:
    print("DRY-RUN (--apply pour écrire)")
