#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backlog transfert — affiches de guerre 7H (2026-07-08).

Contexte : les 341 affiches 7H (campagne CADN OCR, IR bnr_FR595129901_AMR_007)
existent en 5 dérivés (TIFF/JPEG/ALTO/PDF/TEXT). Décision utilisateur = verser
LES DEUX niveaux (préservation + diffusion), sans rétrogradation :
  * 341 TIFF  (corpus AMR_AFF, conservation « À TRANSFERER », publication inconnu)
      -> s3_key_cible = AMR/AMR_AFF/<name>   (convention .tif AMR_AFF réelle)
  * 678 GUE   (corpus AMR_GUE, 339 jpg + 339 ALTO xml, « EN LIGNE - À TRANSFERER ? »,
      publication oui, binaire == dérivés JPEG/ALTO CADN)
      -> s3_key_cible = AMR/AMR_GUE/<name>   (nouveau dossier = corpus_code)

On ne touche NI s3_key, NI conservation/publication. Les dérivés JPEG/ALTO/PDF/TEXT
côté AMR_AFF (« NE PAS GARDER ») et les 8 INCONNU AMR_7H restent inchangés.

Source .gz lue/réécrite en place : results/ref/_ref_files_20260630_dedup.csv.gz
Audit : results/ref/_dedup_gue_7h_audit_20260630.csv
"""
import sys
import pandas as pd

GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"
AUDIT = "results/ref/_dedup_gue_7h_audit_20260630.csv"

df = pd.read_csv(GZ, dtype=str, keep_default_na=False)
print("shape lue:", df.shape)

is7h = df["name"].str.startswith("RBX_AMR_AFF_7H_", na=False)
deriv = df["path"].str.extract(r"Avec OCR/([A-Z]+)/")[0]

# --- lot TIFF (master préservation) ---
m_tif = is7h & (df["corpus_code"] == "AMR_AFF") & (deriv == "TIFF") \
        & (df["conservation_statut"] == "À TRANSFERER")
tif = df[m_tif]
assert len(tif) == 341, f"TIFF attendu 341, obtenu {len(tif)}"
assert (tif["extension"] == ".tif").all()
assert tif["name"].nunique() == len(tif), "noms TIFF non uniques"
assert (tif["s3_key"] == "").all() and (tif["s3_key_cible"] == "").all()
cible_tif = "AMR/AMR_AFF/" + tif["name"]

# --- lot GUE (diffusion en ligne) ---
m_gue = (df["corpus_code"] == "AMR_GUE") & (df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?")
gue = df[m_gue]
assert len(gue) == 678, f"GUE attendu 678, obtenu {len(gue)}"
assert gue["name"].str.startswith("RBX_AMR_AFF_7H_").all()
assert gue["name"].nunique() == len(gue), "noms GUE non uniques"
assert (gue["s3_key"] == "").all() and (gue["s3_key_cible"] == "").all()
cible_gue = "AMR/AMR_GUE/" + gue["name"]

# --- collisions ---
all_new = list(cible_tif) + list(cible_gue)
assert len(set(all_new)) == len(all_new), "collision interne entre nouvelles clés"
existing_real = set(df.loc[df["s3_key"] != "", "s3_key"])
existing_cible = set(df.loc[df["s3_key_cible"] != "", "s3_key_cible"])
inter_r = set(all_new) & existing_real
inter_c = set(all_new) & existing_cible
print("collisions vs s3_key réelles:", len(inter_r), "| vs s3_key_cible existantes:", len(inter_c))
assert not inter_r and not inter_c, "COLLISION -> abandon"

# --- application ---
df.loc[m_tif, "s3_key_cible"] = cible_tif.values
df.loc[m_gue, "s3_key_cible"] = cible_gue.values
print("TIFF ciblés:", m_tif.sum(), "| GUE ciblés:", m_gue.sum())

# --- audit ---
aud = pd.concat([
    pd.DataFrame({"lot": "TIFF", "corpus_code": "AMR_AFF", "name": tif["name"].values,
                  "conservation_statut": tif["conservation_statut"].values,
                  "publication_statut": tif["publication_statut"].values,
                  "s3_key_cible": cible_tif.values}),
    pd.DataFrame({"lot": "GUE", "corpus_code": "AMR_GUE", "name": gue["name"].values,
                  "conservation_statut": gue["conservation_statut"].values,
                  "publication_statut": gue["publication_statut"].values,
                  "s3_key_cible": cible_gue.values}),
])
aud.to_csv(AUDIT, index=False)
print("audit écrit:", AUDIT, aud.shape)

if "--apply" in sys.argv:
    df.to_csv(GZ, index=False, compression="gzip")
    print("GZ réécrite:", GZ, df.shape)
else:
    print("DRY-RUN (ajouter --apply)")
