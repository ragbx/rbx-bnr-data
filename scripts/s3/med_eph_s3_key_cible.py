#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backlog transfert « EN LIGNE - À TRANSFERER ? » — corpus MED_EPH (2026-07-08).

Contexte : 840 lignes MED_EPH, toutes s3_uploaded=False, publication=oui,
s3_key vide, un seul IR bnr_FR595129901_MED_20.xml. Contrairement à MED_PUB,
AUCUN doublon (840 names uniques, 840 checksums uniques) et pas de dossier
« vrac » : les 840 sont toutes des masters uniques -> aucune rétrogradation.

On calcule uniquement `s3_key_cible = MED/MED_EPH/<name>` (règle plate MED
`MED/<token>/<name>`, auto-validée à 100 % sur les corpus MED réellement
versés : MED_MS/MON/IMA/CP/AFF/JOU/PHO/LET/CHA). On ne touche NI `s3_key`
(invariant = fichier réellement versé), NI conservation/publication.

Source .gz lue et réécrite en place : results/ref/_ref_files_20260630_dedup.csv.gz
Audit : results/ref/_dedup_med_eph_audit_20260630.csv
"""
import sys
import pandas as pd

GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"
AUDIT = "results/ref/_dedup_med_eph_audit_20260630.csv"
PREFIX = "MED/MED_EPH/"

df = pd.read_csv(GZ, dtype=str, keep_default_na=False)
print("shape lue:", df.shape)

mask = (df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?") & (df["corpus_code"] == "MED_EPH")
sel = df[mask]
print("MED_EPH À TRANSFERER:", len(sel))

# garde-fous structure
assert len(sel) == 840, f"attendu 840, obtenu {len(sel)}"
assert (sel["name"].str.match(r"^RBX_MED_EPH_.*\.tiff$")).all(), "name inattendu"
assert sel["name"].nunique() == len(sel), "names non uniques"
assert sel["checksum_md5"].nunique() == len(sel), "checksums non uniques"
assert (sel["s3_key"] == "").all(), "s3_key non vide inattendue"
assert (sel["s3_key_cible"] == "").all(), "s3_key_cible déjà renseignée"

cibles = PREFIX + sel["name"]

# collisions : ni avec s3_key réelles, ni avec s3_key_cible existantes, ni entre elles
existing_real = set(df.loc[df["s3_key"] != "", "s3_key"])
existing_cible = set(df.loc[df["s3_key_cible"] != "", "s3_key_cible"])
c = set(cibles)
assert len(c) == len(cibles), "collision interne entre cibles"
inter_r = c & existing_real
inter_c = c & existing_cible
print("collisions vs s3_key réelles:", len(inter_r))
print("collisions vs s3_key_cible existantes:", len(inter_c))
assert not inter_r and not inter_c, "COLLISION -> abandon"

# application
df.loc[mask, "s3_key_cible"] = cibles.values

# audit
audit = pd.DataFrame({
    "name": sel["name"].values,
    "path": sel["path"].values,
    "conservation_statut": sel["conservation_statut"].values,
    "publication_statut": sel["publication_statut"].values,
    "s3_key_cible": cibles.values,
})
audit.to_csv(AUDIT, index=False)
print("audit écrit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    df.to_csv(GZ, index=False, compression="gzip")
    print("GZ réécrite:", GZ, df.shape)
else:
    print("DRY-RUN (ajouter --apply pour réécrire la .gz)")
