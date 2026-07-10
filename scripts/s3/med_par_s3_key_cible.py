#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backlog transfert « EN LIGNE - À TRANSFERER ? » — corpus MED_PAR (2026-07-09).

Contexte : 3 267 lignes MED_PAR « À TRANSFERER » (jpg/JPG/jpeg/png, pas tif),
toutes s3_uploaded=False, publication=oui, s3_key vide, 1 IR
bnr_FR595129901_MED_08.xml. Sous-corpus PRE (2 939), CAT (226), MOR (96),
+ 6 typos (voir plus bas).

⚠️ MED_PAR a DEUX régimes de placement sur S3 (18 223 déjà versés) :
  - `PAR/PAR_<SUB>/MED_PAR_<SUB>_...` (noms SANS RBX_) pour BOU/TUR/TER/CAR/…
  - `MED/MED_PAR/RBX_MED_PAR_...`     (noms AVEC RBX_) pour PRE (475) et CAT (79)
Notre backlog est 100 % en noms `RBX_MED_PAR_…` -> régime 2 = **règle plate
`MED/MED_PAR/<name>`** (MOR n'a pas d'ancre S3 mais suit le même régime RBX_).

Dédup (décidé 2026-07-09) :
  - **48 copies « Patch correctif mars 2025 »** = byte-identiques (même
    checksum_md5) à leur original PAR_PRE (48/48, 0 corrigé) -> rétrogradées
    conservation « À SUPPRIMER » + publication « jamais », PAS de s3_key_cible.
    L'original reste master. (Logique vrac MED_PUB.)
  - 14 checksums en double interne (28 lignes) = même image sous 2 cotes/osiros
    distincts (bénin, type AMR_POP) -> on VERSE les deux (noms distincts).
  - 0 contenu backlog déjà présent sur S3.

Typos (décidé 2026-07-09) : 6 fichiers `RBX_MEd_PAR_PRE_NUM_ROU_D45_00x`
(d minuscule) -> s3_key_cible avec le nom CORRIGÉ `RBX_MED_PAR_…`. ⚠️ le
fichier physique devra être renommé au versement (sinon s3_join_ref ne le
rappariera pas). La colonne `name` (= nom physique) est laissée telle quelle.

Source .gz lue et réécrite en place : results/ref/_ref_files_20260630_dedup.csv.gz
Audit : results/ref/_dedup_med_par_audit_20260630.csv
"""
import sys
import pandas as pd

GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"
AUDIT = "results/ref/_dedup_med_par_audit_20260630.csv"
PREFIX = "MED/MED_PAR/"

df = pd.read_csv(GZ, dtype=str, keep_default_na=False)
print("shape lue:", df.shape)

mask = (df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?") & (df["corpus_code"] == "MED_PAR")
sel = df[mask].copy()
print("MED_PAR À TRANSFERER:", len(sel))

# garde-fous structure
assert len(sel) == 3267, f"attendu 3267, obtenu {len(sel)}"
assert (sel["s3_key"] == "").all(), "s3_key non vide inattendue"
assert (sel["s3_key_cible"] == "").all(), "s3_key_cible déjà renseignée"
assert (sel["publication_statut"] == "oui").all(), "publication inattendue"

# --- partition patch vs master ---
is_patch = sel["path"].str.contains("Patch correctif", regex=False)
patch = sel[is_patch]
master = sel[~is_patch]
print("patch (copies à supprimer):", len(patch))
print("master (à verser):", len(master))
assert len(patch) == 48, f"attendu 48 patch, obtenu {len(patch)}"

# garde-fou : chaque patch a exactement un jumeau master, même name ET même checksum
tw = master.merge(patch[["name", "checksum_md5"]], on=["name", "checksum_md5"], how="inner")
assert len(tw) == len(patch), "un patch sans jumeau master identique (name+checksum) -> ne pas supprimer"
assert master["name"].nunique() == len(master), "noms master non uniques après retrait patch"

# --- cibles master : nom corrigé (typo MEd -> MED) pour la clé seulement ---
cible_name = master["name"].str.replace("RBX_MEd_PAR_", "RBX_MED_PAR_", regex=False)
n_typo = (master["name"] != cible_name).sum()
print("typos corrigés dans la clé:", n_typo)
assert n_typo == 6, f"attendu 6 typos, obtenu {n_typo}"
cibles = PREFIX + cible_name

# collisions
real = set(df.loc[df["s3_key"] != "", "s3_key"])
other_cible = set(df.loc[(df["s3_key_cible"] != "") & ~mask, "s3_key_cible"])
c = set(cibles)
assert len(c) == len(cibles), "collision interne entre cibles"
inter_r, inter_o = c & real, c & other_cible
print("collision vs s3_key réelles:", len(inter_r))
print("collision vs cibles autres corpus:", len(inter_o))
assert not inter_r and not inter_o, "COLLISION -> abandon"

# --- application ---
df.loc[master.index, "s3_key_cible"] = cibles.values
df.loc[patch.index, "conservation_statut"] = "À SUPPRIMER"
df.loc[patch.index, "publication_statut"] = "jamais"

# --- audit ---
rows = []
for idx, r in master.iterrows():
    nm = r["name"]; key = PREFIX + nm.replace("RBX_MEd_PAR_", "RBX_MED_PAR_")
    role = "master (typo clé corrigée)" if nm != nm.replace("RBX_MEd_PAR_", "RBX_MED_PAR_") else "master"
    rows.append({"name": nm, "path": r["path"], "role": role,
                 "new_conservation": r["conservation_statut"], "new_publication": r["publication_statut"],
                 "s3_key_cible": key})
for idx, r in patch.iterrows():
    rows.append({"name": r["name"], "path": r["path"], "role": "doublon patch (byte-identique original)",
                 "new_conservation": "À SUPPRIMER", "new_publication": "jamais", "s3_key_cible": ""})
audit = pd.DataFrame(rows)
audit.to_csv(AUDIT, index=False)
print("audit écrit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    df.to_csv(GZ, index=False, compression="gzip")
    print("GZ réécrite:", GZ, df.shape)
else:
    print("DRY-RUN (ajouter --apply pour réécrire la .gz)")
