#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backlog transfert « EN LIGNE - À TRANSFERER ? » — corpus OBS_JOU (2026-07-09).

Contexte : 553 lignes OBS_JOU, toutes s3_uploaded=False, publication=oui,
s3_key vide, un seul IR bnr_FR595129901_OBS_01.xml, toutes des TIFF de la
série VPC. Corpus déjà partiellement versé (1 240 lignes TRANSFERT_S3_OK),
règle S3 réelle PLATE confirmée par le listing : `OBS/OBS_JOU/<name>`.

Contrairement à MED_EPH, la série VPC est mal rangée. Partition disjointe des
553 (A+B+R = 553, vérifiée) :

  A = 36  doublons de CONTENU : byte-identiques (même checksum_md5) à un
          OBS_JOU déjà versé en .tif sous un nom à padding 4 chiffres
          (`VPC_0040_001.tif`, s3_key = OBS/OBS_JOU/...). Le backlog est une
          copie 3 chiffres (`VPC_040_001.tif`) redondante — le contenu est
          DÉJÀ sur S3. -> rétrogradés conservation « À SUPPRIMER » +
          publication « jamais », PAS de s3_key_cible (décision utilisateur
          2026-07-09, calquée sur la logique vrac MED_PUB et le dedup 34).

  B = 134 TIFF masters dont le JPG dérivé est DÉJÀ en ligne (même basename,
          .jpg versé). On verse le master TIFF à côté (extensions ≠ -> pas de
          collision), décision type AMR_GUE « verser les deux ».

  R = 383 masters uniques, aucun jumeau S3 (contenu ni basename).

B ∪ R (517) reçoivent `s3_key_cible = OBS/OBS_JOU/<name>` (règle plate OBS
auto-validée à 100 % sur les 1 240 OBS_JOU réellement versés). On ne touche NI
`s3_key` (invariant = fichier réellement versé), et pour B/R ni conservation
ni publication.

Source .gz lue et réécrite en place : results/ref/_ref_files_20260630_dedup.csv.gz
Audit : results/ref/_dedup_obs_jou_audit_20260630.csv
"""
import sys
import pandas as pd

GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"
AUDIT = "results/ref/_dedup_obs_jou_audit_20260630.csv"
PREFIX = "OBS/OBS_JOU/"

df = pd.read_csv(GZ, dtype=str, keep_default_na=False)
print("shape lue:", df.shape)

mask = (df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?") & (df["corpus_code"] == "OBS_JOU")
sel = df[mask].copy()
print("OBS_JOU À TRANSFERER:", len(sel))

# garde-fous structure
assert len(sel) == 553, f"attendu 553, obtenu {len(sel)}"
assert (sel["name"].str.match(r"^RBX_OBS_JOU_VPC_.*\.tif$")).all(), "name inattendu"
assert sel["name"].nunique() == len(sel), "names non uniques"
assert sel["checksum_md5"].nunique() == len(sel), "checksums non uniques"
assert (sel["s3_key"] == "").all(), "s3_key non vide inattendue"
assert (sel["s3_key_cible"] == "").all(), "s3_key_cible déjà renseignée"

# référentiel des OBS_JOU DÉJÀ versés (ancre)
versed = df[(df["corpus_code"] == "OBS_JOU") & (df["s3_key"] != "")]
versed_ck = set(versed["checksum_md5"])
versed_base = set(versed["name"].str.rsplit(".", n=1).str[0])
sel_base = sel["name"].str.rsplit(".", n=1).str[0]

# partition
is_A = sel["checksum_md5"].isin(versed_ck)                       # doublon de contenu
is_B = (~is_A) & sel_base.isin(versed_base)                      # jpg dérivé en ligne
is_R = (~is_A) & (~is_B)                                         # master unique
A, B, R = sel[is_A], sel[is_B], sel[is_R]
print(f"A (doublon contenu déjà versé): {len(A)}")
print(f"B (jpg dérivé en ligne):        {len(B)}")
print(f"R (master unique):              {len(R)}")
assert (len(A), len(B), len(R)) == (36, 134, 383), "partition inattendue"
assert len(A) + len(B) + len(R) == len(sel), "partition non couvrante"

# garde-fou A : chaque doublon a bien un jumeau versé (s3_key non vide) même checksum
twin_ck = set(versed.loc[versed["s3_key"] != "", "checksum_md5"])
assert A["checksum_md5"].isin(twin_ck).all(), "un A sans jumeau versé -> ne pas supprimer"

# --- cibles B ∪ R ---
verser = sel[is_B | is_R]
cibles = PREFIX + verser["name"]
existing_real = set(df.loc[df["s3_key"] != "", "s3_key"])
existing_cible = set(df.loc[df["s3_key_cible"] != "", "s3_key_cible"])
c = set(cibles)
assert len(c) == len(cibles), "collision interne entre cibles"
inter_r, inter_c = c & existing_real, c & existing_cible
print("collisions vs s3_key réelles:", len(inter_r))
print("collisions vs s3_key_cible existantes:", len(inter_c))
assert not inter_r and not inter_c, "COLLISION -> abandon"

# --- application ---
df.loc[verser.index, "s3_key_cible"] = cibles.values
df.loc[A.index, "conservation_statut"] = "À SUPPRIMER"
df.loc[A.index, "publication_statut"] = "jamais"

# --- audit ---
rows = []
for idx, r in verser.iterrows():
    rows.append({"name": r["name"], "path": r["path"], "role": "master" if is_R[idx] else "master(jpg en ligne)",
                 "new_conservation": r["conservation_statut"], "new_publication": r["publication_statut"],
                 "s3_key_cible": PREFIX + r["name"]})
for idx, r in A.iterrows():
    rows.append({"name": r["name"], "path": r["path"], "role": "doublon (contenu déjà sur S3)",
                 "new_conservation": "À SUPPRIMER", "new_publication": "jamais", "s3_key_cible": ""})
audit = pd.DataFrame(rows)
audit.to_csv(AUDIT, index=False)
print("audit écrit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    df.to_csv(GZ, index=False, compression="gzip")
    print("GZ réécrite:", GZ, df.shape)
else:
    print("DRY-RUN (ajouter --apply pour réécrire la .gz)")
