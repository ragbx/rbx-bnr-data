#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backlog transfert « EN LIGNE - À TRANSFERER ? » — corpus LAR_PUB (2026-07-09).
Traité à part du lot `reste_s3_key_cible.py` à cause d'un conflit de clé.

81 lignes « À TRANSFERER » = 80 `RBX_LAR_PUB_CDR_013_*` + 1 `RBX_LAR_PUB_VPR_*`.
Les 80 CDR_013 (path `BNR_VERIF/LAR`, ~2,2 Mo/page) entrent en collision de
clé `LAR/LAR_PUB/<name>` avec une version CDR_013 DÉJÀ versée (213 pages, path
`BNR_VERIF/LAR/CDR_13_le_bon`, ~22,5 Mo/page = haute résolution), contenu
DIFFÉRENT (0/80 même checksum). Le dossier « le_bon » et la résolution 10×
supérieure désignent la version versée comme master.

Décision utilisateur 2026-07-09 :
  - 80 CDR_013 backlog = version basse-rés obsolète -> `À SUPPRIMER` + `jamais`,
    pas de s3_key_cible (ne pas écraser « le_bon »).
  - 1 VPR (pas de collision) -> `s3_key_cible = LAR/LAR_PUB/<name>`
    (ancre confirmée : 3 730 LAR_PUB versés à `LAR/LAR_PUB/`).

Source .gz lue et réécrite en place : results/ref/_ref_files_20260630_dedup.csv.gz
Audit : results/ref/_dedup_lar_pub_audit_20260630.csv
"""
import sys
import pandas as pd

GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"
AUDIT = "results/ref/_dedup_lar_pub_audit_20260630.csv"
PREFIX = "LAR/LAR_PUB/"

df = pd.read_csv(GZ, dtype=str, keep_default_na=False)
print("shape lue:", df.shape)

mask = (df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?") & (df["corpus_code"] == "LAR_PUB")
sel = df[mask]
assert len(sel) == 81, f"attendu 81, obtenu {len(sel)}"
assert (sel["s3_key"] == "").all() and (sel["s3_key_cible"] == "").all(), "s3_key/cible inattendu"

is_cdr = sel["name"].str.startswith("RBX_LAR_PUB_CDR_013_")
cdr = sel[is_cdr]      # à supprimer
keep = sel[~is_cdr]    # à verser
print("CDR_013 à rétrograder:", len(cdr), "| autres à verser:", len(keep))
assert len(cdr) == 80 and len(keep) == 1, "partition CDR/VPR inattendue"

# garde-fou : chaque CDR backlog a bien un jumeau versé de même clé (contenu ≠)
real = set(df.loc[df["s3_key"] != "", "s3_key"])
cdr_keys = set(PREFIX + cdr["name"])
assert cdr_keys <= real, "un CDR backlog SANS version versée de même clé -> ne pas supprimer à l'aveugle"

# cible pour keep (VPR)
keep_keys = PREFIX + keep["name"]
c = set(keep_keys)
assert not (c & real), "collision VPR vs s3_key réelles"
assert not (c & set(df.loc[df["s3_key_cible"] != "", "s3_key_cible"])), "collision VPR vs cibles"

# application
df.loc[keep.index, "s3_key_cible"] = keep_keys.values
df.loc[keep.index, "conservation_statut"] = "À TRANSFERER"   # VPR versé : plus EN LIGNE
df.loc[cdr.index, "conservation_statut"] = "À SUPPRIMER (DOUBLON)"
df.loc[cdr.index, "publication_statut"] = "jamais"

# audit
rows = []
for _, r in keep.iterrows():
    rows.append({"name": r["name"], "path": r["path"], "role": "master",
                 "new_conservation": "À TRANSFERER", "new_publication": r["publication_statut"],
                 "s3_key_cible": PREFIX + r["name"]})
for _, r in cdr.iterrows():
    rows.append({"name": r["name"], "path": r["path"], "role": "doublon basse-rés (le_bon versé)",
                 "new_conservation": "À SUPPRIMER (DOUBLON)", "new_publication": "jamais", "s3_key_cible": ""})
audit = pd.DataFrame(rows)
audit.to_csv(AUDIT, index=False)
print("audit écrit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    df.to_csv(GZ, index=False, compression="gzip")
    print("GZ réécrite:", GZ, df.shape)
else:
    print("DRY-RUN (ajouter --apply pour réécrire la .gz)")
