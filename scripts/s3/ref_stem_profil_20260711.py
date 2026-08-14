#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
(Re)calcule la colonne `s3_stem_profil` du ref 20260630 : pour chaque document
(stem de s3_key), les extensions présentes, triées et jointes par « + ».

v2 (2026-07-11) : les fichiers « À SUPPRIMER* » (tous sans s3_key) sont
intégrés au profil de leur document — rattachés par (corpus_code, stem de
name, insensible à la casse) au stem de clé correspondant, leur extension
marquée `(sup)` (ex : jpg(sup)+tif+xml). Les rattachements ambigus (un
basename -> plusieurs stems de clé dans le même corpus) sont exclus. Les
lignes À SUPPRIMER rattachées reçoivent aussi le profil ; les autres lignes
sans clé restent à vide.

Extensions normalisées (minuscules, tiff->tif, jpeg->jpg). Script idempotent :
recalcule tout à chaque exécution (à rejouer après tout ajout de clés).
Backup en _old<n> (premier libre) avant écriture (--apply).
"""
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)
if "s3_stem_profil" in ref.columns:
    ref = ref.drop(columns=["s3_stem_profil"])
    print("colonne existante recalculée")

def norm(e):
    return e.str.lower().replace({"tiff": "tif", "jpeg": "jpg"})

keyed = ref["s3_key"] != ""
sup = ref["conservation_statut"].str.startswith("À SUPPRIMER")
assert not (sup & keyed).any(), "À SUPPRIMER keyé inattendu"

# fichiers keyés : (stem de clé, ext)
kstem = ref.loc[keyed, "s3_key"].str.rsplit(".", n=1).str[0]
kext = norm(ref.loc[keyed, "s3_key"].str.rsplit(".", n=1).str[-1])
pairs = pd.DataFrame({"stem": kstem, "tok": kext})

# rattachement des À SUPPRIMER : (corpus, basename de clé) -> stem de clé
kb = pd.DataFrame({
    "corpus": ref.loc[keyed, "corpus_code"],
    "bstem": kstem.str.rsplit("/", n=1).str[-1].str.lower(),
    "kstem": kstem})
uni = kb.drop_duplicates().groupby(["corpus", "bstem"])["kstem"].agg(["first", "nunique"])
lut = uni.loc[uni["nunique"] == 1, "first"]
print("rattachements ambigus exclus:", int((uni["nunique"] > 1).sum()))

s = ref.loc[sup].copy()
s["bstem"] = s["name"].str.rsplit(".", n=1).str[0].str.lower()
s["kstem"] = pd.MultiIndex.from_frame(s[["corpus_code", "bstem"]]).map(lut)
matched = s["kstem"].notna()
print("À SUPPRIMER rattachés:", int(matched.sum()), "/", len(s))
spairs = pd.DataFrame({
    "stem": s.loc[matched, "kstem"],
    "tok": norm(s.loc[matched, "extension"].str.lstrip(".")) + "(sup)"})

prof = (pd.concat([pairs, spairs]).drop_duplicates()
        .sort_values("tok").groupby("stem")["tok"].agg("+".join))
print("stems:", len(prof))

ref["s3_stem_profil"] = ""
ref.loc[keyed, "s3_stem_profil"] = kstem.map(prof)
ref.loc[s.index[matched], "s3_stem_profil"] = s.loc[matched, "kstem"].map(prof)
assert (ref.loc[keyed, "s3_stem_profil"] != "").all()

print("\n== profils avec (sup) — top 15 ==")
p = ref.loc[ref["s3_stem_profil"].str.contains(r"\(sup\)"), "s3_stem_profil"]
print(p.value_counts().head(15).to_string())
print("\n== distribution globale (top 12) ==")
print(ref.loc[ref["s3_stem_profil"] != "", "s3_stem_profil"].value_counts().head(12).to_string())

if "--apply" in sys.argv:
    n = 3
    while os.path.exists(f"results/ref/_ref_files_20260630_old{n}.csv.gz"):
        n += 1
    bak = f"results/ref/_ref_files_20260630_old{n}.csv.gz"
    shutil.copy2(REF, bak)
    print("backup:", bak)
    ref.to_csv(REF, index=False, compression="gzip")
    print("écrit:", REF, ref.shape)
else:
    print("DRY-RUN (--apply pour écrire)")
