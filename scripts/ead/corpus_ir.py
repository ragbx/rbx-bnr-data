#!/usr/bin/env python3
r"""
corpus_ir.py — Association corpus_code -> instrument(s) de recherche (EAD).

Il n'existe aujourd'hui aucune configuration explicite de ce lien dans le
dépôt : il est calculé à la volée, en filtrant results/ead/ead_cor/
dao_ref_link_brut.csv (colonne `nom_fichier_base`) sur les chaînes de
caractères contenant `corpus_code` — exactement la méthode déjà utilisée par
scripts/corpus/stagemel_extraction_corpus.py pour extraire les DAO d'un
corpus. Ce script matérialise ce lien en CSV, pour ne plus avoir à le
recalculer / le chercher à chaque fois.

`dao_ref_link_brut.csv` est lui-même produit depuis data/ead/bnr par
scripts/ead/dao_ref_link.py (colonne `ir` = nom de fichier EAD dans ce
dossier) : relancer ce script après tout ajout/modif de notice EAD, puis
celui-ci.

Un corpus peut n'avoir AUCUN IR (corpus sans notice EAD — DAO vide, une seule
ligne avec ir/chemin/n_dao vides), UN SEUL, ou PLUSIEURS (une ligne par IR
distinct rattaché).

Entrée : dernier results/ref/_ref_files_*.csv.gz (liste des corpus_code) +
         results/ead/ead_cor/dao_ref_link_brut.csv
Sortie : results/ead/corpus_ir.csv (corpus_code, ir, chemin, n_dao)

Usage : conda run -n rbx-bnr-data python scripts/ead/corpus_ir.py
"""
import re
from glob import glob
from os.path import basename, join

import pandas as pd

EAD_DIR = "data/ead/bnr"
DAO = join("results", "ead", "ead_cor", "dao_ref_link_brut.csv")
OUT = join("results", "ead", "corpus_ir.csv")


def dernier_ref():
    refs = [
        p for p in glob(join("results", "ref", "_ref_files_*.csv.gz"))
        if re.fullmatch(r"_ref_files_\d{8}\.csv\.gz", basename(p))
    ]
    return max(refs)


ref_path = dernier_ref()
print("référentiel :", ref_path)
ref = pd.read_csv(ref_path, usecols=["corpus_code"], low_memory=False)
corpus_codes = sorted(ref["corpus_code"].dropna().unique())
print("corpus_code distincts :", len(corpus_codes))

dao = pd.read_csv(DAO, usecols=["ir", "nom_fichier_base"], low_memory=False)
dao = dao[dao["nom_fichier_base"].notna()]

rows = []
for code in corpus_codes:
    sub = dao[dao["nom_fichier_base"].str.contains(code, regex=False)]
    if sub.empty:
        rows.append({"corpus_code": code, "ir": None, "chemin": None, "n_dao": 0})
        continue
    for ir, n in sub["ir"].value_counts().items():
        rows.append({"corpus_code": code, "ir": ir, "chemin": join(EAD_DIR, ir), "n_dao": int(n)})

out = pd.DataFrame(rows)
out.to_csv(OUT, index=False)
print("écrit :", OUT, out.shape)
print("corpus sans aucun IR :", (out.groupby("corpus_code")["ir"].apply(lambda s: s.isna().all())).sum())
print("corpus avec plusieurs IR :", (out.groupby("corpus_code")["ir"].nunique() > 1).sum())
