#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Résout les 4 diffusion INCONNU du set 7H (2026-07-09) : les jpg+ALTO de la
page 2 des affiches 0027 et 0115 (`RBX_AMR_AFF_7H_0027_02.{jpg,xml}` et
`…_0115_02.{jpg,xml}`), laissées en `INCONNU`/publication `inconnu` alors que
leur master tif est déjà « À TRANSFERER » (cible posée) et que les pages
sœurs de l'affiche sont publiées (EN LIGNE / oui).

Décision utilisateur = ALIGNER sur les sœurs :
  conservation_statut -> « EN LIGNE - À TRANSFERER ? »
  publication_statut  -> « oui »
  s3_key_cible        -> « AMR/AMR_GUE/<name> »  (noms déjà propres, pas d'harmo)

Ciblage par uuid (via amr_gue_fin.csv) pour ne PAS toucher les jumeaux CADN
« NE PAS GARDER (DOUBLONS AZ) » de même nom.

Cibles : results/ref/amr_gue_fin.csv ET results/ref/_ref_files_20260630_dedup.csv.gz
Audit  : results/ref/_dedup_gue_7h_inconnu_audit_20260630.csv
"""
import sys
import datetime
import pandas as pd

FIN = "results/ref/amr_gue_fin.csv"
SRC = "results/ref/amr_gue.csv"
GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"
AUDIT = "results/ref/_dedup_gue_7h_inconnu_audit_20260630.csv"


def redate(df):
    """date_maj_ligne (règle v2) : conservation/publication changés vs source,
    ou s3_key_cible != s3_key source. Aligné sur uuid."""
    s = pd.read_csv(SRC, dtype=str, keep_default_na=False).set_index("uuid")
    today = datetime.date.today().isoformat()
    chg = (
        (df["conservation_statut"].values != df["uuid"].map(s["conservation_statut"]).fillna("").values)
        | (df["publication_statut"].values != df["uuid"].map(s["publication_statut"]).fillna("").values)
        | (df["s3_key_cible"].values != df["uuid"].map(s["s3_key"]).fillna("").values)
    )
    df = df.copy()
    df["date_maj_ligne"] = ""
    df.loc[chg, "date_maj_ligne"] = today
    return df

fin = pd.read_csv(FIN, dtype=str, keep_default_na=False)
sel = fin[fin["conservation_statut"] == "INCONNU"]
print("diffusion INCONNU à aligner:", len(sel))
assert len(sel) == 4, f"attendu 4, obtenu {len(sel)}"
assert set(sel["name"]) == {
    "RBX_AMR_AFF_7H_0027_02.jpg", "RBX_AMR_AFF_7H_0027_02.xml",
    "RBX_AMR_AFF_7H_0115_02.jpg", "RBX_AMR_AFF_7H_0115_02.xml",
}, "noms INCONNU inattendus"
assert (sel["file_type"].isin(["jpeg", "ocr xml"])).all(), "file_type inattendu"
assert (sel["s3_key_cible"] == "").all(), "cible déjà posée"

uuids = set(sel["uuid"])
cibles = {u: "AMR/AMR_GUE/" + n for u, n in zip(sel["uuid"], sel["name"])}

# collision : les nouvelles cibles ne doivent pas déjà exister
existing = set(fin.loc[fin["s3_key_cible"] != "", "s3_key_cible"])
assert not (set(cibles.values()) & existing), "collision cible"

audit = pd.DataFrame({
    "uuid": sel["uuid"].values, "name": sel["name"].values,
    "ancien_statut": "INCONNU", "nouveau_statut": "EN LIGNE - À TRANSFERER ?",
    "nouvelle_publication": "oui", "s3_key_cible": [cibles[u] for u in sel["uuid"]],
})
print(audit[["name", "s3_key_cible"]].to_string(index=False))


def apply_to(df):
    m = df["uuid"].isin(uuids)
    df.loc[m, "conservation_statut"] = "EN LIGNE - À TRANSFERER ?"
    df.loc[m, "publication_statut"] = "oui"
    for u, c in cibles.items():
        df.loc[df["uuid"] == u, "s3_key_cible"] = c
    return df, int(m.sum())


if "--apply" in sys.argv:
    fin2, n1 = apply_to(fin)
    redate(fin2).to_csv(FIN, index=False)
    audit.to_csv(AUDIT, index=False)
    print(f"amr_gue_fin: {n1} lignes maj -> {FIN}")
    gz = pd.read_csv(GZ, dtype=str, keep_default_na=False)
    gz2, n2 = apply_to(gz)
    assert n2 == 4, f"dédup.gz: {n2} lignes touchées (attendu 4)"
    gz2.to_csv(GZ, index=False, compression="gzip")
    gz2.to_pickle("results/ref/_ref_files_20260630_dedup.pkl")
    print(f"dédup.gz + pickle: {n2} lignes maj")
else:
    print("DRY-RUN (ajouter --apply)")
