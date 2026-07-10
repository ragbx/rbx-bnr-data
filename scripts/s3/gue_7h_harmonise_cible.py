#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Harmonisation des s3_key_cible des masters tif 7H sur la convention de
DIFFUSION (jpg/ALTO), décidée 2026-07-09. On ne touche QUE `s3_key_cible`
(le nom physique `name` reste tel quel : le renommage source->clé se fait à
l'upload). Les jpg/ALTO de diffusion, liés au catalogue, restent intacts.

Règle (appliquée au stem du master tif) :
  1. `_<n> recto` / `_<n> verso`  -> `_<nn>` (2 chiffres)
  2. page-1 implicite : stem finissant par le n° d'affiche seul -> + `_01`
  3. padding : dernier `_<n>` 1 chiffre -> `_0<n>`
Validée : reproduit exactement un stem de diffusion pour les 6 affiches
0027, 0079, 0116, 0197, 0226, 0235 (11 cibles modifiées).

Cas 0078 (4 tif master `_01.._04` + `0078.tif` pour 1 seul jpg diffusion) :
question métier -> les 5 tif marqués « … VOIR MARIE » (conservation) et leur
s3_key_cible vidée. Les jpg/ALTO de diffusion de 0078 restent inchangés.

Cibles : results/ref/amr_gue_fin.csv  ET  results/ref/_ref_files_20260630_dedup.csv.gz
(pilotage par uuid). Audit : results/ref/_dedup_gue_7h_harmo_audit_20260630.csv
"""
import re
import sys
import datetime
import pandas as pd

FIN = "results/ref/amr_gue_fin.csv"
SRC = "results/ref/amr_gue.csv"
GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"
AUDIT = "results/ref/_dedup_gue_7h_harmo_audit_20260630.csv"


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


def harmonise(stem: str) -> str:
    s = re.sub(r"_(\d+)\s*recto\b", lambda m: "_%02d" % int(m.group(1)), stem)
    s = re.sub(r"_(\d+)\s*verso\b", lambda m: "_%02d" % int(m.group(1)), s)
    s = re.sub(r"\s+(recto|verso)\b", "", s)
    if re.fullmatch(r"RBX_AMR_AFF_7H_\d{4}", s):
        s += "_01"
    s = re.sub(r"_(\d)$", lambda m: "_0" + m.group(1), s)
    return s


fin = pd.read_csv(FIN, dtype=str, keep_default_na=False)
print("amr_gue_fin:", fin.shape)

is_tif = fin["file_type"] == "tiff"
is_0078 = fin["name"].str.match(r"^RBX_AMR_AFF_7H_0078(_0[1-4])?\.tif$")
# masters tif à harmoniser = recto/verso OU 0027.tif nu ; hors 0078
need_harmo = is_tif & ~is_0078 & (
    fin["name"].str.contains(r"\s+(?:recto|verso)\b", regex=True)
    | (fin["name"] == "RBX_AMR_AFF_7H_0027.tif")
)
print("cibles tif à harmoniser:", int(need_harmo.sum()))
print("tif 0078 à marquer VOIR MARIE:", int(is_0078.sum()))
assert int(need_harmo.sum()) == 11, f"attendu 11 harmo, obtenu {int(need_harmo.sum())}"
assert int(is_0078.sum()) == 5, f"attendu 5 tif 0078, obtenu {int(is_0078.sum())}"

# stems de diffusion (pour valider la règle)
diff_stems = set(fin.loc[fin["file_type"] == "jpeg", "name"].str.rsplit(".", n=1).str[0])

# --- construit le mapping uuid -> nouvelle cible / statut ---
new_cible = {}     # uuid -> nouvelle s3_key_cible
new_statut = {}    # uuid -> nouveau conservation_statut (VOIR MARIE)
audit_rows = []
for _, r in fin[need_harmo].iterrows():
    stem = r["name"].rsplit(".", 1)[0]
    h = harmonise(stem)
    assert h in diff_stems, f"harmonisé {h} ne matche aucun jpg diffusion"
    nc = "AMR/AMR_GUE/" + h + ".tif"
    new_cible[r["uuid"]] = nc
    audit_rows.append({"uuid": r["uuid"], "name": r["name"], "action": "harmonise",
                       "ancienne_cible": r["s3_key_cible"], "nouvelle_cible": nc,
                       "nouveau_statut": ""})
for _, r in fin[is_0078].iterrows():
    ns = (r["conservation_statut"] + " VOIR MARIE").strip()
    new_cible[r["uuid"]] = ""   # vider
    new_statut[r["uuid"]] = ns
    audit_rows.append({"uuid": r["uuid"], "name": r["name"], "action": "voir_marie",
                       "ancienne_cible": r["s3_key_cible"], "nouvelle_cible": "",
                       "nouveau_statut": ns})

audit = pd.DataFrame(audit_rows)
print("\n== harmonisations ==")
print(audit[audit.action == "harmonise"][["name", "nouvelle_cible"]].to_string(index=False))
print("\n== VOIR MARIE ==")
print(audit[audit.action == "voir_marie"][["name", "nouveau_statut"]].to_string(index=False))


def apply_to(df):
    for uuid, nc in new_cible.items():
        df.loc[df["uuid"] == uuid, "s3_key_cible"] = nc
    for uuid, ns in new_statut.items():
        df.loc[df["uuid"] == uuid, "conservation_statut"] = ns
    return df


# collision check sur amr_gue_fin après application
tmp = apply_to(fin.copy())
posed = tmp.loc[tmp["s3_key_cible"] != "", "s3_key_cible"]
assert posed.nunique() == len(posed), "collision de cible après harmonisation"
print("\ncibles posées après:", len(posed), "| uniques:", posed.nunique())

if "--apply" in sys.argv:
    redate(apply_to(fin)).to_csv(FIN, index=False)
    print("écrit:", FIN)
    audit.to_csv(AUDIT, index=False)
    print("audit:", AUDIT, audit.shape)
    gz = pd.read_csv(GZ, dtype=str, keep_default_na=False)
    gz = apply_to(gz)
    gz.to_csv(GZ, index=False, compression="gzip")
    gz.to_pickle("results/ref/_ref_files_20260630_dedup.pkl")
    print("GZ + pickle réécrits:", gz.shape)
else:
    print("\nDRY-RUN (ajouter --apply)")
