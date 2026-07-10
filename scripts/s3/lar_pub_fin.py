#!/usr/bin/env python
"""
Livrable per-corpus LAR_PUB : results/ref/lar_pub.csv -> lar_pub_fin.csv.

Reprend la décision déjà appliquée au ref global .gz (cf. lar_pub_s3_key_cible.py) :
  - 3730 TRANSFERT_S3_OK -> s3_key_cible = s3_key réel (LAR/LAR_PUB/<name>, plat).
  - 80 RBX_LAR_PUB_CDR_013_* = anciennes versions basse-déf (2016, ~2 Mo)
        supersédées par le master HD « CDR_13_le_bon » déjà versé (213 pages,
        ~22,5 Mo) -> conservation « À SUPPRIMER (DOUBLON) », publication « jamais »,
        pas de clé (ne pas écraser le_bon).
  - 1 RBX_LAR_PUB_VPR_011_000.tif = page 000 manquante d'une série versée ->
        À TRANSFERER, s3_key_cible = LAR/LAR_PUB/<name>.

Entrée : results/ref/lar_pub.csv
Sortie : results/ref/lar_pub_fin.csv   (+ audit results/ref/_lar_pub_cible_audit_20260630.csv)
"""
import sys
import datetime
import pandas as pd

SRC = "results/ref/lar_pub.csv"
OUT = "results/ref/lar_pub_fin.csv"
AUDIT = "results/ref/_lar_pub_cible_audit_20260630.csv"
PREFIX = "LAR/LAR_PUB/"

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
TODAY = datetime.date.today().isoformat()
print("shape lue:", df.shape)
assert len(df) == 3811, f"attendu 3811, obtenu {len(df)}"
assert (df["corpus_code"] == "LAR_PUB").all()
assert "s3_key_cible" not in df.columns, "s3_key_cible déjà présente"

ok = df["conservation_statut"] == "TRANSFERT_S3_OK"
el = df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?"
assert int(ok.sum()) == 3730 and int(el.sum()) == 81

ok_names = set(df.loc[ok, "name"])
doublon = el & df["name"].isin(ok_names)          # 80 CDR_013 basse-déf
nouveau = el & ~df["name"].isin(ok_names)          # 1 VPR page manquante
assert int(doublon.sum()) == 80 and int(nouveau.sum()) == 1

# garde-fou : chaque doublon est plus petit que la version S3 de même nom
size = pd.to_numeric(df["size"], errors="coerce")
size_ok = pd.to_numeric(df.loc[ok].set_index("name")["size"], errors="coerce")
d = df.loc[doublon]
assert (size.loc[d.index].values < d["name"].map(size_ok).values).all(), \
    "un doublon n'est pas plus petit que sa version S3"
# garde-fou : chaque doublon a bien un jumeau de clé réelle en S3
real = set(df.loc[df["s3_key"] != "", "s3_key"])
assert set(PREFIX + d["name"]) <= real, "un doublon sans version versée -> ne pas rétrograder"

# --- application --------------------------------------------------------
df.loc[doublon, "conservation_statut"] = "À SUPPRIMER (DOUBLON)"
df.loc[doublon, "publication_statut"] = "jamais"
df.loc[nouveau, "conservation_statut"] = "À TRANSFERER"

df["s3_key_cible"] = ""
df.loc[ok, "s3_key_cible"] = df.loc[ok, "s3_key"]
df.loc[nouveau, "s3_key_cible"] = PREFIX + df.loc[nouveau, "name"]

# contrôles
assert int((df["s3_key_cible"] != "").sum()) == 3731
assert (df.loc[doublon, "s3_key_cible"] == "").all()
assert (df.loc[ok, "s3_key_cible"] == df.loc[ok, "s3_key"]).all()
assert df.loc[df["s3_key_cible"] != "", "s3_key_cible"].is_unique, "collision s3_key_cible"

print("\npublication_statut:", df["publication_statut"].value_counts().to_dict())
print("conservation_statut:", df["conservation_statut"].value_counts().to_dict())
print("s3_key_cible posées:", int((df["s3_key_cible"] != "").sum()),
      "| vides:", int((df["s3_key_cible"] == "").sum()))

audit = df.loc[doublon | nouveau,
               ["name", "size", "conservation_statut", "publication_statut", "s3_key_cible"]].copy()
audit.to_csv(AUDIT, index=False)
print("audit écrit:", AUDIT, audit.shape)

# date_maj_ligne (règle v2) : conservation/publication changés, ou s3_key_cible != s3_key source
df["date_maj_ligne"] = ""
_chg = (
    (df["conservation_statut"].values != orig["conservation_statut"].values)
    | (df["publication_statut"].values != orig["publication_statut"].values)
    | (df["s3_key_cible"].values != orig["s3_key"].values)
)
df.loc[_chg, "date_maj_ligne"] = TODAY
print("date_maj_ligne posée:", int(_chg.sum()), "| vide:", int((~_chg).sum()))

if "--apply" in sys.argv:
    df.to_csv(OUT, index=False)
    print("écrit:", OUT, df.shape)
else:
    print("DRY-RUN (ajouter --apply pour écrire", OUT + ")")
