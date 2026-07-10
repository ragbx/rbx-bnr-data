#!/usr/bin/env python
"""
Construit `s3_key_cible` pour ARA_CPS (audio, 174 l., tout az) et publie les INCONNU.

On n'ajoute que `s3_key_cible` (jamais `s3_key`). Clé cible = ARA/ARA_CPS/<name>
(corpus jamais transféré, aucune clé ARA en S3 : convention posée par analogie
AMR/<corpus>/<name>).

Corpus audio en notice (174/174 ont osiros_id + finding_aid). 8 sous-séries
(HOR/LEO/MAI/PAT/PIL/SAI/TRO/VER) mêlant wav (enregistrement) et jpg (image).
Le paradigme tif=conservation/jpg=diffusion ne s'applique pas ici.

État initial : 146 EN LIGNE - À TRANSFERER ? (oui) + 28 INCONNU.
Les 28 INCONNU = 24 jpg série PAT + 4 wav série SAI, décrits, rattachés à des
séries déjà en ligne.

Arbitrage 2026-07-09 : publier les 28 -> publication=oui, conservation=À TRANSFERER.
Puis EN LIGNE - À TRANSFERER ? -> À TRANSFERER (comme les autres corpus).
=> les 174 en À TRANSFERER / oui, tous avec s3_key_cible.

Entrée : results/ref/ara_cps.csv
Sortie : results/ref/ara_cps_fin.csv   (+ audit results/ref/_ara_cps_cible_audit_20260630.csv)
"""
import sys
import datetime
import pandas as pd

SRC = "results/ref/ara_cps.csv"
OUT = "results/ref/ara_cps_fin.csv"
AUDIT = "results/ref/_ara_cps_cible_audit_20260630.csv"

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
TODAY = datetime.date.today().isoformat()
print("shape lue:", df.shape)
assert len(df) == 174, f"attendu 174, obtenu {len(df)}"
assert (df["corpus_code"] == "ARA_CPS").all(), "corpus_code != ARA_CPS"
assert "s3_key_cible" not in df.columns, "s3_key_cible déjà présente"
assert (df["osiros_id"] != "").all() and (df["finding_aid"] != "").all(), "notice manquante"

# --- publier les 28 INCONNU ---------------------------------------------
inc = df["conservation_statut"] == "INCONNU"
assert int(inc.sum()) == 28, f"INCONNU = {int(inc.sum())}"
df.loc[inc, "publication_statut"] = "oui"
df.loc[inc, "conservation_statut"] = "À TRANSFERER"

# EN LIGNE - À TRANSFERER ? -> À TRANSFERER
df.loc[df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?",
       "conservation_statut"] = "À TRANSFERER"

# --- s3_key_cible : tout le corpus --------------------------------------
assert (df["conservation_statut"] == "À TRANSFERER").all(), "statut résiduel"
assert (df["publication_statut"] == "oui").all(), "publication résiduelle"
df["s3_key_cible"] = "ARA/ARA_CPS/" + df["name"]

assert int((df["s3_key_cible"] != "").sum()) == 174
assert df["s3_key_cible"].is_unique, "collision s3_key_cible"

# --- restitution --------------------------------------------------------
print("\npublication_statut:", df["publication_statut"].value_counts().to_dict())
print("conservation_statut:", df["conservation_statut"].value_counts().to_dict())
print("s3_key_cible posées:", int((df["s3_key_cible"] != "").sum()))
print("ext:", df["extension"].str.lower().value_counts().to_dict())

audit = df.loc[inc, ["name", "extension", "publication_statut",
                     "conservation_statut", "s3_key_cible"]].copy()
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
