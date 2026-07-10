#!/usr/bin/env python
"""
Renseigne `s3_key_cible` pour AMR_PR (4326 l., tout az) et tranche les INCONNU.

La colonne `s3_key_cible` existe déjà (vide) : on la remplit, on ne touche
jamais `s3_key` (invariant S3 réel). Clé cible = AMR/AMR_PR/<name> (plat, comme
les 1101 déjà en S3). Rôle par extension (cf. mémoire) : tif=conservation.

État initial :
  - TRANSFERT_S3_OK (1101) = déjà en S3 (séries CAN/FLO/HDV/HFR + 2 jpg)
  - EN LIGNE - À TRANSFERER ? (110) = diffusion publiée (100 jpg + 10 tif, raw)
  - INCONNU (3013) = 2998 masters COR/REQ (tif+xml, RBX, hors notice) + 15 jpg orphelins raw
  - CORBEILLE (DIFFUSION) 100, À SUPPRIMER 2 -> jamais

Arbitrage 2026-07-09 :
  - Séries COR (2961) + REQ (37) = 2998 masters -> conservation « À TRANSFERER »,
        s3_key_cible posée. publication reste inconnu (hors notice, non diffusés).
  - 15 jpg orphelins raw -> laissés en INCONNU, pas de clé.

s3_key_cible posée sur tout ce qui monte / est en S3 : TRANSFERT_S3_OK
+ EN LIGNE - À TRANSFERER ? + COR/REQ (À TRANSFERER). Rien sur INCONNU restant
(15 jpg) / CORBEILLE / À SUPPRIMER.

Entrée : results/ref/amr_pr.csv
Sortie : results/ref/amr_pr_fin.csv   (+ audit results/ref/_amr_pr_cible_audit_20260630.csv)
"""
import sys
import datetime
import pandas as pd

SRC = "results/ref/amr_pr.csv"
OUT = "results/ref/amr_pr_fin.csv"
AUDIT = "results/ref/_amr_pr_cible_audit_20260630.csv"

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
TODAY = datetime.date.today().isoformat()
print("shape lue:", df.shape)
assert len(df) == 4326, f"attendu 4326, obtenu {len(df)}"
assert (df["corpus_code"] == "AMR_PR").all(), "corpus_code != AMR_PR"
assert "s3_key_cible" in df.columns and (df["s3_key_cible"] == "").all(), \
    "s3_key_cible absente ou déjà remplie"

# --- arbitrage COR/REQ (masters hors notice) ----------------------------
inc = df["conservation_statut"] == "INCONNU"
cor_req = inc & df["name"].str.match(r"^RBX_AMR_PR_(COR|REQ)_")
assert int(cor_req.sum()) == 2998, f"COR/REQ = {int(cor_req.sum())}"
df.loc[cor_req, "conservation_statut"] = "À TRANSFERER"
# les 15 jpg orphelins raw restent INCONNU (pas de clé)
assert int((df["conservation_statut"] == "INCONNU").sum()) == 15

# --- s3_key_cible -------------------------------------------------------
# Déjà en S3 : la cible = le s3_key réel (préserve les sous-dossiers HFR).
# Non montés (BAR EN LIGNE + COR/REQ) : clé plate AMR/AMR_PR/<name>, cohérent
# avec les masters FLO déjà versés à plat. HFR est le seul cas sous-dossiers,
# et il est déjà en S3 (donc pris via s3_key réel).
ok = df["conservation_statut"] == "TRANSFERT_S3_OK"
a_monter = df["conservation_statut"].isin(["EN LIGNE - À TRANSFERER ?", "À TRANSFERER"])
assert int(ok.sum()) == 1101 and int(a_monter.sum()) == 3108

df.loc[ok, "s3_key_cible"] = df.loc[ok, "s3_key"]
df.loc[a_monter, "s3_key_cible"] = "AMR/AMR_PR/" + df.loc[a_monter, "name"]

# les 110 diffusion « EN LIGNE - À TRANSFERER ? » (BAR/GDL/PDS) -> À TRANSFERER
df.loc[df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?",
       "conservation_statut"] = "À TRANSFERER"

# diffusion jpg redondante : si un tif de conservation partage le même stem,
# on écarte le jpg (10 paires série BAR) -> À SUPPRIMER (DIFFUSION), pas de clé.
ext = df["extension"].str.lower()
stem = df["name"].str.replace(r"\.(jpg|tif|xml|xls)$", "", regex=True, case=False)
tif_stems = set(stem[ext == ".tif"])
jpg_redond = (df["conservation_statut"] == "À TRANSFERER") & (ext == ".jpg") \
    & stem.isin(tif_stems)
assert int(jpg_redond.sum()) == 10, f"jpg redondants = {int(jpg_redond.sum())}"
df.loc[jpg_redond, "conservation_statut"] = "À SUPPRIMER (DIFFUSION)"
df.loc[jpg_redond, "s3_key_cible"] = ""

# contrôles
assert int((df["s3_key_cible"] != "").sum()) == 4199
a_transferer = (ok | a_monter) & ~jpg_redond
assert (df.loc[~a_transferer, "s3_key_cible"] == "").all(), "clé posée à tort"
assert df.loc[df["s3_key_cible"] != "", "s3_key_cible"].is_unique, "collision s3_key_cible"

# --- restitution --------------------------------------------------------
print("\npublication_statut:", df["publication_statut"].value_counts().to_dict())
print("conservation_statut:", df["conservation_statut"].value_counts().to_dict())
print("s3_key_cible posées:", int((df["s3_key_cible"] != "").sum()),
      "| vides:", int((df["s3_key_cible"] == "").sum()))

audit = df.loc[cor_req | (df["conservation_statut"] == "INCONNU"),
               ["name", "extension", "conservation_statut",
                "publication_statut", "s3_key_cible"]].copy()
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
