#!/usr/bin/env python
"""
Construit `s3_key_cible` pour AMR_PLA (plans) et tranche les 8 INCONNU.

On n'ajoute que `s3_key_cible` (jamais `s3_key`). Rôle par extension (cf.
mémoire) : TIF=conservation, JPEG=diffusion. Corpus déjà largement trié — la
clé se pose sur tout ce qui doit monter en S3 : conservation « À TRANSFERER »
+ diffusion « EN LIGNE - À TRANSFERER ? ». Les DDE / CORBEILLE / À SUPPRIMER
n'ont pas de clé.

Trois sources :
  - az (124 jpg) = diffusion : 115 EN LIGNE (publiés) + 5 CORBEILLE + 3 INCONNU + 1 À SUPPRIMER
  - AMR_CADN_165-0500 (242) = 121 tif conservation « À TRANSFERER » + 121 jpg DDE (doublons az)
  - AMR_CADN_314-1000 (10) = 5 tif conservation INCONNU + 5 jpg DDE (diffusion)

Arbitrage des 8 INCONNU (2026-07-09) :
  - 5 tif lot 314 (6Fi005, 6Fi022_01..04) = masters de plans déjà diffusés (az) ->
        conservation « À TRANSFERER » (keeper).
  - 3 jpg orphelins az (ANC_005, 6FI004, 6Fi040_03) = diffusion sans jumeau ->
        publication=oui, conservation « À TRANSFERER » (keeper).

Entrée : results/ref/amr_pla.csv
Sortie : results/ref/amr_pla_fin.csv   (+ audit results/ref/_amr_pla_cible_audit_20260630.csv)
"""
import sys
import datetime
import pandas as pd

SRC = "results/ref/amr_pla.csv"
OUT = "results/ref/amr_pla_fin.csv"
AUDIT = "results/ref/_amr_pla_cible_audit_20260630.csv"

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
TODAY = datetime.date.today().isoformat()
print("shape lue:", df.shape)
assert len(df) == 376, f"attendu 376, obtenu {len(df)}"
assert (df["corpus_code"] == "AMR_PLA").all(), "corpus_code != AMR_PLA"
assert "s3_key_cible" not in df.columns, "s3_key_cible déjà présente"

ext = df["extension"].str.lower()

# --- arbitrage des 8 INCONNU --------------------------------------------
inc = df["conservation_statut"] == "INCONNU"
assert int(inc.sum()) == 8, f"INCONNU = {int(inc.sum())}"
tif314 = inc & (ext == ".tif")                     # 5 masters conservation
jpg_orph = inc & (ext == ".jpg")                   # 3 orphelins diffusion
assert int(tif314.sum()) == 5 and int(jpg_orph.sum()) == 3

df.loc[tif314, "conservation_statut"] = "À TRANSFERER"
df.loc[jpg_orph, "conservation_statut"] = "À TRANSFERER"
df.loc[jpg_orph, "publication_statut"] = "oui"

# --- s3_key_cible : tout ce qui monte en S3 -----------------------------
a_transferer = df["conservation_statut"].isin(
    ["À TRANSFERER", "EN LIGNE - À TRANSFERER ?"])
assert int(a_transferer.sum()) == 244, f"keepers = {int(a_transferer.sum())}"
# les 115 diffusion « EN LIGNE - À TRANSFERER ? » -> À TRANSFERER
df.loc[df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?",
       "conservation_statut"] = "À TRANSFERER"

# les masters de conservation keepers restaient publication=inconnu -> oui
conserv_inc = a_transferer & (df["publication_statut"] == "inconnu")
assert int(conserv_inc.sum()) == 115, f"conservation inconnu = {int(conserv_inc.sum())}"
df.loc[conserv_inc, "publication_statut"] = "oui"

df["s3_key_cible"] = ""
df.loc[a_transferer, "s3_key_cible"] = "AMR/AMR_PLA/" + df.loc[a_transferer, "name"]

# contrôles
assert int((df["s3_key_cible"] != "").sum()) == 244
assert (df.loc[a_transferer, "s3_key_cible"].str.startswith("AMR/AMR_PLA/")).all()
# rien sur les non-keepers (DDE / CORBEILLE / À SUPPRIMER)
assert (df.loc[~a_transferer, "s3_key_cible"] == "").all(), "clé posée à tort"
posed = df.loc[df["s3_key_cible"] != "", "s3_key_cible"]
assert posed.is_unique, "collision s3_key_cible"
assert int((df["conservation_statut"] == "INCONNU").sum()) == 0, "INCONNU restant"

# --- restitution ---------------------------------------------------------
print("\npublication_statut:", df["publication_statut"].value_counts().to_dict())
print("conservation_statut:", df["conservation_statut"].value_counts().to_dict())
print("s3_key_cible posées:", int((df["s3_key_cible"] != "").sum()),
      "| vides:", int((df["s3_key_cible"] == "").sum()))

audit = df.loc[inc, ["name", "extension", "source2s3",
                     "publication_statut", "conservation_statut", "s3_key_cible"]].copy()
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
