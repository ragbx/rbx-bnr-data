#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Construit la colonne `s3_key_cible` pour AMR_GUE / affiches de guerre 7H,
à partir du fichier de travail dédié `results/ref/amr_gue.csv` (1 027 lignes,
IR bnr_FR595129901_AMR_007). Même esprit que `amr_ec_s3_key_cible.py` :
on ajoute `s3_key_cible`, on ne touche JAMAIS `s3_key` (invariant S3 réel).

Rappel campagne CADN-OCR (5 dérivés/doc) et décisions utilisateur actées
(cf. mémoire project-ref-integrite-20260630) = verser LES DEUX sans
rétrogradation, et — DÉCISION 2026-07-09 — regrouper TOUT le set 7H (masters
+ dérivés) dans un seul dossier `AMR/AMR_GUE/` :

  - 341 TIFF « À TRANSFERER » (corpus_code réel AMR_AFF, path AMR_CADN…, pub inconnu)
        -> master préservation  ->  AMR/AMR_GUE/<name>
  - 678 « EN LIGNE - À TRANSFERER ? » = 339 jpg + 339 ALTO xml (corpus AMR_GUE,
        path AMR_7H, publication oui) -> dérivés diffusion -> AMR/AMR_GUE/<name>
  - 8 INCONNU (2 jpg + 2 xml + 4 tif, unitid vide, hors notice) -> s3_key_cible VIDE (inchangés)

NB : les 341 TIFF portent corpus_code AMR_AFF mais, comme les 678 dérivés,
le radical de nom `RBX_AMR_AFF_7H_*` et l'IR AMR_007 = même unité « affiches de
guerre 7H ». Le choix 2026-07-09 privilégie l'unité de collection sur le
corpus_code (l'alternative AMR/AMR_AFF/ rejoindrait les 1 074 affiches TIFF
déjà versées mais scinderait masters/dérivés).

Auto-validation : compositions attendues (341/678/8) par assertion ; cohérence
croisée via uuid contre le ref dédup : les 678 AMR_GUE doivent déjà pointer
AMR/AMR_GUE/ (inchangés), les 341 doivent y pointer AMR/AMR_AFF/ (= valeur
qu'on bascule sciemment). Le dédup.gz est réaligné séparément.

Entrée  : results/ref/amr_gue.csv
Sortie  : results/ref/amr_gue_fin.csv   (+ audit _dedup_gue_7h_cible_audit_20260630.csv)
Garde   : results/ref/_ref_files_20260630_dedup.pkl (cross-check, lecture seule)
"""
import sys
import datetime
import pandas as pd

SRC = "results/ref/amr_gue.csv"
OUT = "results/ref/amr_gue_fin.csv"
AUDIT = "results/ref/_dedup_gue_7h_cible_audit_20260630.csv"
PKL = "results/ref/_ref_files_20260630_dedup.pkl"

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
TODAY = datetime.date.today().isoformat()
print("shape lue:", df.shape)
assert len(df) == 1027, f"attendu 1027, obtenu {len(df)}"
assert (df["corpus_code"] == "AMR_GUE").all(), "corpus_code != AMR_GUE"
assert "s3_key_cible" not in df.columns, "s3_key_cible déjà présente"

tiff = (df["file_type"] == "tiff") & (df["conservation_statut"] == "À TRANSFERER")
gue = df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?"
inconnu = df["conservation_statut"] == "INCONNU"
assert int(tiff.sum()) == 341, f"tiff={int(tiff.sum())}"
assert int(gue.sum()) == 678, f"gue={int(gue.sum())}"
assert int(inconnu.sum()) == 8, f"inconnu={int(inconnu.sum())}"
assert (tiff | gue | inconnu).all(), "ligne non classée"

# garde-fous nommage
assert df.loc[tiff, "name"].str.match(r"^RBX_AMR_AFF_7H_.*\.tif$").all(), "nom tiff inattendu"
assert df.loc[gue, "name"].str.match(r"^RBX_AMR_AFF_7H_.*\.(jpg|xml)$").all(), "nom gue inattendu"

df["s3_key_cible"] = ""
df.loc[tiff, "s3_key_cible"] = "AMR/AMR_GUE/" + df.loc[tiff, "name"]
df.loc[gue, "s3_key_cible"] = "AMR/AMR_GUE/" + df.loc[gue, "name"]

# collisions internes + unicité
posed = df.loc[df["s3_key_cible"] != "", "s3_key_cible"]
assert posed.nunique() == len(posed) == 1019, "collision interne / compte cible"
assert posed.str.startswith("AMR/AMR_GUE/").all(), "toutes les cibles doivent être AMR/AMR_GUE/"

# cross-check contre le ref dédup (uuid -> s3_key_cible déjà posée)
ref = pd.read_pickle(PKL)[["uuid", "s3_key_cible"]]
ref = ref[ref["uuid"].isin(set(df["uuid"]))].rename(columns={"s3_key_cible": "cible_ref"})
m = df.merge(ref, on="uuid", how="left")
assert m["cible_ref"].notna().sum() == len(df), "uuid absent du ref dédup"
# 678 dérivés AMR_GUE : inchangés, doivent déjà pointer AMR/AMR_GUE/
d678 = m[m["conservation_statut"] == "EN LIGNE - À TRANSFERER ?"]
assert (d678["cible_ref"] == "AMR/AMR_GUE/" + d678["name"]).all(), "678 AMR_GUE incohérents vs dédup"
# 341 TIFF : bascule sciemment AMR/AMR_AFF/ -> AMR/AMR_GUE/
d341 = m[m["conservation_statut"] == "À TRANSFERER"]
assert (d341["cible_ref"] == "AMR/AMR_AFF/" + d341["name"]).all(), "341 TIFF n'étaient pas en AMR/AMR_AFF/ dans le dédup"
print("cross-check OK : 678 AMR_GUE inchangés, 341 TIFF basculés AMR_AFF->AMR_GUE")

print("\nrépartition s3_key_cible:")
print(df["s3_key_cible"].replace("", "(vide)").str.extract(r"^(AMR/\w+/|\(vide\))")[0].value_counts().to_dict())

# audit compact
audit = df.loc[df["s3_key_cible"] != "", ["name", "file_type", "conservation_statut",
                                          "publication_statut", "s3_key_cible"]]
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
