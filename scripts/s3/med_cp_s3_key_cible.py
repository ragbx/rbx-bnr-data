#!/usr/bin/env python
"""
Construit `s3_key_cible` pour MED_CP (5999 l., tout az) et tranche les INCONNU.

On n'ajoute que `s3_key_cible` (jamais `s3_key`). Convention plate MED/MED_CP/<name>
(confirmée : 4525 déjà en S3 tous à plat). Rôle par extension (tif=conservation,
jpg/jpeg=diffusion) cf. mémoire.

État initial :
  - TRANSFERT_S3_OK 4525 (tif, déjà en S3)
  - EN LIGNE - À TRANSFERER ? 183 (140 jpg + 34 jpeg + 9 tif, oui) -> à monter
  - À SUPPRIMER (doublons S3 - az) 535 (tif, jamais) -> écartés
  - CORBEILLE (DIFFUSION) 106 (jpg, jamais) -> écartés
  - INCONNU 650 : 649 RBX (masters/images de séquences S2/S3 d'articles DÉJÀ en S3,
        décrits en IR mais hors osiros) + 1 _lisez_moi.txt.

Arbitrage 2026-07-09 :
  - INCONNU avec finding_aid (632 : 611 tif + 21 jpeg) -> conservation À TRANSFERER,
        s3_key_cible ; publication reste inconnu (hors osiros).
  - INCONNU sans finding_aid, hors txt (17 : orphelins images + tif « sansnom »/doublon)
        -> laissés INCONNU, pas de clé.
  - _lisez_moi.txt -> À SUPPRIMER (FILE_TYPE) + publication jamais.

date_maj_ligne = date du jour sur les lignes modifiées vs source, vide sinon (cf. mémoire).

Entrée : results/ref/med_cp.csv
Sortie : results/ref/med_cp_fin.csv   (+ audit results/ref/_med_cp_cible_audit_20260630.csv)
"""
import sys
import datetime
import pandas as pd

SRC = "results/ref/med_cp.csv"
OUT = "results/ref/med_cp_fin.csv"
AUDIT = "results/ref/_med_cp_cible_audit_20260630.csv"
PREFIX = "MED/MED_CP/"
TODAY = datetime.date.today().isoformat()

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
print("shape lue:", df.shape)
assert len(df) == 5999, f"attendu 5999, obtenu {len(df)}"
assert (df["corpus_code"] == "MED_CP").all()
assert "s3_key_cible" not in df.columns, "s3_key_cible déjà présente"

ext = df["extension"].str.lower()
ok = df["conservation_statut"] == "TRANSFERT_S3_OK"
el = df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?"
inc = df["conservation_statut"] == "INCONNU"
assert int(ok.sum()) == 4525 and int(el.sum()) == 183 and int(inc.sum()) == 650

# --- partition des INCONNU ----------------------------------------------
txt = inc & (ext == ".txt")
inc_fa = inc & (df["finding_aid"] != "") & ~txt          # 632 avec IR
inc_nofa = inc & (df["finding_aid"] == "") & ~txt        # 17 sans IR (hors txt)
assert int(txt.sum()) == 1
assert int(inc_fa.sum()) == 632, f"INCONNU avec IR = {int(inc_fa.sum())}"
assert int(inc_nofa.sum()) == 17, f"INCONNU sans IR = {int(inc_nofa.sum())}"

# collisions de nom : dans l'ensemble à clé (OK réel + EN LIGNE + INCONNU-fa),
# certains noms sont partagés par 2 fichiers de contenu différent
#   - 2 = master déjà en S3 (OK) + un INCONNU (l'écraserait)
#   - 10 = deux INCONNU revendiquant le même nom
# -> on EXCLUT les 22 INCONNU en collision du transfert (restent INCONNU, sans clé).
keyed = ok | el | inc_fa
kn = df.loc[keyed, "name"]
dupnames = set(kn[kn.duplicated(keep=False)])
inc_fa_coll = inc_fa & df["name"].isin(dupnames)           # 22 à exclure
inc_fa_ok = inc_fa & ~inc_fa_coll                          # 610 à transférer
assert int(inc_fa_coll.sum()) == 22, f"collisions INCONNU = {int(inc_fa_coll.sum())}"
assert int(inc_fa_ok.sum()) == 610

# --- révision des statuts ------------------------------------------------
df.loc[inc_fa_ok, "conservation_statut"] = "À TRANSFERER"   # publication inchangée (inconnu)
df.loc[el, "conservation_statut"] = "À TRANSFERER"          # EN LIGNE -> À TRANSFERER
df.loc[txt, "conservation_statut"] = "À SUPPRIMER (FILE_TYPE)"
df.loc[txt, "publication_statut"] = "jamais"
# inc_nofa (17) + inc_fa_coll (22) : inchangés (INCONNU)

# --- s3_key_cible --------------------------------------------------------
df["s3_key_cible"] = ""
df.loc[ok, "s3_key_cible"] = df.loc[ok, "s3_key"]          # clé réelle
a_monter = el | inc_fa_ok                                   # 183 + 610
df.loc[a_monter, "s3_key_cible"] = PREFIX + df.loc[a_monter, "name"]

# contrôles
attendu = int(ok.sum()) + int(a_monter.sum())              # 4525 + 793 = 5318
assert int((df["s3_key_cible"] != "").sum()) == attendu, attendu
assert (df.loc[ok, "s3_key_cible"] == df.loc[ok, "s3_key"]).all()
# rien sur doublons/corbeille/inc_nofa/collisions/txt
assert (df.loc[inc_nofa | inc_fa_coll | txt, "s3_key_cible"] == "").all()
# pas de collision, ni avec les s3_key réelles existantes
posed = df.loc[df["s3_key_cible"] != "", "s3_key_cible"]
assert posed.is_unique, "collision s3_key_cible interne"
real = set(df.loc[df["s3_key"] != "", "s3_key"])
monter_keys = set(df.loc[a_monter, "s3_key_cible"])
assert not (monter_keys & real), "clé à monter en collision avec une s3_key réelle"

# --- date_maj_ligne (règle v2) ------------------------------------------
# datée si conservation_statut / publication_statut changent, ou si
# s3_key_cible != s3_key (source). Sinon vide.
changed = (
    (df["conservation_statut"].values != orig["conservation_statut"].values)
    | (df["publication_statut"].values != orig["publication_statut"].values)
    | (df["s3_key_cible"].values != orig["s3_key"].values)
)
df["date_maj_ligne"] = ""
df.loc[changed, "date_maj_ligne"] = TODAY

# --- restitution --------------------------------------------------------
print("\npublication_statut:", df["publication_statut"].value_counts().to_dict())
print("conservation_statut:", df["conservation_statut"].value_counts().to_dict())
print("s3_key_cible posées:", int((df["s3_key_cible"] != "").sum()),
      "| vides:", int((df["s3_key_cible"] == "").sum()))
print("date_maj_ligne posée:", int((df["date_maj_ligne"] != "").sum()),
      "| vide:", int((df["date_maj_ligne"] == "").sum()))

audit = df.loc[inc | el | txt,
               ["name", "extension", "finding_aid", "conservation_statut",
                "publication_statut", "s3_key_cible", "date_maj_ligne"]].copy()
audit.to_csv(AUDIT, index=False)
print("audit écrit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    df.to_csv(OUT, index=False)
    print("écrit:", OUT, df.shape)
else:
    print("DRY-RUN (ajouter --apply pour écrire", OUT + ")")
