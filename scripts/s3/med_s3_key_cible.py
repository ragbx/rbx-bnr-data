#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Générique s3_key_cible pour un corpus MED : `med_s3_key_cible.py <corpus> [--apply]`.

Automatise les règles déterministes (convention plate MED/<TOKEN>/<name>) :
  - TRANSFERT_S3_OK              -> s3_key_cible = s3_key réel
  - EN LIGNE - À TRANSFERER ?    -> s3_key_cible = MED/<TOKEN>/<name>, conservation -> À TRANSFERER
  - À SUPPRIMER (*) / CORBEILLE / doublons -> pas de clé, inchangés
  - INCONNU : selon la politique du corpus (cf. CONFIG)
        * "transfer" : les INCONNU RBX-nommés -> À TRANSFERER + clé, publication INCHANGÉE
                       (masters propres hors/near notice, cf. AMR_PR / MED_CP)
        * "hold"     : INCONNU laissés en l'état (backlog non normalisé)
  - garde-fou collisions : toute ligne dont la clé cible dupliquerait une autre
        cible ou une s3_key réelle est EXCLUE du keying (laissée en l'état).
  - date_maj_ligne (v2) : datée si conservation/publication changent, ou
        s3_key_cible != s3_key source ; vide sinon.

Entrée : results/ref/<corpus>.csv        Sortie : results/ref/<corpus>_fin.csv
Audit  : results/ref/_<corpus>_cible_audit_20260630.csv
Ne touche JAMAIS s3_key (invariant S3 réel).
"""
import sys
import datetime
import pandas as pd

CONFIG = {
    "med_eph": {"token": "MED_EPH", "inconnu": "transfer"},
    # dedup : parmi EN LIGNE/INCONNU en doublon de nom À CHECKSUM IDENTIQUE,
    #   mode "keep" -> garder la copie dont le path contient token, autre(s) supprimées
    #   mode "drop" -> supprimer la copie dont le path contient token, garder l'autre
    #   -> copie(s) écartée(s) : conservation "À SUPPRIMER (DOUBLON)"
    "med_ima": {"token": "MED_IMA", "inconnu": "transfer", "dedup": {"mode": "keep", "token": "BNR_VERIF"}},
    "med_mar": {"token": "MED_MAR", "inconnu": "transfer"},
    "med_foo": {"token": "MED_FOO", "inconnu": "hold"},
    "med_pho": {"token": "MED_PHO", "inconnu": "hold"},
    # strip_double_ext : la clé cible normalise un nom en double extension (ex .tif.tif -> .tif)
    "med_mon": {"token": "MED_MON", "inconnu": "transfer", "strip_double_ext": True},
    "med_pla": {"token": "MED_PLA", "inconnu": "transfer", "dedup": {"mode": "drop", "token": "MED_AFF"}},
    "med_pub": {"token": "MED_PUB", "inconnu": "transfer", "dedup": {"mode": "drop", "token": "hors_dossiers"}},
}

args = [a for a in sys.argv[1:] if a != "--apply"]
assert len(args) == 1 and args[0] in CONFIG, f"usage: med_s3_key_cible.py <{'|'.join(CONFIG)}> [--apply]"
corpus = args[0]
cfg = CONFIG[corpus]
TOKEN = cfg["token"]
PREFIX = f"MED/{TOKEN}/"
TODAY = datetime.date.today().isoformat()

SRC = f"results/ref/{corpus}.csv"
OUT = f"results/ref/{corpus}_fin.csv"
AUDIT = f"results/ref/_{corpus}_cible_audit_20260630.csv"

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
print(f"[{corpus}] shape lue:", df.shape)
assert (df["corpus_code"] == TOKEN).all(), f"corpus_code != {TOKEN}"
assert "s3_key_cible" not in df.columns

# --- dédup exacte optionnelle (EN LIGNE/INCONNU même nom + même checksum) -
dd = cfg.get("dedup")
if dd:
    mode, tok = dd["mode"], dd["token"]
    candm = df["conservation_statut"].isin(["EN LIGNE - À TRANSFERER ?", "INCONNU"])
    sub = df[candm]
    dupnames = set(sub["name"][sub["name"].duplicated(keep=False)])
    n_dedup = 0
    for nm, grp in sub[sub["name"].isin(dupnames)].groupby("name"):
        if grp["checksum_md5"].nunique() != 1:
            continue  # conflit de contenu -> laisser le garde-fou collision gérer
        has = grp["path"].str.contains(tok, regex=False)
        if mode == "keep":
            if has.any():
                keep_first = grp.index[has.values][0]
                drop_idx = grp.index.difference([keep_first])
            else:
                drop_idx = grp.index[1:]           # token absent -> garder le 1er
        else:  # drop
            drop_idx = grp.index[has.values]
            if len(drop_idx) >= len(grp):           # tout marqué -> garder le 1er
                drop_idx = grp.index[1:]
        df.loc[drop_idx, "conservation_statut"] = "À SUPPRIMER (DOUBLON)"
        n_dedup += len(drop_idx)
    print(f"  dédup exacte ({mode} {tok}) : {n_dedup} copies -> À SUPPRIMER (DOUBLON)")

cons = df["conservation_statut"]
ok = cons == "TRANSFERT_S3_OK"
el = cons == "EN LIGNE - À TRANSFERER ?"
inc = cons == "INCONNU"

# candidats à clé plate MED/<TOKEN>/<name>
if cfg["inconnu"] == "transfer":
    inc_cand = inc & df["name"].str.startswith("RBX_")
else:
    inc_cand = pd.Series(False, index=df.index)
cand = el | inc_cand
# nom pour la clé : option normalisation double extension (ex .tif.tif -> .tif)
if cfg.get("strip_double_ext"):
    key_name = df["name"].str.replace(r"\.([A-Za-z0-9]+)\.\1$", r".\1", regex=True)
else:
    key_name = df["name"]
cand_key = PREFIX + key_name

# garde-fou collisions : dup interne aux candidats OU contre une s3_key réelle
real = set(df.loc[ok, "s3_key"])
ck = cand_key.where(cand, other="")
internal_dup = cand & ck.duplicated(keep=False) & (ck != "")
against_real = cand & cand_key.isin(real)
collide = internal_dup | against_real
keyed = cand & ~collide

# --- application ---------------------------------------------------------
df["s3_key_cible"] = ""
df.loc[ok, "s3_key_cible"] = df.loc[ok, "s3_key"]
df.loc[keyed, "s3_key_cible"] = cand_key[keyed]
df.loc[keyed, "conservation_statut"] = "À TRANSFERER"   # EN LIGNE + INCONNU keyés
# publication : inchangée (EN LIGNE déjà oui ; INCONNU reste inconnu)

# --- contrôles -----------------------------------------------------------
posed = df.loc[df["s3_key_cible"] != "", "s3_key_cible"]
assert posed.is_unique, "collision s3_key_cible résiduelle"
assert (df.loc[ok, "s3_key_cible"] == df.loc[ok, "s3_key"]).all()
assert not (set(df.loc[keyed, "s3_key_cible"]) & real), "clé keyée en collision s3_key réelle"

# --- date_maj_ligne (v2) -------------------------------------------------
_chg = (
    (df["conservation_statut"].values != orig["conservation_statut"].values)
    | (df["publication_statut"].values != orig["publication_statut"].values)
    | (df["s3_key_cible"].values != orig["s3_key"].values)
)
df["date_maj_ligne"] = ""
df.loc[_chg, "date_maj_ligne"] = TODAY

# --- restitution ---------------------------------------------------------
print("  conservation:", df["conservation_statut"].value_counts().to_dict())
print("  s3_key_cible posées:", int((df["s3_key_cible"] != "").sum()),
      f"(ok={int(ok.sum())} + keyés={int(keyed.sum())}) | collisions exclues={int(collide.sum())}")
print("  date_maj_ligne datées:", int(_chg.sum()), "| vides:", int((~_chg).sum()))

audit = df.loc[el | inc | collide,
               ["name", "extension", "finding_aid", "conservation_statut",
                "publication_statut", "s3_key_cible", "date_maj_ligne"]].copy()
audit.to_csv(AUDIT, index=False)
print("  audit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    df.to_csv(OUT, index=False)
    print("  écrit:", OUT, df.shape)
else:
    print("  DRY-RUN (--apply pour écrire", OUT + ")")
