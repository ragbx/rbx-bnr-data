#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Construit `s3_key_cible` pour le versement CADN d'AMR_DEL (séances 1983-2002,
cotes 499W001-499W140) : tranche les 23 956 INCONNU et bascule les 23 676 jpg
« DDE - NE PAS GARDER (DIFFUSION) » en « À TRANSFERER » avec clé (décision
2026-07-11 : seules images page à page des séances, aucun master tif 499W).

Contexte (2026-07-10) : l'IR Mnesys FRAC512_D_01 s'arrête à 1982 — aucune
daoloc pour ces séances, publication `inconnu` (alto/pdf inchangés, jpg
alignés le 2026-07-11 ; précédent AMR_PR, masters hors notice) sauf txt
`jamais` (préexistant).

Schéma aligné sur les clés DEL_TAP existantes (dossier <cote>_<année>,
ALTO stockés en .xml, séquence 4 chiffres) :
  - alto : AMR/AMR_DEL/DEL_TAP/<cote>_<année>/RBX_AMR_DEL_<cote>_<date>_<seq4>.xml
  - jpg  : idem .jpg (même stem que l'alto apparié, séquence repaddée à 4)
  - pdf  : AMR/AMR_DEL/DEL_TAP/<cote>_<année>/RBX_AMR_DEL_<cote>_<date>.pdf
  - txt  : idem .txt (suffixe _001 source retiré)
Les 128 pdf/txt nommés DEL_<date>[-TOMEx].* sont normalisés en RBX_ ; la cote
vient des alto de la même séance (dossier source, mapping 1:1 vérifié).

Entrée : results/ref/amr_del_cadn.csv (47 632 lignes, source2s3 AMR_CADN_*)
Sortie : results/ref/amr_del_cadn_fin.csv
Audit  : results/ref/_amr_del_cadn_cible_audit_20260630.csv
Ne touche JAMAIS s3_key (invariant S3 réel).
"""
import os
import sys
import datetime
import re
import pandas as pd

SRC = "results/ref/amr_del_cadn.csv"
OUT = "results/ref/amr_del_cadn_fin.csv"
AUDIT = "results/ref/_amr_del_cadn_cible_audit_20260630.csv"
TODAY = datetime.date.today().isoformat()

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
print("shape lue:", df.shape)
assert len(df) == 47632, f"attendu 47632, obtenu {len(df)}"
assert (df["corpus_code"] == "AMR_DEL").all(), "corpus_code != AMR_DEL"
assert df["source2s3"].str.startswith("AMR_CADN").all(), "source hors CADN"
assert "s3_key_cible" not in df.columns, "s3_key_cible déjà présente"
assert (df["s3_key"] == "").all(), "s3_key réelle inattendue sur du CADN"

ext = df["extension"].str.lower()
inc = df["conservation_statut"] == "INCONNU"
assert int(inc.sum()) == 23956, f"INCONNU = {int(inc.sum())}"
assert set(ext[inc]) == {".alto", ".pdf", ".txt"}, "extension INCONNU inattendue"
jpg = df["conservation_statut"] == "DDE - NE PAS GARDER (DIFFUSION)"
assert int(jpg.sum()) == 23676, f"DDE jpg = {int(jpg.sum())}"
assert set(ext[jpg]) == {".jpg"}, "extension DDE inattendue"
peri = inc | jpg
assert peri.all(), "ligne CADN hors périmètre INCONNU/DDE"
seance = df["path"].str.split("/").str[-1]

# --- mapping séance -> (cote, date) via les alto --------------------------
alto = inc & (ext == ".alto")
m = df.loc[alto, "name"].str.extract(r"^RBX_AMR_DEL_(499W\d{3})_(\d{8})_(\d{1,4})\.alto$")
assert not m.isna().any().any(), "nom alto non conforme"
amap = pd.DataFrame({"seance": seance[alto], "cote": m[0], "date": m[1], "seq": m[2]})
sc = amap.groupby("seance")[["cote", "date"]].nunique()
assert len(sc) == 140 and (sc == 1).all().all(), "séance sans cote/date unique"
lut = amap.groupby("seance")[["cote", "date"]].first()
assert lut["cote"].is_unique, "cote partagée entre séances"

cote = seance.map(lut["cote"])
date = seance.map(lut["date"])
assert cote[inc].notna().all(), "séance INCONNU absente du mapping alto"
folder = "AMR/AMR_DEL/DEL_TAP/" + cote + "_" + date.str[:4] + "/"
stem = "RBX_AMR_DEL_" + cote + "_" + date

# --- clés cibles -----------------------------------------------------------
df["s3_key_cible"] = ""
# alto -> .xml, séquence paddée à 4 (convention ALTO DEL_TAP)
df.loc[alto, "s3_key_cible"] = (
    folder[alto] + stem[alto] + "_" + amap.set_index(amap.index)["seq"].str.zfill(4) + ".xml")
# pdf/txt : 1 par séance, nom normalisé RBX_ (couvre DEL_<date>[-TOMEx] et _001)
for e in (".pdf", ".txt"):
    sel = inc & (ext == e)
    assert int(sel.sum()) == 140, f"{e}: {int(sel.sum())} != 140"
    assert seance[sel].nunique() == 140, f"{e}: doublon de séance"
    df.loc[sel, "s3_key_cible"] = folder[sel] + stem[sel] + e

# jpg (2026-07-11) : seules images page à page, même stem que l'alto apparié
mj = df.loc[jpg, "name"].str.extract(r"^RBX_AMR_DEL_(499W\d{3})_(\d{8})_(\d{1,4})\.jpg$")
assert not mj.isna().any().any(), "nom jpg non conforme"
jstem = "RBX_AMR_DEL_" + mj[0] + "_" + mj[1] + "_" + mj[2].str.zfill(4)
astem = "RBX_AMR_DEL_" + amap["cote"] + "_" + amap["date"] + "_" + amap["seq"].str.zfill(4)
assert set(jstem) == set(astem), "appariement jpg↔alto rompu (stems différents)"
df.loc[jpg, "s3_key_cible"] = (
    "AMR/AMR_DEL/DEL_TAP/" + mj[0] + "_" + mj[1].str[:4] + "/" + jstem + ".jpg")

df.loc[inc | jpg, "conservation_statut"] = "À TRANSFERER"
# publication_statut : jpg alignés sur les alto (hors IR Mnesys) ; reste inchangé
df.loc[jpg, "publication_statut"] = "inconnu"

# --- contrôles -------------------------------------------------------------
posed = df.loc[df["s3_key_cible"] != "", "s3_key_cible"]
assert len(posed) == 47632, f"clés posées = {len(posed)}"
assert posed.is_unique, "collision s3_key_cible"
assert posed.str.match(r"^AMR/AMR_DEL/DEL_TAP/499W\d{3}_(19|20)\d{2}/RBX_AMR_DEL_499W\d{3}_\d{8}(_\d{4})?\.(xml|pdf|txt|jpg)$").all()
assert posed[posed.str.endswith((".xml", ".jpg"))].str.match(r".*_\d{4}\.(xml|jpg)$").all(), "xml/jpg sans séquence"
assert int((df["conservation_statut"] == "INCONNU").sum()) == 0, "INCONNU restant"
assert (df["conservation_statut"] == "À TRANSFERER").all(), "statut inattendu"

# --- date_maj_ligne (v2) ---------------------------------------------------
_chg = (
    (df["conservation_statut"].values != orig["conservation_statut"].values)
    | (df["publication_statut"].values != orig["publication_statut"].values)
    | (df["s3_key_cible"].values != orig["s3_key"].values)
)
df["date_maj_ligne"] = ""
df.loc[_chg, "date_maj_ligne"] = TODAY
# reprend la date du fin précédent pour les lignes inchangées depuis (rerun)
if os.path.exists(OUT):
    prev = pd.read_csv(OUT, dtype=str, keep_default_na=False)
    assert prev["uuid"].is_unique and df["uuid"].is_unique, "uuid non unique"
    prev = prev.set_index("uuid").reindex(df["uuid"])
    TRACK = ["conservation_statut", "publication_statut", "s3_key_cible"]
    same = (df[TRACK].values == prev[TRACK].values).all(axis=1)
    df.loc[same, "date_maj_ligne"] = prev.loc[same, "date_maj_ligne"].values
    print("date_maj_ligne reprises du fin précédent:", int(same.sum()))

# --- restitution -----------------------------------------------------------
print("conservation:", df["conservation_statut"].value_counts().to_dict())
print("publication:", df["publication_statut"].value_counts().to_dict())
print("s3_key_cible posées:", len(posed), "| vides:", int((df["s3_key_cible"] == "").sum()))
print("dossiers cibles:", posed.str.split("/").str[3].nunique())
print("date_maj_ligne datées:", int(_chg.sum()), "| vides:", int((~_chg).sum()))

audit = df[["name", "path", "extension", "conservation_statut",
            "publication_statut", "s3_key_cible", "date_maj_ligne"]].copy()
audit.to_csv(AUDIT, index=False)
print("audit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    df.to_csv(OUT, index=False)
    print("écrit:", OUT, df.shape)
else:
    print("DRY-RUN (--apply pour écrire", OUT + ")")
