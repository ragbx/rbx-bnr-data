#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Construit `s3_key_cible` pour le versement CADN d'AMR_PVC (procès-verbaux du
conseil, 141 séances 1983-2002, cotes 246W042-050 puis 498W001-046) : tout le
versement part « À TRANSFERER », jpg compris (seules images, aucun master tif
— jumeau structurel d'AMR_DEL 499W, schéma validé par l'utilisateur 2026-07-11).

Aucune clé AMR_PVC préexistante sur S3 : schéma calqué sur DEL_TAP
(dossier <cote>_<année de séance>, ALTO stockés en .xml, séquence 4 chiffres) :
  - alto : AMR/AMR_PVC/<cote>_<année>/RBX_AMR_PVC_<cote>_<date>_<seq4>.xml
  - jpg  : idem .jpg (même stem que l'alto apparié, 1:1)
  - pdf  : AMR/AMR_PVC/<cote>_<année>/RBX_AMR_PVC_<cote>_<date>.pdf
  - txt  : idem .txt
Les 282 pdf/txt nommés PVC_<date>.* sont normalisés en RBX_ ; la cote vient
des alto de la même séance (une cote 246W/498W couvre plusieurs séances, mais
chaque séance a une cote et une date uniques).
Publication : jpg alignés sur les alto (`inconnu`) ; txt `jamais` préexistant ;
aucun IR ne référence ces séances (finding_aid vide partout).

Entrée : results/ref/amr_pvc.csv (19 170 lignes, source2s3 AMR_CADN_378-1000)
Sortie : results/ref/amr_pvc_fin.csv
Audit  : results/ref/_amr_pvc_cible_audit_20260630.csv
Ne touche JAMAIS s3_key (invariant S3 réel).
"""
import os
import sys
import datetime
import pandas as pd

SRC = "results/ref/amr_pvc.csv"
OUT = "results/ref/amr_pvc_fin.csv"
AUDIT = "results/ref/_amr_pvc_cible_audit_20260630.csv"
TODAY = datetime.date.today().isoformat()
COTE_RE = r"(?:246|498)W\d{3}"

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
print("shape lue:", df.shape)
assert len(df) == 19170, f"attendu 19170, obtenu {len(df)}"
assert (df["corpus_code"] == "AMR_PVC").all(), "corpus_code != AMR_PVC"
assert df["source2s3"].str.startswith("AMR_CADN").all(), "source hors CADN"
assert "s3_key_cible" not in df.columns, "s3_key_cible déjà présente"
assert (df["s3_key"] == "").all(), "s3_key réelle inattendue sur AMR_PVC"
assert (df["finding_aid"] == "").all(), "IR inattendu sur AMR_PVC"

ext = df["extension"].str.lower()
inc = df["conservation_statut"] == "INCONNU"
dde = df["conservation_statut"] == "DDE - NE PAS GARDER (DIFFUSION)"
assert int(inc.sum()) == 9726 and int(dde.sum()) == 9444, "répartition statuts inattendue"
assert set(ext[inc]) == {".alto", ".pdf", ".txt"} and set(ext[dde]) == {".jpg"}
seance = df["path"].str.extract(r"(PVC_\d{8})")[0]
assert seance.notna().all(), "ligne sans séance PVC_<date> dans le path"

# --- mapping séance -> (cote, date) via les alto --------------------------
alto = ext == ".alto"
m = df.loc[alto, "name"].str.extract(rf"^RBX_AMR_PVC_({COTE_RE})_(\d{{8}})_(\d{{4}})\.alto$")
assert not m.isna().any().any(), "nom alto non conforme"
amap = pd.DataFrame({"seance": seance[alto], "cote": m[0], "date": m[1], "seq": m[2]})
sc = amap.groupby("seance")[["cote", "date"]].nunique()
assert len(sc) == 141 and (sc == 1).all().all(), "séance sans cote/date unique"
lut = amap.groupby("seance")[["cote", "date"]].first()
assert (lut["date"] == pd.Series(lut.index, index=lut.index).str[4:]).all(), \
    "date des alto != date du dossier séance"

cote = seance.map(lut["cote"])
date = seance.map(lut["date"])
assert cote.notna().all(), "séance absente du mapping alto"
folder = "AMR/AMR_PVC/" + cote + "_" + date.str[:4] + "/"
stem = "RBX_AMR_PVC_" + cote + "_" + date

# --- clés cibles -----------------------------------------------------------
df["s3_key_cible"] = ""
# alto -> .xml (séquence déjà à 4 chiffres)
df.loc[alto, "s3_key_cible"] = folder[alto] + df.loc[alto, "name"].str[:-5] + ".xml"
# jpg : même stem que l'alto apparié
jpg = ext == ".jpg"
assert set(df.loc[jpg, "name"].str[:-4]) == set(df.loc[alto, "name"].str[:-5]), \
    "appariement jpg↔alto rompu (stems différents)"
mj = df.loc[jpg, "name"].str.extract(rf"^RBX_AMR_PVC_({COTE_RE})_(\d{{8}})_\d{{4}}\.jpg$")
assert not mj.isna().any().any(), "nom jpg non conforme"
df.loc[jpg, "s3_key_cible"] = ("AMR/AMR_PVC/" + mj[0] + "_" + mj[1].str[:4] + "/"
                               + df.loc[jpg, "name"])
# pdf/txt : 1 par séance, nom normalisé RBX_
for e in (".pdf", ".txt"):
    sel = ext == e
    assert int(sel.sum()) == 141 and seance[sel].nunique() == 141, f"{e}: != 141 séances"
    assert df.loc[sel, "name"].str.match(rf"^PVC_\d{{8}}\{e}$").all(), f"nom {e} non conforme"
    df.loc[sel, "s3_key_cible"] = folder[sel] + stem[sel] + e

df["conservation_statut"] = "À TRANSFERER"
# publication : jpg alignés sur les alto (hors IR) ; alto/pdf inconnu, txt jamais
assert (df.loc[jpg, "publication_statut"] == "jamais").all()
df.loc[jpg, "publication_statut"] = "inconnu"

# --- contrôles -------------------------------------------------------------
posed = df["s3_key_cible"]
assert (posed != "").all() and posed.is_unique, "clé manquante ou collision"
assert posed.str.match(
    rf"^AMR/AMR_PVC/{COTE_RE}_(19|20)\d{{2}}/RBX_AMR_PVC_{COTE_RE}_\d{{8}}(_\d{{4}})?\.(xml|jpg|pdf|txt)$").all()
assert posed[posed.str.endswith((".xml", ".jpg"))].str.match(r".*_\d{4}\.(xml|jpg)$").all()

# --- date_maj_ligne (v2, rerun-safe) ---------------------------------------
_chg = (
    (df["conservation_statut"].values != orig["conservation_statut"].values)
    | (df["publication_statut"].values != orig["publication_statut"].values)
    | (df["s3_key_cible"].values != orig["s3_key"].values)
)
df["date_maj_ligne"] = ""
df.loc[_chg, "date_maj_ligne"] = TODAY
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
print("clés posées:", int((posed != "").sum()), "| dossiers cibles:", posed.str.split("/").str[2].nunique())
print("par série:", posed.str.split("/").str[2].str[:4].value_counts().to_dict())
print("date_maj_ligne datées:", int((df["date_maj_ligne"] != "").sum()))

audit = df[["name", "path", "extension", "conservation_statut",
            "publication_statut", "s3_key_cible", "date_maj_ligne"]].copy()
audit.to_csv(AUDIT, index=False)
print("audit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    df.to_csv(OUT, index=False)
    print("écrit:", OUT, df.shape)
else:
    print("DRY-RUN (--apply pour écrire", OUT + ")")
