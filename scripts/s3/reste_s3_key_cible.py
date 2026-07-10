#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backlog transfert « EN LIGNE - À TRANSFERER ? » — LOT « tout le reste » hors
MED_MS (2026-07-09). Traite les 12 corpus restants en une passe.

Règle générale : `s3_key_cible = <PREFIX>/<name>` (plat), PREFIX = ancre S3
réelle si le corpus est déjà versé, sinon convention `<FAMILLE>/<corpus_code>`.
On ne touche NI `s3_key`, NI (pour les masters) conservation/publication.

Dédup uniforme : toute ligne dont le CONTENU (checksum_md5) est déjà présent
sur S3 (n'importe quelle clé) est rétrogradée `À SUPPRIMER` + `jamais` sans
s3_key_cible (précédent OBS grp A). En pratique 2026-07-09 : seul
AMR_PLA en a 1 (`6Fi001.jpg` == `AMR/AMR_PR/RBX_AMR_PR_FLO_038.jpg`).

Fixups de nom pour la CLÉ seulement (colonne `name` = nom physique inchangée,
⚠️ fichier physique à renommer au versement) :
  - MED_MON : `.tif.tif` -> `.tif` (87/89 ; les versés S3 sont en .tif simple).

Décisions utilisateur 2026-07-09 :
  - AMR_CAD (jpg+pdf+tif) : verser TOUT à plat (les 247 sont « À TRANSFERER »,
    0 doublon binaire, 159 docs jpg-only).
  - MED_MON : clé corrigée `.tif`.

Ancres vérifiées : MED_PHO (1063), MED_CP (4525), MED_IMA (5614), MED_MON
(7462), LAR_PUB (3730), AMR_PR (822 à plat, catch-all CAN/PLA/FLO...) déjà
versés à leur PREFIX. Sans ancre (convention) : MED_PLA, MED_MAR, MED_FOO,
ARA_CPS, AMR_CAD, AMR_PLA.

Source .gz lue et réécrite en place : results/ref/_ref_files_20260630_dedup.csv.gz
Audit : results/ref/_dedup_reste_audit_20260630.csv
"""
import sys
import pandas as pd

GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"
AUDIT = "results/ref/_dedup_reste_audit_20260630.csv"

# corpus -> (prefix S3, nb backlog attendu)
CFG = {
    "MED_PHO": ("MED/MED_PHO/", 275),
    "MED_PLA": ("MED/MED_PLA/", 271),
    "AMR_CAD": ("AMR/AMR_CAD/", 247),
    "MED_CP":  ("MED/MED_CP/",  183),
    "MED_IMA": ("MED/MED_IMA/", 180),
    "ARA_CPS": ("ARA/ARA_CPS/", 146),
    "AMR_PLA": ("AMR/AMR_PLA/", 115),
    "AMR_PR":  ("AMR/AMR_PR/",  110),
    "MED_MON": ("MED/MED_MON/",  89),
    # LAR_PUB traité à part (scripts/s3/lar_pub_s3_key_cible.py) : conflit de
    # clé CDR_013 (80 fichiers même nom / contenu différent d'un CDR_013 versé).
    "MED_MAR": ("MED/MED_MAR/",  62),
    "MED_FOO": ("MED/MED_FOO/",   6),
}

def key_name(corpus, name):
    if corpus == "MED_MON" and name.endswith(".tif.tif"):
        return name[:-4]  # .tif.tif -> .tif
    return name

df = pd.read_csv(GZ, dtype=str, keep_default_na=False)
print("shape lue:", df.shape)

s3_ck = set(df.loc[df["s3_key"] != "", "checksum_md5"])
real = set(df.loc[df["s3_key"] != "", "s3_key"])
audit_rows = []
n_master = n_retro = 0

for corpus, (prefix, expN) in CFG.items():
    mask = (df["conservation_statut"] == "EN LIGNE - À TRANSFERER ?") & (df["corpus_code"] == corpus)
    sel = df[mask]
    assert len(sel) == expN, f"{corpus}: attendu {expN}, obtenu {len(sel)}"
    assert (sel["s3_key"] == "").all(), f"{corpus}: s3_key non vide"
    assert (sel["s3_key_cible"] == "").all(), f"{corpus}: s3_key_cible déjà posée"
    assert (sel["publication_statut"] == "oui").all(), f"{corpus}: publication != oui"

    # dédup : contenu déjà sur S3 -> rétrograder
    is_retro = sel["checksum_md5"].isin(s3_ck)
    retro = sel[is_retro]
    master = sel[~is_retro]

    # cibles master
    keys = master["name"].map(lambda n: prefix + key_name(corpus, n))
    # collisions (vs réelles + cibles déjà posées cumulées + interne)
    c = set(keys)
    assert len(c) == len(keys), f"{corpus}: collision interne"
    inter_r = c & real
    inter_c = c & set(df.loc[df["s3_key_cible"] != "", "s3_key_cible"])
    assert not inter_r, f"{corpus}: collision vs s3_key réelles {list(inter_r)[:3]}"
    assert not inter_c, f"{corpus}: collision vs cibles existantes {list(inter_c)[:3]}"

    # application
    df.loc[master.index, "s3_key_cible"] = keys.values
    df.loc[retro.index, "conservation_statut"] = "À SUPPRIMER"
    df.loc[retro.index, "publication_statut"] = "jamais"
    n_master += len(master); n_retro += len(retro)
    print(f"{corpus:9} master {len(master):4} -> {prefix:14} | retro {len(retro)}")

    for _, r in master.iterrows():
        nm = r["name"]; kn = key_name(corpus, nm)
        audit_rows.append({"corpus_code": corpus, "name": nm, "path": r["path"],
                           "role": "master" + (" (nom clé corrigé)" if kn != nm else ""),
                           "new_conservation": r["conservation_statut"], "new_publication": r["publication_statut"],
                           "s3_key_cible": prefix + kn})
    for _, r in retro.iterrows():
        audit_rows.append({"corpus_code": corpus, "name": r["name"], "path": r["path"],
                           "role": "doublon (contenu déjà sur S3)",
                           "new_conservation": "À SUPPRIMER", "new_publication": "jamais", "s3_key_cible": ""})

print(f"\nTOTAL master {n_master} | retro {n_retro}")
audit = pd.DataFrame(audit_rows)
audit.to_csv(AUDIT, index=False)
print("audit écrit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    df.to_csv(GZ, index=False, compression="gzip")
    print("GZ réécrite:", GZ, df.shape)
else:
    print("DRY-RUN (ajouter --apply pour réécrire la .gz)")
