#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Intègre au ref global les *_fin.csv produits les 2026-07-10 (soir) et
2026-07-11 : statuts conservation/publication + colonnes s3_key_cible et
date_maj_ligne (ajoutées au ref, vides hors périmètre).

PRÉCAUTION (demande utilisateur) : les « À TRANSFERER » des fins deviennent
« À TRANSFERER APRES VALIDATION » dans le ref (état pré-upload, à valider —
contrairement aux corpus du 09/07 intégrés en TRANSFERT_S3_OK après upload réel).

- fusion par uuid, uniquement sur les lignes couvertes par les fins ;
- s3_key réelle JAMAIS modifiée ; TRANSFERT_S3_OK inchangés ;
- backup du ref courant en _old3 avant écriture (--apply).
"""
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
BAK = "results/ref/_ref_files_20260630_old3.csv.gz"
FINS = [
    "amr_del_cadn",  # 2026-07-10 soir (révisé 11) — sous-ensemble CADN d'AMR_DEL
    "amr_pho", "med_phd", "med_pbi", "amr_dia", "amr_507w", "amr_3d",
    "amr_pop", "amr_pvc", "amr_puv", "amr_vic",
]
RENAME = {"À TRANSFERER": "À TRANSFERER APRES VALIDATION"}

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)
assert "s3_key_cible" not in ref.columns and "date_maj_ligne" not in ref.columns

fin = pd.concat(
    [pd.read_csv(f"results/ref/{c}_fin.csv", dtype=str, keep_default_na=False)
     for c in FINS], ignore_index=True)
print("fins concaténés:", fin.shape, "| corpus:", fin["corpus_code"].nunique())
assert fin["uuid"].is_unique, "uuid dupliqué entre fins"
fin["conservation_statut"] = fin["conservation_statut"].replace(RENAME)

hit = ref["uuid"].isin(set(fin["uuid"]))
assert int(hit.sum()) == len(fin), f"couverture ref: {int(hit.sum())} != {len(fin)}"
assert ref.loc[hit, "uuid"].is_unique, "uuid dupliqué dans le ref sur le périmètre"

fx = fin.set_index("uuid")
align = fx.reindex(ref.loc[hit, "uuid"])
# garde-fou : même ligne source (nom + s3_key réelle intacte)
assert (align["name"].values == ref.loc[hit, "name"].values).all(), "désalignement name"
assert (align["s3_key"].values == ref.loc[hit, "s3_key"].values).all(), "s3_key divergerait"

before = ref.loc[hit, "conservation_statut"].value_counts().to_dict()
ref["s3_key_cible"] = ""
ref["date_maj_ligne"] = ""
for col in ("conservation_statut", "publication_statut", "s3_key_cible", "date_maj_ligne"):
    ref.loc[hit, col] = align[col].values

after = ref.loc[hit, "conservation_statut"].value_counts().to_dict()
# 29 « À TRANSFERER » préexistants (24 AMR_AFF CADN + 5 AMR_PLA, reliquats
# des chantiers antérieurs) restent volontairement intouchés
assert int((ref.loc[hit, "conservation_statut"] == "À TRANSFERER").sum()) == 0, \
    "À TRANSFERER nu restant dans le périmètre des fins"
reste = ref.loc[~hit & (ref["conservation_statut"] == "À TRANSFERER"), "corpus_code"]
print("À TRANSFERER hors périmètre laissés tels quels:", reste.value_counts().to_dict())
posed = ref.loc[ref["s3_key_cible"] != "", "s3_key_cible"]
newk = ref.loc[(ref["s3_key_cible"] != "") & (ref["s3_key"] == ""), "s3_key_cible"]
assert posed.is_unique, "collision s3_key_cible dans le ref"
assert not (set(newk) & set(ref.loc[ref["s3_key"] != "", "s3_key"])), "cible en collision avec une s3_key réelle"

print("\nlignes touchées:", int(hit.sum()), "sur", len(ref))
print("conservation avant:", before)
print("conservation après:", after)
print("s3_key_cible posées:", len(posed), "| nouvelles (hors S3):", len(newk))
print("date_maj_ligne:", ref.loc[hit, "date_maj_ligne"].value_counts().to_dict())

if "--apply" in sys.argv:
    assert not os.path.exists(BAK), f"{BAK} existe déjà"
    shutil.copy2(REF, BAK)
    print("backup:", BAK)
    ref.to_csv(REF, index=False, compression="gzip")
    print("écrit:", REF, ref.shape)
else:
    print("DRY-RUN (--apply pour écrire)")
