"""Étape 4 — appariement sur le checksum (cas sans doublons) + nouveaux fichiers.

On apparie les az résiduels désormais checksummés (s3_cs__az) avec les ref résiduels
(s2_size__ref) sur (name, checksum_md5). Les fichiers réellement nouveaux (créés et
modifiés en 2026) reçoivent un uuid neuf.

Entrées :
    - extraction az  : bnr_azrael_{NEW_REF_DATE}.csv.gz
    - s2_size__ref   : ref résiduels (étape 2)
    - s3_cs__az      : az résiduels avec checksum (étape 3)
    - s1_meta__ok, s2_size__ok : « ok » déjà résolus (étapes 1 et 2), pour le cumul

Sorties (results/ref/tmp/) :
    - _ok_cumul_s4     : cumul des « ok » jusqu'à l'étape 4 (.csv.gz)  -> étape 5
    - s4_cs__az        : az non résolus, sans doublon (.csv)           -> étape 5 (info)
    - s4_cs__ref       : ref candidats restants, sans doublon (.csv)
    - s4_csdupl__az    : az à doublons de checksum (.csv)              -> étape 5
    - s4_csdupl__ref   : ref à doublons de checksum (.csv)            -> étape 5
"""

from uuid import uuid4

import pandas as pd

from _pipeline import NEW_REF_DATE, az_file, tmp_file

az = pd.read_csv(az_file(NEW_REF_DATE))
az = az[~az['name'].str[-3:].isin(['.db', 'lnk'])]
az['path'] = az['path'].str.replace("\\", "/")

# ref résiduels de l'étape 2 (non trouvés dans le nouvel az par métadonnées/taille)
new_ref_az_ko_right = pd.read_csv(tmp_file("s2_size__ref"))
new_ref_az_ko_right = new_ref_az_ko_right[~new_ref_az_ko_right['name'].str[-3:].isin(['.db', 'lnk'])]

new_ref_az_ko_right.columns = [
    "ref_name",
    "ref_path",
    "ref_size",
    "ref_last_content_modification_date",
    "ref_last_metadata_modification_date",
    "ref_last_content_modification_date_",
    "ref_last_metadata_modification_date_",
    "ref_checksum_md5",
    "ref_uuid"
]

# az résiduels avec checksum calculé à l'étape 3
new_ref_az_ko_left_cs = pd.read_csv(tmp_file("s3_cs__az"))
new_ref_az_ko_left_cs = new_ref_az_ko_left_cs[~new_ref_az_ko_left_cs['name'].str[-3:].isin(['.db', 'lnk'])]

# on traite les nouveaux fichiers (créés ET modifiés en 2026)
new_files = new_ref_az_ko_left_cs[(new_ref_az_ko_left_cs['last_metadata_modification_date_'].str[0:4] == '2026') &
         (new_ref_az_ko_left_cs['last_content_modification_date_'].str[0:4] == '2026')]
new_ref_az_ko_left_cs['filename'] = new_ref_az_ko_left_cs['path'] + "/" + new_ref_az_ko_left_cs['name']
new_files['filename'] = new_files['path'] + "/" + new_files['name']
new_ref_az_ko_left_cs = new_ref_az_ko_left_cs[~new_ref_az_ko_left_cs['filename'].isin(new_files['filename'])]
new_ref_az_ko_left_cs = new_ref_az_ko_left_cs.drop(columns='filename')
new_files = new_files.drop(columns='filename')
new_files['uuid'] = new_files['uuid'].apply(lambda x: uuid4().hex)


# on distingue les cas avec et sans doublons
new_ref_az_ko_right_csnodupl = new_ref_az_ko_right[
    ~new_ref_az_ko_right.duplicated(subset=["ref_name", "ref_checksum_md5"], keep=False)
]

new_ref_az_ko_left_csnodupl = new_ref_az_ko_left_cs[
    ~new_ref_az_ko_left_cs.duplicated(subset=["name", "checksum_md5"], keep=False)
]

new_ref_az_ko_left_csdupl = new_ref_az_ko_left_cs[
    new_ref_az_ko_left_cs.duplicated(subset=["name", "checksum_md5"], keep=False)
]

new_ref_az_ko_right_csdupl = new_ref_az_ko_right[
    new_ref_az_ko_right.duplicated(subset=["ref_name", "ref_checksum_md5"], keep=False)
]


# on doit trouver une valeur vraie
print(len(new_ref_az_ko_left_cs) == len(new_ref_az_ko_left_csnodupl) + len(new_ref_az_ko_left_csdupl))

# on traite les cas sans doublons
new_ref_az = new_ref_az_ko_left_csnodupl.merge(
    new_ref_az_ko_right_csnodupl,
    left_on=["name", "checksum_md5"],
    right_on=["ref_name", "ref_checksum_md5"],
    how="left",
)
new_ref_az_ok = new_ref_az[
    (~new_ref_az["ref_uuid"].isna()) & (~new_ref_az["name"].isna())
]
new_ref_az_ok["uuid"] = new_ref_az_ok["ref_uuid"]
new_ref_az_ok = new_ref_az_ok[
    [
        "name",
        "path",
        "size",
        "last_content_modification_date",
        "last_content_modification_date_",
        "last_metadata_modification_date",
        "last_metadata_modification_date_",
        "checksum_md5",
        "uuid"
    ]
]
new_ref_az_ko = new_ref_az[
    (new_ref_az["ref_uuid"].isna()) & (~new_ref_az["name"].isna())
]
new_ref_az_ko = new_ref_az_ko[
    [
        "name",
        "path",
        "size",
        "last_content_modification_date",
        "last_content_modification_date_",
        "last_metadata_modification_date",
        "last_metadata_modification_date_",
        "checksum_md5",
        "uuid"
    ]
]
print( len(new_ref_az_ko_left_csnodupl) == len(new_ref_az_ok) + len(new_ref_az_ko))


# cumul des « ok » : étape 1 + étape 2 + étape 4 (sans doublon) + nouveaux fichiers
new_ref_az_ok_s1 = pd.read_csv(tmp_file("s1_meta__ok"))
new_ref_az_ok_s2 = pd.read_csv(tmp_file("s2_size__ok"))
new_ref_az_ok_it2 = pd.concat([new_ref_az_ok_s1, new_ref_az_ok_s2, new_ref_az_ok, new_files])
new_ref_az_ok_it2.to_csv(tmp_file("_ok_cumul_s4"), index=False)
print(f"_ok_cumul_s4 : {len(new_ref_az_ok_it2)}")

new_ref_az_ko_left_no_dupl_it2 = new_ref_az_ko
new_ref_az_ko_left_no_dupl_it2.to_csv(tmp_file("s4_cs__az", ext="csv"), index=False)
print(f"s4_cs__az : {len(new_ref_az_ko_left_no_dupl_it2)}")

new_ref_az_ko_right_no_dupl_it2 = new_ref_az_ko_right_csnodupl[~new_ref_az_ko_right_csnodupl['ref_uuid'].isin(new_ref_az_ok_it2['uuid'])]
new_ref_az_ko_right_no_dupl_it2 = new_ref_az_ko_right_no_dupl_it2[
    ( new_ref_az_ko_right_no_dupl_it2['ref_checksum_md5'].isin(new_ref_az_ok_it2['checksum_md5']) )
  | ( new_ref_az_ko_right_no_dupl_it2['ref_checksum_md5'].isin(new_ref_az_ko_left_no_dupl_it2['checksum_md5']) )
]
new_ref_az_ko_right_no_dupl_it2.to_csv(tmp_file("s4_cs__ref", ext="csv"), index=False)
print(f"s4_cs__ref : {len(new_ref_az_ko_right_no_dupl_it2)}")

new_ref_az_ko_left_csdupl_it2 = new_ref_az_ko_left_csdupl
new_ref_az_ko_left_csdupl_it2.to_csv(tmp_file("s4_csdupl__az", ext="csv"), index=False)
print(f"s4_csdupl__az : {len(new_ref_az_ko_left_csdupl_it2)}")

new_ref_az_ko_right_csdupl_it2 = new_ref_az_ko_right_csdupl
new_ref_az_ko_right_csdupl_it2 = new_ref_az_ko_right_csdupl_it2[
    ( new_ref_az_ko_right_csdupl_it2['ref_checksum_md5'].isin(new_ref_az_ok_it2['checksum_md5']) )
  | ( new_ref_az_ko_right_csdupl_it2['ref_checksum_md5'].isin(new_ref_az_ko_left_csdupl_it2['checksum_md5']) )
]
new_ref_az_ko_right_csdupl_it2.to_csv(tmp_file("s4_csdupl__ref", ext="csv"), index=False)
print(f"s4_csdupl__ref : {len(new_ref_az_ko_right_csdupl_it2)}")

print(len(az) == len(new_ref_az_ok_it2) + len(new_ref_az_ko_left_no_dupl_it2) + len(new_ref_az_ko_left_csdupl))
