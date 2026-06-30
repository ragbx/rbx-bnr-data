"""Étape 1 — appariement sur les métadonnées exactes.

Entrées :
    - ref précédent  : _ref_files_{OLD_REF_DATE}.csv.gz   (uuid + checksum_md5)
    - extraction az  : bnr_azrael_{NEW_REF_DATE}.csv.gz    (sans uuid/checksum)

On apparie az <-> ref (partie 'az' du ref) sur la clé complète :
    (name, path, size, last_content_modification_date, last_metadata_modification_date)
Aucun checksum n'est relu : les fichiers inchangés récupèrent directement uuid +
checksum_md5 depuis le ref.

Sorties (results/ref/tmp/) :
    - ref_notaz_{date}         : partie NON-az du ref précédent (source2s3 != 'az').
                                 Hors champ du matching (l'extraction azrael ne couvre
                                 que les fichiers az) : mise de côté ici pour être
                                 reconcaténée par l'étape 55 (avant le maillon s3/dao/oai).
    - s1_meta__ok_{date}       : appariés (uuid + checksum_md5 récupérés)
    - s1_meta__az_{date}       : az non appariés        -> étape 2
    - s1_meta__ref_{date}      : ref non appariés       -> étape 2

    len(az) == len(s1_meta__ok) + len(s1_meta__az)
"""

import pandas as pd

from _pipeline import NEW_REF_DATE, OLD_REF_DATE, ref_file, az_file, tmp_file

ref = pd.read_csv(ref_file(OLD_REF_DATE))
ref = ref[~ref['name'].str[-3:].isin(['.db', 'lnk'])]
ref['path'] = ref['path'].str.replace("\\", "/")

# uniquement pour les fichiers antérieurs à 2026-05-01
#ref["m"] = ref["last_content_modification_date"]
#ref["c"] = ref["last_metadata_modification_date"]
#ref["last_metadata_modification_date"] = ref["m"]
#ref["last_content_modification_date"] = ref["c"]

ref_az = ref[ref['source2s3'] == 'az']
ref_notaz = ref[ref['source2s3'] != 'az']
ref_notaz.to_csv(tmp_file("ref_notaz"), index=False)
ref_az = ref_az[["name", "path", "size", "last_content_modification_date",
        "last_metadata_modification_date", "checksum_md5", "uuid"]]

ref_az.columns = ["ref_name", "ref_path", "ref_size", "ref_last_content_modification_date",
        "ref_last_metadata_modification_date", "ref_checksum_md5", "ref_uuid"]

az = pd.read_csv(az_file(NEW_REF_DATE))
az = az[~az['name'].str[-3:].isin(['.db', 'lnk'])]
az['path'] = az['path'].str.replace("\\", "/")

new_ref_az = az.merge(
    ref_az,
    left_on=["name", "path", "size", "last_content_modification_date",
            "last_metadata_modification_date"],
    right_on=["ref_name", "ref_path", "ref_size", "ref_last_content_modification_date",
            "ref_last_metadata_modification_date"],
    how="outer"
)

new_ref_az['last_content_modification_date_'] = pd.to_datetime(
                                new_ref_az["last_content_modification_date"], unit="s"
                                                  ).dt.strftime("%Y-%m-%d")
new_ref_az['last_metadata_modification_date_'] = pd.to_datetime(
                                new_ref_az["last_metadata_modification_date"], unit="s"
                                                ).dt.strftime("%Y-%m-%d")
new_ref_az['ref_last_content_modification_date_'] = pd.to_datetime(
                                new_ref_az["ref_last_content_modification_date"], unit="s"
                                                    ).dt.strftime("%Y-%m-%d")
new_ref_az['ref_last_metadata_modification_date_'] = pd.to_datetime(
                                new_ref_az["ref_last_metadata_modification_date"], unit="s"
                                                ).dt.strftime("%Y-%m-%d")

new_ref_az_ok_ = new_ref_az[~new_ref_az['ref_uuid'].isna()]
new_ref_az_ok = new_ref_az_ok_[~new_ref_az_ok_['name'].isna()]
new_ref_az_ko_right = new_ref_az_ok_[new_ref_az_ok_['name'].isna()]
new_ref_az_ko_left = new_ref_az[new_ref_az['ref_uuid'].isna()]
print(len(new_ref_az) == len(new_ref_az_ok) + len(new_ref_az_ko_left) + len(new_ref_az_ko_right))
print(len(az) == len(new_ref_az_ok) + len(new_ref_az_ko_left) )

print(len(new_ref_az_ok))
new_ref_az_ok = new_ref_az_ok[['name', 'path', 'size', 'last_content_modification_date', 'last_content_modification_date_',
                  'last_metadata_modification_date', 'last_metadata_modification_date_', 'ref_checksum_md5', 'ref_uuid']]
new_ref_az_ok.columns = ['name', 'path', 'size', 'last_content_modification_date', 'last_content_modification_date_',
                  'last_metadata_modification_date', 'last_metadata_modification_date_', 'checksum_md5', 'uuid']
new_ref_az_ok.to_csv(tmp_file("s1_meta__ok"), index=False)


print(len(new_ref_az_ko_left))
new_ref_az_ko_left = new_ref_az_ko_left[['name', 'path', 'size', 'last_content_modification_date',
                  'last_metadata_modification_date', 'last_content_modification_date_', 'last_metadata_modification_date_']]
new_ref_az_ko_left.to_csv(tmp_file("s1_meta__az"), index=False)

print(len(new_ref_az_ko_right))
new_ref_az_ko_right = new_ref_az_ko_right[['ref_name', 'ref_path', 'ref_size', 'ref_last_content_modification_date',
                  'ref_last_metadata_modification_date', 'ref_last_content_modification_date_', 'ref_last_metadata_modification_date_', 'ref_checksum_md5', 'ref_uuid']]
new_ref_az_ko_right.columns = ['name', 'path', 'size', 'last_content_modification_date',
                  'last_metadata_modification_date', 'last_content_modification_date_', 'last_metadata_modification_date_', 'checksum_md5', 'uuid']
new_ref_az_ko_right['last_content_modification_date_'] = pd.to_datetime(
                                new_ref_az_ko_right["last_metadata_modification_date"], unit="s"
                                                  ).dt.strftime("%Y-%m-%d")
new_ref_az_ko_right['last_metadata_modification_date_'] = pd.to_datetime(
                                new_ref_az_ko_right["last_metadata_modification_date"], unit="s"
                                                ).dt.strftime("%Y-%m-%d")
new_ref_az_ko_right.to_csv(tmp_file("s1_meta__ref"), index=False)
