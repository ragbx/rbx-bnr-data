from os.path import join

import pandas as pd

"""
ref_az = les fichiers az contenant dans le dernier ref

az = la dernière extraction de azrael

new_ref_az = az + uuid mais attention outer
    |
    |--  new_ref_az_ok : uuid trouvé
    |
    |--  new_ref_az_ko_left : correspondance ref dans az non trouvé
    |
    |--  new_ref_az_ko_right : correspondance az dans ref non trouvé

donc :
    len(az) == len(new_ref_az_ok) + len(new_ref_az_ko_left)
"""

old_ref_date = '20251226'
new_ref_date = '20260502'

ref = pd.read_csv(join("results", "ref", f"_ref_files_{old_ref_date}.csv.gz"))
ref = ref[~ref['name'].str[-3:].isin(['.db', 'lnk'])]
ref['path'] = ref['path'].str.replace("\\", "/")

# uniquement pour les fichiers antérieurs à 2026-05-01
ref["m"] = ref["last_content_modification_date"]
ref["c"] = ref["last_metadata_modification_date"]
ref["last_metadata_modification_date"] = ref["m"]
ref["last_content_modification_date"] = ref["c"]

ref_az = ref[ref['source2s3'] == 'az']
ref_notaz = ref[ref['source2s3'] != 'az']
ref_notaz.to_csv(join("results", "ref", "tmp", f"ref_notaz_{new_ref_date}.csv.gz"), index=False)
ref_az = ref_az[["name", "path", "size", "last_content_modification_date",
        "last_metadata_modification_date", "checksum_md5", "uuid"]]

ref_az.columns = ["ref_name", "ref_path", "ref_size", "ref_last_content_modification_date",
        "ref_last_metadata_modification_date", "ref_checksum_md5", "ref_uuid"]

az = pd.read_csv(join("data", "az", f"bnr_azrael_{new_ref_date}.csv.gz"))
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
new_ref_az_ok.to_csv(join("results", "ref", "tmp", f"new_ref_az_ok_it1_{new_ref_date}.csv.gz"), index=False)


print(len(new_ref_az_ko_left))
new_ref_az_ko_left = new_ref_az_ko_left[['name', 'path', 'size', 'last_content_modification_date',
                  'last_metadata_modification_date', 'last_content_modification_date_', 'last_metadata_modification_date_']]
new_ref_az_ko_left.to_csv(join("results", "ref", "tmp", f"new_ref_az_ko_left_it1_{new_ref_date}.csv.gz"), index=False)

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
new_ref_az_ko_right.to_csv(join("results", "ref", "tmp", f"new_ref_az_ko_right_it1_{new_ref_date}.csv.gz"), index=False)
