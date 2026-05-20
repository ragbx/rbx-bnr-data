from os.path import join
from uuid import uuid4
import pandas as pd

old_ref_date = "20251226"
new_ref_date = "20260502"

az = pd.read_csv(join("data", "az", f"bnr_azrael_{new_ref_date}.csv.gz"))
az = az[~az['name'].str[-3:].isin(['.db', 'lnk'])]
az['path'] = az['path'].str.replace("\\", "/")

# on charge les fichiers présents dans le ref précédent et non trouvé dans le nouvel az
new_ref_az_ko_right = pd.read_csv(
    join("results", "ref", "tmp", f"new_ref_az_ko_right_it1_{new_ref_date}.csv.gz")
)
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

# on charge les fichiers du nouvel az non trouvés dans le ref précédent, pour lesquels on a calcué le checksum
new_ref_az_ko_left_cs = pd.read_csv(
    join("data", "az", f"bnr_azrael_{new_ref_date}_nouuid-cs.csv.gz")
)
new_ref_az_ko_left_cs = new_ref_az_ko_left_cs[~new_ref_az_ko_left_cs['name'].str[-3:].isin(['.db', 'lnk'])]
# new_ref_az_ko_left_cs = new_ref_az_ko_left_cs.drop(columns=['filename'])

# on traite les nouveaux fichiers
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


# on essaie de gérer les doublons
# new_ref_az2 = new_ref_az_ko_left_cs_csdupl.merge(
#     new_ref_az_ko_right_csdupl,
#     left_on=["name", "checksum_md5"],
#     right_on=["ref_name", "ref_checksum_md5"],
#     how="left",
# )
# new_ref_az2_ok = new_ref_az2[
#     (~new_ref_az2["ref_uuid"].isna()) & (~new_ref_az2["name"].isna())
# ]
# new_ref_az2_ok["uuid"] = new_ref_az2_ok["ref_uuid"]
# new_ref_az2_ok = new_ref_az2_ok[
#     [
#         "name",
#         "path",
#         "size",
#         "last_content_modification_date",
#         "last_metadata_modification_date",
#         "checksum_md5",
#         "uuid",
#     ]
# ]
# new_ref_az2_ko = new_ref_az2[
#     (new_ref_az2["ref_uuid"].isna()) & (~new_ref_az2["name"].isna())
# ]
# new_ref_az2_ko = new_ref_az2_ko[
#     [
#         "name",
#         "path",
#         "size",
#         "last_content_modification_date",
#         "last_metadata_modification_date",
#         "checksum_md5",
#         "uuid",
#     ]
# ]

new_ref_az_ok_tmp = pd.read_csv(join("results", "ref", "tmp", f"new_ref_az_ok_it1_{new_ref_date}.csv.gz"))
new_ref_az_ok_it2 = pd.concat([new_ref_az_ok_tmp, new_ref_az_ok, new_files])
new_ref_az_ok_it2.to_csv(join("results", "ref", "tmp", f"new_ref_az_ok_it2_{new_ref_date}.csv.gz"), index=False)
print(f"new_ref_az_ok_it2 : {len(new_ref_az_ok_it2)}")

new_ref_az_ko_left_no_dupl_it2 = new_ref_az_ko
new_ref_az_ko_left_no_dupl_it2.to_csv(join("results", "ref", "tmp", f"new_ref_az_ko_left_no_dupl_it2_{new_ref_date}.csv"), index=False)
print(f"new_ref_az_ko_left_no_dupl_it2 : {len(new_ref_az_ko_left_no_dupl_it2)}")

new_ref_az_ko_right_no_dupl_it2 = new_ref_az_ko_right_csnodupl[~new_ref_az_ko_right_csnodupl['ref_uuid'].isin(new_ref_az_ok_it2['uuid'])]
new_ref_az_ko_right_no_dupl_it2 = new_ref_az_ko_right_no_dupl_it2[
    ( new_ref_az_ko_right_no_dupl_it2['ref_checksum_md5'].isin(new_ref_az_ok_it2['checksum_md5']) )
  | ( new_ref_az_ko_right_no_dupl_it2['ref_checksum_md5'].isin(new_ref_az_ko_left_no_dupl_it2['checksum_md5']) )
]
new_ref_az_ko_right_no_dupl_it2.to_csv(join("results", "ref", "tmp", f"new_ref_az_ko_right_no_dupl_it2_{new_ref_date}.csv"), index=False)
print(f"new_ref_az_ko_right_no_dupl_it2 : {len(new_ref_az_ko_right_no_dupl_it2)}")

new_ref_az_ko_left_csdupl_it2 = new_ref_az_ko_left_csdupl
new_ref_az_ko_left_csdupl_it2.to_csv(join("results", "ref", "tmp", f"new_ref_az_ko_left_csdupl_it2_{new_ref_date}.csv"), index=False)
print(f"new_ref_az_ko_left_csdupl_it2: {len(new_ref_az_ko_left_csdupl_it2)}")

new_ref_az_ko_right_csdupl_it2 = new_ref_az_ko_right_csdupl
new_ref_az_ko_right_csdupl_it2 = new_ref_az_ko_right_csdupl_it2[
    ( new_ref_az_ko_right_csdupl_it2['ref_checksum_md5'].isin(new_ref_az_ok_it2['checksum_md5']) )
  | ( new_ref_az_ko_right_csdupl_it2['ref_checksum_md5'].isin(new_ref_az_ko_left_csdupl_it2['checksum_md5']) )
]
new_ref_az_ko_right_csdupl_it2.to_csv(join("results", "ref", "tmp", f"new_ref_az_ko_right_csdupl_it2_{new_ref_date}.csv"), index=False)
print(f"new_ref_az_ko_right_csdupl_it2: {len(new_ref_az_ko_right_csdupl_it2)}")

print(len(az) == len(new_ref_az_ok_it2) + len(new_ref_az_ko_left_no_dupl_it2) + len(new_ref_az_ko_left_csdupl))

#len(new_ref_az_ok) + len(new_ref_az_ko) + len(new_ref_az_ko_left_cs_csdupl)
