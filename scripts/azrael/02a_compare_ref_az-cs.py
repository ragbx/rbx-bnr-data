from os.path import join

import pandas as pd

old_ref_date = '20251226'
new_ref_date = '20260502'

ref_ko = pd.read_csv(join("results", "ref", "tmp", f"new_ref_az_ko_right_{new_ref_date}.csv.gz"))
ref_ko.columns = ["ref_name", "ref_path", "ref_size", "ref_last_content_modification_date",
        "ref_last_metadata_modification_date", "ref_checksum_md5", "ref_uuid"]
az_cs = pd.read_csv(join("data", "az", f"bnr_azrael_{new_ref_date}_nouuid-cs.csv.gz"))
#az_cs = az_cs.drop(columns=['filename'])

# on retire les doublons cs
ref_ko_csnodupl = ref_ko[~ref_ko.duplicated(subset='ref_checksum_md5', keep=False)]
az_cs_csnodupl = az_cs[~az_cs.duplicated(subset='checksum_md5', keep=False)]

new_ref_az = az_cs_csnodupl.merge(
    ref_ko_csnodupl,
    left_on=["name", "checksum_md5"],
    right_on=["ref_name", "ref_checksum_md5"],
    how="outer"
)
new_ref_az_ok = new_ref_az[(~new_ref_az['ref_uuid'].isna()) & (~new_ref_az['name'].isna())]
new_ref_az_ko = new_ref_az[(new_ref_az['ref_uuid'].isna()) & (~new_ref_az['name'].isna())]

# on essaie de gérer les doublons
ref_ko_csdupl = ref_ko[ref_ko.duplicated(subset='ref_checksum_md5', keep=False)]
az_cs_csdupl = az_cs[az_cs.duplicated(subset='checksum_md5', keep=False)]
