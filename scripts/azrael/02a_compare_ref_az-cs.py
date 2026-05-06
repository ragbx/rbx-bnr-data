from os.path import join

import pandas as pd

old_ref_date = '20251226'
new_ref_date = '20260502'

new_ref_az_ko_right = pd.read_csv(join("results", "ref", "tmp", f"new_ref_az_ko_right_{new_ref_date}.csv.gz"))
new_ref_az_ko_right.columns = ["ref_name", "ref_path", "ref_size", "ref_last_content_modification_date",
        "ref_last_metadata_modification_date", "ref_checksum_md5", "ref_uuid"]
new_ref_az_ko_left_cs = pd.read_csv(join("data", "az", f"bnr_azrael_{new_ref_date}_nouuid-cs.csv.gz"))
#new_ref_az_ko_left_cs = new_ref_az_ko_left_cs.drop(columns=['filename'])

# on retire les doublons cs
new_ref_az_ko_right_csnodupl = new_ref_az_ko_right[~new_ref_az_ko_right.duplicated(subset='ref_checksum_md5', keep=False)]
new_ref_az_ko_left_cs_csnodupl = new_ref_az_ko_left_cs[~new_ref_az_ko_left_cs.duplicated(subset='checksum_md5', keep=False)]

new_ref_az = new_ref_az_ko_left_cs_csnodupl.merge(
    new_ref_az_ko_right_csnodupl,
    left_on=["name", "checksum_md5"],
    right_on=["ref_name", "ref_checksum_md5"],
    how="left"
)
new_ref_az_ok = new_ref_az[(~new_ref_az['ref_uuid'].isna()) & (~new_ref_az['name'].isna())]
new_ref_az_ko = new_ref_az[(new_ref_az['ref_uuid'].isna()) & (~new_ref_az['name'].isna())]

# on essaie de gérer les doublons
new_ref_az_ko_right_csdupl = new_ref_az_ko_right[new_ref_az_ko_right.duplicated(subset='ref_checksum_md5', keep=False)]
new_ref_az_ko_left_cs_csdupl = new_ref_az_ko_left_cs[new_ref_az_ko_left_cs.duplicated(subset='checksum_md5', keep=False)]

new_ref_az2 = new_ref_az_ko_left_cs_csnodupl.merge(
    new_ref_az_ko_right_csnodupl,
    left_on=["name", "checksum_md5"],
    right_on=["ref_name", "ref_checksum_md5"],
    how="left"
)
new_ref_az2_ok = new_ref_az2[(~new_ref_az2['ref_uuid'].isna()) & (~new_ref_az2['name'].isna())]
new_ref_az2_ko = new_ref_az2[(new_ref_az2['ref_uuid'].isna()) & (~new_ref_az2['name'].isna())]
