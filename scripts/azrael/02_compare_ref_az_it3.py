from os.path import join

import pandas as pd

old_ref_date = "20251226"
new_ref_date = "20260502"

# on traite les fichiers doublons
new_ref_az_ko_left_csdupl = pd.read_csv(join("results", "ref", "tmp", f"new_ref_az_ko_left_csdupl_it2_{new_ref_date}.csv"))
new_ref_az_ko_right_csdupl = pd.read_csv(join("results", "ref", "tmp", f"new_ref_az_ko_right_csdupl_it2_{new_ref_date}.csv"))

z = new_ref_az_ko_left_csdupl.merge(new_ref_az_ko_right_csdupl, left_on=['name', 'path'], right_on=['ref_name', 'ref_path'], how='outer')
z_ok = z[(~z['ref_uuid'].isna()) & (~z['name'].isna())]
z_ok["uuid"] = z_ok["ref_uuid"]
z_ok = z_ok[
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

z_ko_left = z[z['ref_uuid'].isna()]
print(len(z_ko_left))
z_ko_right = z[z['name'].isna()]
print(len(z_ko_right))

new_ref_az_ok_tmp = pd.read_csv(join("results", "ref", "tmp", f"new_ref_az_ok_it2_{new_ref_date}.csv.gz"))
new_ref_az_ok_it3 = pd.concat([new_ref_az_ok_tmp, z_ok])
new_ref_az_ok_it3.to_csv(join("results", "ref", "tmp", f"new_ref_az_ok_it3_{new_ref_date}.csv.gz"), index=False)
print(f"new_ref_az_ok_it3 : {len(new_ref_az_ok_it3)}")

new_ref_az_ko_right_no_dupl_it3 = pd.read_csv(join("results", "ref", "tmp", f"new_ref_az_ko_right_no_dupl_it2_{new_ref_date}.csv"))
new_ref_az_ko_right_no_dupl_it3.to_csv(join("results", "ref", "tmp", f"new_ref_az_ko_right_no_dupl_it3_{new_ref_date}.csv"), index=False)

new_ref_az_ko_left_no_dupl_it3 = pd.read_csv(join("results", "ref", "tmp", f"new_ref_az_ko_left_no_dupl_it2_{new_ref_date}.csv"))
new_ref_az_ko_left_no_dupl_it3.to_csv(join("results", "ref", "tmp", f"new_ref_az_ko_left_no_dupl_it3_{new_ref_date}.csv"), index=False)
