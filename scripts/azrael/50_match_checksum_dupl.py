"""Étape 5 — appariement des cas à doublons de checksum.

On départage les fichiers partageant le même (name, checksum_md5) en s'appuyant en
plus sur le chemin (name, path).

Entrées :
    - s4_csdupl__az, s4_csdupl__ref : cas à doublons (étape 4)
    - _ok_cumul_s4                  : cumul des « ok » jusqu'à l'étape 4

Sortie principale (interface, consommée par le maillon s3/dao/oai hors dépôt) :
    - _az_ok_all_{date}.csv.gz  : TOUS les fichiers az résolus (name..checksum_md5, uuid)

Sorties annexes (résiduels non résolus, recopiés depuis l'étape 4) :
    - s5_cs__az, s5_cs__ref (.csv)
"""

import pandas as pd

from _pipeline import tmp_file

# cas à doublons de checksum (étape 4)
new_ref_az_ko_left_csdupl = pd.read_csv(tmp_file("s4_csdupl__az", ext="csv"))
new_ref_az_ko_right_csdupl = pd.read_csv(tmp_file("s4_csdupl__ref", ext="csv"))

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

# OK final = cumul étape 4 + résolus des doublons
new_ref_az_ok_tmp = pd.read_csv(tmp_file("_ok_cumul_s4"))
new_ref_az_ok_it3 = pd.concat([new_ref_az_ok_tmp, z_ok])
new_ref_az_ok_it3.to_csv(tmp_file("_az_ok_all"), index=False)
print(f"_az_ok_all : {len(new_ref_az_ok_it3)}")

# résiduels sans doublon non résolus (recopiés depuis l'étape 4, pour information)
new_ref_az_ko_right_no_dupl_it3 = pd.read_csv(tmp_file("s4_cs__ref", ext="csv"))
new_ref_az_ko_right_no_dupl_it3.to_csv(tmp_file("s5_cs__ref", ext="csv"), index=False)

new_ref_az_ko_left_no_dupl_it3 = pd.read_csv(tmp_file("s4_cs__az", ext="csv"))
new_ref_az_ko_left_no_dupl_it3.to_csv(tmp_file("s5_cs__az", ext="csv"), index=False)
