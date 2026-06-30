"""Étape 2 — appariement sur la clé relâchée (name, path, size).

Les fichiers dont seule une date de modification a changé entre le ref et l'az ont
échoué à l'étape 1 (clé complète) bien qu'ils soient identiques. On les réapparie
ici sur (name, path, size) -- toujours SANS relire les fichiers sur disque -- et on
récupère ainsi uuid ET checksum_md5 depuis le ref.

On ne traite que les lignes uniques sur la clé des deux côtés ; les doublons et les
non-appariés repartent dans le résiduel.

Entrées (results/ref/tmp/) :  s1_meta__az, s1_meta__ref
Sorties (results/ref/tmp/) :
    - s2_size__ok   : appariés (uuid + checksum_md5 récupérés)   -> versés dans « ok »
    - s2_size__az   : az résiduels                                -> étape 3 (checksum)
    - s2_size__ref  : ref résiduels                               -> étape 4

    len(s1_meta__az)  == len(s2_size__ok) + len(s2_size__az)
    len(s1_meta__ref) == len(s2_size__ok) + len(s2_size__ref)
"""

import pandas as pd

from _pipeline import tmp_file

KEY = ["name", "path", "size"]

left = pd.read_csv(tmp_file("s1_meta__az"))
# s1_meta__ref est déjà sauvegardé sans le préfixe ref_ (renommé par l'étape 1)
right = pd.read_csv(tmp_file("s1_meta__ref"))

# lignes uniques sur la clé (des deux côtés) -> appariement sûr
left_dup_mask = left.duplicated(subset=KEY, keep=False)
right_dup_mask = right.duplicated(subset=KEY, keep=False)

left_u = left[~left_dup_mask]
# on suffixe le ref pour récupérer checksum_md5/uuid sans collision de colonnes
right_u = right[~right_dup_mask].add_suffix("_ref").rename(
    columns={f"{k}_ref": k for k in KEY}
)

m = left_u.merge(right_u, on=KEY, how="inner")

# s2_size__ok : métadonnées az (à jour) + checksum_md5/uuid récupérés du ref
ok = m[
    [
        "name",
        "path",
        "size",
        "last_content_modification_date",
        "last_content_modification_date_",
        "last_metadata_modification_date",
        "last_metadata_modification_date_",
        "checksum_md5_ref",
        "uuid_ref",
    ]
].rename(columns={"checksum_md5_ref": "checksum_md5", "uuid_ref": "uuid"})

# résiduel = doublons + non-appariés
matched_keys = set(m[KEY].apply(tuple, axis=1))
left_key = left[KEY].apply(tuple, axis=1)
ko_left = left[left_dup_mask | ~left_key.isin(matched_keys)]

right_key = right[KEY].apply(tuple, axis=1)
ko_right = right[right_dup_mask | ~right_key.isin(matched_keys)]

# contrôles de conservation
print("s1_meta__az :", len(left), "==", len(ok), "+", len(ko_left), "?",
      len(left) == len(ok) + len(ko_left))
print("s1_meta__ref:", len(right), "==", len(ok), "+", len(ko_right), "?",
      len(right) == len(ok) + len(ko_right))
print(f"s2_size__ok  : {len(ok)}")
print(f"s2_size__az  : {len(ko_left)}  (-> étape 3 : checksum)")
print(f"s2_size__ref : {len(ko_right)} (-> étape 4)")

ok.to_csv(tmp_file("s2_size__ok"), index=False)
ko_left.to_csv(tmp_file("s2_size__az"), index=False)
ko_right.to_csv(tmp_file("s2_size__ref"), index=False)
