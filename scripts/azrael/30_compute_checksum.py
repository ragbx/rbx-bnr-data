"""Étape 3 — calcul des checksum_md5 manquants (SEULE étape qui lit le disque).

C'est la seule étape coûteuse : elle relit physiquement les fichiers azrael
résiduels (sortie s2_size__az de l'étape 2) sous root_path pour calculer leur MD5.
Grâce aux étapes 1 et 2, le volume est très réduit (résiduel only).

Entrée  : s2_size__az  (az non résolus par métadonnées/taille)
Sortie  : s3_cs__az    (mêmes fichiers + checksum_md5 calculé)  -> étape 4

NB : root_path doit pointer vers la racine des fichiers azrael montée localement.
"""

from os.path import join

import pandas as pd
from bnr.azrael import Azrael2list

from _pipeline import NEW_REF_DATE, tmp_file

# root_path = '\\\\ntrbx.local\mediatheque\BNR'
root_path = "../bnr"
print(root_path)

# on repart du résiduel de l'étape 2 et on (ré)initialise les colonnes attendues
df = pd.read_csv(tmp_file("s2_size__az"))
df = df[["name", "path", "size",
         "last_content_modification_date", "last_metadata_modification_date"]].copy()
df["checksum_md5"] = pd.NA
df["uuid"] = pd.NA

az2list = Azrael2list(root_path=root_path, code_disk=None, az=df)
az2list.get_all_checksum(new_checksum_file_name="new_checksum")
az2list.save_list(filename=tmp_file("s3_cs__az"))
