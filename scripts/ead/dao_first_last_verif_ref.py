"""
Vérifie que les fichiers des plages first/last développées par
dao_first_last_developpe.py existent dans le fichier de référence (et sur S3).

Pour chaque fichier développé (bornes et intermédiaires implicites), recherche
dans results/ref/_ref_files_*.csv.gz le plus récent une ligne de même `name`
(nom de fichier avec extension) et indique s'il y figure, s'il est versé sur S3
(s3_key présent), avec son uuid, son statut de conservation et son chemin S3.

L'enjeu porte surtout sur les fichiers de conservation (role preservation:*) :
ce sont eux qui sont suivis dans le référentiel. Les liens d'accès (.jpg de
diffusion) sont souvent absents du référentiel, qui ne recense pas les dérivés.

À lancer depuis la racine du dépôt. Produit
results/ead/ead_cor/dao_first_last_verif_ref.csv (la liste développée enrichie
des colonnes ref_*) et affiche une synthèse par role : trouvés dans ref, versés
sur S3, manquants.
"""

import re
from glob import glob
from os.path import basename, join

import pandas as pd

DEVELOPPE = join("results", "ead", "ead_cor", "dao_first_last_developpe.csv")
SORTIE = join("results", "ead", "ead_cor", "dao_first_last_verif_ref.csv")


def dernier_ref():
    """Chemin du référentiel de fichiers le plus récent (_ref_files_AAAAMMJJ)."""
    refs = [
        p
        for p in glob(join("results", "ref", "_ref_files_*.csv.gz"))
        if re.fullmatch(r"_ref_files_\d{8}\.csv\.gz", basename(p))
    ]
    return max(refs)


dev = pd.read_csv(DEVELOPPE)
dev["name"] = dev["href"].map(basename)

ref_path = dernier_ref()
print(f"référentiel : {ref_path}")
ref = pd.read_csv(
    ref_path,
    low_memory=False,
    usecols=["name", "path", "uuid", "conservation_statut", "s3_key"],
)
# une ligne par nom, en privilégiant celles versées sur S3
ref = ref.sort_values("s3_key", na_position="last").drop_duplicates(subset=["name"])
ref = ref.rename(
    columns={
        "path": "ref_path",
        "uuid": "ref_uuid",
        "conservation_statut": "ref_conservation_statut",
        "s3_key": "ref_s3_key",
    }
)

resultat = dev.merge(ref, on="name", how="left")
resultat["trouve_ref"] = resultat["ref_path"].notna()
resultat["dans_s3"] = resultat["ref_s3_key"].notna()
resultat.to_csv(SORTIE, index=False)

print(f"\n{len(resultat)} fichiers développés vérifiés\n")
synthese = resultat.groupby("role").agg(
    total=("name", "size"),
    trouves_ref=("trouve_ref", "sum"),
    sur_s3=("dans_s3", "sum"),
)
synthese["manquants_ref"] = synthese["total"] - synthese["trouves_ref"]
print(synthese.to_string())

manquants = resultat[~resultat["trouve_ref"]]
print(f"\n{len(manquants)} fichiers absents du référentiel.")
print("\nstatuts de conservation des fichiers trouvés :")
trouves = resultat[resultat["trouve_ref"]]
print(
    trouves.groupby(["role", "ref_conservation_statut"]).size().to_string()
)
print(f"\ndétail : {SORTIE}")
