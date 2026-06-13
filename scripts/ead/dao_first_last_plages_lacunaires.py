"""
Identifie les plages first/last « non contiguës » : celles dont une partie des
fichiers développés par dao_first_last_developpe.py est absente du référentiel.

Une plage est décrite dans l'EAD par ses seules bornes first/last (cf.
dao_first_last_developpe.py). Quand la numérotation réelle des fichiers n'est
pas continue entre les deux bornes (sous-séries, numéros sautés), le
développement « sur-génère » des noms qui n'existent pas : la plage apparaît
alors lacunaire à la vérification contre le référentiel.

Ce script s'appuie sur dao_first_last_verif_ref.csv : il regroupe les fichiers
par plage (ancrée sur son nom first, identifiant exact) et ne retient que les
plages où au moins un fichier manque. Une plage à fort taux de manquants sur un
grand nombre de fichiers (ex. 170/598) signale une numérotation non contiguë,
plutôt que de vrais fichiers perdus ; une plage à un ou deux manquants isolés
est un meilleur candidat de fichier réellement absent.

À lancer depuis la racine du dépôt, après dao_first_last_verif_ref.py. Produit
results/ead/ead_cor/dao_first_last_plages_lacunaires.csv (une ligne par plage
lacunaire) et affiche une synthèse.
"""

from os.path import join

import pandas as pd

VERIF = join("results", "ead", "ead_cor", "dao_first_last_verif_ref.csv")
SORTIE = join("results", "ead", "ead_cor", "dao_first_last_plages_lacunaires.csv")

verif = pd.read_csv(VERIF, low_memory=False)

# seuls les fichiers de conservation sont recensés dans le référentiel ; les
# liens d'accès (.jpg dérivés) en sont structurellement absents et fausseraient
# la détection de discontinuité.
verif = verif[verif["role"].str.startswith("preservation:")]

# nom de la borne first de chaque ligne : identifiant exact de sa plage
premiers = (
    verif[verif["position"] == "first"]
    .groupby(["ir", "id_composant", "role"])["name"]
    .first()
    .rename("name_first")
)
verif = verif.merge(premiers, on=["ir", "id_composant", "role"], how="left")

plages = (
    verif.groupby(["ir", "id_composant", "role", "name_first"])
    .agg(
        unitid=("unitid", "first"),
        taille_plage=("taille_plage", "first"),
        presents=("trouve_ref", "sum"),
        sur_s3=("dans_s3", "sum"),
        name_last=("name", "max"),
    )
    .reset_index()
)
plages["manquants"] = plages["taille_plage"] - plages["presents"]
plages["taux_presence"] = (100 * plages["presents"] / plages["taille_plage"]).round(1)

lacunaires = plages[plages["manquants"] > 0].sort_values(
    "manquants", ascending=False
)
lacunaires = lacunaires[
    [
        "ir",
        "id_composant",
        "unitid",
        "role",
        "name_first",
        "name_last",
        "taille_plage",
        "presents",
        "manquants",
        "taux_presence",
    ]
]
lacunaires.to_csv(SORTIE, index=False)

print(f"{len(plages)} plages first/last, dont {len(lacunaires)} lacunaires")
print(f"manquants cumulés : {int(lacunaires['manquants'].sum())}")
print("\nrépartition des plages lacunaires par taux de présence :")
tranches = pd.cut(
    lacunaires["taux_presence"],
    [0, 25, 50, 75, 99, 100],
    include_lowest=True,
)
print(tranches.value_counts().sort_index().to_string())
print("\ntop plages lacunaires (probables numérotations non contiguës) :")
print(
    lacunaires.head(15)[
        ["ir", "name_first", "taille_plage", "presents", "manquants", "taux_presence"]
    ].to_string(index=False)
)
print(f"\ndétail : {SORTIE}")
