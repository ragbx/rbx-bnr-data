import re
from datetime import datetime
from os.path import join

import pandas as pd


# Fonction pour extraire le préfixe et les numéros
def extraire_prefixe_et_numeros(nom_fichier):
    # Expression régulière pour extraire le préfixe et les numéros
    match = re.match(r"(.*?)(\d+)_(\d+)(?:\.\w+)?$", nom_fichier)
    if match:
        prefix = match.group(1)
        numero1 = match.group(2)
        numero2 = match.group(3)
        return prefix, numero1, numero2
    return None, None, None


# Fonction pour générer les fichiers intermédiaires
def generer_fichiers_intermediaires(debut, fin):
    prefix_debut, num1_debut, num2_debut = extraire_prefixe_et_numeros(debut)
    prefix_fin, num1_fin, num2_fin = extraire_prefixe_et_numeros(fin)

    if None in (prefix_debut, num1_debut, num2_debut, prefix_fin, num1_fin, num2_fin):
        return []

    if prefix_debut != prefix_fin or num1_debut != num1_fin:
        return []

    # Conserver le format des numéros
    num2_debut_int = int(num2_debut)
    num2_fin_int = int(num2_fin)

    fichiers = []
    for i in range(num2_debut_int, num2_fin_int + 1):
        nom_fichier = f"{prefix_debut}{num1_debut}_{i:0{len(num2_debut)}}"
        fichiers.append((f"{nom_fichier}.jpg", nom_fichier))

    return fichiers


# Lire le fichier CSV
date = datetime.now().strftime("%Y%m%d")
input_path = join("results", "dao", f"liste_dao_{date}.csv.gz")
df = pd.read_csv(input_path, compression="gzip")

# Listes pour stocker les résultats
resultats = []
resultats_ko = []

# Pour chaque ligne du DataFrame
for _, row in df.iterrows():
    # Cas 1 : daoloc_first et daoloc_last sont présents
    if pd.notna(row["daoloc_first"]) and pd.notna(row["daoloc_last"]):
        # Vérifier si le format est du type RBX_MED_LBI_T01_L1_D013_001.jpg
        if "_" in row["daoloc_first"] and "_" in row["daoloc_last"]:
            fichiers = generer_fichiers_intermediaires(
                row["daoloc_first"], row["daoloc_last"]
            )

        if fichiers:
            for fichier, fichier_base in fichiers:
                resultats.append(
                    {
                        "finding_aid": row["finding_aid"],
                        "unitid": row["unitid"],
                        "dao": None,
                        "daoloc_first": row["daoloc_first"],
                        "daoloc_last": row["daoloc_last"],
                        "nom_fichier": fichier,
                        "nom_fichier_base": fichier_base,
                    }
                )
        else:
            resultats_ko.append(
                {
                    "finding_aid": row["finding_aid"],
                    "unitid": row["unitid"],
                    "dao": None,
                    "daoloc_first": row["daoloc_first"],
                    "daoloc_last": row["daoloc_last"],
                    "nom_fichier": None,
                    "nom_fichier_base": None,
                    "raison": "Impossible de générer les fichiers",
                }
            )

    # Cas 2 : dao est présent mais pas daoloc_first/daoloc_last
    else:
        dao_base = row["dao"]
        if dao_base and isinstance(dao_base, str):
            dao_base = re.sub(r"(?:\.\w+)?$", "", dao_base)  # Retirer l'extension

        resultats.append(
            {
                "finding_aid": row["finding_aid"],
                "unitid": row["unitid"],
                "dao": row["dao"],
                "daoloc_first": None,
                "daoloc_last": None,
                "nom_fichier": row["dao"],
                "nom_fichier_base": dao_base,
            }
        )

# Créer des DataFrames à partir des résultats
df_resultats = pd.DataFrame(resultats)
df_resultats_ko = pd.DataFrame(resultats_ko)

# Sauvegarder dans des fichiers CSV
output_path = join("results", "dao", f"liste_dao_flat_{date}.csv.gz")
output_path_ko = join("results", "dao", f"liste_dao_flat_{date}_ko.csv.gz")

df_resultats.to_csv(output_path, index=False)
df_resultats_ko.to_csv(output_path_ko, index=False)
