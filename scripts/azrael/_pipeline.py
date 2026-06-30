"""Configuration centrale du pipeline azrael -> ref.

Toutes les dates et conventions de chemins sont définies ICI et importées par les
scripts numérotés (10_..._60). À chaque nouveau référentiel, il suffit de mettre à
jour OLD_REF_DATE / NEW_REF_DATE ci-dessous : plus aucune date n'est codée en dur
dans les scripts (c'était la cause des désynchronisations it2/it3).

Voir README.md pour l'enchaînement complet des étapes.

Convention de nommage des fichiers intermédiaires (dans results/ref/tmp/) :
    s{n}_{quoi}__{rôle}_{date}.csv.gz
        n     : numéro d'étape (1 = match métadonnées, 2 = match taille, ...)
        rôle  : ok  -> résolus (uuid + checksum_md5 connus)
                az  -> à résoudre, côté azrael (ex « ko_left »)
                ref -> à résoudre, côté référence  (ex « ko_right »)
"""

from os.path import join

# --- Dates de référence : SEUL endroit à modifier pour un nouveau ref ----------
OLD_REF_DATE = "20260502"
NEW_REF_DATE = "20260630"

# --- Répertoires ---------------------------------------------------------------
REF_DIR = join("results", "ref")
TMP_DIR = join("results", "ref", "tmp")
AZ_DIR = join("data", "az")


def ref_file(date):
    """Référentiel consolidé _ref_files_{date}.csv.gz (interface externe)."""
    return join(REF_DIR, f"_ref_files_{date}.csv.gz")


def az_file(date=NEW_REF_DATE):
    """Extraction azrael brute bnr_azrael_{date}.csv.gz (interface externe)."""
    return join(AZ_DIR, f"bnr_azrael_{date}.csv.gz")


def tmp_file(name, date=NEW_REF_DATE, ext="csv.gz"):
    """Fichier intermédiaire de la chaîne de matching.

    Ex : tmp_file("s1_meta__ok") -> results/ref/tmp/s1_meta__ok_20260630.csv.gz
    """
    return join(TMP_DIR, f"{name}_{date}.{ext}")
