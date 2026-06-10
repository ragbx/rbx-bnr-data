from os.path import join

import pandas as pd

ref_date = "20260502"

ref = pd.read_csv(
    join("results", "ref", f"_ref_files_{ref_date}.csv.gz"),
    usecols=["path", "conservation_statut"],
    low_memory=False,
)

# chaînes de caractères marquant, dans conservation_statut, un traitement s3 réalisé
STATUTS_TRAITES_S3 = ("TRANSFERT_S3_OK", "CORBEILLE", "SUPPRIMER", "NE PAS GARDER")


def repartition(s):
    """Répartition des valeurs sous la forme "valeur (n) ; valeur (n)"."""
    vc = s.fillna("NON RENSEIGNÉ").value_counts()
    return " ; ".join(f"{k} ({v})" for k, v in vc.items())


# fichiers dont le traitement s3 n'est pas réalisé
ko = ref[~ref["conservation_statut"].apply(lambda v: any(chaine in str(v) for chaine in STATUTS_TRAITES_S3))]

repertoires = (
    ko.groupby("path")
    .agg(
        nb_fichiers=("conservation_statut", "size"),
        conservation_statut=("conservation_statut", repartition),
    )
    .reset_index()
    .rename(columns={"path": "répertoire"})
    .sort_values("répertoire")
)

repertoires.to_csv(join("results", "s3", "repertoires_traitement_s3_ko.csv"), index=False)
