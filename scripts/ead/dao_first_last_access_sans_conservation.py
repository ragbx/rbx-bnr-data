"""
Catégorise les liens d'accès des plages first/last qui n'ont pas de
contrepartie de conservation déclarée dans l'EAD.

dao_first_last_developpe.py ne développe un access:<média> que lorsque le
groupe ne porte aucun preservation:<média> (cf. cette doc). Les fichiers
access:image ainsi développés sont des images diffusées sans master de
conservation *déclaré* — mais ce master peut malgré tout exister dans le
référentiel sous un autre nom (extension de conservation .tif/.jp2).

Ce script recherche, pour chaque access:image développé, une image de même
radical (nom de base sans extension, casse et tirets normalisés) dans le
référentiel le plus récent, et classe le lien en trois catégories :
  - conservation_existe : un master de conservation (.tif/.jp2/.tiff) existe ;
                          l'EAD ne l'a simplement pas déclaré en preservation ;
  - diffusion_seule     : seule une image de diffusion (.jpg/.png) existe ;
                          pas de master de conservation ;
  - absent              : aucune image de ce radical dans le référentiel.

Quand une image est trouvée, le script renvoie son nom, son extension, son
statut de conservation, son uuid et son chemin S3 (vide si non versé).

À lancer depuis la racine du dépôt, après dao_first_last_verif_ref.py. Produit
results/ead/ead_cor/dao_first_last_access_sans_conservation.csv et affiche une
synthèse par catégorie, statut et IR.
"""

import re
from glob import glob
from os.path import basename, join, splitext

import pandas as pd

VERIF = join("results", "ead", "ead_cor", "dao_first_last_verif_ref.csv")
SORTIE = join(
    "results", "ead", "ead_cor", "dao_first_last_access_sans_conservation.csv"
)

FAMILLE_IMAGE = {".tif", ".tiff", ".jp2", ".png", ".jpg", ".jpeg"}
CONSERVATION = {".tif", ".tiff", ".jp2"}
# priorité de choix quand plusieurs images partagent un radical : conservation
# d'abord, puis jp2, puis diffusion
PRIORITE_EXT = {".tif": 0, ".tiff": 0, ".jp2": 1, ".png": 2, ".jpeg": 3, ".jpg": 3}


def radical(nom):
    """Nom de base sans extension(s) d'image, casse et tirets normalisés."""
    s = splitext(basename(str(nom)))[0]
    while splitext(s)[1].lower() in FAMILLE_IMAGE:
        s = splitext(s)[0]
    return s.lower().replace("-", "_")


def radicaux_acces(href):
    """Radicaux candidats d'un lien d'accès : le radical brut, plus le radical
    après renommage FLRS (RBX_MED_FLRS_ → RBX_MED_, + → _) comme dans
    ead_bnr2mnesys.py, le nom de conservation perdant le segment FLRS."""
    base = radical(href)
    flrs = base.replace("rbx_med_flrs_", "rbx_med_").replace("+", "_")
    return [base] if flrs == base else [base, flrs]


def dernier_ref():
    """Chemin du référentiel de fichiers le plus récent (_ref_files_AAAAMMJJ)."""
    refs = [
        p
        for p in glob(join("results", "ref", "_ref_files_*.csv.gz"))
        if re.fullmatch(r"_ref_files_\d{8}\.csv\.gz", basename(p))
    ]
    return max(refs)


verif = pd.read_csv(VERIF, low_memory=False)
acces = verif[verif["role"] == "access:image"].copy()
# les colonnes ref_* de verif portent le match exact par nom .jpg ; on les
# écarte pour reconstruire le rapprochement par radical (toutes extensions).
acces = acces.drop(
    columns=[c for c in acces.columns if c.startswith("ref_")]
    + ["name", "trouve_ref", "dans_s3"],
    errors="ignore",
)

ref_path = dernier_ref()
print(f"référentiel : {ref_path}")
ref = pd.read_csv(
    ref_path,
    low_memory=False,
    usecols=["name", "extension", "uuid", "conservation_statut", "s3_key"],
)
ref["ext"] = ref["extension"].str.lower()
images = ref[ref["ext"].isin(FAMILLE_IMAGE)].copy()
images["radical"] = images["name"].map(radical)
images["conservation"] = images["ext"].isin(CONSERVATION)
# une image par radical, la plus « conservation » d'abord
images["prio"] = images["ext"].map(PRIORITE_EXT)
images = images.sort_values("prio").drop_duplicates("radical")
images = images.rename(
    columns={
        "name": "ref_name",
        "ext": "ref_extension",
        "uuid": "ref_uuid",
        "conservation_statut": "ref_conservation_statut",
        "s3_key": "ref_s3_key",
    }
)[
    [
        "radical",
        "ref_name",
        "ref_extension",
        "ref_uuid",
        "ref_conservation_statut",
        "ref_s3_key",
        "conservation",
    ]
]

# radical retenu pour l'accès : le premier candidat (brut, puis FLRS) présent
# dans le référentiel ; à défaut le radical brut (restera sans correspondance).
radicaux_ref = set(images["radical"])
acces["radical"] = acces["href"].map(
    lambda h: next(
        (c for c in radicaux_acces(h) if c in radicaux_ref), radical(h)
    )
)

resultat = acces.merge(images, on="radical", how="left")
resultat["categorie"] = "absent"
resultat.loc[resultat["ref_name"].notna(), "categorie"] = "diffusion_seule"
resultat.loc[resultat["conservation"] == True, "categorie"] = "conservation_existe"

colonnes = [
    "ir",
    "id_composant",
    "unitid",
    "role",
    "href",
    "position",
    "taille_plage",
    "categorie",
    "ref_name",
    "ref_extension",
    "ref_conservation_statut",
    "ref_uuid",
    "ref_s3_key",
]
resultat[colonnes].to_csv(SORTIE, index=False)

print(f"\n{len(resultat)} liens access sans conservation déclarée, par catégorie :")
print(resultat["categorie"].value_counts().to_string())

cons = resultat[resultat["categorie"] == "conservation_existe"]
print(
    "\nstatut de conservation des masters trouvés (conservation_existe) :"
)
print(cons["ref_conservation_statut"].value_counts().to_string())

manques = resultat[resultat["categorie"].isin(["diffusion_seule", "absent"])]
print(f"\naccess sans master de conservation : {len(manques)}, par IR :")
print(manques["ir"].value_counts().head(15).to_string())
print(f"\ndétail : {SORTIE}")
