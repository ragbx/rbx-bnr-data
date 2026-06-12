"""
Tente d'apparier les liens de diffusion orphelins (dao_sans_conservation.csv)
avec des fichiers de conservation du référentiel results/ref/_ref_files_*.csv.gz
(le plus récent), y compris ceux sans s3_key que ead_bnr2mnesys.py ignore.

Trois méthodes d'appariement, de la plus stricte à la plus souple, au sein
d'une même famille de média (image, audio, video, pdf) :
  - exacte      : nom de base sans extension identique, avec les normalisations
                  de ead_bnr2mnesys.py (variante audio _96kHz24B/_TI retirée,
                  renommage FLRS RBX_MED_FLRS_ → RBX_MED_ et + → _) ;
  - normalisee  : en plus, casse ignorée, tirets assimilés aux underscores,
                  extensions de média intermédiaires retirées (x.tif.jpg → x) ;
  - padding     : en plus, zéros de tête des nombres ignorés (_001 ↔ _1).

À lancer depuis la racine du dépôt. Produit
results/ead/ead_cor/dao_appariement_conservation.csv : une ligne par couple
(orphelin, candidat) — ou une ligne à méthode "aucun" si rien n'est trouvé —
en écartant les candidats jpg quand un candidat tif existe pour l'orphelin —
avec le nom, le chemin, l'uuid, le statut de conservation et le s3_key du
candidat.
Affiche une synthèse par méthode et par statut.
"""

import re
from glob import glob
from os.path import basename, join, splitext

import pandas as pd

ORPHELINS = join("results", "ead", "ead_cor", "dao_sans_conservation.csv")
SORTIE = join("results", "ead", "ead_cor", "dao_appariement_conservation.csv")

FAMILLES_MEDIA = {
    "image": [".tif", ".tiff", ".jp2", ".png", ".jpg", ".jpeg"],
    "audio": [".wav", ".flac", ".mp3"],
    "video": [".mov", ".mp4", ".wmv"],
    "pdf": [".pdf"],
}
EXT2FAMILLE = {ext: fam for fam, exts in FAMILLES_MEDIA.items() for ext in exts}

MOTIF_VARIANTE_AUDIO = re.compile(r"_(\d+kHz\d+B|TI)$", re.IGNORECASE)
MOTIF_NOMBRE = re.compile(r"\d+")


def dernier_ref():
    """Chemin du référentiel de fichiers le plus récent (_ref_files_AAAAMMJJ)."""
    refs = [
        p
        for p in glob(join("results", "ref", "_ref_files_*.csv.gz"))
        if re.fullmatch(r"_ref_files_\d{8}\.csv\.gz", basename(p))
    ]
    return max(refs)


def stem(nom):
    """Nom de base sans extension, les extensions de média intermédiaires
    étant aussi retirées (x.tif.jpg → x)."""
    s = splitext(basename(nom))[0]
    while splitext(s)[1].lower() in EXT2FAMILLE:
        s = splitext(s)[0]
    return s


def cles_exactes(s, cote_ead=False):
    """Clés d'appariement de la méthode exacte : le stem, avec et sans variante
    audio ; côté EAD s'ajoute la normalisation FLRS vers le nom de conservation."""
    cles = {s}
    if cote_ead:
        cles.add(s.replace("RBX_MED_FLRS_", "RBX_MED_").replace("+", "_"))
    cles |= {MOTIF_VARIANTE_AUDIO.sub("", c) for c in list(cles)}
    return cles


def normalise(cle):
    """Clé de la méthode normalisee : casse et tirets ignorés."""
    return cle.lower().replace("-", "_")


def depadde(cle):
    """Clé de la méthode padding : zéros de tête des nombres retirés."""
    return MOTIF_NOMBRE.sub(lambda m: str(int(m.group())), normalise(cle))


orphelins = pd.read_csv(ORPHELINS)
ref_path = dernier_ref()
print(f"référentiel : {ref_path}")
ref = pd.read_csv(ref_path, low_memory=False)

# une ligne par nom de fichier, en privilégiant celles avec s3_key
ref = ref.sort_values("s3_key", na_position="last").drop_duplicates(subset=["name"])
ref["famille"] = ref["extension"].str.lower().map(EXT2FAMILLE)
ref = ref.dropna(subset=["famille"]).reset_index(drop=True)
extensions = ref["extension"].str.lower().tolist()

# index du référentiel par clé la plus souple (padding) : (clé, famille) -> lignes
index_ref = {}
for i, (nom, famille) in enumerate(zip(ref["name"], ref["famille"])):
    for cle in cles_exactes(stem(nom)):
        index_ref.setdefault((depadde(cle), famille), []).append(i)

lignes = []
for orphelin in orphelins.itertuples(index=False):
    famille = orphelin.role.split(":")[1]
    cles = cles_exactes(stem(orphelin.href), cote_ead=True)
    cles_norm = {normalise(c) for c in cles}

    candidats = sorted(
        {
            i
            for cle in cles
            for i in index_ref.get((depadde(cle), famille), [])
        }
    )
    if not candidats:
        lignes.append([*orphelin, "aucun", "", "", "", "", ""])
        continue
    # un candidat tif rend les candidats jpg redondants
    if any(extensions[i] in (".tif", ".tiff") for i in candidats):
        candidats = [i for i in candidats if extensions[i] not in (".jpg", ".jpeg")]
    for i in candidats:
        candidat = ref.iloc[i]
        cles_ref = cles_exactes(stem(candidat["name"]))
        if cles & cles_ref:
            methode = "exacte"
        elif cles_norm & {normalise(c) for c in cles_ref}:
            methode = "normalisee"
        else:
            methode = "padding"
        lignes.append(
            [
                *orphelin,
                methode,
                candidat["name"],
                candidat["path"],
                candidat["uuid"] if pd.notna(candidat["uuid"]) else "",
                candidat["conservation_statut"],
                candidat["s3_key"] if pd.notna(candidat["s3_key"]) else "",
            ]
        )

resultat = pd.DataFrame(
    lignes,
    columns=list(orphelins.columns)
    + [
        "methode",
        "ref_name",
        "ref_path",
        "ref_uuid",
        "ref_conservation_statut",
        "ref_s3_key",
    ],
)
resultat.to_csv(SORTIE, index=False)

# synthèse : un orphelin compte une fois, pour sa meilleure méthode
ORDRE_METHODES = {"exacte": 0, "normalisee": 1, "padding": 2, "aucun": 3}
meilleure = (
    resultat.assign(rang=resultat["methode"].map(ORDRE_METHODES))
    .sort_values("rang")
    .drop_duplicates(subset=["ir", "id_composant", "role", "href"])
)
print(f"\n{len(orphelins)} orphelins, par meilleure méthode d'appariement :")
print(meilleure["methode"].value_counts().to_string())
print("\nstatuts de conservation des candidats (toutes lignes) :")
appariees = resultat[resultat["methode"] != "aucun"]
print(
    appariees.groupby(
        [appariees["ref_conservation_statut"], appariees["ref_s3_key"] != ""]
    )
    .size()
    .rename_axis(["conservation_statut", "s3_key"])
    .to_string()
)
print(f"\ndétail : {SORTIE}")
