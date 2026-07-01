"""Stage B — apparie les liens dao (dao_ref_link_brut.csv) aux fichiers du ref.

Produit une TABLE D'ASSOCIATION séparée fichier <-> unitid (décision : on ne met
pas dao_unitid dans le ref, on garde toutes les paires à côté) :
    results/ead/ead_cor/dao_ref_link.csv
    colonnes : uuid, name, source, finding_aid, unitid
    une ligne par (fichier de conservation, unitid dao) — un fichier peut donc
    apparaître plusieurs fois (décrit par bnr ET mnesys, ou plusieurs cotes).

Appariement : par STEM (nom de fichier sans extension, extensions média
intermédiaires retirées). Le href de diffusion (ex. RBX_X_001.jpg) et le fichier
de conservation (RBX_X_001.tif, .xml, .alto…) partagent le même stem ; l'unitid
est donc rattaché à TOUS les fichiers de conservation de même stem (tif+jpg+ocr).

Résolution hiérarchique (décision) : pour un même (href, ir), on ne garde que le
composant le plus profond (unitid le plus spécifique = enfant). Les conflits
inter-sources (bnr/mnesys) et même-profondeur sont conservés (toutes les paires).

Périmètre : corpus décrits par une dao (AMR + MED). La presse (PRA) tire son
unitid du chemin, hors dao : elle est reprise de l'ancien ref par le coalesce de
60_merge (unitid_old via uuid+checksum) — rien à faire ici.

À lancer depuis la racine du dépôt :
    python scripts/ead/dao_ref_apparie.py [--ref <ref.csv.gz>] [--brut <brut.csv>]
"""

import argparse
import re
from os.path import join, splitext

import pandas as pd

BRUT = join("results", "ead", "ead_cor", "dao_ref_link_brut.csv")
REF = join("results", "ref", "tmp", "_ref_files_20260630_tmp_s3.csv.gz")
SORTIE = join("results", "ead", "ead_cor", "dao_ref_link.csv")

# extensions média retirées pour obtenir le stem (nom d'œuvre partagé diffusion/conservation)
EXT_MEDIA = (".tif", ".tiff", ".jp2", ".jpg", ".jpeg", ".png")

MOTIF_NOMBRE = re.compile(r"\d+")


def stem(nom):
    """Nom d'œuvre partagé diffusion/conservation : nom sans extension, et sans
    extension média intermédiaire (RBX_X_001.tif / .jpg / .alto -> RBX_X_001)."""
    s = splitext(str(nom))[0]
    bas = s.lower()
    for e in EXT_MEDIA:
        if bas.endswith(e):
            return s[: -len(e)]
    return s


def flrs(s):
    """Normalisation côté cote EAD : renommage FLRS et « + » -> « _ »."""
    return str(s).replace("RBX_MED_FLRS_", "RBX_MED_").replace("+", "_")


def normalise(c):
    """Clé « normalisée » : casse ignorée, tirets assimilés aux underscores."""
    return str(c).lower().replace("-", "_")


def depadde(c):
    """Clé « padding » : en plus, zéros de tête des nombres retirés (_001 <-> _1)."""
    return MOTIF_NOMBRE.sub(lambda m: str(int(m.group())), normalise(c))


def main():
    """Apparie les liens dao (dao_ref_link_brut.csv) aux fichiers du ref par stem,
    en cascade exacte -> normalisée -> padding, et écrit la table d'association
    fichier <-> unitid (results/ead/ead_cor/dao_ref_link.csv, Stage B)."""
    p = argparse.ArgumentParser(description="Appariement dao -> fichiers du ref (table séparée)")
    p.add_argument("--ref", default=REF, help="référentiel de fichiers (colonnes name, uuid)")
    p.add_argument("--brut", default=BRUT, help="dao_ref_link_brut.csv (sortie de dao_ref_link.py)")
    p.add_argument("--out", default=SORTIE, help="table d'association de sortie")
    a = p.parse_args()

    b = pd.read_csv(a.brut, low_memory=False)
    b = b[b["unitid"].notna()].copy()

    # résolution hiérarchique : par (href_base, ir) on garde la profondeur max (enfant)
    prof_max = b.groupby(["href_base", "ir"])["profondeur"].transform("max")
    b = b[b["profondeur"] == prof_max]

    # paires distinctes (stem, source, finding_aid, unitid)
    b["stem"] = b["href_base"].map(stem)
    paires = b[["stem", "source", "finding_aid", "unitid"]].drop_duplicates()

    # ref : un stem peut correspondre à plusieurs fichiers (tif+jpg+ocr)
    ref = pd.read_csv(a.ref, low_memory=False, usecols=["name", "uuid"])
    ref["stem"] = ref["name"].map(stem)

    # Appariement stem dao -> stem ref en cascade (de la plus stricte à la plus
    # souple), sur les stems dao DISTINCTS pour rester léger. Chaque niveau ne
    # traite que le résiduel du précédent -> pas de faux positifs sur ce qui
    # matche déjà exactement.
    stems_dao = pd.DataFrame({"stem": sorted(paires["stem"].unique())})
    stems_dao["fl"] = stems_dao["stem"].map(flrs)
    ref_keys = pd.DataFrame({"ref_stem": ref["stem"].unique()})
    ref_keys["k_norm"] = ref_keys["ref_stem"].map(normalise)
    ref_keys["k_pad"] = ref_keys["ref_stem"].map(depadde)

    liens = []  # (stem dao, ref_stem, methode)
    reste = stems_dao

    # 1) exacte : stem dao == stem ref
    m = reste.merge(ref_keys[["ref_stem"]], left_on="stem", right_on="ref_stem")
    m["methode"] = "exacte"
    liens.append(m[["stem", "ref_stem", "methode"]])
    reste = reste[~reste["stem"].isin(m["stem"])]

    # 2) normalisée : normalise(flrs(stem)) == normalise(stem ref)
    reste = reste.assign(k=reste["fl"].map(normalise))
    idx_norm = ref_keys.drop_duplicates("k_norm")[["k_norm", "ref_stem"]]
    m = reste.merge(idx_norm, left_on="k", right_on="k_norm")
    m["methode"] = "normalisee"
    liens.append(m[["stem", "ref_stem", "methode"]])
    reste = reste[~reste["stem"].isin(m["stem"])]

    # 3) padding : depadde(flrs(stem)) == depadde(stem ref)
    reste = reste.assign(k=reste["fl"].map(depadde))
    idx_pad = ref_keys.drop_duplicates("k_pad")[["k_pad", "ref_stem"]]
    m = reste.merge(idx_pad, left_on="k", right_on="k_pad")
    m["methode"] = "padding"
    liens.append(m[["stem", "ref_stem", "methode"]])
    reste = reste[~reste["stem"].isin(m["stem"])]

    liens = pd.concat(liens, ignore_index=True)

    # stem dao -> paires unitid ; ref_stem -> fichiers ; on relie via ref_stem
    paires_l = paires.merge(liens, on="stem")
    assoc = paires_l.merge(ref, left_on="ref_stem", right_on="stem", suffixes=("", "_ref"))
    assoc = assoc[["uuid", "name", "source", "finding_aid", "unitid", "methode"]]
    assoc = assoc.drop_duplicates()
    assoc.to_csv(a.out, index=False)

    n_fic = assoc["uuid"].nunique()
    print(f"table d'association : {len(assoc)} paires (fichier x unitid)")
    print(f"  fichiers de conservation reliés à une dao : {n_fic} ({n_fic / len(ref) * 100:.1f}%)")
    print(f"  unitid distincts   : {assoc['unitid'].nunique()}")
    print("  par méthode d'appariement :")
    print(assoc.drop_duplicates(["uuid", "methode"])["methode"].value_counts().to_string())
    multi = assoc.groupby("uuid").size()
    print(f"  fichiers à plusieurs unitid : {(multi > 1).sum()} (max {multi.max()})")
    print(f"  stems dao non appariés : {len(reste)} / {len(stems_dao)} "
          f"({len(reste) / len(stems_dao) * 100:.1f}%)")
    print(f"écrit : {a.out}")


if __name__ == "__main__":
    main()
