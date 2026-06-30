"""Jointure OAI → fichiers : attache osiros_id + oai_set au niveau fichier.

Le moissonnage (bnr_moissonnage.py) produit `data/oai/oai_records_{date}.csv.gz`
au niveau **notice** : (identifier, cote, title, setname, osiros_id).

Les fichiers du référentiel portent une **cote** dans la colonne `unitid` (issue de
l'appariement dao/EAD à partir du chemin). Après normalisation (`_` → espace, espaces
multiples réduits), `unitid` correspond à la `cote` OAI. On s'en sert pour reporter
`osiros_id` et `setname` sur chaque fichier.

Vérifié empiriquement (ref 20260502 × oai 20260430) : unitid_norm == cote_norm sur
99,8 % des notices appariées, et oai_set == setname.

Usage :
    python scripts/oai/oai_join_ref.py --input <fichiers.csv[.gz]> --output <sortie.csv.gz> \
        [--oai-date AAAAMMJJ] [--unitid-col unitid]

`--input` doit contenir une colonne `unitid` (les fichiers déjà appariés au dao).
Sortie = l'entrée + colonnes `oai_osiros_id` et `oai_setname`.

Importable : `from oai_join_ref import charger_oai, ajouter_oai`.
"""

import argparse
import re
from glob import glob
from os.path import basename, join

import pandas as pd


def normaliser_cote(s):
    """'1_D_10' / '1  D 10' → '1 D 10'. Renvoie NaN tel quel."""
    if pd.isna(s):
        return s
    return re.sub(r"\s+", " ", str(s).replace("_", " ")).strip()


def charger_oai(oai_date=None):
    """Charge la table de correspondance cote_normalisée → (osiros_id, setname).

    oai_date=None → fichier oai_records le plus récent disponible.
    Retourne (chemin_utilisé, dataframe[cote_n, osiros_id, setname]).
    """
    if oai_date:
        path = join("data", "oai", f"oai_records_{oai_date}.csv.gz")
    else:
        cands = sorted(glob(join("data", "oai", "oai_records_*.csv.gz")))
        if not cands:
            raise FileNotFoundError("Aucun data/oai/oai_records_*.csv.gz trouvé")
        path = cands[-1]

    oai = pd.read_csv(path)
    oai["cote_n"] = oai["cote"].map(normaliser_cote)
    oai = oai[oai["cote_n"].notna() & (oai["cote_n"] != "")]

    # une cote → une notice : on signale et on tranche par la première
    dup = oai["cote_n"].duplicated(keep=False)
    if dup.any():
        print(f"[oai] {int(dup.sum())} cotes en collision (plusieurs notices) "
              f"→ on garde la première")
    oai = oai.drop_duplicates("cote_n", keep="first")
    return path, oai[["cote_n", "osiros_id", "setname"]]


def ajouter_oai(df, oai, unitid_col="unitid"):
    """Ajoute oai_osiros_id + oai_setname à df via la cote normalisée."""
    df = df.copy()
    df["_cote_n"] = df[unitid_col].map(normaliser_cote)
    m = df.merge(
        oai.rename(columns={"osiros_id": "oai_osiros_id", "setname": "oai_setname"}),
        left_on="_cote_n", right_on="cote_n", how="left",
    )
    return m.drop(columns=["_cote_n", "cote_n"])


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Jointure OAI → fichiers (osiros_id, oai_set)")
    p.add_argument("--input", required=True,
                   help="CSV de fichiers contenant une colonne unitid")
    p.add_argument("--output", required=True, help="CSV de sortie (.csv.gz)")
    p.add_argument("--oai-date", default=None,
                   help="Horodatage de oai_records (défaut: le plus récent)")
    p.add_argument("--unitid-col", default="unitid")
    a = p.parse_args()

    oai_path, oai = charger_oai(a.oai_date)
    print(f"[oai] table : {oai_path} ({len(oai)} cotes uniques)")

    df = pd.read_csv(a.input, low_memory=False)
    out = ajouter_oai(df, oai, a.unitid_col)
    n = int(out["oai_osiros_id"].notna().sum())
    print(f"[oai] {n}/{len(out)} fichiers rattachés à une notice")
    out.to_csv(a.output, index=False)
    print(f"[oai] écrit : {a.output}")
