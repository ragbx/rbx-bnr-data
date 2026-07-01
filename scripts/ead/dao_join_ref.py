"""Jointure DAO → fichiers : attache dao_finding_aid + dao_unitid au niveau fichier.

Consomme la table d'association produite par dao_ref_apparie.py
(results/ead/ead_cor/dao_ref_link.csv : uuid, name, source, finding_aid, unitid,
methode), où un fichier peut avoir PLUSIEURS unitid (décrit par bnr et mnesys, ou
plusieurs cotes).

Règle d'injection (décision) : on n'injecte qu'UN seul couple par fichier,
en priorisant la source **bnr** ; s'il en reste plusieurs, on garde le premier
(ordre déterministe : source bnr avant mnesys, puis finding_aid, puis unitid).

Sortie = l'entrée + colonnes dao_finding_aid et dao_unitid. Ces colonnes sont
lues par 60_merge (coalesce : valeur dao si présente, sinon finding_aid/unitid de
l'ancien ref via uuid+checksum — ce qui couvre notamment la presse PRA).

Usage :
    python scripts/ead/dao_join_ref.py --input <fichiers.csv[.gz]> --output <sortie.csv.gz> \
        [--dao results/ead/ead_cor/dao_ref_link.csv]

`--input` doit contenir une colonne `uuid`.
Importable : `from dao_join_ref import charger_dao, ajouter_dao`.
"""

import argparse
from os.path import join

import pandas as pd

DAO = join("results", "ead", "ead_cor", "dao_ref_link.csv")

# priorité de source : bnr avant mnesys
RANG_SOURCE = {"bnr": 0, "mnesys": 1}


def charger_dao(dao_path=DAO):
    """Réduit la table d'association à UN couple (dao_finding_aid, dao_unitid) par
    uuid, en priorisant bnr puis le premier (ordre déterministe)."""
    d = pd.read_csv(dao_path, low_memory=False)
    d = d[d["uuid"].notna() & d["unitid"].notna()].copy()
    d["_rang"] = d["source"].map(RANG_SOURCE).fillna(9)
    d = d.sort_values(["uuid", "_rang", "finding_aid", "unitid"])
    d = d.drop_duplicates("uuid", keep="first")
    d = d.rename(columns={"finding_aid": "dao_finding_aid", "unitid": "dao_unitid"})
    return d[["uuid", "dao_finding_aid", "dao_unitid"]]


def ajouter_dao(df, dao):
    """Ajoute dao_finding_aid + dao_unitid à df via l'uuid."""
    df = df.copy()
    return df.merge(dao, on="uuid", how="left")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Jointure DAO → fichiers (dao_finding_aid, dao_unitid)")
    p.add_argument("--input", required=True, help="CSV de fichiers contenant une colonne uuid")
    p.add_argument("--output", required=True, help="CSV de sortie (.csv.gz)")
    p.add_argument("--dao", default=DAO, help="table d'association dao_ref_link.csv")
    a = p.parse_args()

    dao = charger_dao(a.dao)
    print(f"[dao] table : {a.dao} ({len(dao)} fichiers reliés à une dao, 1 unitid retenu chacun)")

    df = pd.read_csv(a.input, low_memory=False)
    out = ajouter_dao(df, dao)
    n = int(out["dao_unitid"].notna().sum())
    print(f"[dao] {n}/{len(out)} fichiers reçoivent un dao_unitid ({n / len(out) * 100:.1f}%)")
    out.to_csv(a.output, index=False)
    print(f"[dao] écrit : {a.output}")
