#!/usr/bin/env python3
r"""
dedup_a_transferer.py — Liste dédoublonnée des fichiers restant à transférer sur S3.

À partir d'un inventaire (CSV éventuellement .gz, ex. results/corpus/med_aff.csv.gz)
décrivant des fichiers et leur état de transfert S3, produit UN CSV listant toutes
les lignes pas-encore-sur-S3, chacune annotée du cas rencontré (`traitement`) et de
l'action à mener (`action` = « transférer » / « ne pas transférer »). Une seule
ligne par fichier est marquée « transférer » ; ses doublons sont « ne pas transférer ».

Méthode
-------
1. Périmètre : on ne garde que les lignes PAS ENCORE sur S3, c.-à-d. dont la colonne
   `s3_uploaded` ne vaut pas True (par défaut ; cf. --flag-col).

2. Doublons : deux lignes désignent le même fichier si elles partagent la même CLÉ,
   par défaut (`checksum_md5`, `name`) — même contenu ET même nom (cf. --key).

3. Départage (quelle ligne conserver dans un groupe de doublons) :
     a. on classe d'abord les lignes par `name` puis `path` (cf. --sort) ;
     b. dans chaque groupe, on garde celle dont la date de modification
        (`last_metadata_modification_date` par défaut, cf. --date-col) est la PLUS
        RÉCENTE ; à défaut (dates égales ou absentes), la PREMIÈRE selon le
        classement name/path.

   Note : `last_content_modification_date` ne varie jamais entre doublons (même
   contenu = même date de contenu) ; c'est `last_metadata_modification_date` qui
   départage réellement, d'où le défaut.

Sortie
------
Un CSV (non compressé) écrit dans le dossier de l'inventaire, nommé
`<inventaire>_a_transferer.csv` (cf. --output), conservant toutes les colonnes
d'origine plus `traitement` et `action`.

Exemples
--------
  python dedup_a_transferer.py results/corpus/med_aff.csv.gz
  python dedup_a_transferer.py inv.csv.gz --key checksum_md5 --date-col last_content_modification_date
  python dedup_a_transferer.py inv.csv.gz --output a_transferer.csv
"""

import argparse
import sys
from pathlib import Path

import pandas as pd


def charger(input_csv: Path) -> pd.DataFrame:
    """Lit l'inventaire (gère .gz via l'extension)."""
    return pd.read_csv(input_csv)


def selectionner_a_transferer(df, flag_col):
    """Garde les lignes pas encore sur S3 : `flag_col` différent de True.

    Les valeurs True/False peuvent être des booléens ou les chaînes « True »/« False ».
    """
    flag = df[flag_col].map(lambda v: str(v).strip().lower() == "true")
    return df[~flag].copy()


TRAITEMENT_COL = "traitement"  # colonne décrivant le cas et la décision
ACTION_COL = "action"          # colonne : « transférer » / « ne pas transférer »
A_TRANSFERER = "transférer"
NE_PAS_TRANSFERER = "ne pas transférer"


def annoter(df, key, sort_cols, date_col):
    """Annote CHAQUE ligne (pas-encore-sur-S3) du cas et de l'action à mener.

    Dans un groupe de doublons (même `key`), une seule ligne est conservée : la plus
    récemment modifiée selon `date_col` ; à dates égales/absentes, la première selon
    `sort_cols` (name, path). Les autres sont écartées.

    Ajoute deux colonnes :
      - `traitement` : le cas rencontré et la décision (unique / doublon conservé /
        doublon écarté) ;
      - `action`     : « transférer » pour la ligne conservée, « ne pas transférer »
        pour les doublons écartés.
    """
    # 1) ordre de base : name, path (le « sinon, le premier »).
    df = df.sort_values(sort_cols, kind="stable", na_position="last")
    # 2) date exploitable : timestamps Unix (float) -> numérique ; repli datetime
    #    si la colonne n'est pas numérique (ex. dates ISO).
    date = pd.to_numeric(df[date_col], errors="coerce")
    if date.isna().all():
        date = pd.to_datetime(df[date_col], errors="coerce")
    df = df.assign(_date=date)

    # 3) annotations par groupe : nb de copies, date la plus récente, nb de lignes à cette date.
    grp = df.groupby(key, dropna=False)
    df["_nb_copies"] = grp["_date"].transform("size")
    date_max = grp["_date"].transform("max")
    df["_n_au_max"] = (df["_date"] == date_max).groupby([df[c] for c in key]).transform("sum")

    # 4) la plus récente en tête (tri stable -> conserve l'ordre name/path en cas d'égalité) ;
    #    la première ligne de chaque clé est celle qu'on conserve.
    df = df.sort_values("_date", kind="stable", ascending=False, na_position="last")
    df["_garde"] = ~df.duplicated(subset=key, keep="first")

    # 4b) doublons résiduels : parmi les lignes conservées, celles encore en doublon
    #     sur une SEULE composante de la clé (md5 seul -> même contenu sous des noms
    #     différents ; name seul -> même nom pour des contenus différents, risque de
    #     collision de clé S3) ne sont PAS transférées.
    garde_idx = df.index[df["_garde"]]
    df["_disq"] = False
    df["_conflit"] = ""
    for col in key:
        flag = pd.Series(False, index=df.index)
        flag.loc[garde_idx] = df.loc[garde_idx, col].duplicated(keep=False).values
        df["_disq"] = df["_disq"] | flag
        df.loc[flag, "_conflit"] = df.loc[flag, "_conflit"].map(
            lambda s: f"{s}, {col}" if s else col
        )

    # 5) colonnes lisibles. On ne transfère que les lignes conservées ET non disqualifiées.
    transfere = df["_garde"] & ~df["_disq"]
    df[ACTION_COL] = transfere.map({True: A_TRANSFERER, False: NE_PAS_TRANSFERER})
    df[TRAITEMENT_COL] = df.apply(_traitement, axis=1)

    df = df.drop(columns=["_date", "_nb_copies", "_n_au_max", "_garde", "_disq", "_conflit"])
    # Sortie reclassée lisiblement par name, path (doublons regroupés).
    return df.sort_values(sort_cols, kind="stable", na_position="last")


# Raisons lisibles d'un doublon résiduel sur une composante de la clé.
RAISON_CONFLIT = {
    "checksum_md5": "même contenu sous des noms différents",
    "name": "même nom pour des contenus différents",
}


def _traitement(row):
    """Décrit le cas et la décision pour une ligne."""
    # Doublon écarté lors du dédoublonnage principal (sur la clé complète).
    if not row["_garde"]:
        nb = int(row["_nb_copies"])
        return f"doublon ({nb} copies, {nb - 1} écartée(s)) : écarté — copie non retenue"
    # Conservé mais en doublon résiduel sur une composante de la clé -> non transféré.
    if row["_disq"]:
        cols = [c.strip() for c in row["_conflit"].split(",")]
        raisons = " ; ".join(RAISON_CONFLIT.get(c, c) for c in cols)
        return f"doublon résiduel sur {row['_conflit']} : non transféré ({raisons})"
    # Conservé et propre.
    nb = int(row["_nb_copies"])
    if nb == 1:
        return "unique : à transférer"
    if int(row["_n_au_max"]) == 1:
        motif = "modif. la plus récente"
    elif pd.isna(row["_date"]):
        motif = "1er (dates absentes)"
    else:
        motif = "1er (dates égales)"
    return f"doublon ({nb} copies, {nb - 1} écartée(s)) : conservé — {motif}"


def main():
    parser = argparse.ArgumentParser(
        description="Liste dédoublonnée des fichiers restant à transférer sur S3."
    )
    parser.add_argument("input_csv", type=Path, help="inventaire CSV (.csv ou .csv.gz)")
    parser.add_argument("--key", nargs="+", default=["checksum_md5", "name"],
                        help="colonnes définissant un doublon (défaut : checksum_md5 name)")
    parser.add_argument("--sort", nargs="+", default=["name", "path"],
                        help="ordre de classement / départage (défaut : name path)")
    parser.add_argument("--date-col", default="last_metadata_modification_date",
                        help="date de modification pour le départage "
                             "(défaut : last_metadata_modification_date)")
    parser.add_argument("--flag-col", default="s3_uploaded",
                        help="colonne booléenne « déjà sur S3 » (défaut : s3_uploaded)")
    parser.add_argument("--output", type=Path, default=None,
                        help="CSV de sortie (défaut : <inventaire>_a_transferer.csv)")
    args = parser.parse_args()

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    if not args.input_csv.is_file():
        print(f"Inventaire introuvable : {args.input_csv}", file=sys.stderr)
        sys.exit(1)

    df = charger(args.input_csv)

    requises = set(args.key) | set(args.sort) | {args.date_col, args.flag_col}
    manquantes = [c for c in requises if c not in df.columns]
    if manquantes:
        print(f"Colonne(s) absente(s) : {', '.join(sorted(manquantes))}", file=sys.stderr)
        print(f"Colonnes disponibles : {', '.join(df.columns)}", file=sys.stderr)
        sys.exit(1)

    candidats = selectionner_a_transferer(df, args.flag_col)
    annote = annoter(candidats, args.key, args.sort, args.date_col)

    # Nom de sortie : on retire les suffixes .gz puis .csv de l'inventaire.
    stem = args.input_csv.name
    for suf in (".gz", ".csv"):
        if stem.endswith(suf):
            stem = stem[: -len(suf)]
    output = args.output or args.input_csv.with_name(f"{stem}_a_transferer.csv")
    annote.to_csv(output, index=False, encoding="utf-8")

    a_transferer = (annote[ACTION_COL] == A_TRANSFERER).sum()
    print(f"Inventaire           : {len(df)} ligne(s)")
    print(f"Pas encore sur S3    : {len(candidats)} ligne(s) ({args.flag_col} != True)")
    print(f"  -> à transférer    : {a_transferer} (clé = {' + '.join(args.key)})")
    print(f"  -> ne pas transf.  : {len(candidats) - a_transferer} (doublons écartés)")
    print(f"CSV annoté écrit     : {output}")
    print("Répartition par cas (colonne « traitement ») :")
    for cas, n in annote[TRAITEMENT_COL].value_counts().items():
        print(f"  {n:6d}  {cas}")


if __name__ == "__main__":
    main()
