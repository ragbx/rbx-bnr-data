#!/usr/bin/env python3
r"""stats.py — Statistiques de conversion : pour chaque fichier source, la taille
du TIFF de depart et la taille du JPEG produit a chaque taux de compression.

Lit un (ou plusieurs) CSV recap produit par convert.py (colonnes : src, corpus_code,
src_size, dst, quality, dst_size, width, height) et construit un tableau pivote,
une ligne par fichier source (stem) et une colonne par taux (taille du JPEG, puis
ratio de compression src_size/taille_qXX) :

  stem, src, corpus_code, src_size, taille_q80, taille_q85, ..., ratio_q80, ratio_q85, ...

Usage :
  python stats.py conversion_20260818.csv --output stats.csv
  python stats.py conversion_*.csv --output stats.csv   # plusieurs recaps concatenes
"""

import argparse
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description="Tableau pivote des tailles JPEG par taux, a partir du/des recap(s) de convert.py."
    )
    parser.add_argument("recap_csv", nargs="+", type=Path,
                        help="un ou plusieurs CSV recap produits par convert.py")
    parser.add_argument("--output", type=Path, default=None,
                        help="CSV de sortie (defaut : stats_AAAAMMJJHHMMSS.csv)")
    args = parser.parse_args()

    manquants = [p for p in args.recap_csv if not p.is_file()]
    if manquants:
        parser.error("Fichier(s) introuvable(s) : " + ", ".join(str(p) for p in manquants))

    df = pd.concat([pd.read_csv(p) for p in args.recap_csv], ignore_index=True)
    df["stem"] = df["src"].map(lambda p: Path(p).stem)

    pivot = df.pivot_table(index=["stem", "src", "corpus_code", "src_size"], columns="quality", values="dst_size")
    qualites = list(pivot.columns)
    pivot.columns = [f"taille_q{q}" for q in qualites]
    pivot = pivot.reset_index()

    # Ratio de compression src_size/taille_qXX (ex. 5.0 = fichier 5x plus petit que le TIFF).
    for q in qualites:
        pivot[f"ratio_q{q}"] = (pivot["src_size"] / pivot[f"taille_q{q}"]).round(2)

    pivot = pivot.sort_values("stem")

    output = args.output or Path(f"stats_{pd.Timestamp.now():%Y%m%d%H%M%S}.csv")
    pivot.to_csv(output, index=False)

    print(f"{len(pivot)} fichier(s) source, taux : {sorted(int(q) for q in df['quality'].unique())}")
    print(f"Stats ecrites : {output}")


if __name__ == "__main__":
    main()
