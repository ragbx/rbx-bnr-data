#!/usr/bin/env python3
r"""
pivot_recap.py — Analyse d'un fichier recap en tableau croisé (pandas.pivot_table).

Lit un CSV recap (produit par concat_csv.py / la chaîne de conversion) dont les
colonnes attendues sont :

  source, source_format, source_taille_octets, source_largeur_px,
  source_hauteur_px, fichier, format, qualite, taille_octets, largeur_px,
  hauteur_px, fichier_source_csv

et construit un tableau croisé dynamique :

  - index   : par défaut « source » (l'image d'origine, cf. --index),
  - colonnes : par défaut [« format », « qualite »] (cf. --columns),
  - valeurs : par défaut [« taille_Mo », « taux_compression »] (cf. --values),
  - agrégation : par défaut « mean » (cf. --aggfunc).

Deux colonnes dérivées sont ajoutées avant le pivot et utilisables comme valeurs :
  - taille_Mo     : taille_octets / 1024² (poids du fichier produit en Mio),
  - ratio_taille  : source_taille_octets / taille_octets (ratio de compression).

Exemples :
  python pivot_recap.py results/img/recap_global_20260630.csv
  python pivot_recap.py recap.csv --values taille_octets --aggfunc mean median
  python pivot_recap.py recap.csv --index source --columns format qualite \
      --values ratio_taille --output pivot.xlsx
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

MO = 1024 * 1024  # octets par Mio
# Dossier de sortie par défaut, ancré à la racine du projet (scripts/img/ -> ../../).
RESULTS_DIR = Path(__file__).resolve().parents[2] / "results" / "img"

# Nom du corpus dans le chemin source : .../corpus_<type>_<AAAAMMJJ>_<n>/...
# Le <type> peut contenir des « _ » (ex. manuscrits_plans) -> capture non gourmande.
CORPUS_RE = re.compile(r"corpus_(.+?)_\d{8}_\d+")


def extraire_corpus(source: str) -> str:
    """Déduit le nom du corpus (type de document) du chemin source.

    Ex. .../corpus_iconographie_20260502_1/... -> « iconographie »,
        .../corpus_manuscrits_plans_20260502_4/... -> « manuscrits_plans ».
    Retourne « inconnu » si le motif est absent.
    """
    m = CORPUS_RE.search(source)
    return m.group(1) if m else "inconnu"


def charger(input_csv: Path) -> pd.DataFrame:
    """Charge le recap et ajoute les colonnes dérivées (corpus, taille_Mo, ratio_taille)."""
    df = pd.read_csv(input_csv)
    df["corpus"] = df["source"].map(extraire_corpus)
    df["taille_Mo"] = df["taille_octets"] / MO
    # Ratio de compression source/sortie ; éviter la division par zéro.
    df["ratio_taille"] = df["source_taille_octets"] / df["taille_octets"].where(
        df["taille_octets"] > 0
    )
    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tableau croisé (pivot_table) d'un fichier recap de conversion."
    )
    parser.add_argument("input_csv", type=Path, help="CSV recap à analyser")
    parser.add_argument("--index", nargs="+", default=["source"],
                        help="colonne(s) en index (défaut : source)")
    parser.add_argument("--columns", nargs="+", default=["format", "qualite"],
                        help="colonne(s) en colonnes (défaut : format qualite)")
    parser.add_argument("--values", nargs="+", default=["taille_octets", "ratio_taille"],
                        help="colonne(s) de valeurs (défaut : taille_octets ratio_taille)")
    parser.add_argument("--aggfunc", nargs="+", default=["mean"],
                        help="fonction(s) d'agrégation (défaut : mean ; ex. mean median sum count)")
    parser.add_argument("--decimales", type=int, default=3,
                        help="arrondi des valeurs numériques (défaut : 3)")
    parser.add_argument("--output", type=Path, default=None,
                        help="fichier Excel de sortie "
                             "(défaut : results/img/<nom_entrée>_pivot.xlsx)")
    args = parser.parse_args()

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    if not args.input_csv.is_file():
        print(f"CSV introuvable : {args.input_csv}", file=sys.stderr)
        sys.exit(1)

    df = charger(args.input_csv)

    manquantes = [c for c in args.index + args.columns + args.values if c not in df.columns]
    if manquantes:
        print(f"Colonne(s) absente(s) du CSV : {', '.join(manquantes)}", file=sys.stderr)
        print(f"Colonnes disponibles : {', '.join(df.columns)}", file=sys.stderr)
        sys.exit(1)

    aggfunc = args.aggfunc[0] if len(args.aggfunc) == 1 else args.aggfunc

    def croiser(index):
        return pd.pivot_table(
            df, index=index, columns=args.columns, values=args.values, aggfunc=aggfunc,
        ).round(args.decimales)

    # Onglet 1 : index choisi (défaut « source »). Onglet 2 : agrégé par corpus.
    pivot = croiser(args.index)
    pivot_corpus = croiser(["corpus"])

    # Sortie dans results/img, nom dérivé du fichier d'entrée.
    output = args.output or RESULTS_DIR / f"{args.input_csv.stem}_pivot.xlsx"
    output.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output) as writer:
        pivot.to_excel(writer, sheet_name="pivot")
        pivot_corpus.to_excel(writer, sheet_name="par_corpus")

    print(f"Tableau croisé écrit : {output}")
    print(f"  onglet « pivot »     : {pivot.shape[0]} ligne(s) × {pivot.shape[1]} colonne(s) "
          f"(index={args.index})")
    print(f"  onglet « par_corpus » : {pivot_corpus.shape[0]} ligne(s) × "
          f"{pivot_corpus.shape[1]} colonne(s) (index=['corpus'])")
    print(f"  columns={args.columns}  values={args.values}  aggfunc={aggfunc}")
    with pd.option_context("display.max_rows", 20, "display.width", 200):
        print(pivot_corpus)


if __name__ == "__main__":
    main()
