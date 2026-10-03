#!/usr/bin/env python3
r"""
pivot_recap_conversion.py — Équivalent de img1/pivot_recap.py, mais pour les
récapitulatifs produits par img2/convert.py (colonnes : src, corpus_code,
manifest, src_size, dst, quality, dst_size, width, height), tels qu'on les
trouve directement sous un dossier --dest (ex. F:\corpus\conversion_*.csv).

Ne modifie rien sous scripts/img/img2 : ce script lit seulement les CSV
« conversion_*.csv » déjà produits par convert.py, sans y toucher.

Étapes :
  1. Concatène tous les « conversion_*.csv » trouvés à la racine du dossier
     donné (pas de récursion : c'est là que convert.py les écrit).
  2. Complète corpus_code quand la colonne est absente (anciens recaps,
     avant son ajout à convert.py) à partir du nom de fichier source
     (motif RBX_<corpus_code>_..., ex. RBX_AMR_AFF_... -> AMR_AFF).
  3. Ajoute deux colonnes dérivées : taille_Mo (dst_size / 1024²) et
     ratio_taille (src_size / dst_size).
  4. Écrit un tableau croisé (comme pivot_recap.py) :
       - onglet « pivot »      : index=src,         columns=quality
       - onglet « par_corpus » : index=corpus_code, columns=quality
     valeurs : taille_Mo, ratio_taille (aggfunc mean par défaut).

Usage :
  python pivot_recap_conversion.py F:\corpus
  python pivot_recap_conversion.py F:\corpus --output results/img/recap_corpus_F_pivot.xlsx
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
from openpyxl.styles import PatternFill

MO = 1024 * 1024  # octets par Mio
# Dossier de sortie par défaut, ancré à la racine du projet (scripts/img/ -> ../).
RESULTS_DIR = Path(__file__).resolve().parents[2] / "results" / "img"

# Nom de fichier source : RBX_<corpus_code>_..., corpus_code pouvant contenir
# un ou plusieurs « _ » (ex. RBX_AMR_AFF_001... -> AMR_AFF). Capture non gourmande
# suivie d'un segment numérique/alphanumérique pour éviter de tout avaler.
BASENAME_RE = re.compile(r"^RBX_([A-Za-z0-9]+_[A-Za-z0-9]+)_")

EXPECTED_FIELDS = {
    "src", "corpus_code", "manifest", "src_size", "dst", "quality", "dst_size", "width", "height",
}


def deduire_corpus_code(src: str) -> str:
    """Déduit corpus_code du nom du fichier source quand la colonne est absente."""
    m = BASENAME_RE.match(Path(src).stem)
    return m.group(1) if m else "inconnu"


def charger(input_dir: Path, pattern: str) -> pd.DataFrame:
    """Concatène les conversion_*.csv (racine du dossier, pas de récursion)."""
    fichiers = sorted(input_dir.glob(pattern))
    if not fichiers:
        print(f"Aucun CSV « {pattern} » trouvé sous {input_dir}.", file=sys.stderr)
        sys.exit(1)

    frames = []
    for f in fichiers:
        df = pd.read_csv(f)
        inconnues = set(df.columns) - EXPECTED_FIELDS
        if inconnues:
            print(f"Attention : colonne(s) inattendue(s) dans {f} : {sorted(inconnues)}", file=sys.stderr)
        df["fichier_source_csv"] = str(f)
        frames.append(df)
        print(f"Lu {f} ({len(df)} ligne(s)).")

    df = pd.concat(frames, ignore_index=True, sort=False)

    if "corpus_code" not in df.columns:
        df["corpus_code"] = pd.NA
    manquants = df["corpus_code"].isna()
    if manquants.any():
        df.loc[manquants, "corpus_code"] = df.loc[manquants, "src"].map(deduire_corpus_code)
        print(f"corpus_code déduit du nom de fichier pour {manquants.sum()} ligne(s) "
              f"(colonne absente du CSV d'origine).")

    df["source_taille_Mo"] = df["src_size"] / MO
    df["taille_Mo"] = df["dst_size"] / MO
    df["ratio_taille"] = df["src_size"] / df["dst_size"].where(df["dst_size"] > 0)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tableau croisé (pivot_table) des récapitulatifs conversion_*.csv de convert.py (img2)."
    )
    parser.add_argument("input_dir", type=Path, help="dossier --dest de convert.py (ex. F:\\corpus)")
    parser.add_argument("--pattern", default="conversion_*.csv",
                        help="motif glob des CSV à concaténer (défaut : conversion_*.csv)")
    parser.add_argument("--values", nargs="+", default=["taille_Mo", "ratio_taille"],
                        help="colonne(s) de valeurs ventilées par qualité (défaut : taille_Mo ratio_taille)")
    parser.add_argument("--aggfunc", nargs="+", default=["mean"],
                        help="fonction(s) d'agrégation (défaut : mean ; ex. mean median sum count)")
    parser.add_argument("--decimales", type=int, default=3,
                        help="arrondi des valeurs numériques (défaut : 3)")
    parser.add_argument("--output", type=Path, default=None,
                        help="fichier Excel de sortie "
                             "(défaut : results/img/recap_corpus_<nom_dossier>_<AAAAMMJJ>_pivot.xlsx)")
    args = parser.parse_args()

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    if not args.input_dir.is_dir():
        print(f"Dossier introuvable : {args.input_dir}", file=sys.stderr)
        sys.exit(1)

    df = charger(args.input_dir, args.pattern)

    aggfunc = args.aggfunc[0] if len(args.aggfunc) == 1 else args.aggfunc

    def croiser(index):
        return pd.pivot_table(
            df, index=index, columns=["quality"], values=args.values, aggfunc=aggfunc,
        ).round(args.decimales)

    pivot = croiser(["src"])
    pivot_corpus = croiser(["corpus_code"])

    # Taille du TIFF source en Mo : constante par fichier (indépendante de la
    # qualité de sortie), donc une seule colonne plutôt que ventilée par qualité.
    fichiers_uniques = df.drop_duplicates(subset="src")[["src", "corpus_code", "source_taille_Mo"]]
    taille_source_src = fichiers_uniques.set_index("src")["source_taille_Mo"].round(args.decimales)
    taille_source_corpus = (
        fichiers_uniques.groupby("corpus_code")["source_taille_Mo"].mean().round(args.decimales)
    )
    pivot.insert(0, ("source_taille_Mo", ""), taille_source_src)
    pivot_corpus.insert(0, ("source_taille_Mo", ""), taille_source_corpus)

    resolu = args.input_dir.resolve()
    lecteur = resolu.drive.rstrip(":\\")
    nom_dossier = resolu.name or lecteur
    if lecteur and lecteur.upper() != nom_dossier.upper():
        nom_dossier = f"{lecteur}_{nom_dossier}"
    stamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    output = args.output or RESULTS_DIR / f"recap_corpus_{nom_dossier}_{stamp}_pivot.xlsx"
    output.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pivot.to_excel(writer, sheet_name="pivot")
        pivot_corpus.to_excel(writer, sheet_name="par_corpus")
        fond_blanc = PatternFill(fill_type="solid", fgColor="FFFFFFFF")
        for ws in writer.sheets.values():
            for row in ws.iter_rows():
                for cell in row:
                    cell.fill = fond_blanc

    print(f"Tableau croisé écrit : {output}")
    print(f"  onglet « pivot »      : {pivot.shape[0]} ligne(s) × {pivot.shape[1]} colonne(s) (index=src)")
    print(f"  onglet « par_corpus » : {pivot_corpus.shape[0]} ligne(s) × "
          f"{pivot_corpus.shape[1]} colonne(s) (index=corpus_code)")
    print(f"  columns=['quality']  values={args.values}  aggfunc={aggfunc}")
    with pd.option_context("display.max_rows", 20, "display.width", 200):
        print(pivot_corpus)


if __name__ == "__main__":
    main()
