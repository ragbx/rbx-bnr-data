#!/usr/bin/env python3
r"""
concat_csv.py — Concatène les récapitulatifs CSV produits par tif_convert.py.

Parcourt récursivement un dossier à la recherche de fichiers « *.csv » et
fusionne en un seul CSV ceux qui ont le format produit par tif_convert.py.
Une colonne « fichier_source_csv » est ajoutée pour garder la trace du CSV
d'origine de chaque ligne.

Caractéristiques :
  - parcours récursif (les CSV peuvent être dans des sous-dossiers)
  - en-tête vérifié : on saute (avec avertissement) tout CSV dont les colonnes
    ne correspondent pas au format attendu, plutôt que de mélanger des schémas
  - tri stable des lignes par chemin du fichier produit
  - le CSV de sortie est exclu de la recherche (pas d'auto-concaténation)

Exemples :
  python concat_csv.py C:\data\diffusion
  python concat_csv.py C:\data\diffusion --output recap_global.csv
  python concat_csv.py C:\data\diffusion --pattern "tif_convert_*.csv"
"""

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path

# En-tête attendu (doit rester aligné avec tif_convert.py).
EXPECTED_FIELDS = [
    "source", "source_format", "source_taille_octets",
    "source_largeur_px", "source_hauteur_px",
    "fichier", "format", "qualite", "taille_octets", "largeur_px", "hauteur_px",
]

# Colonne ajoutée pour tracer le CSV d'origine de chaque ligne.
PROVENANCE_FIELD = "fichier_source_csv"


def find_csv_files(root, pattern, output):
    """Liste les CSV à concaténer.

    Exclut le fichier de sortie ainsi que tous les CSV de concaténation déjà
    produits (« tif_convert_concat_*.csv »), pour ne jamais ré-agréger un
    récapitulatif global lors d'une relance dans le même dossier.
    """
    output = output.resolve()
    files = []
    for path in sorted(root.rglob(pattern)):
        if not path.is_file():
            continue
        if path.resolve() == output:
            continue
        if path.match("tif_convert_concat_*.csv"):
            continue
        files.append(path)
    return files


def read_rows(path):
    """Lit un CSV et retourne ses lignes (dicts). Vérifie l'en-tête.

    Retourne (rows, None) si OK, ou ([], message) si l'en-tête ne correspond pas.
    """
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != EXPECTED_FIELDS:
            return [], (
                f"en-tête inattendu : {reader.fieldnames!r} "
                f"(attendu : {EXPECTED_FIELDS!r})"
            )
        rows = list(reader)
    return rows, None


def main():
    parser = argparse.ArgumentParser(
        description="Concatène récursivement les récapitulatifs CSV de tif_convert.py."
    )
    parser.add_argument("input_dir", type=Path, help="dossier à parcourir (récursif)")
    parser.add_argument("--pattern", default="*.csv",
                        help="motif glob des CSV à concaténer (défaut : *.csv)")
    parser.add_argument("--output", type=Path, default=None,
                        help="CSV de sortie (défaut : tif_convert_concat_AAAAMMJJ_HHMMSS.csv "
                             "à la racine du dossier parcouru)")
    args = parser.parse_args()

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    root = args.input_dir.resolve()
    if not root.is_dir():
        logging.error("Dossier introuvable : %s", root)
        sys.exit(1)

    if args.output is None:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = root / f"tif_convert_concat_{stamp}.csv"

    csv_files = find_csv_files(root, args.pattern, args.output)
    if not csv_files:
        logging.warning("Aucun CSV « %s » trouvé sous %s.", args.pattern, root)
        return

    all_rows, lus, ignores = [], 0, 0
    for path in csv_files:
        rows, problem = read_rows(path)
        if problem:
            ignores += 1
            logging.warning("Ignoré %s : %s", path, problem)
            continue
        for row in rows:
            row[PROVENANCE_FIELD] = str(path)
        all_rows.extend(rows)
        lus += 1
        logging.info("Lu %s (%d ligne(s)).", path, len(rows))

    if not all_rows:
        logging.warning("Aucune ligne à écrire (%d CSV ignoré(s)).", ignores)
        return

    # Tri stable par chemin du fichier produit, pour un ordre reproductible.
    all_rows.sort(key=lambda r: r["fichier"])

    fieldnames = EXPECTED_FIELDS + [PROVENANCE_FIELD]
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    logging.info(
        "Écrit %s : %d ligne(s) issues de %d CSV (%d ignoré(s)).",
        args.output, len(all_rows), lus, ignores,
    )


if __name__ == "__main__":
    main()
