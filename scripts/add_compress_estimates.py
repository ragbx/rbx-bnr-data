#!/usr/bin/env python3
"""Ajoute deux colonnes d'estimation de poids apres compression au referentiel.

- jpeg_q85_size : poids estime en JPEG q85, base sur `size` avec un ratio 1:12
- jp2_lossy15_size : poids estime en JP2 lossy, base sur `size` avec un ratio 1:15

Toutes les colonnes d'origine sont conservees. Traitement en streaming (gz->gz).
"""
import csv
import gzip
import sys

SRC = "results/ref/_ref_files_20260502.csv.gz"
DST = "results/ref/_ref_files_20260502_compress.csv.gz"

JPEG_RATIO = 12  # JPEG q85 ~1:12
JP2_RATIO = 15   # JP2 lossy ~1:15


def estimate(size_str, ratio):
    if size_str in ("", None):
        return ""
    try:
        return str(int(round(float(size_str) / ratio)))
    except (ValueError, TypeError):
        return ""


def main():
    n = 0
    with gzip.open(SRC, "rt", newline="") as fin, \
         gzip.open(DST, "wt", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        header = next(reader)
        size_idx = header.index("size")
        writer.writerow(header + ["jpeg_q85_size", "jp2_lossy15_size"])
        for row in reader:
            size = row[size_idx]
            row.append(estimate(size, JPEG_RATIO))
            row.append(estimate(size, JP2_RATIO))
            writer.writerow(row)
            n += 1
            if n % 500000 == 0:
                print(f"  {n} lignes...", file=sys.stderr)
    print(f"OK : {n} lignes ecrites -> {DST}", file=sys.stderr)


if __name__ == "__main__":
    main()
