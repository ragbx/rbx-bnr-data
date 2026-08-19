#!/usr/bin/env python3
r"""
poids_par_parametres.py — Poids (Mo) de chaque image selon les paramètres de conversion.

Lit un CSV concaténé (produit par concat_csv.py) et écrit, POUR CHAQUE CORPUS,
DEUX fichiers :

  1. Tableau croisé par image (« <base>_<corpus>.csv ») :
       - une ligne par image source,
       - une colonne par jeu de paramètres de conversion,
       - chaque cellule = poids du fichier produit, en Mo.

  2. Récapitulatif agrégé par jeu de paramètres (« <base>_<corpus>_agrege.csv ») :
       nb d'images, poids total / moyen / min / max en Mo, et taux de
       compression moyen (source / sortie) et gain moyen en %.

Le « corpus » (= type de document : manuscrits_plans, iconographie, presse…)
est déduit du chemin source : c'est le composant situé juste après le dossier
repère (« conservation » par défaut, cf. --marqueur), conformément à
l'arborescence <test>/conservation/<type>/<COLLECTION>/*.tif.

La « signature » de paramètres combine le format, la qualité Q, et — si une
réduction de résolution a été appliquée — le facteur f et le plancher rmin.
Ces deux derniers ne figurent pas en colonne du CSV : ils sont lus dans le nom
du fichier produit (suffixe « _q60_f50_rmin2000 » posé par tif_convert.py).
Exemples de signatures : « jp2_q60 », « jpeg_q75_f50_rmin2000 ».

Dans le tableau croisé, une colonne « source_Mo » rappelle le poids de
l'image source d'origine.

Exemples :
  python poids_par_parametres.py recap_global.csv
  python poids_par_parametres.py recap_global.csv --output poids.csv --decimales 2
  python poids_par_parametres.py recap_global.csv --marqueur conservation
"""

import argparse
import csv
import logging
import re
import sys
from pathlib import Path

MO = 1024 * 1024  # octets par Mo (Mio)

# Suffixe posé par tif_convert.target_path : _q{Q} puis éventuellement _f{f}_rmin{plancher}.
SUFFIXE_RE = re.compile(r"_q(\d+)(?:_f(\d+)_rmin(\d+))?$")


def signature(row):
    """Construit l'étiquette de paramètres d'une ligne (format, Q, et f/rmin si réduction)."""
    label = f"{row['format']}_q{row['qualite']}"
    # Le facteur et le plancher ne sont que dans le nom de fichier produit.
    stem = Path(row["fichier"]).stem
    m = SUFFIXE_RE.search(stem)
    if m and m.group(2) is not None:
        label += f"_f{m.group(2)}_rmin{m.group(3)}"
    return label


def corpus_de(source, marqueur):
    """Déduit le corpus (type de document) du chemin source.

    C'est le composant de chemin situé juste après le dossier repère `marqueur`
    (ex. conservation/iconographie/... -> « iconographie »). Si le repère est
    absent ou en dernière position, retourne « inconnu ».
    """
    parts = Path(source).parts
    try:
        i = parts.index(marqueur)
    except ValueError:
        return "inconnu"
    return parts[i + 1] if i + 1 < len(parts) else "inconnu"


def ecrire_sorties(rows, base_output, decimales):
    """Écrit le tableau croisé par image et le récapitulatif agrégé pour un lot de lignes.

    `base_output` est le chemin de base : on écrit `<base>.csv` (par image) et
    `<base>_agrege.csv` (agrégé). Retourne (nb_images, liste_signatures).
    """
    # tableau[source] = {"source_Mo": .., <signature>: <Mo>, ...}
    # agregats[signature] = liste de (taille_source_octets, taille_sortie_octets)
    tableau = {}
    signatures = set()
    agregats = {}
    for row in rows:
        src = row["source"]
        sig = signature(row)
        signatures.add(sig)
        src_oct = int(row["source_taille_octets"])
        dst_oct = int(row["taille_octets"])
        entree = tableau.setdefault(src, {"source_Mo": round(src_oct / MO, decimales)})
        if sig in entree:
            logging.warning("Doublon de paramètres « %s » pour %s : dernière valeur conservée.", sig, src)
        entree[sig] = round(dst_oct / MO, decimales)
        agregats.setdefault(sig, []).append((src_oct, dst_oct))

    colonnes_param = sorted(signatures)
    out_tableau = base_output.with_name(base_output.stem + ".csv")
    out_agrege = base_output.with_name(base_output.stem + "_agrege.csv")

    # 1) Tableau croisé par image.
    fieldnames = ["source", "source_Mo"] + colonnes_param
    with open(out_tableau, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, restval="")
        writer.writeheader()
        for src in sorted(tableau):
            row = {"source": src}
            row.update(tableau[src])
            writer.writerow(row)

    # 2) Récapitulatif agrégé par jeu de paramètres.
    fieldnames_ag = [
        "parametres", "nb_images",
        "poids_total_Mo", "poids_moyen_Mo", "poids_min_Mo", "poids_max_Mo",
        "taux_compression_moyen", "gain_moyen_pct",
    ]
    with open(out_agrege, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_ag)
        writer.writeheader()
        for sig in colonnes_param:
            paires = agregats[sig]
            n = len(paires)
            tailles = [d for _, d in paires]
            total = sum(tailles)
            # Taux de compression et gain calculés par image puis moyennés (chaque image pèse pareil).
            taux = [s / d for s, d in paires if d > 0]
            gains = [(1 - d / s) * 100 for s, d in paires if s > 0]
            writer.writerow({
                "parametres": sig,
                "nb_images": n,
                "poids_total_Mo": round(total / MO, decimales),
                "poids_moyen_Mo": round(total / n / MO, decimales),
                "poids_min_Mo": round(min(tailles) / MO, decimales),
                "poids_max_Mo": round(max(tailles) / MO, decimales),
                "taux_compression_moyen": round(sum(taux) / len(taux), 2) if taux else "",
                "gain_moyen_pct": round(sum(gains) / len(gains), 1) if gains else "",
            })

    return len(tableau), colonnes_param


def main():
    parser = argparse.ArgumentParser(
        description="Poids (Mo) de chaque image selon les paramètres de conversion."
    )
    parser.add_argument("input_csv", type=Path, help="CSV concaténé (sortie de concat_csv.py)")
    parser.add_argument("--output", type=Path, default=None,
                        help="base des CSV de sortie (défaut : <input>_poids.csv) ; "
                             "le corpus est inséré : <base>_<corpus>.csv et <base>_<corpus>_agrege.csv")
    parser.add_argument("--marqueur", default="conservation",
                        help="dossier repère ; le corpus est le composant juste après "
                             "dans le chemin source (défaut : conservation)")
    parser.add_argument("--decimales", type=int, default=3,
                        help="nombre de décimales pour les Mo (défaut : 3)")
    args = parser.parse_args()

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.input_csv.is_file():
        logging.error("CSV introuvable : %s", args.input_csv)
        sys.exit(1)

    if args.output is None:
        args.output = args.input_csv.with_name(args.input_csv.stem + "_poids.csv")

    # Regroupement des lignes par corpus (type de document déduit du chemin source).
    par_corpus = {}
    with open(args.input_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            corpus = corpus_de(row["source"], args.marqueur)
            par_corpus.setdefault(corpus, []).append(row)

    if not par_corpus:
        logging.warning("Aucune ligne dans %s.", args.input_csv)
        return

    base = args.output  # ex. recap_poids.csv -> recap_poids_<corpus>.csv
    for corpus in sorted(par_corpus):
        base_corpus = base.with_name(f"{base.stem}_{corpus}")
        n_img, sigs = ecrire_sorties(par_corpus[corpus], base_corpus, args.decimales)
        logging.info(
            "[%s] %s.csv + %s_agrege.csv : %d image(s), %d jeu(x) de paramètres (%s).",
            corpus, base_corpus, base_corpus.name, n_img, len(sigs), ", ".join(sigs),
        )


if __name__ == "__main__":
    main()
