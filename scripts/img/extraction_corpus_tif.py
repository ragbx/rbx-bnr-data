"""Construction de trois corpus d'images tif a partir du referentiel des fichiers.

A partir de results/ref/_ref_files_<date>.csv.gz, on construit trois corpus :
  - corpus 1 : tous les corpus_code commencant par PRA_
  - corpus 2 : MED_CP, MED_IMA, MED_AFF, AMR_AFF, AMR_DEL
  - corpus 3 : MED_MS, MED_PLA, MED_MON

Contraintes :
  - on ne retient que les fichiers tif effectivement deposes sur S3
    (s3_uploaded = True), avec une exception pour MED_PLA (aucun depot S3) :
    on garde ses tif dont la source S3 est "az" (source2s3 = "az") ;
  - les trois corpus reunis comptent au plus 600 documents et ne depassent pas
    200 Go ;
  - tous les corpus_code (des trois corpus) contiennent le meme nombre de
    documents (1 document = 1 tif unique, identifie par son uuid).

Methode : on applique un nombre N de documents identique a chaque corpus_code.
N est le plus grand entier tel que (a) N <= effectif du plus petit corpus_code,
(b) N * nombre_total_de_corpus_codes <= 600 et (c) la taille totale reste
<= 200 Go. Les N documents de chaque corpus_code sont tires aleatoirement
(graine fixe -> resultat reproductible).

Sortie : un manifeste CSV.gz par corpus dans results/corpus/, reprenant les
colonnes du referentiel pour les fichiers retenus.
"""

import argparse
from datetime import datetime
from os.path import join

import numpy as np
import pandas as pd

DATE_REF = "20260502"
MAX_SIZE_GO = 200
MAX_DOCS_TOTAL = 600
SEED = 42  # graine par defaut : reproduit le corpus d'origine

# Definition des trois corpus. La cle est le slug utilise pour nommer le fichier.
CORPORA = {
    "presse": {"prefix": "PRA_"},
    "iconographie": {"codes": ["MED_CP", "MED_IMA", "MED_AFF", "AMR_AFF", "AMR_DEL"]},
    "manuscrits_plans": {"codes": ["MED_MS", "MED_PLA", "MED_MON"]},
}


def codes_of(tif: pd.DataFrame, spec: dict) -> list:
    """Liste triee des corpus_code correspondant a la definition d'un corpus."""
    if "prefix" in spec:
        mask = tif["corpus_code"].str.startswith(spec["prefix"])
    else:
        mask = tif["corpus_code"].isin(spec["codes"])
    return sorted(tif.loc[mask, "corpus_code"].unique())


def main():
    parser = argparse.ArgumentParser(
        description="Construit les trois corpus d'images tif (memes contraintes ; "
                    "changer --seed pour un nouveau tirage)."
    )
    parser.add_argument("--seed", type=int, default=SEED,
                        help=f"graine du tirage aleatoire (defaut : {SEED}). "
                             "Une graine differente produit un autre corpus a contraintes egales.")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y%m%d")

    ref = pd.read_csv(
        join("results", "ref", f"_ref_files_{DATE_REF}.csv.gz"), low_memory=False
    )

    # Fichiers tif deposes sur S3, dedoublonnes (le referentiel contient des
    # lignes en double). Exception MED_PLA (aucun depot S3) : on garde les tif
    # dont la source S3 est "az".
    uploaded = ref["s3_uploaded"].astype(str).str.lower() == "true"
    med_pla_az = (ref["corpus_code"] == "MED_PLA") & (ref["source2s3"] == "az")
    tif = ref[(ref["extension"] == ".tif") & (uploaded | med_pla_az)].drop_duplicates(
        subset="uuid"
    )
    print(f"Referentiel : {len(ref)} lignes -> {len(tif)} tif uniques retenus")

    # Liste des corpus_code par corpus, puis melange reproductible + tailles
    # cumulees pour chacun.
    corpus_codes = {name: codes_of(tif, spec) for name, spec in CORPORA.items()}
    all_codes = [c for codes in corpus_codes.values() for c in codes]

    print(f"Graine aleatoire : {args.seed}")
    rng = np.random.default_rng(args.seed)
    shuffled = {}
    cumsizes = {}
    for code in all_codes:
        sub = tif[tif["corpus_code"] == code].sample(frac=1, random_state=rng)
        shuffled[code] = sub
        cumsizes[code] = sub["size"].to_numpy().cumsum()

    # Recherche du N commun a tous les corpus_code.
    n_avail = min(len(s) for s in shuffled.values())          # effectifs dispo
    n_docs = MAX_DOCS_TOTAL // len(all_codes)                 # plafond documents
    n_cap = min(n_avail, n_docs)

    total = np.zeros(n_cap)                                   # taille cumulee
    for code in all_codes:
        total += cumsizes[code][:n_cap]
    fits = np.nonzero(total <= MAX_SIZE_GO * 1e9)[0]
    n = int(fits[-1] + 1) if len(fits) else 0

    print(f"\nN = {n} documents / corpus_code "
          f"(dispo={n_avail}, plafond docs={n_docs}, {len(all_codes)} corpus_codes)")

    # Construction et ecriture de chaque corpus.
    total_docs = 0
    total_go = 0.0
    for name, codes in corpus_codes.items():
        selected = pd.concat([shuffled[code].iloc[:n] for code in codes])
        total_docs += len(selected)
        total_go += selected["size"].sum() / 1e9

        print(f"\n=== Corpus '{name}' : {len(selected)} documents ===")
        for code in codes:
            part = shuffled[code].iloc[:n]
            print(f"  {code:12s} {len(part):5d} docs  {part['size'].sum() / 1e9:7.2f} Go")
        print(f"  {'TOTAL':12s} {len(selected):5d} docs  "
              f"{selected['size'].sum() / 1e9:7.2f} Go")

        out = join("results", "corpus", f"corpus_{name}_{today}.csv.gz")
        selected.to_csv(out, index=False)
        print(f"  -> {out}")

    print(f"\nTotal des trois corpus : {total_docs} documents / {total_go:.2f} Go "
          f"(plafonds {MAX_DOCS_TOTAL} docs, {MAX_SIZE_GO} Go)")


if __name__ == "__main__":
    main()
