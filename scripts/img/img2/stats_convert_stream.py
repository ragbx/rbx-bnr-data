#!/usr/bin/env python3
r"""stats_convert_stream.py — Statistiques de compression sur une sortie de
convert_stream.py, alors que les TIFF source n'existent nulle part sur la
machine (jamais copies, cf. convert_stream.py) : leur taille est recuperee
dans le ref (colonne `size`), pas mesuree sur disque.

Etapes :
  1. Liste tous les JPEG sous --dest (recursif), releve leur taille.
  2. Deduit stem (nom sans `_qNN` ni extension) et qualite (75/80/85/90/...)
     depuis le nom de fichier (convention posee par convert.py/convert_stream.py :
     page001_q80.jpg).
  3. Reconstruit la cle "stem" du ref (chemin relatif, sans extension) et
     rapproche chaque JPEG du TIFF source correspondant dans le ref (colonne
     `size`), en comparant sur le chemin+nom sans extension (insensible a la
     casse) plutot que sur l'extension exacte (le ref peut avoir .tif ou .tiff).
  4. Pivot par stem : taille (Mo) de chaque qualite + ratio taille_source/taille_jpeg.
  5. Pivot par corpus_code : medianes des memes valeurs, + nb_documents.

Usage :
  python stats_convert_stream.py --dest /mnt/jpeg_stream --out-dir results/img
"""

import argparse
import posixpath as pp
from pathlib import Path

import pandas as pd

MO = 1024 * 1024


def lister_jpeg(dest: Path) -> pd.DataFrame:
    rows = []
    for p in dest.rglob("*"):
        if p.is_file() and p.suffix.lower() in (".jpg", ".jpeg"):
            rows.append((p.stat().st_size, str(p.relative_to(dest)).replace("\\", "/")))
    if not rows:
        raise SystemExit(f"Aucun JPEG trouve sous {dest}")
    df = pd.DataFrame(rows, columns=["taille", "rel_path"])
    return df


def deduire_stem_qualite(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    nom_sans_ext = df["rel_path"].apply(lambda p: pp.splitext(p)[0])
    q = nom_sans_ext.str.extract(r"_q(\d{2,3})$")[0]
    df["qualite"] = "q" + q
    df.loc[q.isna(), "qualite"] = "inconnue"
    df["stem_key"] = nom_sans_ext.str.replace(r"_q\d{2,3}$", "", regex=True).str.lower()
    return df


def charger_ref_tailles(ref_path: str, corpus_list: list) -> pd.DataFrame:
    """Charge les masters TIFF du ref avec DEUX cles de rapprochement possibles,
    calquees sur common.relkey (download.py/convert_stream.py) :
      - stem_key       : depuis s3_key quand il existe (cas normal)
      - stem_key_repli : depuis <fonds>/<corpus_code>/<name>, pour les lignes du
        ref qui n'ont PAS de s3_key (fichiers pas encore "keyes" mais bien
        presents et convertis via le repli de relkey)."""
    ref = pd.read_csv(ref_path, dtype=str, low_memory=False,
                       usecols=["path", "name", "s3_key", "corpus_code", "uuid", "extension", "size"])
    ref = ref[ref["extension"].str.lower().isin([".tif", ".tiff"])]
    if corpus_list:
        ref = ref[ref["corpus_code"].isin(corpus_list)]
    ref["size"] = ref["size"].astype(float)

    avec_s3 = ref[ref["s3_key"].notna()].copy()
    avec_s3["stem_key"] = avec_s3["s3_key"].apply(lambda s: pp.splitext(s)[0]).str.lower()
    avec_s3 = avec_s3.drop_duplicates(subset="stem_key", keep="first")

    sans_s3 = ref[ref["s3_key"].isna()].copy()
    sans_s3["stem_key"] = (
        sans_s3["corpus_code"].str.split("_").str[0] + "/" + sans_s3["corpus_code"] + "/"
        + sans_s3["name"].apply(lambda n: pp.splitext(n)[0])
    ).str.lower()
    sans_s3 = sans_s3.drop_duplicates(subset="stem_key", keep="first")

    cols = ["stem_key", "size", "corpus_code", "uuid"]
    return avec_s3[cols], sans_s3[cols]


def construire_pivots(df: pd.DataFrame):
    df = df.copy()
    df["taille_Mo"] = df["taille"] / MO

    qualites = sorted(
        [q for q in df["qualite"].unique() if q != "inconnue"],
        key=lambda q: int(q[1:]),
    )

    piv = df.pivot_table(index="stem_key", columns="qualite", values="taille_Mo", aggfunc="first")
    piv = piv.reindex(columns=qualites)

    source = df.drop_duplicates("stem_key").set_index("stem_key")[["size_source_Mo", "corpus_code", "match"]]
    piv = piv.join(source)

    for q in qualites:
        piv[f"ratio_source_{q}"] = piv["size_source_Mo"] / piv[q]

    piv = piv.reset_index().rename(columns={"size_source_Mo": "taille_source_Mo"})

    value_cols = qualites + [f"ratio_source_{q}" for q in qualites]
    par_corpus = piv.groupby("corpus_code")[value_cols].median()
    par_corpus.insert(0, "nb_documents", piv.groupby("corpus_code").size())
    par_corpus = par_corpus.reset_index().sort_values("corpus_code")

    return piv.round(3), par_corpus.round(3)


def main():
    parser = argparse.ArgumentParser(
        description="Stats de compression pour une sortie convert_stream.py "
                    "(TIFF source absents du disque, taille recuperee dans le ref)."
    )
    parser.add_argument("--dest", required=True, help="racine des JPEG produits par convert_stream.py")
    parser.add_argument("--ref", default="results/ref/_ref_files_20260630.csv.gz",
                        help="chemin du ref (CSV.gz)")
    parser.add_argument("--corpus", nargs="*", default=None,
                        help="restreindre le rapprochement ref a ces corpus_code (defaut : tous)")
    parser.add_argument("--out-dir", default="results/img", help="dossier de sortie des CSV")
    parser.add_argument("--tag", default=None, help="suffixe des fichiers de sortie (defaut : nom du dossier --dest)")
    args = parser.parse_args()

    dest = Path(args.dest)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = args.tag or dest.name

    print(f"Scan de {dest} ...")
    df = lister_jpeg(dest)
    df = deduire_stem_qualite(df)
    print(f"{len(df)} JPEG trouves ({df['stem_key'].nunique()} documents distincts).")

    print("Chargement du ref (taille des TIFF source) ...")
    ref_s3, ref_repli = charger_ref_tailles(args.ref, args.corpus)
    ref_s3["size_source_Mo"] = ref_s3["size"] / MO
    ref_repli["size_source_Mo"] = ref_repli["size"] / MO

    # 1ere passe : rapprochement par s3_key
    df = df.merge(ref_s3[["stem_key", "size_source_Mo", "corpus_code"]], on="stem_key", how="left")

    # 2e passe : pour les non-apparies, repli <fonds>/<corpus_code>/<name> (comme relkey)
    manque = df["size_source_Mo"].isna()
    if manque.any():
        repli = df.loc[manque, ["stem_key"]].merge(
            ref_repli[["stem_key", "size_source_Mo", "corpus_code"]], on="stem_key", how="left"
        )
        df.loc[manque, "size_source_Mo"] = repli["size_source_Mo"].to_numpy()
        df.loc[manque, "corpus_code"] = repli["corpus_code"].to_numpy()

    df["match"] = df["size_source_Mo"].notna()
    n_non_apparies = (~df["match"]).sum()
    if n_non_apparies:
        print(f"ATTENTION : {n_non_apparies} JPEG sans TIFF source retrouve dans le ref "
              f"(exclus des ratios, gardes dans le detail par stem).")
        out_manquants = out_dir / f"non_apparies_{tag}.csv"
        df.loc[~df["match"], ["rel_path", "stem_key", "qualite", "taille"]].to_csv(out_manquants, index=False)
        print(f"Liste des non-apparies : {out_manquants}")

    piv_stem, piv_corpus = construire_pivots(df)

    out_stem = out_dir / f"pivot_taille_qualite_{tag}.csv"
    out_corpus = out_dir / f"pivot_taille_qualite_par_corpus_{tag}.csv"
    piv_stem.to_csv(out_stem, index=False)
    piv_corpus.to_csv(out_corpus, index=False)

    print(f"Pivot par document : {out_stem} ({len(piv_stem)} lignes)")
    print(f"Pivot par corpus   : {out_corpus} ({len(piv_corpus)} lignes)")


if __name__ == "__main__":
    main()
