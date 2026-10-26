"""Compare un listing S3 (list9.py, concat_listing.py...) au fichier de référence, sur les préfixes listés.

Le périmètre est celui du listing : les lignes du ref dont la s3_key tombe dans un des dossiers
de deux niveaux présents dans le listing (MED/MED_MS, PAR/PAR_BOU...). Un dossier vidé sur S3
n'apparaît donc pas. Comparaison par clé :
  s3_seul    objet du listing sans ligne au ref (attendu : JPEG et METS produits après le ref)
  ref_seul   ligne du ref absente du listing (fichier annoncé sur S3 mais introuvable)
  uuid, checksum_md5, size   clé commune, valeur différente entre le ref et l'étiquette S3
Les lignes du ref sans s3_key (non versées, à supprimer) sont hors comparaison.

Sortie : results/s3/{listing}_vs_ref.csv, une ligne par écart (écart, corpus, type, clé,
valeur ref, valeur S3, date S3), réécrite à chaque lancement ; synthèse à l'écran.

Usage (depuis la racine du projet) :
  python scripts/s3/listing_vs_ref.py data/s3/listing9-mediatheque-patarch-communicable_20261026.csv.gz
  python scripts/s3/listing_vs_ref.py <listing> --ref results/ref/_ref_files_20261023.csv.gz
"""

import argparse
from os.path import basename, join

import pandas as pd
from list_recent import corpus_of, type_of

REF = join("results", "ref", "_ref_files_20261023.csv.gz")
COMPARES = ["uuid", "checksum_md5", "size"]


def dossier(cles):
    return cles.str.split("/").str[:2].str.join("/")


def main():
    ap = argparse.ArgumentParser(description="Écarts entre un listing S3 et le fichier de référence.")
    ap.add_argument("listing", help="listing concaténé (.csv.gz)")
    ap.add_argument("--ref", default=REF, help="fichier de référence (défaut : %(default)s)")
    ap.add_argument("--out-dir", default=join("results", "s3"))
    args = ap.parse_args()

    s3 = pd.read_csv(args.listing, dtype=str)
    ref = pd.read_csv(args.ref, dtype=str, usecols=["s3_key", "corpus_code", "conservation_statut", *COMPARES],
                      low_memory=False)
    ref = ref[ref.s3_key.notna() & dossier(ref.s3_key).isin(set(dossier(s3.key)))]
    print(f"{len(s3)} objets au listing, {len(ref)} lignes du ref sur les mêmes dossiers.")

    m = ref.merge(s3, left_on="s3_key", right_on="key", how="outer", suffixes=("_ref", "_s3"), indicator=True)
    m["cle"] = m.s3_key.fillna(m.key)
    ecarts = [m[m._merge == "right_only"].assign(ecart="s3_seul"),
              m[m._merge == "left_only"].assign(ecart="ref_seul")]
    communs = m[m._merge == "both"]
    for col in COMPARES:
        a, b = communs[f"{col}_ref"], communs[f"{col}_s3"]
        if col == "size":
            a, b = a.astype(float), b.astype(float)
        diff = communs[a.fillna("") != b.fillna("")] if col != "size" else communs[a != b]
        ecarts.append(diff.assign(ecart=col, valeur_ref=diff[f"{col}_ref"], valeur_s3=diff[f"{col}_s3"]))
    out = pd.concat(ecarts, ignore_index=True)
    out["corpus"] = out.corpus_code.fillna(out.cle.map(corpus_of))
    out["type"] = out.cle.map(type_of)
    out = out.reindex(columns=["ecart", "corpus", "type", "cle", "valeur_ref", "valeur_s3", "last_modified",
                               "conservation_statut"]).rename(columns={"last_modified": "date_s3"})

    nom = basename(args.listing).removesuffix(".csv.gz")
    chemin = join(args.out_dir, f"{nom}_vs_ref.csv")
    out.to_csv(chemin, index=False)
    print(f"Clés communes : {len(communs)}")
    if out.empty:
        print("Aucun écart.")
    else:
        print(out.pivot_table(index="corpus", columns=["ecart", "type"], values="cle", aggfunc="count",
                              fill_value=0).to_string())
    print(f"\n{len(out)} écart(s) → {chemin}")


if __name__ == "__main__":
    main()
