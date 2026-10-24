"""Nouveau ref 20261023 — étape 4 : fusion finale.

    - recalcul de s3_stem_profil (logique de scripts/s3/ref_stem_profil_20260711.py v2) :
      par stem de s3_key, extensions présentes triées et jointes par « + » ; les
      « À SUPPRIMER* » sans clé sont rattachés par (corpus_code, stem de name) et
      marqués `(sup)` ; rattachements ambigus exclus ;
    - retrait de la colonne de travail maj_{NEW} ;
    - colonnes dans l'ordre de l'ancien ref.

Lecture/écriture en texte (dtype=str) : les valeurs reprises de l'ancien ref gardent
leur format d'origine.

Entrée : results/ref/tmp/_ref_files_{NEW}_tmp_az_s3.csv.gz
Sortie : results/ref/_ref_files_{NEW}.csv.gz (refus d'écraser un fichier existant)
"""

from os.path import exists, join

import pandas as pd

OLD_REF_DATE = "20260630"
NEW_REF_DATE = "20261023"
SORTIE = join("results", "ref", f"_ref_files_{NEW_REF_DATE}.csv.gz")
assert not exists(SORTIE), f"{SORTIE} existe déjà"

colonnes = list(pd.read_csv(join("results", "ref", f"_ref_files_{OLD_REF_DATE}.csv.gz"), nrows=0).columns)
ref = pd.read_csv(join("results", "ref", "tmp", f"_ref_files_{NEW_REF_DATE}_tmp_az_s3.csv.gz"),
                  dtype=str, keep_default_na=False)
print("lu :", ref.shape)
ref = ref.drop(columns=[f"maj_{NEW_REF_DATE}", "s3_stem_profil"])


def norm(e):
    return e.str.lower().replace({"tiff": "tif", "jpeg": "jpg"})


keyed = ref["s3_key"] != ""
sup = ref["conservation_statut"].str.startswith("À SUPPRIMER")
assert not (sup & keyed).any(), "À SUPPRIMER keyé inattendu"

kstem = ref.loc[keyed, "s3_key"].str.rsplit(".", n=1).str[0]
kext = norm(ref.loc[keyed, "s3_key"].str.rsplit(".", n=1).str[-1])
pairs = pd.DataFrame({"stem": kstem, "tok": kext})

kb = pd.DataFrame({
    "corpus": ref.loc[keyed, "corpus_code"],
    "bstem": kstem.str.rsplit("/", n=1).str[-1].str.lower(),
    "kstem": kstem})
uni = kb.drop_duplicates().groupby(["corpus", "bstem"])["kstem"].agg(["first", "nunique"])
lut = uni.loc[uni["nunique"] == 1, "first"]
print("rattachements ambigus exclus :", int((uni["nunique"] > 1).sum()))

s = ref.loc[sup].copy()
s["bstem"] = s["name"].str.rsplit(".", n=1).str[0].str.lower()
s["kstem"] = pd.MultiIndex.from_frame(s[["corpus_code", "bstem"]]).map(lut)
matched = s["kstem"].notna()
print("À SUPPRIMER rattachés :", int(matched.sum()), "/", len(s))
spairs = pd.DataFrame({
    "stem": s.loc[matched, "kstem"],
    "tok": norm(s.loc[matched, "extension"].str.lstrip(".")) + "(sup)"})

prof = (pd.concat([pairs, spairs]).drop_duplicates()
        .sort_values("tok").groupby("stem")["tok"].agg("+".join))
print("stems :", len(prof))

ref["s3_stem_profil"] = ""
ref.loc[keyed, "s3_stem_profil"] = kstem.map(prof)
ref.loc[s.index[matched], "s3_stem_profil"] = s.loc[matched, "kstem"].map(prof)
assert (ref.loc[keyed, "s3_stem_profil"] != "").all()

print("\n== distribution s3_stem_profil (top 12) ==")
print(ref.loc[ref["s3_stem_profil"] != "", "s3_stem_profil"].value_counts().head(12).to_string())

assert set(ref.columns) == set(colonnes)
ref = ref[colonnes]
assert ref["uuid"].is_unique and not ref.loc[keyed, "s3_key"].duplicated().any()
ref.to_csv(SORTIE, index=False, compression="gzip")
print("\nécrit :", SORTIE, ref.shape)
