"""Nouveau ref 20261023 — étape 3 : croisement avec le listing s3 PRA_DEL_RAM.

Le listing data/s3/s3_listing_{NEW}_PRA_DEL_RAM.csv.gz n'est complet que pour les
corpus PRA_*, AMR_DEL et AMR_RAM (préfixes MED/MED_PRA/, AMR/AMR_DEL/, AMR/AMR_RAM/).
Il ne porte pas de colonne s3_bucket : ces préfixes sont tous dans le bucket
communicable (vérifié sur le listing 20260630).

1. Lignes du ref dans le périmètre (corpus_code PRA_* / AMR_DEL / AMR_RAM, et s3_key
   vide ou dans les 3 préfixes) : état s3 réécrit depuis le listing, par uuid
   (règles de scripts/s3/s3_join_ref.py, sauf la clé d'un fichier non versé, qui est
   conservée : clé cible des « À CHERCHER »). Les 178 fichiers PRA_* versés sous
   TCG/TCG_PRA/ (préfixe non listé) et toutes les lignes hors périmètre gardent
   leur état s3 de l'ancien ref.
2. Objets du listing inconnus du ref (ni uuid ni checksum) : JPEG + METS produits par
   la chaîne img3 directement sur s3, sans passage par az. Ajoutés comme lignes
   non-az (source2s3 = SOURCE_IMG3), sans name ni path (pas sur az), size/uuid/
   checksum/dates du listing, conservation_statut = 'TRANSFERT_S3_OK'. corpus_code,
   publication_statut, finding_aid, unitid, osiros_id, oai_set hérités du fichier du
   ref de même stem (JPEG -> TIFF de la même page) ; pour un METS (<fascicule>_mets),
   d'une page du même fascicule ; à défaut, du dossier de clé.

3. METS (img3 et UnivLille) : file_type = 'mets'.

s3_stem_profil n'est pas recalculé ici (à rejouer à la fusion finale).

Entrées : results/ref/tmp/_ref_files_{NEW}_tmp_az.csv.gz
          data/s3/s3_listing_{NEW}_PRA_DEL_RAM.csv.gz
Sortie  : results/ref/tmp/_ref_files_{NEW}_tmp_az_s3.csv.gz
"""

from os.path import join

import pandas as pd

NEW_REF_DATE = "20261023"
TMP_DIR = join("results", "ref", "tmp")
MAJ = f"maj_{NEW_REF_DATE}"
PREFIXES = ("MED/MED_PRA/", "AMR/AMR_DEL/", "AMR/AMR_RAM/")
BUCKET = "mediatheque-patarch-communicable"
SOURCE_IMG3 = "conversion_tif"
HERITES = ["corpus_code", "publication_statut", "finding_aid", "unitid", "osiros_id", "oai_set"]

ref = pd.read_csv(join(TMP_DIR, f"_ref_files_{NEW_REF_DATE}_tmp_az.csv.gz"), low_memory=False)
colonnes = list(ref.columns)
s3 = pd.read_csv(join("data", "s3", f"s3_listing_{NEW_REF_DATE}_PRA_DEL_RAM.csv.gz"))
assert s3["key"].str.startswith(PREFIXES).all() and s3["uuid"].is_unique


def date_s3(s):
    return pd.to_datetime(s, utc=True).dt.strftime("%Y%m%d").astype(int)


# --- 1. état s3 des lignes du périmètre ---------------------------------------------
cle = ref["s3_key"].fillna("")
perimetre = (ref["corpus_code"].str.match(r"^(PRA_|AMR_DEL$|AMR_RAM$)")
             & ((cle == "") | cle.str.startswith(PREFIXES)))
print("lignes du ref dans le périmètre :", perimetre.sum())

lst = s3.set_index("uuid")
u = ref.loc[perimetre, "uuid"]
vu = u.isin(lst.index)
chk = ref.loc[perimetre][vu.values]
assert (chk["checksum_md5"].values == lst.loc[chk["uuid"], "checksum_md5"].values).all(), "checksum discordant"
avant = ref.loc[perimetre, ["s3_key", "s3_uploaded", "s3_uploaded_date"]].copy()
ref_bucket_avant = ref.loc[perimetre, "s3_bucket"].copy()
# non versé : s3_uploaded = False, mais la clé est conservée (clé cible des « À CHERCHER »)
idx_vu, idx_non = u.index[vu.values], u.index[~vu.values]
ref.loc[perimetre, "s3_uploaded"] = vu.values
ref.loc[idx_vu, "s3_key"] = u[vu.values].map(lst["key"])
ref.loc[idx_vu, "s3_uploaded_date"] = u[vu.values].map(date_s3(lst["last_modified"]))
ref.loc[idx_vu, "s3_bucket"] = BUCKET
ref.loc[idx_non, ["s3_uploaded_date", "s3_bucket"]] = pd.NA
avant["s3_bucket"] = ref_bucket_avant
apres = ref.loc[perimetre, ["s3_key", "s3_uploaded", "s3_uploaded_date", "s3_bucket"]]
for c in avant.columns:
    diff = ~((avant[c] == apres[c]) | (avant[c].isna() & apres[c].isna()))
    print(f"  {c} modifiés :", int(diff.sum()))

# --- 2. objets s3 inconnus du ref : dérivés img3 --------------------------------------
inc = s3[~s3["uuid"].isin(ref["uuid"])].copy()
assert not inc["checksum_md5"].isin(ref["checksum_md5"]).any()
print("objets s3 inconnus du ref :", len(inc))

# fichiers absents d'az : pas de name ni de path, seulement la clé s3
inc["dossier"] = inc["key"].str.rsplit("/", n=1).str[0]
epoch = (pd.to_datetime(inc["last_modified"], utc=True) - pd.Timestamp("1970-01-01", tz="UTC")).dt.total_seconds()
inc["last_content_modification_date"] = epoch
inc["last_metadata_modification_date"] = epoch
inc["extension"] = inc["key"].str.rsplit("/", n=1).str[-1].str.extract(r".*(\..*)$")[0]
inc["file_type"] = inc["extension"].map({".jpg": "jpeg", ".xml": "ocr xml"}).fillna("autre")
inc["source2s3"] = SOURCE_IMG3
inc["conservation_statut"] = "TRANSFERT_S3_OK"
inc["s3_key"] = inc["key"]
inc["s3_uploaded"] = True
inc["s3_uploaded_date"] = date_s3(inc["last_modified"])
inc["s3_bucket"] = BUCKET

# héritage : fichier du ref de même stem de clé, sinon page du même fascicule, sinon dossier
src = ref[ref["s3_key"].fillna("").str.startswith(PREFIXES)].copy()
src["stem"] = src["s3_key"].str.rsplit(".", n=1).str[0]
src["dossier"] = src["s3_key"].str.rsplit("/", n=1).str[0]
par_stem = src.drop_duplicates("stem").set_index("stem")[HERITES]
src["fascicule"] = src["stem"].str.rsplit("_", n=1).str[0]
par_fascicule = src.sort_values("stem").drop_duplicates("fascicule").set_index("fascicule")[HERITES]
par_dossier = src.groupby("dossier")[HERITES].agg(lambda s: s.mode().iloc[0] if s.notna().any() else pd.NA)

inc["stem"] = inc["key"].str.rsplit(".", n=1).str[0]
inc["fascicule"] = inc["stem"].str.replace(r"_mets$", "", regex=True)
h = inc[["stem"]].join(par_stem, on="stem")[HERITES]
via = pd.Series("stem", index=inc.index)
manque = h["corpus_code"].isna()
hf = inc.loc[manque, ["fascicule"]].join(par_fascicule, on="fascicule")[HERITES]
h.loc[manque] = hf
via[manque & h["corpus_code"].notna()] = "fascicule"
manque = h["corpus_code"].isna()
hd = inc.loc[manque, ["dossier"]].join(par_dossier, on="dossier")[HERITES]
h.loc[manque] = hd
via[manque & h["corpus_code"].notna()] = "dossier"
via[h["corpus_code"].isna()] = "aucun"
inc[HERITES] = h
print("héritage :", pd.crosstab(inc["extension"], via).to_dict())
inc[MAJ] = SOURCE_IMG3
inc = inc.reindex(columns=colonnes)

out = pd.concat([ref, inc], ignore_index=True)

# METS : file_type 'mets' (img3 *_mets.xml, UnivLille *.mets.xml, jusqu'ici en 'ocr xml')
mets = out["s3_key"].fillna(out["name"]).fillna("").str.lower().str.contains(r"[._]mets\.xml$")
print("METS :", mets.sum(), out.loc[mets].groupby(["source2s3", "file_type"]).size().to_dict())
out.loc[mets, "file_type"] = "mets"
print(out[MAJ].value_counts().to_dict())
print("lignes :", len(out), "| uuid uniques :", out["uuid"].is_unique,
      "| s3_key uniques :", not out["s3_key"].dropna().duplicated().any())
print("corpus_code img3 :", inc["corpus_code"].value_counts(dropna=False).to_dict())
out.to_csv(join(TMP_DIR, f"_ref_files_{NEW_REF_DATE}_tmp_az_s3.csv.gz"), index=False)
