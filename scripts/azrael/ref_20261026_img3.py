"""Nouveau ref 20261026 : ajout des JPEG et METS produits par la chaîne img3 les 24-25/10/2026.

Pas de nouvelle extraction Azrael : le ref 20261023 est repris tel quel, et on lui ajoute les
objets du listing S3 data/s3/listing9-mediatheque-patarch-communicable_20261026.csv.gz
(list9.py + concat_listing.py) qu'il ne connaît pas. Ce listing couvre les dossiers des corpus
convertis : MED_MS, MED_VDM, MED_PAR (PAR/, AMR/PAR_LAB, MED/MED_PAR), MED_AFF, MED_IMA, MED_CP,
MUS_VAI, MED_MON, MED_PUB.

1. Contrôle : chaque ligne du ref dont la s3_key tombe dans un dossier du listing y est, avec
   le même uuid, le même checksum_md5 et la même taille (listing_vs_ref.py : aucun écart le
   26/10/2026). L'état S3 de ces lignes n'est donc pas réécrit.
2. Objets du listing inconnus du ref (ni clé, ni uuid) : ajoutés comme lignes non-az, comme
   à l'étape 2 de ref_20261023_03_s3.py (source2s3 = conversion_tif, sans name ni path, size,
   uuid, checksum et dates du listing, conservation_statut = TRANSFERT_S3_OK, file_type jpeg ou
   mets). corpus_code, publication_statut, finding_aid, unitid, osiros_id, oai_set hérités :
     - JPEG : du TIFF de même stem de clé (stem du TIFF sans espace ni point final, comme
       med_mets.jpeg_key : « _001 .tif » donne « _001.jpg ») ;
     - METS : de la notice dont il porte le nom, <dossier>/<med_mets.unit_id(corpus, unitid)>_mets.xml ;
     - à défaut, valeur la plus fréquente du dossier de clé.
3. s3_stem_profil recalculé sur tout le ref (logique de ref_20261023_04_final.py).

Lecture/écriture en texte (dtype=str) : les valeurs reprises gardent leur format d'origine.

Entrées : results/ref/_ref_files_20261023.csv.gz
          data/s3/listing9-mediatheque-patarch-communicable_20261026.csv.gz
Sortie  : results/ref/_ref_files_20261026.csv.gz (refus d'écraser un fichier existant)
"""

import sys
from os.path import exists, join

import pandas as pd

sys.path.insert(0, join("scripts", "img", "img3"))
from med_mets import unit_id  # noqa: E402  (nom du METS d'une notice)

OLD_REF_DATE = "20261023"
NEW_REF_DATE = "20261026"
LISTING = join("data", "s3", "listing9-mediatheque-patarch-communicable_20261026.csv.gz")
SORTIE = join("results", "ref", f"_ref_files_{NEW_REF_DATE}.csv.gz")
BUCKET = "mediatheque-patarch-communicable"
SOURCE_IMG3 = "conversion_tif"
HERITES = ["corpus_code", "publication_statut", "finding_aid", "unitid", "osiros_id", "oai_set"]
assert not exists(SORTIE), f"{SORTIE} existe déjà"

ref = pd.read_csv(join("results", "ref", f"_ref_files_{OLD_REF_DATE}.csv.gz"), dtype=str, keep_default_na=False)
colonnes = list(ref.columns)
s3 = pd.read_csv(LISTING, dtype=str, keep_default_na=False)
print("ref :", ref.shape, "| listing :", s3.shape)
assert s3["key"].is_unique and s3["uuid"].is_unique and (s3["uuid"] != "").all()


def dossier2(cles):
    return cles.str.split("/").str[:2].str.join("/")


def date_s3(s):
    return pd.to_datetime(s, utc=True).dt.strftime("%Y%m%d")


# --- 1. contrôle des lignes du ref dans le périmètre du listing -------------------------
keyed = ref["s3_key"] != ""
perimetre = keyed & dossier2(ref["s3_key"]).isin(set(dossier2(s3["key"])))
lst = s3.set_index("key")
p = ref[perimetre]
absents = ~p["s3_key"].isin(lst.index)
assert not absents.any(), f"{int(absents.sum())} lignes du ref absentes du listing"
for col in ("uuid", "checksum_md5"):
    assert (p[col].values == lst.loc[p["s3_key"], col].values).all(), f"{col} discordant"
assert (p["size"].astype(float).values == lst.loc[p["s3_key"], "size"].astype(float).values).all(), "taille discordante"
print("lignes du ref dans le périmètre, identiques sur S3 :", len(p))

# --- 2. objets du listing inconnus du ref --------------------------------------------
inc = s3[~s3["key"].isin(ref["s3_key"])].copy()
assert not inc["uuid"].isin(ref["uuid"]).any(), "uuid d'un objet nouveau déjà au ref"
print("objets inconnus du ref :", len(inc), "| checksum déjà au ref :", int(inc["checksum_md5"].isin(ref["checksum_md5"]).sum()))

lm = pd.to_datetime(inc["last_modified"], utc=True)
epoch = ((lm - pd.Timestamp("1970-01-01", tz="UTC")).dt.total_seconds()).map(str)
inc["name"] = ""
inc["path"] = ""
inc["size"] = inc["size"].astype(float).map(str)
inc["last_content_modification_date"] = epoch
inc["last_metadata_modification_date"] = epoch
mets = inc["key"].str.lower().str.endswith("_mets.xml")
inc["extension"] = inc["key"].str.rsplit("/", n=1).str[-1].str.extract(r".*(\..*)$")[0]
inc["file_type"] = "jpeg"
inc.loc[mets, "file_type"] = "mets"
assert inc.loc[~mets, "extension"].eq(".jpg").all(), "objet nouveau ni JPEG ni METS"
inc["source2s3"] = SOURCE_IMG3
inc["conservation_statut"] = "TRANSFERT_S3_OK"
inc["s3_key"] = inc["key"]
inc["s3_uploaded"] = "True"
inc["s3_uploaded_date"] = date_s3(inc["last_modified"])
inc["s3_bucket"] = BUCKET

# sources de l'héritage : les TIFF du ref dans le périmètre
src = ref[perimetre & (ref["file_type"] == "tiff")].copy()
src["stem"] = src["s3_key"].str.rsplit(".", n=1).str[0].str.rstrip(" .")
src["dossier"] = src["s3_key"].str.rsplit("/", n=1).str[0]
par_stem = src.drop_duplicates("stem").set_index("stem")[HERITES]
notices = src[src["unitid"] != ""].copy()
notices["mets"] = [f"{d}/{unit_id(c, u.strip())}_mets.xml"
                   for d, c, u in zip(notices["dossier"], notices["corpus_code"], notices["unitid"])]
par_mets = notices.drop_duplicates("mets").set_index("mets")[HERITES]
par_dossier = src.groupby("dossier")[HERITES].agg(lambda s: s.mode().iloc[0])

inc["dossier"] = inc["key"].str.rsplit("/", n=1).str[0]
h = pd.DataFrame(index=inc.index, columns=HERITES)
via = pd.Series("aucun", index=inc.index)
jpg = inc.index[~mets]
h.loc[jpg] = inc.loc[jpg, ["key"]].assign(stem=inc.loc[jpg, "key"].str.rsplit(".", n=1).str[0]) \
    .join(par_stem, on="stem")[HERITES].values
via[jpg] = "stem"
m_idx = inc.index[mets]
h.loc[m_idx] = inc.loc[m_idx, ["key"]].join(par_mets, on="key")[HERITES].values
via[m_idx] = "notice"
manque = h["corpus_code"].isna()
h.loc[manque] = inc.loc[manque, ["dossier"]].join(par_dossier, on="dossier")[HERITES].values
via[manque] = "dossier"
via[h["corpus_code"].isna()] = "aucun"
inc[HERITES] = h.fillna("")
print("héritage :", pd.crosstab(inc["file_type"], via).to_dict())
assert (via != "aucun").all(), "objet sans héritage"

inc = inc.reindex(columns=colonnes).fillna("")
out = pd.concat([ref, inc], ignore_index=True)

# --- 3. s3_stem_profil (logique de ref_20261023_04_final.py) ----------------------------
out = out.drop(columns="s3_stem_profil")


def norm(e):
    return e.str.lower().replace({"tiff": "tif", "jpeg": "jpg"})


keyed = out["s3_key"] != ""
sup = out["conservation_statut"].str.startswith("À SUPPRIMER")
assert not (sup & keyed).any(), "À SUPPRIMER keyé inattendu"
kstem = out.loc[keyed, "s3_key"].str.rsplit(".", n=1).str[0]
kext = norm(out.loc[keyed, "s3_key"].str.rsplit(".", n=1).str[-1])
pairs = pd.DataFrame({"stem": kstem, "tok": kext})
kb = pd.DataFrame({"corpus": out.loc[keyed, "corpus_code"],
                   "bstem": kstem.str.rsplit("/", n=1).str[-1].str.lower(), "kstem": kstem})
uni = kb.drop_duplicates().groupby(["corpus", "bstem"])["kstem"].agg(["first", "nunique"])
lut = uni.loc[uni["nunique"] == 1, "first"]
s = out.loc[sup].copy()
s["bstem"] = s["name"].str.rsplit(".", n=1).str[0].str.lower()
s["kstem"] = pd.MultiIndex.from_frame(s[["corpus_code", "bstem"]]).map(lut)
matched = s["kstem"].notna()
spairs = pd.DataFrame({"stem": s.loc[matched, "kstem"],
                       "tok": norm(s.loc[matched, "extension"].str.lstrip(".")) + "(sup)"})
prof = (pd.concat([pairs, spairs]).drop_duplicates().sort_values("tok").groupby("stem")["tok"].agg("+".join))
out["s3_stem_profil"] = ""
out.loc[keyed, "s3_stem_profil"] = kstem.map(prof)
out.loc[s.index[matched], "s3_stem_profil"] = s.loc[matched, "kstem"].map(prof)
assert (out.loc[keyed, "s3_stem_profil"] != "").all()

out = out[colonnes]
assert out["uuid"].is_unique and not out.loc[keyed, "s3_key"].duplicated().any()
print("\ncorpus des lignes ajoutées :")
print(inc.groupby(["corpus_code", "file_type"]).size().unstack(fill_value=0).to_string())
nouv = out.loc[len(ref):]
print("\ns3_stem_profil des JPEG ajoutés :", nouv.loc[nouv.file_type == "jpeg", "s3_stem_profil"].value_counts().to_dict())
out.to_csv(SORTIE, index=False, compression="gzip")
print("\nécrit :", SORTIE, out.shape)
