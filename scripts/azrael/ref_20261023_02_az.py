"""Nouveau ref 20261023 — étape 2 : intégration des fichiers az dans l'ancien ref.

Part de l'ancien ref complet (az + non-az) et du motif calculé à l'étape 1 :
    - lignes non-az et lignes az « present » : reprises telles quelles ;
    - « taille_differente » : ligne de l'ancien ref conservée (uuid, statuts, corpus,
      s3), mais size + 2 dates prises sur az, checksum_md5 remplacé par le nouveau
      calcul (vide tant que 35_checksum_absents.py n'a pas tourné), colonnes mix
      vidées (elles décrivaient l'ancienne version) ;
    - « nouveau » : ligne créée — uuid4, extension/file_type depuis le nom (règles de
      60_merge_new_old_ref.py), source2s3 = 'az', conservation_statut = 'INCONNU',
      publication_statut = 'inconnu', s3_uploaded = False, corpus_code hérité du
      dossier voisin le plus proche dans l'ancien ref (profondeur >= 3), sinon
      CORPUS_NOUVEAUX (corpus créés pour ce ref), sinon '_INCONNU' ;
    - ORDI_ERIC_20260826 : fichiers dont le checksum existe ailleurs dans le ref
      -> conservation_statut = 'À SUPPRIMER (DOUBLON AZ)'.

Colonne de travail `maj_{NEW}` (hors_az / present / taille_differente / nouveau),
à retirer à la fusion finale.

Entrées : results/ref/_ref_files_{OLD}.csv.gz
          results/ref/tmp/az_motif_{NEW}.csv.gz
          results/ref/tmp/az_absents_ref_{NEW}_cs.csv.gz (facultatif : checksums)
Sortie  : results/ref/tmp/_ref_files_{NEW}_tmp_az.csv.gz
"""

import mimetypes
from os.path import exists, join
from uuid import uuid4

import pandas as pd

OLD_REF_DATE = "20260630"
NEW_REF_DATE = "20261023"
TMP_DIR = join("results", "ref", "tmp")
MAJ = f"maj_{NEW_REF_DATE}"
MIX = ["mix_objectIdentifierValue", "mix_fileSize", "mix_dateTimeCreated", "mix_formatName",
       "mix_byteOrder", "mix_compressionScheme", "mix_imageWidth", "mix_imageHeight",
       "mix_xSamplingFrequency", "mix_ySamplingFrequency", "mix_samplingFrequencyUnit",
       "mix_colorSpace", "mix_scanningSoftwareName", "mix_formatVersion"]
DATES = ["last_content_modification_date", "last_metadata_modification_date"]
cles = ["name", "path"]


def get_file_type(df):
    """Règles de 60_merge_new_old_ref.py (extension + mimetype)."""
    mimetype = df["name"].apply(lambda n: mimetypes.guess_type(n)[0] or "N/A")
    ft = mimetype.copy()
    ft[mimetype == "image/jpeg"] = "jpeg"
    ft[df["extension"] == ".jp2"] = "jpeg2000"
    ft[mimetype == "image/tiff"] = "tiff"
    ft[mimetype == "text/plain"] = "ocr txt"
    ft[mimetype == "application/pdf"] = "pdf"
    ft[mimetype == "application/xml"] = "ocr xml"
    ft[df["extension"] == ".alto"] = "ocr xml"
    ft[mimetype.str[:4] == "vide"] = "video"
    ft[df["extension"] == ".MTS"] = "video"
    ft[mimetype.str[:4] == "audi"] = "audio"
    ft[~ft.isin(["jpeg", "jpeg2000", "tiff", "pdf", "ocr txt", "ocr xml", "video", "audio"])] = "autre"
    return ft


ref = pd.read_csv(join("results", "ref", f"_ref_files_{OLD_REF_DATE}.csv.gz"), low_memory=False)
colonnes = list(ref.columns)
motif = pd.read_csv(join(TMP_DIR, f"az_motif_{NEW_REF_DATE}.csv.gz"))

cs_file = join(TMP_DIR, f"az_absents_ref_{NEW_REF_DATE}_cs.csv.gz")
if exists(cs_file):
    cs = pd.read_csv(cs_file)[cles + ["checksum_md5"]].rename(columns={"checksum_md5": "cs_new"})
else:
    print("checksums pas encore calculés :", cs_file)
    cs = pd.DataFrame(columns=cles + ["cs_new"])

# --- ancien ref : reprise + mise à jour des tailles différentes ----------------------
ref = ref.merge(motif[cles + ["size"] + DATES + ["motif"]].rename(
    columns={"size": "az_size", **{d: "az_" + d for d in DATES}}), on=cles, how="left")
ref[MAJ] = ref["motif"].fillna("hors_az")
td = ref[MAJ] == "taille_differente"
ref.loc[td, "size"] = ref.loc[td, "az_size"]
for d in DATES:
    ref.loc[td, d] = ref.loc[td, "az_" + d]
ref.loc[td, MIX] = pd.NA
ref = ref.merge(cs, on=cles, how="left")
ref.loc[td, "checksum_md5"] = ref.loc[td, "cs_new"]
ref = ref[colonnes + [MAJ]]

# --- nouveaux fichiers ---------------------------------------------------------------
new = motif[motif["motif"] == "nouveau"].drop(columns="motif").merge(cs, on=cles, how="left")
new = new.rename(columns={"cs_new": "checksum_md5"})
new["uuid"] = [uuid4().hex for _ in range(len(new))]
new["extension"] = new["name"].str.extract(r".*(\..*)$")[0]
new["file_type"] = get_file_type(new)
new["source2s3"] = "az"
new["conservation_statut"] = "INCONNU"
new["publication_statut"] = "inconnu"
new["s3_uploaded"] = False

# corpus_code : dossier voisin le plus proche (ancêtre de profondeur >= 3)
corpus_par_dossier = ref[ref["source2s3"] == "az"].groupby("path")["corpus_code"].agg(
    lambda s: s.value_counts().index[0])


def corpus_voisin(path):
    seg = path.split("/")
    for n in range(len(seg), 2, -1):
        prefixe = "/".join(seg[:n])
        sous = corpus_par_dossier[(corpus_par_dossier.index == prefixe)
                                  | corpus_par_dossier.index.str.startswith(prefixe + "/")]
        if len(sous):
            return sous.value_counts().index[0]
    return "_INCONNU"


dossiers = pd.Series(new["path"].unique())
new["corpus_code"] = new["path"].map(dict(zip(dossiers, dossiers.apply(corpus_voisin))))
# corpus nouveaux (aucun voisin dans l'ancien ref)
CORPUS_NOUVEAUX = {"BNR_VERIF/AMR/AMR_540W": "AMR_540W"}
for prefixe, code in CORPUS_NOUVEAUX.items():
    new.loc[(new["path"] + "/").str.startswith(prefixe + "/"), "corpus_code"] = code
new[MAJ] = "nouveau"
new = new.reindex(columns=colonnes + [MAJ])

out = pd.concat([ref, new], ignore_index=True)

# ORDI_ERIC_20260826 : un fichier dont le checksum existe ailleurs dans le ref est un
# doublon. Si toutes les copies sont dans le dossier, on garde celle au chemin le plus
# court (Chromos paquet H plutôt que Eric/Chromos paquet H).
DOSSIER_DOUBLONS = "BNR_VERIF/ORDI_ERIC_20260826"
dans = (out["path"] + "/").str.startswith(DOSSIER_DOUBLONS + "/")
hors = out.loc[~dans & out["checksum_md5"].notna(), "checksum_md5"]
doublon = dans & out["checksum_md5"].isin(hors)
interne = out[dans & ~doublon & out["checksum_md5"].notna()]
interne = interne.assign(prof=interne["path"].str.count("/")).sort_values(["prof", "path"])
doublon |= out.index.isin(interne[interne["checksum_md5"].duplicated()].index)
out.loc[doublon, "conservation_statut"] = "À SUPPRIMER (DOUBLON AZ)"
print("doublons ORDI_ERIC :", doublon.sum(), out.loc[doublon, "path"].value_counts().to_dict())
print(out[MAJ].value_counts().to_dict())
print("lignes :", len(out), "| uuid uniques :", out["uuid"].is_unique,
      "| name+path uniques :", not out.duplicated(cles).any())
print("checksum_md5 manquants (az à jour) :",
      out.loc[out[MAJ].isin(["nouveau", "taille_differente"]), "checksum_md5"].isna().sum())
print("corpus_code des nouveaux :", new["corpus_code"].value_counts().to_dict())
out.to_csv(join(TMP_DIR, f"_ref_files_{NEW_REF_DATE}_tmp_az.csv.gz"), index=False)
