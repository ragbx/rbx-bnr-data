#!/usr/bin/env python3
r"""manifestes_publies.py - Manifestes des TIFF sans JPEG de diffusion des corpus publies.

Un corpus est publie des qu'au moins un de ses fichiers a publication_statut = oui au ref ;
on traite alors tous ses TIFF, publies ou non. Sont retenus les TIFF au statut
TRANSFERT_S3_OK (donc hors « A SUPPRIMER ») sans JPEG rattache (s3_stem_profil sans « jpg » ;
« jpg(sup) » ne compte pas).

Chaque TIFF est range dans le groupe qui fera un METS (publies_mets.py), selon la regle :
  notice            le TIFF a un unitid : la notice de l'IR, comme med_mets.py ;
  notice_rattachee  pas d'unitid, mais le nom du document (voir ci-dessous) est celui d'une
                    notice existante du corpus (unit_id) : le TIFF en est une vue que le ref ne
                    rattache pas ; il rejoint la notice ;
  document          pas d'unitid, corpus de REGLE_DOCUMENT, nom RBX_... termine par un numero
                    de vue : le document est le nom sans ce numero
                    (RBX_MED_MON_FOL_301337_009.tif -> RBX_MED_MON_FOL_301337) ;
  tiff              sinon : un METS par TIFF, nomme d'apres le TIFF.
Le METS est <repertoire du TIFF>/<groupe>_mets.xml, ou <groupe> est unit_id(corpus, unitid)
pour une notice, le nom du document sinon (caracteres hors [A-Za-z0-9_-] remplaces par « _ »).
Le METS d'un TIFF se deduit ainsi de son nom ; la table de correspondance le donne aussi.

Sorties sous results/img/ (manifestes : extrait du ref, toutes les colonnes, texte d'origine,
plus unitid_mets, document_mets, regle et mets_key ; liste <manifeste>_tiff.txt a cote) :
  manifeste_corpus_publies.csv.gz             tout le perimetre (sans les colonnes de groupe) ;
  manifeste_corpus_publies_lot.csv.gz         groupes qu'aucun JPEG ne couvre encore, TIFF tous
                                              sur az : a lancer sans --overwrite ;
  manifeste_corpus_publies_partielles.csv.gz  notices dont une partie des TIFF a deja son JPEG
                                              (et son METS sur S3), dont les notices rattachees :
                                              tous leurs TIFF, y compris les convertis, a lancer
                                              avec --overwrite, apres decision ;
  manifeste_corpus_publies_hors_az.csv.gz     groupes dont un TIFF n'est pas sur az (source2s3
                                              autre que az, ex. UnivLille) : seulement sur S3 ;
  corpus_publies_tiff_mets.csv                TIFF -> METS pour les trois manifestes precedents ;
  corpus_publies_ecarts.csv                   TIFF du perimetre non traites, avec leur motif.

Usage (depuis la racine du projet) :
  python scripts/img/img3/manifestes_publies.py
  python scripts/img/img3/manifestes_publies.py results/ref/_ref_files_20261026.csv.gz
"""

import argparse
import posixpath
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from med_mets import unit_id  # noqa: E402  (nom du METS d'une notice)

RESULTS_DIR = Path(__file__).resolve().parents[3] / "results"
REF = RESULTS_DIR / "ref" / "_ref_files_20261026.csv.gz"
OUT = RESULTS_DIR / "img"

# corpus ou le nom sans numero de vue redonne la notice (verifie le 19/11/2026 sur les TIFF
# pourvus d'un unitid : 89 a 100 %) ; ailleurs, une image = une notice, ou noms irreguliers
REGLE_DOCUMENT = {"MED_VDM", "MED_MON", "MED_PAR", "MED_FLR", "AMR_AFF", "MED_AFF", "MED_IMA",
                  "MUS_VAI", "MED_MS", "OBS_JOU", "MED_EPH"}
VUE = re.compile(r"^(RBX_.+?)_(\d+[a-zA-Z]?|\d+bis)$")

# notices que med_mets.py ne sait pas traiter (motif d'ecart) ; les TIFF sans unitid de ces
# corpus n'ont pas besoin de l'IR et sont traites, sauf la presse
CORPUS_ECARTES = {
    "LAR_PUB": "TIFF a JPEG old-style refuses par libtiff",
    "MED_FLR": "IR nomme FR595126101_MED_FLRS.xml : notices introuvables",
    "CSV_PAL": "IR nomme FR595126101_CSV.xml : notices introuvables",
    "AMR_AFF": "pas d'IR bnr2mnesys",
    "AMR_CAD": "pas d'IR bnr2mnesys",
    "AMR_OBJ": "pas d'IR bnr2mnesys",
    "AMR_PHO": "pas d'IR bnr2mnesys",
    "AMR_PLA": "pas d'IR bnr2mnesys",
    "AMR_PR": "pas d'IR bnr2mnesys",
}
PRESSE = {"PRA_ERT": "presse : chaine presse_mets.py"}
GROUPE = ["unitid_mets", "document_mets", "regle", "mets_key"]


def propre(nom: str) -> str:
    """Nom de groupe utilisable dans une cle S3 : hors [A-Za-z0-9_-] -> « _ »."""
    return re.sub(r"[^A-Za-z0-9_-]+", "_", nom).strip("_")


def document(corpus: str, stem: str) -> tuple:
    """(nom du document, regle) d'un TIFF sans unitid."""
    stem = stem.strip(" .")
    m = VUE.match(stem)
    if corpus in REGLE_DOCUMENT and m and len(m.group(1)) > len(f"RBX_{corpus}"):
        return propre(m.group(1)), "document"
    return propre(stem), "tiff"


def ecrire(df: pd.DataFrame, chemin: Path) -> None:
    """Manifeste .csv.gz et liste des TIFF (path/name) a cote."""
    chemin.parent.mkdir(parents=True, exist_ok=True)
    df = df.sort_values(["corpus_code", "s3_key"])
    df.to_csv(chemin, index=False)
    tiff = chemin.with_name(chemin.name.removesuffix(".csv.gz") + "_tiff.txt")
    tiff.write_text("".join(f"{p}/{n}\n" for p, n in zip(df.path, df.name)), encoding="utf-8")
    go = df["size"].astype(float).sum() / 1e9
    n = df.mets_key.nunique() if "mets_key" in df else "-"
    print(f"{chemin.relative_to(RESULTS_DIR.parent)} : {len(df)} TIFF, {n} METS, {go:.1f} Go")
    if "regle" in df:
        print(df.groupby(["corpus_code", "regle"]).agg(tiff=("s3_key", "size"), mets=("mets_key", "nunique")).to_string())
    print()


def main():
    ap = argparse.ArgumentParser(description="Manifestes des TIFF sans JPEG des corpus publies.")
    ap.add_argument("ref", nargs="?", default=str(REF), help="fichier de reference (defaut : %(default)s)")
    args = ap.parse_args()

    ref = pd.read_csv(args.ref, dtype=str, low_memory=False)
    colonnes = list(ref.columns)
    publies = set(ref.loc[ref.publication_statut == "oui", "corpus_code"])
    metss = set(ref.loc[ref.file_type == "mets", "s3_key"].dropna().str.upper())
    tiff = ref[(ref.file_type == "tiff") & (ref.conservation_statut == "TRANSFERT_S3_OK")
               & ref.corpus_code.isin(publies)].copy()
    tiff["u"] = tiff.unitid.fillna("").str.strip()
    tiff["jpg"] = tiff.s3_stem_profil.fillna("").str.split("+").map(lambda extensions: "jpg" in extensions)
    tiff["rep"] = tiff.s3_key.map(posixpath.dirname)
    tiff["douteux"] = tiff.mix_imageWidth.isna() | (tiff.mix_compressionScheme == "JPEG (old-style)")
    perimetre = tiff[~tiff.jpg]
    ecrire(perimetre[colonnes], OUT / "manifeste_corpus_publies.csv.gz")

    # groupes : notices (tous leurs TIFF, convertis ou non) et TIFF sans unitid du perimetre
    pool = tiff[(tiff.u != "") | ~tiff.jpg].copy()
    pool["unitid_mets"], pool["document_mets"], pool["regle"] = pool.u, "", "notice"
    notices = {}  # (corpus, nom du METS en majuscules) -> unitid
    for corpus, u in pool.loc[pool.u != "", ["corpus_code", "u"]].drop_duplicates().itertuples(index=False):
        notices.setdefault((corpus, unit_id(corpus, u).upper()), u)
    sans = pool.u == ""
    for i, corpus, key in pool.loc[sans, ["corpus_code", "s3_key"]].itertuples():
        nom, regle = document(corpus, posixpath.splitext(posixpath.basename(key))[0])
        if (corpus, nom.upper()) in notices:
            pool.at[i, "unitid_mets"], pool.at[i, "regle"] = notices[(corpus, nom.upper())], "notice_rattachee"
        else:
            pool.at[i, "document_mets"], pool.at[i, "regle"] = nom, regle
    est_notice = pool.unitid_mets != ""
    pool["groupe"] = [f"{c}|{u}" if u else f"{c}|{r}|{d}"
                      for c, u, r, d in zip(pool.corpus_code, pool.unitid_mets, pool.rep, pool.document_mets)]
    pool["mets_key"] = [f"{r}/{unit_id(c, u) if u else d}_mets.xml"
                        for c, u, r, d in zip(pool.corpus_code, pool.unitid_mets, pool.rep, pool.document_mets)]

    par_groupe = pool.groupby("groupe").agg(jpg=("jpg", "any"), sans=("jpg", lambda s: (~s).any()),
                                           douteux=("douteux", "any"), reps=("rep", "nunique"),
                                           rattache=("regle", lambda s: (s == "notice_rattachee").any()))
    pool = pool[pool.groupe.isin(par_groupe.index[par_groupe.sans])]
    g = par_groupe.loc[pool.groupe]
    motif = pd.Series("", index=pool.index)
    for corpus, raison in PRESSE.items():
        motif[pool.corpus_code == corpus] = raison
    for corpus, raison in CORPUS_ECARTES.items():
        motif[(motif == "") & est_notice.loc[pool.index] & (pool.corpus_code == corpus)] = raison
    motif[(motif == "") & g.douteux.values] = "groupe avec un TIFF sans dimensions au ref ou a JPEG old-style"
    motif[(motif == "") & (g.reps.values > 1)] = "notice dans plusieurs repertoires de s3_key (refusee par med_mets)"
    motif[(motif == "") & ~est_notice.loc[pool.index] & pool.mets_key.str.upper().isin(metss)] = \
        "le METS du document existe deja sur S3 sous ce nom"
    retenu = pool[motif == ""]
    partiel = par_groupe.loc[retenu.groupe].jpg.values
    # les TIFF sont lus sur az (<source> = son montage) : un groupe dont un TIFF n'y est pas
    # (source2s3 autre que az, ex. UnivLille, absent de l'extraction az) part dans son manifeste
    hors = retenu.groupe.isin(retenu.loc[retenu.source2s3 != "az", "groupe"]).values

    ecrire(retenu.loc[~partiel & ~hors, colonnes + GROUPE], OUT / "manifeste_corpus_publies_lot.csv.gz")
    ecrire(retenu.loc[partiel & ~hors, colonnes + GROUPE], OUT / "manifeste_corpus_publies_partielles.csv.gz")
    ecrire(retenu.loc[hors, colonnes + GROUPE], OUT / "manifeste_corpus_publies_hors_az.csv.gz")
    corresp = retenu[["s3_key", "uuid", "corpus_code", "regle", "mets_key"]].rename(columns={"s3_key": "tiff_key"})
    corresp.assign(manifeste=["hors_az" if h else "partielles" if p else "lot" for p, h in zip(partiel, hors)]) \
        .sort_values("tiff_key").to_csv(OUT / "corpus_publies_tiff_mets.csv", index=False, encoding="utf-8")

    ecarts = perimetre[~perimetre.index.isin(retenu.index)]
    ecarts = ecarts.assign(motif=motif.reindex(ecarts.index).fillna("groupe sans TIFF a convertir"))
    ecarts = ecarts[["corpus_code", "unitid", "publication_statut", "s3_key", "path", "name", "size", "motif"]]
    ecarts.sort_values(["corpus_code", "s3_key"]).to_csv(OUT / "corpus_publies_ecarts.csv", index=False, encoding="utf-8")
    print(f"results/img/corpus_publies_ecarts.csv : {len(ecarts)} TIFF")
    print(ecarts.groupby(["motif", "corpus_code"]).size().to_string())
    print("\nCORPUS =", sorted(retenu.corpus_code.unique()))


if __name__ == "__main__":
    main()
