#!/usr/bin/env python3
r"""manifestes_lot.py - Manifestes des TIFF a convertir en JPEG de diffusion, extraits du fichier de reference.

Un manifeste est un extrait du ref (.csv.gz, toutes les colonnes, texte d'origine) et sa
liste de TIFF (<manifeste>_tiff.txt : path/name, pour les copier sous <source>), comme
results/med_ms/manifeste_MED_MS.csv.gz. TIFF hors « A SUPPRIMER », au statut
TRANSFERT_S3_OK, tries par corpus_code puis s3_key.

Deux manifestes :
  results/med_vdm/manifeste_MED_VDM_non_publie.csv.gz
      MED_VDM dont publication_statut n'est pas « oui » (les TIFF publies sont dans
      manifeste_MED_VDM.csv.gz).
  results/img/manifeste_10_corpus.csv.gz
      MED_IMA, MED_CP, MED_AFF, LAR_PUB, MUS_VAI, MED_MON, MED_PUB, AMR_PHO, LAI et AMR_AFF :
      TIFF publies (publication_statut = oui) sans JPEG rattache au document (s3_stem_profil
      sans « jpg » ; « jpg(sup) », a supprimer, ne compte pas).

Un troisieme, extrait du precedent, pour les corpus convertibles sans probleme connu :
  results/img/manifeste_6_corpus.csv.gz
      MED_IMA, MED_CP, MED_AFF, MUS_VAI, MED_MON et MED_PUB, que lot_mets.py traite.
      Ecartes : LAR_PUB (3 127 TIFF a JPEG old-style, refuses par libtiff comme VDM_ALS1_D01),
      LAI (med_mets.unit_id ne gere pas un corpus sans « _ »), AMR_PHO et AMR_AFF (aucune
      notice dans l'IR : pas de bnr2mnesys/FR595126101_AMR_*.xml), et toute notice dont un
      TIFF n'a ni dimensions ni compression au ref ou est a JPEG old-style (1 notice MED_CP).

Usage (depuis la racine du projet) :
  python scripts/img/img3/manifestes_lot.py
  python scripts/img/img3/manifestes_lot.py results/ref/_ref_files_20261023.csv.gz
"""

import argparse
from pathlib import Path

import pandas as pd

RESULTS_DIR = Path(__file__).resolve().parents[3] / "results"
REF = RESULTS_DIR / "ref" / "_ref_files_20261023.csv.gz"
CORPUS = ["MED_IMA", "MED_CP", "MED_AFF", "LAR_PUB", "MUS_VAI", "MED_MON", "MED_PUB", "AMR_PHO", "LAI", "AMR_AFF"]
CORPUS_SANS_PROBLEME = ["MED_IMA", "MED_CP", "MED_AFF", "MUS_VAI", "MED_MON", "MED_PUB"]


def ecrire(df: pd.DataFrame, chemin: Path) -> None:
    """Manifeste .csv.gz et liste des TIFF (path/name) a cote."""
    chemin.parent.mkdir(parents=True, exist_ok=True)
    df = df.sort_values(["corpus_code", "s3_key"])
    df.to_csv(chemin, index=False)
    tiff = chemin.with_name(chemin.name.removesuffix(".csv.gz") + "_tiff.txt")
    tiff.write_text("".join(f"{p}/{n}\n" for p, n in zip(df.path, df.name)), encoding="utf-8")
    go = df["size"].astype(float).sum() / 1e9
    print(f"{chemin.relative_to(RESULTS_DIR.parent)} : {len(df)} TIFF, {go:.1f} Go")
    print(df.groupby("corpus_code").size().to_string())


def main():
    ap = argparse.ArgumentParser(description="Manifestes des TIFF MED_VDM non publies et de 10 corpus sans JPEG.")
    ap.add_argument("ref", nargs="?", default=str(REF), help="fichier de reference (defaut : %(default)s)")
    args = ap.parse_args()

    ref = pd.read_csv(args.ref, dtype=str, low_memory=False)
    tiff = ref[(ref.file_type == "tiff") & (ref.conservation_statut == "TRANSFERT_S3_OK")]

    ecrire(tiff[(tiff.corpus_code == "MED_VDM") & (tiff.publication_statut != "oui")],
           RESULTS_DIR / "med_vdm" / "manifeste_MED_VDM_non_publie.csv.gz")

    sans_jpeg = ~tiff.s3_stem_profil.fillna("").str.split("+").map(lambda extensions: "jpg" in extensions)
    dix = tiff[tiff.corpus_code.isin(CORPUS) & (tiff.publication_statut == "oui") & sans_jpeg]
    ecrire(dix, RESULTS_DIR / "img" / "manifeste_10_corpus.csv.gz")

    # une notice est traitee d'un bloc : un TIFF douteux ecarte toute sa notice
    douteux = dix.mix_imageWidth.isna() | (dix.mix_compressionScheme == "JPEG (old-style)")
    notices_douteuses = pd.MultiIndex.from_frame(dix.loc[douteux, ["corpus_code", "unitid"]])
    sain = ~pd.MultiIndex.from_frame(dix[["corpus_code", "unitid"]]).isin(notices_douteuses)
    sans_probleme = dix[dix.corpus_code.isin(CORPUS_SANS_PROBLEME) & sain]
    ecrire(sans_probleme, RESULTS_DIR / "img" / "manifeste_6_corpus.csv.gz")
    for name in dix[dix.corpus_code.isin(CORPUS_SANS_PROBLEME) & ~sain].name:
        print(f"notice ecartee : {name}")


if __name__ == "__main__":
    main()
