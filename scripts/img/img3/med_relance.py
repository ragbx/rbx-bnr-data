#!/usr/bin/env python3
r"""med_relance.py - Notices des fonds MED (MED_MS, MED_VDM, MED_PAR) a relancer apres un lot med_lot.sh.

Lit le journal du lot (nohup.out : lignes « ECHEC <notice> : <message> » et
« ECHEC ENVOI <notice> : ... ») et retrouve chaque notice dans les manifestes
results/med_*/manifeste_MED_*.csv.gz. Les notices refusees d'emblee, que le
journal ne nomme pas, sont reperees comme le fait med_mets.py (TIFF repartis
dans plusieurs repertoires de s3_key) ; leur nombre est controle contre le journal.

Chaque echec est classe d'apres son message (cause), avec le correctif apporte a
la chaine s'il y en a un et l'action qui s'ensuit :
  relancer            cause corrigee : le manifeste de relance est ecrit
  relancer_overwrite  un JPEG different est deja sur S3 : a relancer avec --overwrite,
                      apres decision (un manifeste est ecrit, a lancer en connaissance de cause)
  bloque              cause non corrigee : pas de manifeste, la relance echouerait a nouveau

Sorties sous results/med_relance/ :
  notices_a_relancer.csv                    une ligne par notice
  manifeste_<FONDS>_<action>.csv.gz         extrait du manifeste (toutes colonnes), a passer a
                                            med_mets.py, pour les actions relancer et relancer_overwrite

Usage (depuis la racine du projet) :
  python scripts/img/img3/med_relance.py ~/Telechargements/nohup.out
  python scripts/img/img3/med_relance.py nohup1.out nohup2.out --out-dir results/med_relance
"""

import argparse
import glob
import re
import sys
from os.path import dirname, join
from pathlib import Path

import pandas as pd

import med_mets as mm

RESULTS_DIR = Path(__file__).resolve().parents[3] / "results"
MANIFESTES = str(RESULTS_DIR / "med_*" / "manifeste_MED_*.csv.gz")

# (code, motif du message, libelle, correctif, action) ; le premier motif trouve l'emporte
CAUSES = [
    ("libtiff_limite_memoire", re.compile(r"Cumulated memory allocation"),
     "balises du TIFF > 50 Mo cumules (limite libtiff)",
     "jpeg_encode.load_master : unlimited=True", "relancer"),
    ("pillow_limite_pixels", re.compile(r"DecompressionBombError"),
     "image > 179 Mpx (garde anti-bombe de Pillow dans tiff_mix)",
     "presse_mets.tiff_mix : MAX_IMAGE_PIXELS = None", "relancer"),
    ("jpeg_existant_s3", re.compile(r"objet deja present et different"),
     "un JPEG different est deja sur S3 (cle du TIFF, extension .jpg)",
     "aucun : comparer au JPEG existant, puis --overwrite", "relancer_overwrite"),
    ("master_jpeg_nomme_tif", re.compile(r"no attribute 'tag_v2'"),
     "un master est un JPEG nomme .tif",
     "aucun : tiff_mix et le MIX supposent un TIFF", "bloque"),
    ("tiff_jpeg_old_style", re.compile(r"Unknown pseudo-tag 65538"),
     "TIFF a JPEG old-style, refuse par libtiff",
     "aucun", "bloque"),
    ("master_illisible", re.compile(r"cannot identify image file"),
     "master non reconnu comme image (Pillow)",
     "aucun : verifier le fichier", "bloque"),
]
ECHEC = re.compile(r"^ECHEC (?:ENVOI )?(\S+) : (.*)$")
FIN_DE_BLOC = re.compile(r"^(\[\d\d:\d\d:\d\d\] \d+/\d+ fascicules|ECHEC |Termine|\[\d{4}-)")
REFUSEES = re.compile(r"(\d+) refuse\(s\) d'emblee")
TERMINE = re.compile(r"(\d+) fascicule\(s\) en echec/refuse\(s\)/invalide\(s\)")


def lire_journal(chemin: str):
    """({notice: bloc de texte de son dernier echec}, nb refusees annoncees, nb en echec annonces)."""
    echecs, refusees, annonces = {}, 0, 0
    bloc = None
    with open(chemin, encoding="utf-8", errors="replace") as f:
        for ligne in f:
            ligne = ligne.rstrip("\n")
            m = ECHEC.match(ligne)
            if m:
                bloc = echecs[m.group(1)] = [ligne]
            elif FIN_DE_BLOC.match(ligne):
                bloc = None
            elif bloc is not None:
                bloc.append(ligne)
            if m := REFUSEES.search(ligne):
                refusees += int(m.group(1))
            if m := TERMINE.search(ligne):
                annonces += int(m.group(1))
    return {k: "\n".join(v) for k, v in echecs.items()}, refusees, annonces


def classer(bloc: str):
    for code, motif, libelle, correctif, action in CAUSES:
        if motif.search(bloc):
            return code, libelle, correctif, action
    return "autre", "cause non reconnue : lire le journal", "aucun", "bloque"


def lire_manifestes(motif: str) -> pd.DataFrame:
    fichiers = sorted(glob.glob(motif))
    if not fichiers:
        sys.exit(f"Aucun manifeste trouve : {motif}")
    df = pd.concat([pd.read_csv(f, dtype=str, low_memory=False) for f in fichiers], ignore_index=True)
    df["unitid"] = df.unitid.fillna("").str.strip()
    df["fascicule"] = [mm.unit_id(c, u) for c, u in zip(df.corpus_code, df.unitid)]
    return df


def main():
    ap = argparse.ArgumentParser(description="Liste des notices MED a relancer, d'apres le journal d'un lot.")
    ap.add_argument("journal", nargs="+", help="journal(s) du lot (nohup.out)")
    ap.add_argument("--manifestes", default=MANIFESTES, help="motif des manifestes (defaut : results/med_*/manifeste_MED_*.csv.gz)")
    ap.add_argument("--out-dir", default=str(RESULTS_DIR / "med_relance"))
    args = ap.parse_args()

    echecs, refusees_annoncees, echecs_annonces = {}, 0, 0
    for chemin in args.journal:
        e, r, a = lire_journal(chemin)
        echecs.update(e)
        refusees_annoncees += r
        echecs_annonces += a

    ref = lire_manifestes(args.manifestes)
    notices = ref.groupby("fascicule", sort=True)
    lignes = []
    for fasc, g in notices:
        rep = sorted({dirname(k) for k in g.s3_key})
        if fasc in echecs:
            code, libelle, correctif, action = classer(echecs[fasc])
            detail = echecs[fasc].splitlines()[0]
        elif len(rep) > 1:  # refusee d'emblee par med_mets.build_jobs
            code, libelle, correctif, action = ("refusee_repertoires", f"TIFF dans {len(rep)} repertoires de s3_key (original et photocopie)",
                                                "aucun : notice a decouper ou a ecarter", "bloque")
            detail = ", ".join(rep)
        else:
            continue
        lignes.append({
            "fonds": g.corpus_code.iloc[0], "fascicule": fasc, "unitid": g.unitid.iloc[0], "pages": len(g),
            "tiff_go": round(g["size"].astype(float).sum() / 1e9, 2), "cause": code, "libelle": libelle,
            "correctif": correctif, "action": action, "journal": detail[:200]})
    liste = pd.DataFrame(lignes)

    inconnues = sorted(set(echecs) - set(liste.fascicule))
    refusees = int((liste.cause == "refusee_repertoires").sum())
    print(f"{len(liste)} notice(s) : {len(liste) - refusees} en echec, {refusees} refusee(s) d'emblee.")
    if inconnues:
        print(f"ATTENTION : {len(inconnues)} notice(s) du journal absente(s) des manifestes : {', '.join(inconnues)}", file=sys.stderr)
    # « en echec/refuse(s)/invalide(s) » du journal compte les refusees d'emblee
    if refusees != refusees_annoncees or len(liste) != echecs_annonces:
        print(f"ATTENTION : le journal annonce {echecs_annonces} notice(s) non traitee(s) dont {refusees_annoncees} refusee(s) ; "
              f"trouvees : {len(liste)} dont {refusees}.", file=sys.stderr)

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    liste.to_csv(out / "notices_a_relancer.csv", index=False)
    for (fonds, action), g in liste[liste.action != "bloque"].groupby(["fonds", "action"]):
        extrait = ref[ref.fascicule.isin(g.fascicule)].drop(columns="fascicule")
        chemin = out / f"manifeste_{fonds}_{action}.csv.gz"
        extrait.to_csv(chemin, index=False)
        print(f"{chemin.relative_to(RESULTS_DIR.parent)} : {g.shape[0]} notice(s), {len(extrait)} TIFF")
    print()
    print(liste.groupby(["action", "cause"]).agg(notices=("fascicule", "count"), pages=("pages", "sum"),
                                                tiff_go=("tiff_go", "sum")).round(1).to_string())


if __name__ == "__main__":
    main()
