#!/usr/bin/env python3
r"""
stagemel_recap_draft.py — Brouillon de recap Excel par corpus, à partir de cas1/cas2/cas3.

Reproduit la même structure que les recap_<code>_<date>.xlsx de la stagiaire
(CAS / <CODE> / STATUT / CHEMIN / PROBLEMES / À FAIRE), mais seulement pour ce
qui est mécaniquement dérivable :

  - UUID : ajouté quand disponible — celui du REF pour cas1/cas3 ; pour cas2,
           uniquement si un candidat ERREUR DAO a été rapproché (uuid du cas1/cas3
           correspondant), sinon vide (par définition, un vrai cas2 n'a pas d'uuid).

  - CAS : renommé en libellés courts plutôt que « CAS 1/2/3 » — APPARIÉ (cas1,
          dans REF+DAO), DAO SEUL (cas2), REF SEUL (cas3).

  - cas1 (APPARIÉ) : STATUT = conservation_statut du REF (libellé simplifié) ;
           CHEMIN = path du REF ;
           PROBLEMES = « Pas de format tif » si le fichier est un .jpg sans .tif
           de même clé ailleurs dans le corpus, sinon vide ;
           À FAIRE = rempli UNIQUEMENT pour les statuts dont l'action est sans
           ambiguïté (aujourd'hui : TRANSFERT_S3_OK -> « Rien à faire »). Pour
           tout autre statut (CORBEILLE, EN LIGNE, INCONNU...), la cellule est
           laissée vide et un avertissement est affiché : la politique à
           appliquer (transférer ? supprimer ? cf. CONFIG de med_s3_key_cible.py)
           doit être validée au cas par cas, elle n'est pas inventée ici.
           Exclues du recap : les lignes À SUPPRIMER (*) dont le master (.tif)
           est déjà TRANSFERT_S3_OK — recherché sur tout le ref, tous corpus
           confondus (pas seulement celui traité), rien à trancher dessus.

  - cas2 (DAO SEUL) : STATUT = « SEUL DAO » par défaut (DAO attribuée mais fichier
           introuvable dans REF) ; sauf si sa clé normalisée (casse/ponctuation
           ignorées) correspond à une clé du cas1/cas3, auquel cas STATUT =
           « ERREUR DAO (candidat) » — À VALIDER contre la notice EAD, cf.
           méthodologie du stage (les cas d'erreur de nommage réels doivent être
           confirmés à la main, ce script ne fait que proposer le rapprochement).
           À FAIRE = « À numériser » pour les SEUL DAO, vide pour les candidats
           ERREUR DAO (dépend de la confirmation). La correspondance exacte
           cherche d'abord parmi les fichiers pas encore À SUPPRIMER, et ne se
           rabat sur un fichier À SUPPRIMER que si rien d'autre n'a matché
           (STATUT le signale alors explicitement) : pointer vers un fichier
           voué à disparaître serait trompeur si un autre candidat existe.
           Pas de rapprochement flou/approchant : un fichier « non retrouvé
           dans REF » reste tel quel, sans piste automatique proposée — trop
           de faux positifs sur les corpus à foliotation dense (cf. mémoire :
           tout folio voisin d'un manuscrit MED_MS ressortait à >0.85 sans lien
           réel), retiré le 2026-08-22.

  - cas3 (REF SEUL) : laissé tel quel (rare), sans STATUT/À FAIRE dérivé.

  - Non couvert : les fichiers « Zébulon » (aucune source de données dans le
    dépôt pour ce dossier partagé) -- absent de ce brouillon.

Usage
-----
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_recap_draft.py MUS_ARC
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_recap_draft.py MUS_ARC --date 20260821
"""
import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl.styles import PatternFill

sys.path.insert(0, str(Path(__file__).parent))
from stagemel_extraction_corpus import dernier_ref  # noqa: E402

CAS_DIR = Path("results/corpus/stagemel/cas")
OUT_DIR = Path("results/corpus/stagemel/recap")

# Actions sans ambiguïté : les statuts harmonisés du ref (fusion 2026-07-11) sont
# déjà des décisions prises, pas des diagnostics à interpréter — cf. mémoire
# project_ref_merge_20260711. Seule la famille INCONNU* reste une politique à
# définir par corpus (cf. CONFIG de scripts/s3/med_s3_key_cible.py), donc
# volontairement absente d'ACTION_SUR.
ACTION_SUR = {
    "TRANSFERT_S3_OK": "Rien à faire",
    "À TRANSFERER": "À transférer",
    "À TRANSFERER APRES VALIDATION": "À transférer (après validation)",
    "À TRANSFERER VOIR MARIE": "À transférer (voir Marie)",
    "EN LIGNE - À TRANSFERER ?": "À transférer (à confirmer)",
    "À SUPPRIMER (DIFFUSION)": "À supprimer",
    "À SUPPRIMER (REMPLACEMENT)": "À supprimer",
    "À SUPPRIMER (DOUBLON)": "À supprimer",
    "À SUPPRIMER (DOUBLON AZ)": "À supprimer",
    "À SUPPRIMER (DOUBLON S3-AZ)": "À supprimer",
    "À SUPPRIMER (FILE_TYPE)": "À supprimer",
    "À SUPPRIMER (RENOMMAGE)": "À supprimer",
    "À SUPPRIMER (MED_PAR)": "À supprimer",
    "À SUPPRIMER (TESTS)": "À supprimer",
    "À SUPPRIMER (DOUBLON SOURCE - À VERIFIER)": "À supprimer (vérifier échantillon avant purge)",
    "À SUPPRIMER (DIFFUSION - MASTER DÉJÀ SUR S3)": "À supprimer",
    "À CHERCHER": "À chercher",
    "S3_KEY À CONSTRUIRE": "S3 key à construire",
    "DOUBLON - À VOIR": "À examiner (doublon)",
}


COLONNES = ["CAS", "UUID", "STATUT", "CHEMIN", "PROBLEMES", "À FAIRE"]

# Noms courts pour la colonne CAS, plus parlants que "CAS 1/2/3" (cf. définition
# des cas dans stagemel_cas_merge.py : présence croisée uuid REF / clé DAO).
CAS_APPARIE = "APPARIÉ"    # cas1 : dans REF et DAO
CAS_DAO_SEUL = "DAO SEUL"  # cas2 : dans DAO seulement
CAS_REF_SEUL = "REF SEUL"  # cas3 : dans REF seulement


def norm_key(k):
    return re.sub(r"[^A-Z0-9]", "", str(k).upper())


def est_a_supprimer(statut):
    return isinstance(statut, str) and statut.startswith("À SUPPRIMER")


def connus_ref(cas1, cas3):
    """key/uuid/path/conservation_statut de cas1+cas3 (fichiers du REF), scindés en
    deux lots : les fichiers pas-encore-supprimés (prioritaires pour un rapprochement)
    et ceux déjà marqués À SUPPRIMER (recours seulement si rien d'autre ne matche —
    proposer comme correspondance un fichier voué à disparaître serait trompeur)."""
    cols = ["key", "uuid", "path", "conservation_statut"]
    parts = [df_[cols] for df_ in (cas1, cas3) if not df_.empty]
    vide = pd.DataFrame(columns=cols)
    if not parts:
        return vide, vide
    ref = pd.concat(parts, ignore_index=True)
    a_supprimer = ref["conservation_statut"].map(est_a_supprimer)
    return ref[~a_supprimer], ref[a_supprimer]


def charger(corpus_code, date):
    paths = {
        cas: CAS_DIR / f"{corpus_code}_{cas}_{date}.csv.gz"
        for cas in ("cas1", "cas2", "cas3")
    }
    dfs = {}
    for cas, p in paths.items():
        dfs[cas] = pd.read_csv(p, low_memory=False) if p.exists() else pd.DataFrame()
    if dfs["cas1"].empty and dfs["cas2"].empty:
        raise FileNotFoundError(
            f"Aucun cas1/cas2 pour {corpus_code} à la date {date} dans {CAS_DIR} "
            "— lancer stagemel_cas_merge.py d'abord."
        )
    return dfs


def tif_sibling_absent(df_cas1):
    """Clés .jpg de cas1 n'ayant pas de .tif de même clé dans le même cas1."""
    tif_keys = set(df_cas1.loc[df_cas1["extension"] == ".tif", "key"])
    return ~df_cas1["key"].isin(tif_keys)


def masters_tif_stems():
    """Stems (nom sans extension, MAJ) des .tif déjà TRANSFERT_S3_OK, tout le ref
    confondu (tous corpus) — un À SUPPRIMER dont le master est déjà en sécurité sur
    S3 n'a pas besoin d'être revu, même si ce master est dans un autre corpus_code
    (cf. mémoire project_stagemel_recap : cas MED_MS/AMR_PR, volontaire, pas un bug)."""
    ref_path = dernier_ref()
    ref = pd.read_csv(ref_path, usecols=["name", "extension", "conservation_statut"], low_memory=False)
    tif_ok = ref["extension"].str.lower().isin([".tif", ".tiff"]) & (ref["conservation_statut"] == "TRANSFERT_S3_OK")
    return set(ref.loc[tif_ok, "name"].str.rsplit(".", n=1).str[0].str.upper())


def build_cas1(df, corpus_code, masters, avertissements):
    if df.empty:
        return pd.DataFrame(columns=["CAS", corpus_code] + COLONNES[1:])

    sans_tif = tif_sibling_absent(df) & (df["extension"] == ".jpg")

    out = pd.DataFrame({
        "CAS": CAS_APPARIE,
        corpus_code: df["name"],
        "UUID": df["uuid"],
        "STATUT": df["conservation_statut"],
        "CHEMIN": df["path"],
        "PROBLEMES": sans_tif.map({True: "Pas de format tif", False: ""}),
        "À FAIRE": df["conservation_statut"].map(ACTION_SUR).fillna(""),
    })

    a_valider = sorted(set(df["conservation_statut"].dropna()) - set(ACTION_SUR))
    for s in a_valider:
        avertissements.append(f"cas1 : action à valider pour le statut « {s} » (politique non définie)")

    # À SUPPRIMER dont le master est déjà sur S3 : rien à décider, on n'encombre pas le recap.
    a_master = df["name"].str.rsplit(".", n=1).str[0].str.upper().isin(masters)
    exclure = df["conservation_statut"].map(est_a_supprimer) & a_master
    if exclure.any():
        avertissements.append(
            f"cas1 : {int(exclure.sum())} ligne(s) À SUPPRIMER exclue(s) du recap (master déjà TRANSFERT_S3_OK)"
        )
    return out[~exclure.to_numpy()]


def build_cas2(df, corpus_code, cas1, cas3, avertissements):
    if df.empty:
        return pd.DataFrame(columns=["CAS", corpus_code] + COLONNES[1:])

    # Fichiers connus du REF (cas1+cas3), scindés prioritaire / À SUPPRIMER (cf.
    # connus_ref) — un rapprochement, exact ou flou, ne doit pointer vers un
    # fichier déjà marqué À SUPPRIMER que si aucun autre candidat n'existe.
    prioritaire, secours = connus_ref(cas1, cas3)
    uuid_prioritaire = {norm_key(k): u for k, u in zip(prioritaire["key"], prioritaire["uuid"])}
    uuid_secours = {norm_key(k): u for k, u in zip(secours["key"], secours["uuid"])}

    key_norm = df["key"].map(norm_key)
    dans_prioritaire = key_norm.isin(uuid_prioritaire)
    dans_secours = key_norm.isin(uuid_secours) & ~dans_prioritaire
    candidat = dans_prioritaire | dans_secours
    n_candidats, n_candidats_secours = int(candidat.sum()), int(dans_secours.sum())
    if n_candidats:
        msg = f"cas2 : {n_candidats} candidat(s) ERREUR DAO (clé proche d'un cas1/cas3) — à confirmer via la notice EAD"
        if n_candidats_secours:
            msg += f" (dont {n_candidats_secours} vers un fichier déjà À SUPPRIMER — vérifier en priorité)"
        avertissements.append(msg)

    statut = pd.Series("SEUL DAO", index=df.index)
    statut = statut.mask(dans_prioritaire, "ERREUR DAO (candidat)")
    statut = statut.mask(dans_secours, "ERREUR DAO (candidat, fichier À SUPPRIMER)")
    a_faire = candidat.map({True: "", False: "À numériser"})
    uuid_col = key_norm.map(uuid_prioritaire)
    uuid_col = uuid_col.where(uuid_col.notna(), key_norm.map(uuid_secours))

    problemes = candidat.map({True: "Clé proche d'un fichier connu (casse/ponctuation ?)", False: "Fichier non retrouvé dans REF"})

    out = pd.DataFrame({
        "CAS": CAS_DAO_SEUL,
        corpus_code: df["nom_fichier_base"],
        "UUID": uuid_col,
        "STATUT": statut,
        "CHEMIN": "",
        "PROBLEMES": problemes,
        "À FAIRE": a_faire,
    })
    return out


def build_cas3(df, corpus_code):
    if df.empty:
        return pd.DataFrame(columns=["CAS", corpus_code] + COLONNES[1:])
    return pd.DataFrame({
        "CAS": CAS_REF_SEUL,
        corpus_code: df["name"],
        "UUID": df["uuid"],
        "STATUT": "",
        "CHEMIN": df["path"],
        "PROBLEMES": "",
        "À FAIRE": "",
    })


FOND_BLANC = PatternFill(fill_type="solid", fgColor="FFFFFFFF")


def appliquer_fond_blanc(ws):
    """Fond blanc systématique (au lieu du fond transparent par défaut d'Excel/openpyxl)."""
    for row in ws.iter_rows():
        for cell in row:
            cell.fill = FOND_BLANC


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("corpus_code")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"))
    args = parser.parse_args()

    dfs = charger(args.corpus_code, args.date)
    avertissements = []
    masters = masters_tif_stems()

    cas1 = build_cas1(dfs["cas1"], args.corpus_code, masters, avertissements)
    cas2 = build_cas2(dfs["cas2"], args.corpus_code, dfs["cas1"], dfs["cas3"], avertissements)
    cas3 = build_cas3(dfs["cas3"], args.corpus_code)

    recap = pd.concat([cas1, cas2, cas3], ignore_index=True)
    pivot = recap.groupby(["CAS", "STATUT"], dropna=False).size().rename("nombre").reset_index()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{args.corpus_code.lower()}_recap_draft_{args.date}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        recap.to_excel(writer, sheet_name="recap", index=False)
        pivot.to_excel(writer, sheet_name="pivot", index=False)
        appliquer_fond_blanc(writer.sheets["recap"])
        appliquer_fond_blanc(writer.sheets["pivot"])

    print(f"Recap brouillon écrit : {out_path}  ({len(recap)} lignes)")
    print(pivot.to_string(index=False))
    if avertissements:
        print("\nÀ valider avant de considérer ce recap comme définitif :")
        for a in avertissements:
            print(f"  - {a}")


if __name__ == "__main__":
    main()
