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

  - cas2 (DAO SEUL) : STATUT = « SEUL DAO » par défaut (DAO attribuée mais fichier
           introuvable dans REF) ; sauf si sa clé normalisée (casse/ponctuation
           ignorées) correspond à une clé du cas1/cas3, auquel cas STATUT =
           « ERREUR DAO (candidat) » — À VALIDER contre la notice EAD, cf.
           méthodologie du stage (les cas d'erreur de nommage réels doivent être
           confirmés à la main, ce script ne fait que proposer le rapprochement).
           À FAIRE = « À numériser » pour les SEUL DAO, vide pour les candidats
           ERREUR DAO (dépend de la confirmation).
           Pour les SEUL DAO restants (pas de correspondance exacte), colonnes
           REF_PROCHE / REF_PROCHE_UUID / REF_PROCHE_CHEMIN / REF_PROCHE_SIMILARITE :
           la clé cas1/cas3 la plus ressemblante (difflib, seuil SIMILARITE_MIN),
           proposée comme piste, sans changer le STATUT ni l'À FAIRE — un
           rapprochement plausible n'est pas une confirmation.

  - cas3 (REF SEUL) : laissé tel quel (rare), sans STATUT/À FAIRE dérivé.

  - Non couvert : les fichiers « Zébulon » (aucune source de données dans le
    dépôt pour ce dossier partagé) -- absent de ce brouillon.

Usage
-----
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_recap_draft.py MUS_ARC
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_recap_draft.py MUS_ARC --date 20260821
"""
import argparse
import difflib
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

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
    "À CHERCHER": "À chercher",
    "S3_KEY À CONSTRUIRE": "S3 key à construire",
    "DOUBLON - À VOIR": "À examiner (doublon)",
}


COLONNES = ["CAS", "UUID", "STATUT", "CHEMIN", "PROBLEMES", "À FAIRE",
            "REF_PROCHE", "REF_PROCHE_UUID", "REF_PROCHE_CHEMIN", "REF_PROCHE_SIMILARITE"]

# En dessous de ce seuil (ratio difflib, 0-1), pas de proposition : trop de bruit.
SIMILARITE_MIN = 0.75

# Noms courts pour la colonne CAS, plus parlants que "CAS 1/2/3" (cf. définition
# des cas dans stagemel_cas_merge.py : présence croisée uuid REF / clé DAO).
CAS_APPARIE = "APPARIÉ"    # cas1 : dans REF et DAO
CAS_DAO_SEUL = "DAO SEUL"  # cas2 : dans DAO seulement
CAS_REF_SEUL = "REF SEUL"  # cas3 : dans REF seulement


def norm_key(k):
    return re.sub(r"[^A-Z0-9]", "", str(k).upper())


DERNIER_GROUPE_CHIFFRES = re.compile(r"\d+(?!.*\d)")

# Une clé plate MED_MS_<manuscrit>_<folio> normalisée en RBXMEDMS<manuscrit><folio>
# perd la frontière entre ses deux groupes de chiffres : neutraliser tous les
# chiffres regrouperait 45 000 clés dans un seul bucket. On calcule donc le
# squelette sur la clé BRUTE (séparateurs conservés), en ne neutralisant que le
# dernier groupe de chiffres (le niveau le plus fin, ex. le folio) ; l'identifiant
# parent (ex. le manuscrit) reste discriminant. Bucket max observé sur MED_MS :
# ~1 900 (contre 45 337 sans cette précaution).
def skeleton(key_brute):
    return DERNIER_GROUPE_CHIFFRES.sub(lambda m: "#" * len(m.group()), str(key_brute).upper())


# Au-delà, le bucket est trop générique pour qu'un rapprochement soit fiable
# (et trop coûteux à comparer un par un) : on l'ignore plutôt que de proposer du bruit.
TAILLE_BUCKET_MAX = 2000


def index_ref(cas1, cas3):
    """Index skeleton(clé brute) -> {key_norm: {key, uuid, path}}, pour cas1+cas3 (fichiers du REF)."""
    parts = [df_[["key", "uuid", "path"]] for df_ in (cas1, cas3) if not df_.empty]
    buckets = {}
    if not parts:
        return buckets
    ref = pd.concat(parts, ignore_index=True)
    for key, uuid, path in zip(ref["key"], ref["uuid"], ref["path"]):
        buckets.setdefault(skeleton(key), {}).setdefault(norm_key(key), {"key": key, "uuid": uuid, "path": path})
    return buckets


def plus_proche(key_brute, buckets, avertissements):
    """Meilleure correspondance REF par similarité de clé (difflib), restreinte au même skeleton."""
    bucket = buckets.get(skeleton(key_brute))
    if not bucket:
        return None
    if len(bucket) > TAILLE_BUCKET_MAX:
        msg = f"cas2 : bucket « {skeleton(key_brute)} » ignoré ({len(bucket)} clés, trop générique/coûteux)"
        if msg not in avertissements:
            avertissements.append(msg)
        return None
    key_norm = norm_key(key_brute)
    proches = difflib.get_close_matches(key_norm, bucket.keys(), n=1, cutoff=SIMILARITE_MIN)
    if not proches:
        return None
    meilleur = proches[0]
    score = difflib.SequenceMatcher(None, key_norm, meilleur).ratio()
    return {**bucket[meilleur], "similarite": round(score, 2)}


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


def build_cas1(df, corpus_code, avertissements):
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
        **{c: None for c in COLONNES[6:]},  # REF_PROCHE* : sans objet pour un APPARIÉ
    })

    a_valider = sorted(set(df["conservation_statut"].dropna()) - set(ACTION_SUR))
    for s in a_valider:
        avertissements.append(f"cas1 : action à valider pour le statut « {s} » (politique non définie)")
    return out


def build_cas2(df, corpus_code, cas1, cas3, avertissements):
    if df.empty:
        return pd.DataFrame(columns=["CAS", corpus_code] + COLONNES[1:])

    # uuid indexé par clé normalisée, pour les candidats ERREUR DAO uniquement
    # (un vrai cas2 n'a par définition pas d'uuid : le fichier n'est pas dans REF).
    cle_uuid = [df_[["key", "uuid"]] for df_ in (cas1, cas3) if not df_.empty]
    connus = pd.concat(cle_uuid, ignore_index=True) if cle_uuid else pd.DataFrame(columns=["key", "uuid"])
    uuid_par_cle_norm = {norm_key(k): u for k, u in zip(connus["key"], connus["uuid"])}

    key_norm = df["key"].map(norm_key)
    candidat = key_norm.isin(uuid_par_cle_norm)
    n_candidats = int(candidat.sum())
    if n_candidats:
        avertissements.append(
            f"cas2 : {n_candidats} candidat(s) ERREUR DAO (clé proche d'un cas1/cas3) — à confirmer via la notice EAD"
        )

    statut = candidat.map({True: "ERREUR DAO (candidat)", False: "SEUL DAO"})
    a_faire = candidat.map({True: "", False: "À numériser"})

    # Pour les SEUL DAO restants, propose la clé REF la plus ressemblante (piste,
    # pas une confirmation) — cf. mémoire feedback_recap_diagnostic_pas_resolution :
    # ça n'écrase ni STATUT ni À FAIRE.
    buckets = index_ref(cas1, cas3)
    proches = df["key"].where(~candidat).map(lambda k: plus_proche(k, buckets, avertissements) if pd.notna(k) else None)
    n_proches = int(proches.notna().sum())
    if n_proches:
        avertissements.append(
            f"cas2 : {n_proches} rapprochement(s) possible(s) avec le REF (similarité >= {SIMILARITE_MIN}) — à vérifier, non confirmés"
        )

    problemes = candidat.map({True: "Clé proche d'un fichier connu (casse/ponctuation ?)", False: "Fichier non retrouvé dans REF"})
    problemes = problemes.mask(proches.notna(), problemes + " (rapprochement possible : voir REF_PROCHE)")

    out = pd.DataFrame({
        "CAS": CAS_DAO_SEUL,
        corpus_code: df["nom_fichier_base"],
        "UUID": key_norm.map(uuid_par_cle_norm),
        "STATUT": statut,
        "CHEMIN": "",
        "PROBLEMES": problemes,
        "À FAIRE": a_faire,
        "REF_PROCHE": proches.map(lambda p: p["key"] if p else None),
        "REF_PROCHE_UUID": proches.map(lambda p: p["uuid"] if p else None),
        "REF_PROCHE_CHEMIN": proches.map(lambda p: p["path"] if p else None),
        "REF_PROCHE_SIMILARITE": proches.map(lambda p: p["similarite"] if p else None),
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
        **{c: None for c in COLONNES[6:]},
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("corpus_code")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"))
    args = parser.parse_args()

    dfs = charger(args.corpus_code, args.date)
    avertissements = []

    cas1 = build_cas1(dfs["cas1"], args.corpus_code, avertissements)
    cas2 = build_cas2(dfs["cas2"], args.corpus_code, dfs["cas1"], dfs["cas3"], avertissements)
    cas3 = build_cas3(dfs["cas3"], args.corpus_code)

    recap = pd.concat([cas1, cas2, cas3], ignore_index=True)
    pivot = recap.groupby(["CAS", "STATUT"], dropna=False).size().rename("nombre").reset_index()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{args.corpus_code.lower()}_recap_draft_{args.date}.xlsx"
    with pd.ExcelWriter(out_path) as writer:
        recap.to_excel(writer, sheet_name="recap", index=False)
        pivot.to_excel(writer, sheet_name="pivot", index=False)

    print(f"Recap brouillon écrit : {out_path}  ({len(recap)} lignes)")
    print(pivot.to_string(index=False))
    if avertissements:
        print("\nÀ valider avant de considérer ce recap comme définitif :")
        for a in avertissements:
            print(f"  - {a}")


if __name__ == "__main__":
    main()
