#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Reconstruit une colonne `s3_key_cible` pour les fichiers AMR_EC de statut
conservation INCONNU, à partir de la convention cible hand-made
`data/s3/amr_ec_V1.xlsx` (feuille Sheet1 : source `Chemin` -> décomposition
`niveau 0..8`).

Périmètre de ce run (décisions actées le 2026-07-07) :
  - Colonne cible = nouvelle colonne `s3_key_cible` (on ne touche pas `s3_key`,
    qui reste l'invariant « fichier réellement versé sur S3 »).
  - On résout les familles à convention non ambiguë :
      * microfilm  MI_EC_T*  (extrapolation identité de la convention MI_EC_nn)
      * originaux  Reg_EC    (cible V1 explicite, identité)
      * LEURIDAN             (base V1 + sous-dossier/nom = « nom actuel avec _ »)
      * décennales 1933-1992 (niveau4/5 dérivés de la période/leaf source)
  - On LAISSE VIDE : Décès/Mariages lots A/B tardifs (ambigu, V1 vide),
    Naissances (edge pré-1880), Cadastre (à supprimer/déplacer).

La règle de nommage de chaque famille "renommée" est AUTO-VALIDÉE contre les
ancres non-vides de V1 (le script s'arrête si une ancre n'est pas reproduite).
"""
import re
import sys
import argparse
import pandas as pd

REF = "results/ref/_ref_files_20260630_AMR_EC.csv.gz"
XLSX = "data/s3/amr_ec_V1.xlsx"
ROOT = "BNR_VERIF/AMR/AMR_EC/"
LVL = [f"niveau {i}" for i in range(9)]


# ---------------------------------------------------------------- helpers
def norm_us(s: str) -> str:
    """Espaces et tirets (et leurs combinaisons) -> underscore unique."""
    return re.sub(r"[ \-]+", "_", s.strip())


def norm_td(s: str) -> str:
    """Comme norm_us mais retire l'espace juste après un `TD` de tête.
    `TD 1933-1942 - A` -> `TD1933_1942_A`."""
    return norm_us(re.sub(r"^TD\s+", "TD", s.strip()))


def norm_mi(s: str) -> str:
    """Normalise le préfixe microfilm `Mi_EC` -> `MI_EC`."""
    return s.replace("Mi_EC", "MI_EC")


def nat_key(s: str):
    """Clé de tri naturel (les nombres comparés numériquement)."""
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", s)]


def v1_prefix(row) -> str | None:
    """Préfixe cible V1 = concat des niveaux gauche-contigus, en s'arrêtant au
    premier blanc, à une annotation `fichier :`, ou None si `à supprimer/déplacer`."""
    parts = []
    for c in LVL:
        v = row[c]
        if pd.isna(v):
            break
        t = str(v).strip()
        if t.lower().startswith("fichier"):
            break
        if "supprimer" in t.lower() or "déplacer" in t.lower():
            return None
        parts.append(t.rstrip("/"))
    return "/".join(parts) if parts else None


# ---------------------------------------------------------------- mapping V1
def load_mapping():
    m = pd.read_excel(XLSX, sheet_name="Sheet1")
    m["tgt"] = m.apply(v1_prefix, axis=1)
    lookup = dict(zip(m["Chemin"].astype(str), m["tgt"]))
    return m, lookup


# ---------------------------------------------------------------- validation
def validate_rules(m):
    """Vérifie que norm_us / norm_td / la dérivation niveau4 reproduisent
    exactement toutes les ancres non-vides de V1. Lève AssertionError sinon."""
    errs = []

    # LEURIDAN : niveau5 = norm_us(leaf source)
    leur = m[m["Chemin"].astype(str).str.contains("TABLES LEURIDAN", na=False)]
    for _, r in leur[leur["niveau 5"].notna()].iterrows():
        n5 = str(r["niveau 5"])
        if n5.lower().startswith("fichier"):
            continue
        leaf = str(r["Chemin"]).split("/")[-1]
        got = norm_us(leaf)
        if got != n5:
            errs.append(f"LEURIDAN leaf={leaf!r} attendu={n5!r} obtenu={got!r}")

    # DECENNALES : niveau4 = {periode}_D ; niveau5 = norm_td(leaf)
    dec = m[m["Chemin"].astype(str).str.contains(
        "Tables décennales de décès 1933-1992", na=False)]
    for _, r in dec.iterrows():
        chem = str(r["Chemin"])
        per = re.search(r"1933-1992/TD (\d{4}-\d{4})", chem)
        if r["niveau 4"] is not None and pd.notna(r["niveau 4"]) and per:
            want4 = f"{per.group(1)}_D"
            if str(r["niveau 4"]) != want4:
                errs.append(f"DEC niveau4 {chem}: attendu={want4} obtenu={r['niveau 4']}")
        n5 = r["niveau 5"]
        if pd.notna(n5) and not str(n5).lower().startswith("fichier"):
            leaf = chem.split("/")[-1]
            got = norm_td(leaf)
            if got != str(n5):
                errs.append(f"DEC leaf={leaf!r} attendu={n5!r} obtenu={got!r}")

    if errs:
        print("ÉCHEC VALIDATION des règles de nommage :", file=sys.stderr)
        for e in errs[:40]:
            print("  -", e, file=sys.stderr)
        raise AssertionError(f"{len(errs)} ancre(s) V1 non reproduite(s)")
    print("Validation règles OK (LEURIDAN + décennales : toutes les ancres V1 reproduites)")


# ---------------------------------------------------------------- familles
def family(rel: str) -> str:
    if rel.startswith("AMR_TDE"):
        return "TDE"
    if "TABLES LEURIDAN" in rel:
        return "leuridan"
    if "décennale" in rel.lower():
        return "decennales"
    if "Décès" in rel:
        return "deces"
    if "Mariages" in rel:
        return "mariages"
    if "microfilm" in rel:
        return "microfilm"
    if "1881-1907" in rel:
        return "originaux"
    if "Naissances" in rel or "NAISS" in rel:
        return "naissances"
    return "autre"


def resolve(df, lookup):
    """Ajoute la colonne s3_key_cible. Renvoie (df, stats)."""
    keys = pd.Series(pd.NA, index=df.index, dtype="object")

    # -- microfilm _T : identité, extrapolation convention MI_EC_nn
    mic = df[df["fam"] == "microfilm"]
    for i, r in mic.iterrows():
        folder = norm_mi(r["rel"].split("/")[-1])          # MI_EC_T01
        fname = norm_mi(r["name"])
        keys[i] = f"AMR/AMR_EC/actes/1588-1880_AD59/{folder}/{fname}"

    # -- originaux Reg_EC : cible V1 explicite, identité
    ori = df[df["fam"] == "originaux"]
    for i, r in ori.iterrows():
        tgt = lookup.get(r["rel"])
        if tgt:
            keys[i] = f"{tgt}/{r['name']}"

    # -- LEURIDAN : base + sous-dossier(norm) + nom actuel avec _
    BASE_L = "AMR/AMR_EC/TD/Tables_Leuridan_AM_Dupont/tables_leuridan"
    leu = df[df["fam"] == "leuridan"]
    for i, r in leu.iterrows():
        sub = norm_us(r["rel"].split("/")[-1])
        stem, dot, ext = r["name"].rpartition(".")
        fname = norm_us(stem) + (dot + ext.lower() if dot else "")
        keys[i] = f"{BASE_L}/{sub}/{fname}"

    # -- décennales : niveau4 dérivé période, niveau5 dérivé leaf, fichiers renommés+rang
    BASE_D = "AMR/AMR_EC/TD/1933-1992_AM_Dupont"
    dec = df[df["fam"] == "decennales"].copy()
    # rang naturel dans chaque dossier source (rel)
    for rel, grp in dec.groupby("rel"):
        per = re.search(r"1933-1992/TD (\d{4}-\d{4})", rel)
        if not per:
            continue
        n4 = f"{per.group(1)}_D"
        n5 = norm_td(rel.split("/")[-1])
        ordered = grp.loc[grp["name"].map(nat_key).sort_values().index]
        for rank, (i, r) in enumerate(ordered.iterrows(), start=1):
            keys[i] = f"{BASE_D}/{n4}/{n5}/{n5}_{rank:03d}.jpg"

    df = df.copy()
    df["s3_key_cible"] = keys
    return df


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true",
                    help="écrit s3_key_cible dans le fichier ref (sinon dry-run)")
    args = ap.parse_args()

    m, lookup = load_mapping()
    validate_rules(m)

    ref = pd.read_csv(REF, dtype=str)
    inc_mask = ref["conservation_statut"] == "INCONNU"
    inc = ref[inc_mask].copy()
    inc["rel"] = inc["path"].str.replace("^" + re.escape(ROOT), "", regex=True)
    inc["fam"] = inc["rel"].apply(family)

    resolved = resolve(inc, lookup)

    # -- rapport
    print(f"\nINCONNU total : {len(resolved)}")
    got = resolved["s3_key_cible"].notna()
    print(f"s3_key_cible remplies : {got.sum()}  |  vides : {(~got).sum()}\n")
    rep = (resolved.assign(ok=got)
           .groupby(["fam", "ok"]).size().unstack(fill_value=0))
    print(rep.to_string())
    print("\nÉchantillons :")
    for f in ["microfilm", "originaux", "leuridan", "decennales"]:
        s = resolved[(resolved.fam == f) & got]
        if len(s):
            r = s.iloc[0]
            print(f"  [{f}] {r['path'].split(ROOT)[-1]}/{r['name']}")
            print(f"        -> {r['s3_key_cible']}")

    if args.write:
        ref["s3_key_cible"] = pd.NA
        ref.loc[inc_mask, "s3_key_cible"] = resolved["s3_key_cible"].values
        ref.to_csv(REF, index=False, compression="gzip")
        print(f"\nÉCRIT : {REF} (colonne s3_key_cible ajoutée, {got.sum()} clés)")
    else:
        print("\n(dry-run : rien écrit — relancer avec --write)")


if __name__ == "__main__":
    main()
