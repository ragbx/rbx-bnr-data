#!/usr/bin/env python
"""
Construit `s3_key_cible` pour AMR_CAD (cadastre) et tranche le sort des INCONNU.

On n'ajoute que `s3_key_cible` (jamais `s3_key` = invariant S3 réel) et on
révise `publication_statut` / `conservation_statut` selon l'arbitrage 2026-07-09.

Rôle = extension (cf. mémoire) : TIF=conservation, JPEG/PDF=diffusion. Ici sans
incidence sur la clé (tous → AMR/AMR_CAD/<name>), le rôle sert au recensement.

Arbitrage des 267 « inconnu » :
  - Groupe A (78) = fichiers RBX-nommés jamais poussés (cotes 1804/1847/PLA
        entièrement non publiées + feuilles manquantes 1884) -> publication=oui,
        conservation="À TRANSFERER", s3_key_cible = AMR/AMR_CAD/<name>.
  - Groupe B = 189 scans sources « cadastre collection AD59 » (FRAD059_*, jpg) :
        * 9 = jumeau checksum exact du cadastre 1826 déjà publié (RBX_AMR_CAD_1826)
              -> conservation="À SUPPRIMER (DOUBLONS)", s3_key_cible vide.
        * 180 restants mis de côté -> conservation="INCONNU FRAD59", s3_key_cible vide.
  - 1 .doc reste « jamais » / « À SUPPRIMER (FILE_TYPE) », s3_key_cible vide.

Les 247 déjà publiés (oui) reçoivent leur s3_key_cible ; statuts inchangés.

Entrée : results/ref/amr_cad.csv
Sortie : results/ref/amr_cad_fin.csv   (+ audit results/ref/_amr_cad_cible_audit_20260630.csv)
Garde  : results/ref/_ref_files_20260630_dedup.pkl (cross-check checksum, lecture seule)
"""
import sys
import datetime
import pandas as pd

SRC = "results/ref/amr_cad.csv"
OUT = "results/ref/amr_cad_fin.csv"
AUDIT = "results/ref/_amr_cad_cible_audit_20260630.csv"

df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
orig = df.copy()
TODAY = datetime.date.today().isoformat()
print("shape lue:", df.shape)
assert len(df) == 515, f"attendu 515, obtenu {len(df)}"
assert (df["corpus_code"] == "AMR_CAD").all(), "corpus_code != AMR_CAD"
assert "s3_key_cible" not in df.columns, "s3_key_cible déjà présente"

sub = df["path"].str.extract(r"AMR_CAD/([^/]+)")[0]
ext = df["extension"].str.lower()

# --- populations ---------------------------------------------------------
frad = sub == "cadastre collection AD59"          # 189 jpg + 1 doc
is_doc = ext == ".doc"
frad_jpg = frad & ~is_doc                          # 189 scans sources
oui = df["publication_statut"] == "oui"            # 247 déjà publiés
groupe_a = (df["publication_statut"] == "inconnu") & ~frad_jpg  # 78 RBX-nommés

assert int(frad_jpg.sum()) == 189, f"FRAD059 jpg = {int(frad_jpg.sum())}"
assert int(is_doc.sum()) == 1, f"doc = {int(is_doc.sum())}"
assert int(oui.sum()) == 247, f"oui = {int(oui.sum())}"
assert int(groupe_a.sum()) == 78, f"groupe A = {int(groupe_a.sum())}"
# les groupe A portent bien le nommage RBX
assert df.loc[groupe_a, "name"].str.match(r"^RBX_AMR_(CAD|PLA)_").all(), "nom groupe A inattendu"

# 9 FRAD059 = jumeau checksum d'un publié RBX
ck_pub = set(df.loc[oui, "checksum_md5"])
frad_dup = frad_jpg & df["checksum_md5"].isin(ck_pub)
frad_reste = frad_jpg & ~frad_dup
assert int(frad_dup.sum()) == 9, f"FRAD059 doublons = {int(frad_dup.sum())}"
assert int(frad_reste.sum()) == 180, f"FRAD059 reste = {int(frad_reste.sum())}"

# --- révision des statuts ------------------------------------------------
# les 247 déjà publiés étaient « EN LIGNE - À TRANSFERER ? » -> À TRANSFERER
df.loc[oui, "conservation_statut"] = "À TRANSFERER"
df.loc[groupe_a, "publication_statut"] = "oui"
df.loc[groupe_a, "conservation_statut"] = "À TRANSFERER"
df.loc[frad_dup, "conservation_statut"] = "À SUPPRIMER (DOUBLONS)"
df.loc[frad_reste, "conservation_statut"] = "INCONNU FRAD59"
# PDF : diffusion redondante (jpg présent pour les 29) -> à supprimer, pas de clé
is_pdf = ext == ".pdf"
assert int(is_pdf.sum()) == 29, f"pdf = {int(is_pdf.sum())}"
df.loc[is_pdf, "conservation_statut"] = "À SUPPRIMER (DIFFUSION)"
df.loc[is_pdf, "publication_statut"] = "non"
# doc : inchangé (jamais / À SUPPRIMER (FILE_TYPE))

# --- s3_key_cible --------------------------------------------------------
df["s3_key_cible"] = ""
avec_cible = (oui | groupe_a) & ~is_pdf            # 325 - 29 pdf = 296
df.loc[avec_cible, "s3_key_cible"] = "AMR/AMR_CAD/" + df.loc[avec_cible, "name"]

assert int((df["s3_key_cible"] != "").sum()) == 296, \
    f"s3_key_cible posées = {int((df['s3_key_cible'] != '').sum())}"
assert (df.loc[avec_cible, "s3_key_cible"].str.startswith("AMR/AMR_CAD/")).all()
assert (df.loc[is_pdf, "s3_key_cible"] == "").all(), "clé posée sur pdf"
# aucune clé sur les mis-de-côté / doublons / doc
assert (df.loc[frad_jpg | is_doc, "s3_key_cible"] == "").all(), "clé posée à tort"
# pas de collision de clé
posed = df.loc[df["s3_key_cible"] != "", "s3_key_cible"]
assert posed.is_unique, "collision s3_key_cible"

# --- restitution ---------------------------------------------------------
print("\npublication_statut:", df["publication_statut"].value_counts().to_dict())
print("conservation_statut:", df["conservation_statut"].value_counts().to_dict())
print("s3_key_cible posées:", int((df["s3_key_cible"] != "").sum()),
      "| vides:", int((df["s3_key_cible"] == "").sum()))

audit = df.loc[frad_jpg | groupe_a,
               ["name", "extension", "publication_statut",
                "conservation_statut", "s3_key_cible"]].copy()
audit.to_csv(AUDIT, index=False)
print("audit écrit:", AUDIT, audit.shape)

# date_maj_ligne (règle v2) : conservation/publication changés, ou s3_key_cible != s3_key source
df["date_maj_ligne"] = ""
_chg = (
    (df["conservation_statut"].values != orig["conservation_statut"].values)
    | (df["publication_statut"].values != orig["publication_statut"].values)
    | (df["s3_key_cible"].values != orig["s3_key"].values)
)
df.loc[_chg, "date_maj_ligne"] = TODAY
print("date_maj_ligne posée:", int(_chg.sum()), "| vide:", int((~_chg).sum()))

if "--apply" in sys.argv:
    df.to_csv(OUT, index=False)
    print("écrit:", OUT, df.shape)
else:
    print("DRY-RUN (ajouter --apply pour écrire", OUT + ")")
