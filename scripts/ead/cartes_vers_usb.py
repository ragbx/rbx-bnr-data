"""Copie les images des cartes et plans de la BN-R depuis Azraël (Z:) — et S3 au besoin — vers une clé USB.

Liste des documents : results/ead/indexation/cartes_anciennes_bnr.xlsx, onglet « Cartes et
plans » (produit par cartes_anciennes.py ; colonnes IR, cote BN-R, titre, année, plan de ville).
Fichiers : référentiel results/ref/_ref_files_*.csv.gz le plus récent, apparié par
finding_aid (bnr_<IR>.xml) + unitid (cote BN-R) ; chemin sur Azraël = <source>/<path>/<name>.

Par défaut, seuls les fichiers de conservation (conservation_statut = TRANSFERT_S3_OK) sont
copiés, en JPEG et TIFF ; les copies de diffusion « À SUPPRIMER (DIFFUSION) » sont écartées.

Chemin sur Azraël : un fichier peut figurer plusieurs fois dans le référentiel (copie en
_CORBEILLE) ; on garde le premier chemin qui existe, hors _CORBEILLE d'abord ; sont aussi
essayés le chemin sans « _CORBEILLE/BNR_VERIF/ » et les fichiers du même nom (ou du même nom
sans année intercalée, cadastre de 1819) que le référentiel situe sur Azraël.

--s3 : les fichiers absents d'Azraël (TIFF entrés par le disque externe AMR_CADN_165-0500 :
cadastre de 1819, plans 6 Fi) sont téléchargés depuis S3 (s3_bucket / s3_key du référentiel),
avec le compte en lecture seule user_ro de scripts/s3/conf.yml.

Rangement sur la clé :
    <destination>/<IR court>/<année>_<cote BN-R>_<titre>/<fichier>
    <destination>/inventaire_cartes.csv   (une ligne par fichier : cote, titre, origine, statut…)

Reprise : un fichier déjà présent avec la bonne taille n'est pas recopié (un téléchargement
interrompu reste en « .partiel » et est repris de zéro). Contrôle : taille après copie ;
--md5 compare en plus au checksum_md5 du référentiel (ou au MD5 de la source sur Azraël).

Exemples :
    python scripts/ead/cartes_vers_usb.py E:/cartes_bnr --simulation
    python scripts/ead/cartes_vers_usb.py E:/cartes_bnr --md5                 # Azraël seul
    python scripts/ead/cartes_vers_usb.py E:/cartes_bnr --md5 --s3            # + les TIFF de S3
    python scripts/ead/cartes_vers_usb.py E:/cartes_bnr --format tif --selection plans-de-ville
"""
import argparse
import csv
import glob
import hashlib
import os
import re
import shutil
import sys
import time
import unicodedata
from os import makedirs
from os.path import basename, dirname, exists, getsize, join, realpath

import pandas as pd

RACINE = dirname(dirname(dirname(realpath(__file__))))
LISTE = join(RACINE, "results/ead/indexation/cartes_anciennes_bnr.xlsx")
SOURCE = "Z:/"  # \\srvbnr.ntrbx.local\BNR
TYPES = {"jpg": ["jpeg"], "tif": ["tiff"], "tous": ["jpeg", "tiff"]}
FAT32_MAX = 4 * 1024 ** 3 - 1
COLONNES = ["name", "path", "size", "checksum_md5", "file_type", "finding_aid", "unitid",
            "source2s3", "conservation_statut", "s3_key", "s3_bucket"]


def dernier_ref():
    refs = sorted(f for f in glob.glob(join(RACINE, "results/ref/_ref_files_*.csv.gz"))
                  if re.search(r"_ref_files_\d{8}\.csv\.gz$", f))
    if not refs:
        sys.exit("aucun référentiel results/ref/_ref_files_AAAAMMJJ.csv.gz")
    return refs[-1]


def lire_ref(chemin, types, statut, finding_aids):
    """Lignes utiles du référentiel, lu par morceaux (il compte près de 3 millions de lignes) :
    fichiers des IR retenus, et fichiers sur Azraël (pour les équivalents par nom)."""
    garde = []
    for m in pd.read_csv(chemin, dtype=str, usecols=COLONNES, chunksize=250_000):
        m = m[m["file_type"].isin(types)]
        if statut != "tous":
            m = m[m["conservation_statut"] == statut]
        garde.append(m[m["finding_aid"].isin(finding_aids) | (m["source2s3"] == "az")])
    return pd.concat(garde, ignore_index=True)


def propre(texte, longueur=60):
    """Texte → nom de dossier sûr (FAT32/exFAT/NTFS) : sans accents ni caractères interdits."""
    t = unicodedata.normalize("NFD", str(texte))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', " ", t)
    t = re.sub(r"\s+", " ", t).strip(" .")
    return t[:longueur].rstrip(" .") or "sans_titre"


def cle_nom(nom):
    """Nom sans année intercalée : RBX_AMR_PLA_1819_1G1001_01.jpg → RBX_AMR_PLA_1G1001_01.jpg
    (cadastre de 1819 : nom sur Azraël ≠ nom du référentiel apparié à l'IR)."""
    return re.sub(r"_1[6-9]\d\d_", "_", nom)


def chemin_source(source, lignes, sur_az):
    """Lignes du référentiel d'un même fichier → premier chemin existant sur Azraël, ou None."""
    nom = lignes.iloc[0]["name"]
    chemins = [(str(p), nom) for p in lignes["path"]] + sur_az.get(nom, []) + sur_az.get(cle_nom(nom), [])
    candidats = []
    for p, n in sorted(dict.fromkeys(chemins), key=lambda x: "_CORBEILLE" in x[0]):
        p = p.replace("\\", "/")
        candidats.append((p, n))
        if "_CORBEILLE/BNR_VERIF/" in p:
            candidats.append((p.replace("_CORBEILLE/BNR_VERIF/", ""), n))
    for p, n in candidats:
        f = join(source, p, n)
        if exists(f):
            return f
    return None


def objet_s3(lignes):
    """(bucket, clé) S3 du fichier d'après le référentiel, ou None."""
    for _, r in lignes.iterrows():
        if isinstance(r["s3_key"], str) and r["s3_key"] and isinstance(r["s3_bucket"], str) and r["s3_bucket"]:
            return r["s3_bucket"], r["s3_key"]
    return None


def md5(fichier):
    h = hashlib.md5()
    with open(fichier, "rb") as f:
        while bloc := f.read(1024 * 1024):
            h.update(bloc)
    return h.hexdigest()


def go(octets):
    return f"{octets / 1e9:.1f} Go"


def client_s3():
    sys.path.insert(0, join(RACINE, "scripts/s3"))
    from rbx_s3 import Rbx_client  # noqa: E402  (scripts/s3/conf.yml, hors dépôt)
    return Rbx_client(user="user_ro").s3_client


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("destination", help="dossier sur la clé USB, ex. E:/cartes_bnr")
    p.add_argument("--format", choices=TYPES, default="tous", help="tous (défaut), jpg ou tif")
    p.add_argument("--statut", default="TRANSFERT_S3_OK",
                   help="conservation_statut retenu (défaut TRANSFERT_S3_OK : fichiers de conservation ; "
                        "« tous » pour ne pas filtrer, copies de diffusion comprises)")
    p.add_argument("--selection", choices=["tout", "plans-de-ville"], default="tout")
    p.add_argument("--s3", action="store_true", help="télécharger depuis S3 les fichiers absents d'Azraël")
    p.add_argument("--source", default=SOURCE, help="racine d'Azraël (défaut Z:/)")
    p.add_argument("--liste", default=LISTE)
    p.add_argument("--ref", default=None, help="référentiel (défaut : le plus récent)")
    p.add_argument("--simulation", action="store_true", help="n'écrit rien : volume, place libre, fichiers manquants")
    p.add_argument("--md5", action="store_true", help="vérifie chaque copie par MD5 (plus lent)")
    p.add_argument("--limite", type=int, default=0, help="ne traiter que les N premiers fichiers (essai)")
    p.add_argument("--cote", action="append", default=[], help="ne traiter que cette cote BN-R (répétable, essai)")
    a = p.parse_args()

    cartes = pd.read_excel(a.liste, sheet_name="Cartes et plans")
    if a.selection == "plans-de-ville":
        cartes = cartes[cartes["plan de ville"] == "oui"]
    if a.cote:
        cartes = cartes[cartes["cote BN-R"].isin(a.cote)]
    cartes = cartes.assign(finding_aid="bnr_" + cartes["IR"] + ".xml")

    ref_chemin = a.ref or dernier_ref()
    ref = lire_ref(ref_chemin, TYPES[a.format], a.statut, set(cartes["finding_aid"]))
    az = ref[ref["source2s3"] == "az"]
    sur_az = {}  # nom de fichier, ou nom sans année (cle_nom) → [(chemin, nom réel)]
    for chemin, nom in zip(az["path"].astype(str), az["name"].astype(str)):
        sur_az.setdefault(nom, []).append((chemin, nom))
        if cle_nom(nom) != nom:
            sur_az.setdefault(cle_nom(nom), []).append((chemin, nom))
    fichiers = ref.merge(cartes[["finding_aid", "cote BN-R", "IR", "titre", "date", "année"]],
                         left_on=["finding_aid", "unitid"], right_on=["finding_aid", "cote BN-R"])
    print(f"référentiel : {basename(ref_chemin)} ; documents retenus : {len(cartes)} ; format : {a.format} ; "
          f"statut : {a.statut} ; sélection : {a.selection} ; S3 : {'oui' if a.s3 else 'non'}")

    # un fichier = (cote, nom) ; plusieurs lignes possibles (copies en _CORBEILLE)
    plan = []
    for (cote, nom), lignes in fichiers.groupby(["cote BN-R", "name"], sort=False):
        r = lignes.iloc[0]
        annee = str(int(float(r["année"]))) if pd.notna(r["année"]) else "sd"
        dossier = join(a.destination, r["IR"].replace("FR595129901_", ""), propre(f"{annee}_{cote}_{r['titre']}"))
        source = chemin_source(a.source, lignes, sur_az)
        s3 = objet_s3(lignes)
        taille_ref = int(float(r["size"])) if pd.notna(r["size"]) else 0
        md5_ref = r["checksum_md5"] if isinstance(r["checksum_md5"], str) else ""
        if source:
            origine = "Azraël"
            taille = getsize(source)  # la copie équivalente peut différer du référentiel
            md5_ref = md5_ref if basename(source) == nom else ""  # MD5 du référentiel : fichier même seulement
        elif a.s3 and s3:
            origine, taille = "S3", taille_ref
        else:
            origine, taille = "", taille_ref
        plan.append({"cote BN-R": cote, "titre": r["titre"], "date": r["date"], "IR": r["IR"], "fichier": nom,
                     "taille": taille, "md5 référentiel": md5_ref, "origine": origine, "source": source,
                     "s3": s3, "destination": join(dossier, nom)})
    plan.sort(key=lambda x: x["destination"])
    sans_fichier = sorted(set(cartes["cote BN-R"]) - set(f["cote BN-R"] for f in plan))
    if a.limite:
        plan = plan[:a.limite]

    manquants = [f for f in plan if not f["origine"]]
    deja = [f for f in plan if exists(f["destination"]) and getsize(f["destination"]) == f["taille"]]
    a_copier = [f for f in plan if f["origine"] and f not in deja]
    besoin = sum(f["taille"] for f in a_copier)
    par_origine = {o: [f for f in a_copier if f["origine"] == o] for o in ("Azraël", "S3")}
    print(f"fichiers : {len(plan)} ({go(sum(f['taille'] for f in plan))}) ; déjà sur la clé : {len(deja)} ; "
          f"à copier : {len(a_copier)} ({go(besoin)}) dont Azraël {len(par_origine['Azraël'])} "
          f"({go(sum(f['taille'] for f in par_origine['Azraël']))}), S3 {len(par_origine['S3'])} "
          f"({go(sum(f['taille'] for f in par_origine['S3']))}) ; manquants : {len(manquants)}"
          + ("" if a.s3 else f" (dont {sum(1 for f in manquants if f['s3'])} sur S3 : option --s3)"))
    print(f"documents sans fichier {a.format} dans le référentiel : {len(sans_fichier)}"
          + (f" ({', '.join(sans_fichier[:10])}{'…' if len(sans_fichier) > 10 else ''})" if sans_fichier else ""))
    gros = [f for f in a_copier if f["taille"] > FAT32_MAX]
    if gros:
        print(f"ATTENTION : {len(gros)} fichier(s) de plus de 4 Go : clé à formater en exFAT ou NTFS (pas FAT32)")

    racine_dest = a.destination
    while not exists(racine_dest) and dirname(racine_dest) != racine_dest:
        racine_dest = dirname(racine_dest)
    libre = shutil.disk_usage(racine_dest).free if exists(racine_dest) else 0
    print(f"place libre sur {racine_dest} : {go(libre)}")
    if a.simulation:
        for f in manquants[:20]:
            print("  manquant :", f["cote BN-R"], f["fichier"], "(sur S3)" if f["s3"] else "")
        return
    if besoin > libre:
        sys.exit(f"place insuffisante : il faut {go(besoin)}, libre {go(libre)}. "
                 "Choisir --format jpg ou --selection plans-de-ville, ou une clé plus grande.")

    client = client_s3() if par_origine["S3"] else None
    debut, copie = time.time(), 0
    for i, f in enumerate(plan, 1):
        if not f["origine"]:
            f["statut"] = "manquant (sur S3 : option --s3)" if f["s3"] else "introuvable"
            continue
        if f in deja:
            f["statut"] = "déjà présent"
            continue
        makedirs(dirname(f["destination"]), exist_ok=True)
        try:
            if f["origine"] == "S3":
                partiel = f["destination"] + ".partiel"
                client.download_file(f["s3"][0], f["s3"][1], partiel)
                os.replace(partiel, f["destination"])
            else:
                shutil.copyfile(f["source"], f["destination"])
            ok, controle = getsize(f["destination"]) == f["taille"], "taille"
            if ok and a.md5:
                attendu = f["md5 référentiel"] or (md5(f["source"]) if f["source"] else "")
                if attendu:
                    ok, controle = md5(f["destination"]) == attendu, "MD5"
            f["statut"] = f"copié (contrôle {controle})" if ok else f"ERREUR : contrôle {controle}"
        except Exception as e:  # réseau, S3, disque : on note et on continue
            f["statut"] = f"ERREUR : {e}"
        copie += f["taille"]
        vitesse = copie / max(time.time() - debut, 1)
        reste = (besoin - copie) / vitesse if vitesse else 0
        print(f"[{i}/{len(plan)}] {f['origine']} : {f['statut']} {f['fichier']}  "
              f"({go(copie)} / {go(besoin)}, reste ≈ {reste / 60:.0f} min)")

    inventaire = join(a.destination, "inventaire_cartes.csv")
    makedirs(a.destination, exist_ok=True)
    with open(inventaire, "w", encoding="utf-8-sig", newline="") as sortie:
        champs = ["cote BN-R", "titre", "date", "IR", "fichier", "taille", "md5 référentiel", "origine",
                  "statut", "destination"]
        w = csv.DictWriter(sortie, fieldnames=champs, delimiter=";", extrasaction="ignore")
        w.writeheader()
        for f in plan:
            w.writerow({**f, "destination": f["destination"].replace(a.destination, "").lstrip("/\\")})
    erreurs = [f for f in plan if str(f.get("statut", "")).startswith("ERREUR")]
    print(f"terminé : {sum(str(f.get('statut', '')).startswith('copié') for f in plan)} copiés, "
          f"{len(deja)} déjà présents, {len(manquants)} manquants, {len(erreurs)} erreurs ; inventaire : {inventaire}")


if __name__ == "__main__":
    main()
