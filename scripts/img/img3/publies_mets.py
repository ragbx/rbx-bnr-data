#!/usr/bin/env python3
r"""publies_mets.py - med_mets.py (JPEG de diffusion et METS) pour les corpus publies, y compris
les TIFF sans unitid.

Meme traitement que med_mets.py (et ses options, celles de presse_mets.py), sur un manifeste
de manifestes_publies.py : results/img/manifeste_corpus_publies_lot.csv.gz, ou
manifeste_corpus_publies_partielles.csv.gz (avec --overwrite). Ce qui change :
- tous les corpus du manifeste sont acceptes (CORPORA n'est plus consulte) ;
- le groupe d'un TIFF est lu dans les colonnes du manifeste : unitid_mets rempli -> la notice
  de l'IR, comme med_mets.py (regles notice et notice_rattachee) ; sinon document_mets, le
  document deduit du nom du TIFF (regles document et tiff), qui donne le nom du METS :
  <repertoire>/<document_mets>_mets.xml. Un document n'a pas de notice : son METS porte le
  titre du set OAI du corpus, ni cote, ni date, ni ARK ;
- avant de lancer, le METS de chaque groupe est compare a la colonne mets_key du manifeste
  (table corpus_publies_tiff_mets.csv) : tout ecart arrete le lot.

Usage :
  python publies_mets.py results/img/manifeste_corpus_publies_lot.csv.gz --source /mnt/bnr --out-dir /chemin/sortie --validate [--upload]
  python publies_mets.py --verifier results/img/manifeste_corpus_publies_lot.csv.gz   # groupes et noms seulement
"""

import posixpath
import sys
from collections import defaultdict
from os.path import join

import pandas as pd

import med_mets as mm
import presse_mets as pm

COLONNES = ["name", "path", "s3_key", "size", "checksum_md5", "uuid", "corpus_code", "conservation_statut",
            "mix_dateTimeCreated", "unitid", "unitid_mets", "document_mets", "mets_key"]
_mods_notice = mm.mods_notice


def load_extract(path: str) -> pd.DataFrame:
    """Manifeste de manifestes_publies.py : TIFF pourvus d'une s3_key et d'un groupe."""
    df = pd.read_csv(path, dtype=str, usecols=COLONNES, low_memory=False)
    df[["unitid_mets", "document_mets"]] = df[["unitid_mets", "document_mets"]].fillna("")
    df = df[df.s3_key.str.match(r".+\.tiff?$", case=False, na=False)
            & ((df.unitid_mets.str.strip() != "") | (df.document_mets != ""))]
    return df[~df.conservation_statut.str.startswith("À SUPPRIMER", na=False)]


def build_jobs(ref: pd.DataFrame, source: str, titles: dict):
    """Un job par groupe : notice (unitid_mets) comme med_mets.build_jobs, ou document
    (document_mets) sans notice."""
    groupes = defaultdict(list)
    for row in ref.itertuples(index=False):
        u = row.unitid_mets.strip()
        groupes[(row.corpus_code, "notice", u) if u else
                 (row.corpus_code, posixpath.dirname(row.s3_key), row.document_mets)].append(row)

    jobs, refused, anomalies, notices, prefixes = [], [], [], {}, {}
    for (corpus, genre, nom), rows in sorted(groupes.items()):
        est_notice = genre == "notice"
        fasc = mm.unit_id(corpus, nom) if est_notice else nom
        dirs = sorted({posixpath.dirname(r.s3_key) for r in rows})
        if len(dirs) > 1:
            refused.append((fasc, corpus, f"TIFF dans {len(dirs)} repertoires de s3_key ({', '.join(dirs)})"))
            continue
        if corpus not in titles:
            refused.append((fasc, corpus, f"titre inconnu pour {corpus} (pas de set OAI)"))
            continue
        prefix = f"{dirs[0]}/{fasc}"
        if prefix.upper() in prefixes:
            refused.append((fasc, corpus, f"meme METS que le groupe {prefixes[prefix.upper()]} ({prefix}_mets.xml)"))
            continue
        attendu = {r.mets_key for r in rows}
        if attendu != {f"{prefix}_mets.xml"}:
            refused.append((fasc, corpus, f"METS {prefix}_mets.xml, le manifeste attend {', '.join(sorted(attendu))}"))
            continue
        prefixes[prefix.upper()] = nom
        notice = {}
        if est_notice:
            if corpus not in notices:
                notices[corpus] = mm.load_notices(corpus, anomalies)
            notice = notices[corpus].get(nom)
            if notice is None:
                anomalies.append(f"{corpus} {nom} : notice absente de l'IR, METS sans intitule, date ni ARK")
                notice = {}

        pages, labels = {}, {}
        rows = sorted(rows, key=lambda r: mm.natural(r.s3_key))
        for seq, row in enumerate(rows, 1):
            page = f"{seq:04d}"
            rel = row.path.split("/") if isinstance(row.path, str) else []
            pages[page] = {"tif": {
                "key": row.s3_key, "size": int(float(row.size)), "md5": row.checksum_md5,
                "uuid": row.uuid if isinstance(row.uuid, str) else "", "local": join(source, *rel, row.name),
                "created_ref": row.mix_dateTimeCreated if isinstance(row.mix_dateTimeCreated, str) else ""}}
            labels[page] = mm.view_label(posixpath.splitext(posixpath.basename(row.s3_key))[0], fasc)
        jobs.append({"fasc": fasc, "corpus": corpus, "title": titles[corpus], "serie": corpus,
                     "cote": nom if est_notice else "", "notice": notice,
                     "arks": {r: notice.get(r, "") for r in ("current", "previous")},
                     "pages": pages, "labels": labels, "prefix": prefix})
    return jobs, refused, anomalies


def mods_notice(job: dict, n_pages: int):
    """MODS de med_mets.py ; un document sans notice n'a pas de cote."""
    root = _mods_notice(job, n_pages)
    if not job["cote"]:
        for ident in root.findall(pm.q("mods", "identifier")):
            if ident.get("displayLabel") == "cote":
                root.remove(ident)
    return root


mm.load_extract = load_extract
mm.build_jobs = build_jobs
mm.mods_notice = mods_notice


def verifier(manifeste: str) -> int:
    """Groupes, refus et anomalies du manifeste, sans rien convertir."""
    jobs, refused, anomalies = build_jobs(load_extract(manifeste), "/source", pm.load_titles(pm.SETS_PATH))
    print(f"{manifeste} : {len(jobs)} METS, {sum(len(j['pages']) for j in jobs)} vues, {len(refused)} groupes refuses")
    for fasc, corpus, msg in refused:
        print(f"  REFUS {corpus} {fasc} : {msg}")
    for a in anomalies:
        print(f"  ANOMALIE {a}")
    return 2 if refused else 0


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--verifier":
        sys.exit(verifier(sys.argv[2]))
    mm.main()
