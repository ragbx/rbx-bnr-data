"""
Reporte les sites nommés (data/geo/noms_points_sites_20261117.xlsx, onglet « à nommer »,
colonne « libellé retenu ») dans les tables tenues à la main du référentiel des lieux,
avant régénération par referentiel_lieux_roubaix.py (décision du 2026-11-18).

Une ligne du classeur = un site ; plusieurs lignes au même libellé retenu sont un même
lieu (fusions), qui garde l'identifiant le plus solide (BD TOPO, puis OSM, puis saisi).
Selon le point du site :
  - point BD TOPO « écarté » (validé « ne pas conserver ») : il revient au référentiel —
    validation « validé » dans referentiel_lieux_roubaix_ecartes.csv, libelle_manuel si le
    libellé retenu diffère du nom de la source ;
  - point déjà au référentiel (y compris par son nom) : libelle_manuel si le libellé
    retenu diffère ;
  - autre point (saisi, proposé, OSM) : nouveau lieu « bnr:<clé du libellé> », dans
    lieux_disparus.csv s'il est « disparu », sinon dans lieux_ajoutes.csv, avec l'état
    « actuel » ou « à classer » (colonne « actuel / disparu » vide).
Les variantes retenues vont dans lieux_ajoutes.csv (une ligne sans coord_wkt pour un lieu
existant ou disparu). Un site de RATTACHEMENTS est rattaché à un lieu existant d'un autre
thésaurus (Square Destombes → rue Square Pierre Destombes, 2026-11-18) : pas de point, son
libellé passe en variante. Les sites hors de Roubaix sont gardés (exception de Château Vaissier
du 2026-11-14, étendue le 2026-11-18). Relançable : les lignes de ces lieux sont remplacées.

À lancer depuis la racine du dépôt, puis régénérer le référentiel :
    python scripts/ead/sites_vers_referentiel.py
    python scripts/ead/referentiel_lieux_roubaix.py
"""
import csv
import sys
from collections import defaultdict
from datetime import date
from os.path import basename, dirname, exists, join

import openpyxl

sys.path.insert(0, dirname(__file__))
from referentiel_lieux_roubaix import (  # noqa: E402
    A_CLASSER, LIEUX_AJOUTES, LIEUX_DISPARUS, NE_PAS_CONSERVER, SORTIE as REFERENTIEL, SORTIE_ECARTES, cle,
)

GEO = join("data", "geo")
CLASSEUR = join(GEO, "noms_points_sites_20261117.xlsx")
SITES_CARTE = join(GEO, "sites_carte.csv")
COLS_AJOUTES = ["lieu_id", "libelle", "categorie", "etat", "coord_wkt", "variantes", "source_coord", "source",
                "remarque"]
# Sites rattachés à un lieu existant d'un autre thésaurus (libellé retenu → lieu_id) : le site
# n'y devient pas un point, son libellé y passe en variante.
RATTACHEMENTS = {
    # Décision du 2026-11-18 : un square qui a un tracé de voie est une rue ; le point saisi
    # « Square Destombes » doublait la rue Square Pierre Destombes (~100 m).
    "Square Destombes": "bdtopo:VOIE_NOM0000002339446803",
}
AUJOURDHUI = date.today().isoformat()
MENTION = f"site validé, nom retenu (noms_points_sites_20261117.xlsx), report du {AUJOURDHUI}"


def lire(fichier):
    with open(fichier, encoding="utf-8") as f:
        r = csv.DictReader(f)
        return r.fieldnames, list(r)


def ecrire(fichier, colonnes, lignes):
    with open(fichier, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=colonnes, lineterminator="\n")
        w.writeheader()
        w.writerows(lignes)


def ajouter_remarque(r, texte):
    if texte not in r["remarque"]:
        r["remarque"] = f"{r['remarque']} ; {texte}" if r["remarque"].strip() else texte


def type_point(s):
    return str(s or "").split(" (")[0]


def main():
    for fichier in (REFERENTIEL, SORTIE_ECARTES, LIEUX_AJOUTES, LIEUX_DISPARUS, CLASSEUR):
        verrou = join(dirname(fichier), f".~lock.{basename(fichier)}#")
        if exists(verrou):
            sys.exit(f"{fichier} est ouvert (verrou {verrou}) : le fermer avant de relancer.")

    ws = openpyxl.load_workbook(CLASSEUR, data_only=True)["à nommer"]
    entetes = [c.value for c in ws[1]]
    sites = [dict(zip(entetes, [c.value for c in r])) for r in ws.iter_rows(min_row=2)]
    sites = [s for s in sites if s["n°"] is not None and (s["libellé retenu"] or "").strip()]
    categories = {r["nom_propose"]: r["categorie"] for r in lire(SITES_CARTE)[1]}

    col_ref, ref = lire(REFERENTIEL)
    col_ec, ecartes = lire(SORTIE_ECARTES)
    col_aj, ajoutes = lire(LIEUX_AJOUTES)
    col_dis, disparus = lire(LIEUX_DISPARUS)
    col_aj = col_aj + [c for c in COLS_AJOUTES if c not in col_aj]
    ref_par_id, ec_par_id = {r["lieu_id"]: r for r in ref}, {r["lieu_id"]: r for r in ecartes}
    points_par_cle = defaultdict(list)
    for r in ref:
        if r["thesaurus"] == "points":
            points_par_cle[cle(r["libelle"])].append(r["lieu_id"])

    # Fusions : un lieu par libellé retenu, identifiant le plus solide en tête.
    rang = {"écarté": 0, "référentiel": 0, "OSM": 1}
    par_libelle = defaultdict(list)
    for s in sites:
        par_libelle[s["libellé retenu"].strip()].append(s)

    nouveaux_aj, nouveaux_dis, bilan = {}, {}, defaultdict(list)
    for libelle, groupe in par_libelle.items():
        groupe.sort(key=lambda s: (rang.get(type_point(s["point"]), 2), s["n°"]))
        s = groupe[0]
        variantes = sorted({v.strip() for g in groupe for v in (g["variantes retenues"] or "").split("|")
                            if v.strip() and v.strip() != libelle})
        etat_classeur = next((g["actuel / disparu"] for g in groupe if g["actuel / disparu"]), None)
        hors = any(g["quartier"] == "hors Roubaix" for g in groupe)
        remarques = " ; ".join(dict.fromkeys(g["remarques de validation"] for g in groupe if g["remarques de validation"]))
        lieu_id = s["identifiant"] or ""
        if libelle in RATTACHEMENTS:
            lieu_id = RATTACHEMENTS[libelle]
            r = ref_par_id[lieu_id]
            variantes = sorted(set(variantes) | {libelle} - {r["libelle"]})
            ajouter_remarque(r, f"site « {libelle} » rattaché à ce lieu (décision du 2026-11-18)")
            bilan["rattaché à un lieu existant"].append(f"{libelle} → {r['libelle']}")
            nouveaux_aj[lieu_id] = {
                "lieu_id": lieu_id, "libelle": r["libelle"], "categorie": "", "etat": "", "coord_wkt": "",
                "variantes": " | ".join(variantes), "source_coord": "", "source": "",
                "remarque": f"variantes seules : {MENTION}",
            }
            continue
        # Repérage par le nom d'un point déjà au référentiel, sauf celui que ce report a créé
        # (bnr:<clé>), qui reste un nouveau lieu et dont la ligne est réécrite.
        homonymes = [i for i in points_par_cle.get(cle(libelle), []) if i != f"bnr:{cle(libelle)}"]
        if not lieu_id and len(homonymes) == 1:
            lieu_id = homonymes[0]
        fusion = f" (fusion des n° {', '.join(str(g['n°']) for g in groupe)})" if len(groupe) > 1 else ""

        if lieu_id in ec_par_id:  # point BD TOPO écarté : retour au référentiel
            r = ec_par_id[lieu_id]
            if r["validation"].strip().lower() == NE_PAS_CONSERVER:
                r["validation"] = "validé"
            nom_source = r["nom_bdtopo"] or r["libelle"]
            r["libelle_manuel"] = "" if libelle == nom_source else libelle
            ajouter_remarque(r, f"réintégré : {MENTION}{fusion}")
            if hors:
                ajouter_remarque(r, "hors Roubaix : gardé (décision du 2026-11-18)")
            bilan["écarté BD TOPO réintégré"].append(libelle)
        elif lieu_id in ref_par_id:  # déjà au référentiel
            r = ref_par_id[lieu_id]
            nom_source = r["nom_bdtopo"] or r["nom_ban"] or ""
            courant = r["libelle_manuel"] or r["libelle"]
            if libelle != courant:
                r["libelle_manuel"] = "" if libelle == nom_source else libelle
                ajouter_remarque(r, f"libellé : {MENTION}")
                bilan["déjà au référentiel, renommé"].append(f"{courant} → {libelle}")
            else:
                bilan["déjà au référentiel"].append(libelle)
        else:  # nouveau lieu
            lieu_id = f"bnr:{cle(libelle)}"
            source_coord = " ".join(x for x in (type_point(s["point"]), s["identifiant"] or "") if x)
            nom_point = s["nom de la source du point"]
            if nom_point and nom_point != libelle:
                source_coord += f" (« {nom_point} »)"
            categorie = categories.get(s["libellé proposé"]) or libelle.split()[0]
            notes = "; ".join(x for x in (remarques, "hors Roubaix : gardé (décision du 2026-11-18)" if hors else "",
                                          MENTION + fusion) if x)
            source = f"BnR ({s['notices']} notices dans les IR)"
            if etat_classeur == "disparu":
                nouveaux_dis[lieu_id] = {
                    "lieu_id": lieu_id, "libelle": libelle, "categorie": categorie, "date_debut": "",
                    "date_fin": "", "remplacee_par": "", "trace_wkt": s["position"], "source_trace": source_coord,
                    "source": source, "remarque": notes,
                }
                bilan["nouveau, disparu"].append(libelle)
            else:
                etat = "actuel" if etat_classeur == "actuel" else A_CLASSER
                nouveaux_aj[lieu_id] = {
                    "lieu_id": lieu_id, "libelle": libelle, "categorie": categorie, "etat": etat,
                    "coord_wkt": s["position"], "variantes": " | ".join(variantes), "source_coord": source_coord,
                    "source": source, "remarque": notes,
                }
                bilan[f"nouveau, {etat}"].append(libelle)
                continue
        if variantes:  # variantes d'un lieu existant ou disparu : ligne sans position
            nouveaux_aj[lieu_id] = {
                "lieu_id": lieu_id, "libelle": libelle, "categorie": "", "etat": "", "coord_wkt": "",
                "variantes": " | ".join(variantes), "source_coord": "", "source": "",
                "remarque": f"variantes seules : {MENTION}",
            }

    doublons = [i for i in nouveaux_aj if i in nouveaux_dis and nouveaux_aj[i]["coord_wkt"]]
    if doublons:
        sys.exit(f"identifiants en double : {doublons}")
    # Points créés par un report précédent pour un site désormais rattaché : retirés.
    retires = {f"bnr:{cle(l)}" for l in RATTACHEMENTS}
    ajoutes = [r for r in ajoutes if r["lieu_id"] not in nouveaux_aj and r["lieu_id"] not in retires]
    ajoutes += list(nouveaux_aj.values())
    disparus = [r for r in disparus if r["lieu_id"] not in nouveaux_dis] + list(nouveaux_dis.values())
    ecrire(SORTIE_ECARTES, col_ec, ecartes)
    ecrire(REFERENTIEL, col_ref, ref)
    ecrire(LIEUX_AJOUTES, col_aj, [{c: r.get(c, "") for c in col_aj} for r in ajoutes])
    ecrire(LIEUX_DISPARUS, col_dis, disparus)

    print(f"{len(sites)} lignes du classeur → {len(par_libelle)} lieux")
    for k, v in bilan.items():
        print(f"  {k:32s} {len(v):3d}")
    print(f"  dont lignes de variantes seules : {sum(1 for r in nouveaux_aj.values() if not r['coord_wkt'])}")
    print("Relancer : python scripts/ead/referentiel_lieux_roubaix.py")


if __name__ == "__main__":
    main()
