"""Verse dans la couche historique du référentiel les rues du plan de 1889 validées « oui »
à la relecture QGIS (results/ead/indexation/plan_1889/rues_1889_relecture.csv, décision
utilisateur du 2026-11-20) :
  - « renommée » → data/geo/noms_anciens.csv (nom de 1889 rattaché à la voie actuelle) ;
  - « disparue » / « disparue en partie » → data/geo/lieux_disparus.csv, avec le tracé relevé
    sur le plan (rues_1889_traces.csv) ; une voie déjà au référentiel sous le même nom (voie
    TOPO sans géométrie) garde son lieu_id, sinon un lieu bnr:<nom> est créé ; une voie
    disparue sous l'avenue des Nations Unies est remplacée par celle-ci (59512_4005).
Idempotent : une ligne déjà présente (même lieu_id et même ancien nom, même lieu disparu)
n'est pas redoublée ; un lieu disparu déjà présent sans tracé reçoit celui de 1889.
Régénérer ensuite le référentiel (scripts/ead/referentiel_lieux_roubaix.py).
"""
import csv
import re
import unicodedata
from pathlib import Path

from shapely import wkt
from shapely.geometry import MultiLineString

D = Path("results/ead/indexation/plan_1889")
GEO = Path("data/geo")
RELECTURE = D / "rues_1889_relecture.csv"
TRACES = D / "rues_1889_traces.csv"
REFERENTIEL = GEO / "referentiel_lieux_roubaix.csv"
NOMS_ANCIENS = GEO / "noms_anciens.csv"
LIEUX_DISPARUS = GEO / "lieux_disparus.csv"
NATIONS_UNIES = ("Avenue des Nations Unies", "59512_4005")
SOURCE = "plan de Roubaix de 1889 (AMR 6Fi022)"
RELU = "relecture QGIS du plan de 1889, décision utilisateur du 2026-11-20"


def lire(chemin):
    with open(chemin, encoding="utf-8") as f:
        lecteur = csv.DictReader(f)
        return lecteur.fieldnames, list(lecteur)


def ecrire(chemin, colonnes, lignes):
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=colonnes, lineterminator="\n")
        w.writeheader()
        w.writerows(lignes)


def cle(nom):
    """Nom sans accents, sans « de / du / des / de la / d' » après le type de voie."""
    s = unicodedata.normalize("NFKD", nom).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[-'’]", " ", s)
    return re.sub(r"^(\w+) (de la |de l |des |du |de |d )", r"\1 ", s).split()


def slug(nom):
    return "bnr:" + " ".join(re.sub(r"[^a-z0-9 ]", " ", " ".join(cle(nom))).split())


def main():
    _, relecture = lire(RELECTURE)
    _, traces = lire(TRACES)
    _, referentiel = lire(REFERENTIEL)
    col_na, noms_anciens = lire(NOMS_ANCIENS)
    col_ld, disparus = lire(LIEUX_DISPARUS)

    traces_par_nom = {}
    for t in traces:
        if t["coord_wkt"]:
            traces_par_nom.setdefault(t["nom_1889"], []).append(wkt.loads(t["coord_wkt"]))
    rues = {tuple(cle(r["libelle"])): r for r in referentiel if r["thesaurus"] == "rues"}
    deja_na = {(n["lieu_id"], n["nom_ancien"]) for n in noms_anciens}
    disparus_par_id = {d["lieu_id"]: d for d in disparus}
    bilan = {"noms anciens ajoutés": [], "noms anciens déjà là": [], "disparus ajoutés": [],
             "disparus déjà là (tracé ajouté)": [], "disparus déjà là": []}

    vus = set()
    for r in relecture:
        if r["decision"] != "oui" or r["nom_1889"] in vus:
            continue
        vus.add(r["nom_1889"])
        nom = r["nom_corrige"] or r["nom_1889"]
        remarque = RELU + (f" ; {r['remarque_relecture']}" if r["remarque_relecture"] else "")
        if r["constat"] == "renommée":
            if (r["lieu_id_actuel"], nom) in deja_na:
                bilan["noms anciens déjà là"].append(nom)
                continue
            noms_anciens.append({"lieu_id": r["lieu_id_actuel"], "libelle": r["libelle_actuel"], "nom_ancien": nom,
                                 "date_debut": "", "date_fin": "", "source": SOURCE, "remarque": remarque})
            bilan["noms anciens ajoutés"].append(f"{nom} → {r['libelle_actuel']}")
            continue
        # disparue / disparue en partie
        geoms = []
        for g in traces_par_nom.get(r["nom_1889"], []):
            geoms.extend(g.geoms if g.geom_type == "MultiLineString" else [g])
        trace = (geoms[0] if len(geoms) == 1 else MultiLineString(geoms)).wkt if geoms else ""
        existant = rues.get(tuple(cle(nom)))
        lieu_id = existant["lieu_id"] if existant and "tronçon" not in nom else slug(nom)
        libelle = existant["libelle"] if existant and "tronçon" not in nom else nom
        if libelle != nom:
            remarque += f" ; « {nom} » sur le plan"
        remplacee = NATIONS_UNIES[1] if r["emprise_actuelle"] == NATIONS_UNIES[0] else ""
        if lieu_id in disparus_par_id:
            d = disparus_par_id[lieu_id]
            if not d["trace_wkt"] and trace:
                d["trace_wkt"], d["source_trace"] = trace, f"relevé sur le {SOURCE}"
                d["remarque"] = (d["remarque"] + " ; " if d["remarque"] else "") + f"tracé : {RELU}"
                bilan["disparus déjà là (tracé ajouté)"].append(libelle)
            else:
                bilan["disparus déjà là"].append(libelle)
            continue
        d = {"lieu_id": lieu_id, "libelle": libelle, "categorie": nom.split()[0].lower(), "date_debut": "",
             "date_fin": "", "remplacee_par": remplacee, "trace_wkt": trace,
             "source_trace": f"relevé sur le {SOURCE}" if trace else "", "source": SOURCE, "remarque": remarque}
        disparus.append(d)
        disparus_par_id[lieu_id] = d
        bilan["disparus ajoutés"].append(f"{libelle} ({lieu_id})")

    ecrire(NOMS_ANCIENS, col_na, noms_anciens)
    ecrire(LIEUX_DISPARUS, col_ld, disparus)
    for k, v in bilan.items():
        print(f"{k} : {len(v)}")
        for x in v:
            print("   ", x)


if __name__ == "__main__":
    main()
