"""Cohérence entre les points des sites et l'indexation des quartiers et des rues dans les IR (2026-11-17).

    conda run -n rbx-bnr-data python scripts/ead/coherence_sites_indexation.py

Lecture seule. Pour chaque notice reliée à un site de la carte (results/ead/indexation/sites_carte_notices.csv,
points de data/geo/sites_carte.csv) qui indexe aussi des quartiers ou des rues (geogname des IR réécrits,
results/ead/indexation/lieux_carte_notices.csv) :

  - quartier : le point est-il dans l'un des quartiers indexés (tolérance TOLERANCE_QUARTIER_M) ? Les points
    hors des quartiers de Roubaix ne sont pas contrôlés ;
  - rue : distance du point à la rue indexée la plus proche ; « proche » ≤ RUE_PROCHE_M, « éloignée » jusqu'à
    RUE_LOIN_M, au-delà « loin ».

Une incohérence peut venir du point (mal placé), du lien notice ↔ site (homonyme, simple mention) ou de
l'indexation de la notice (quartier ou rue erronés) : rien n'est tranché ici.

Sortie : results/ead/indexation/coherence_sites_indexation_<date>.xlsx
  - « par site » : nombre de notices contrôlées, de quartiers incohérents, de rues loin ;
  - « quartiers » : notices dont aucun quartier indexé ne contient le point ;
  - « rues » : notices dont toutes les rues indexées sont à plus de RUE_PROCHE_M du point.
"""
import csv
import sys
from collections import defaultdict
from datetime import date
from os.path import dirname, join

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from shapely.geometry import Point
from shapely.ops import transform

sys.path.insert(0, dirname(__file__))
from sites_candidats import geometries_thesaurus  # noqa: E402
from referentiel_lieux_roubaix import GEO, VERS_L93  # noqa: E402

RACINE = dirname(dirname(dirname(__file__)))
SITES = join(GEO, "sites_carte.csv")
SITES_NOTICES = join(RACINE, "results/ead/indexation/sites_carte_notices.csv")
LIEUX_NOTICES = join(RACINE, "results/ead/indexation/lieux_carte_notices.csv")
SORTIE = join(RACINE, f"results/ead/indexation/coherence_sites_indexation_{date.today():%Y%m%d}.xlsx")
TOLERANCE_QUARTIER_M = 30
RUE_PROCHE_M = 150
RUE_LOIN_M = 400


def lire(f):
    with open(f, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def main():
    sites = {r["point_id"]: r for r in lire(SITES) if r["afficher"] != "non"}
    pts = {k: transform(VERS_L93, Point(float(r["lon"]), float(r["lat"]))) for k, r in sites.items()}
    nom = {k: r["nom"] or r["nom_propose"] for k, r in sites.items()}
    quartiers, rues = geometries_thesaurus("quartiers"), geometries_thesaurus("rues")
    index = defaultdict(lambda: {"quartiers": set(), "rues": set()})
    titres = {}
    for r in lire(LIEUX_NOTICES):
        if r["thesaurus"] in ("quartiers", "rues"):
            index[(r["ir"], r["c_id"])][r["thesaurus"]].add(r["libelle"])
    inconnus = set()
    controle = defaultdict(lambda: {"notices": 0, "q_ok": 0, "q_ko": 0, "r_ok": 0, "r_eloignee": 0, "r_loin": 0})
    lignes_q, lignes_r = [], []
    for n in lire(SITES_NOTICES):
        k, cle = n["point_id"], (n["ir"], n["c_id"])
        if k not in sites or cle not in index:
            continue
        titres[cle] = n
        p, ix, c = pts[k], index[cle], controle[k]
        c["notices"] += 1
        commun = {"site": nom[k], "quartier du point": sites[k]["quartier"] or "hors Roubaix", "ir": n["ir"],
                  "date": n["date"], "titre": n["titre"], "ark": n["ark"], "c_id": n["c_id"]}
        qs = [q for q in ix["quartiers"] if q in quartiers]
        inconnus |= {q for q in ix["quartiers"] if q not in quartiers} | {r for r in ix["rues"] if r not in rues}
        if qs and sites[k]["quartier"]:
            if any(quartiers[q].distance(p) <= TOLERANCE_QUARTIER_M for q in qs):
                c["q_ok"] += 1
            else:
                c["q_ko"] += 1
                lignes_q.append({**commun, "quartiers indexés": " | ".join(sorted(qs)),
                                 "distance (m)": round(min(quartiers[q].distance(p) for q in qs)),
                                 "rues indexées": " | ".join(sorted(ix["rues"]))})
        rs = [r for r in ix["rues"] if r in rues]
        if rs:
            d, r_proche = min((rues[r].distance(p), r) for r in rs)
            if d <= RUE_PROCHE_M:
                c["r_ok"] += 1
            else:
                c["r_eloignee" if d <= RUE_LOIN_M else "r_loin"] += 1
                lignes_r.append({**commun, "rue la plus proche": r_proche, "distance (m)": round(d),
                                 "écart": "éloignée" if d <= RUE_LOIN_M else "loin",
                                 "rues indexées": " | ".join(sorted(rs)), "quartiers indexés": " | ".join(sorted(qs))})

    wb = Workbook()
    feuilles = [
        ("par site", ["site", "quartier du point", "notices contrôlées", "quartier cohérent", "quartier incohérent",
                      "rue proche", f"rue éloignée ({RUE_PROCHE_M}-{RUE_LOIN_M} m)", f"rue loin (> {RUE_LOIN_M} m)"],
         sorted(({"site": nom[k], "quartier du point": sites[k]["quartier"] or "hors Roubaix",
                  "notices contrôlées": c["notices"], "quartier cohérent": c["q_ok"], "quartier incohérent": c["q_ko"],
                  "rue proche": c["r_ok"], f"rue éloignée ({RUE_PROCHE_M}-{RUE_LOIN_M} m)": c["r_eloignee"],
                  f"rue loin (> {RUE_LOIN_M} m)": c["r_loin"]} for k, c in controle.items()),
                key=lambda r: -(r["quartier incohérent"] + r[f"rue loin (> {RUE_LOIN_M} m)"]))),
        ("quartiers", ["site", "quartier du point", "quartiers indexés", "distance (m)", "rues indexées", "ir", "date",
                       "titre", "ark", "c_id"], sorted(lignes_q, key=lambda r: (r["site"], -r["distance (m)"]))),
        ("rues", ["site", "quartier du point", "écart", "rue la plus proche", "distance (m)", "rues indexées",
                  "quartiers indexés", "ir", "date", "titre", "ark", "c_id"],
         sorted(lignes_r, key=lambda r: (r["site"], -r["distance (m)"]))),
    ]
    for i, (titre, cols, rangs) in enumerate(feuilles):
        ws = wb.active if i == 0 else wb.create_sheet()
        ws.title = titre
        ws.append(cols)
        for r in rangs:
            ws.append([r.get(c, "") for c in cols])
            for j, c in enumerate(cols, 1):
                if c == "ark" and r.get(c):
                    ws.cell(ws.max_row, j).hyperlink = r[c]
                    ws.cell(ws.max_row, j).style = "Hyperlink"
        for j, c in enumerate(cols, 1):
            ws.cell(1, j).font = Font(bold=True)
            ws.column_dimensions[ws.cell(1, j).column_letter].width = {"titre": 50, "ark": 40, "rues indexées": 40,
                                                                          "site": 34}.get(c, 18)
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(wrap_text=True, vertical="top")
        ws.freeze_panes = "B2"
        ws.auto_filter.ref = ws.dimensions
    wb.save(SORTIE)
    tot = {x: sum(c[x] for c in controle.values()) for x in ("notices", "q_ok", "q_ko", "r_ok", "r_eloignee", "r_loin")}
    print(f"{tot['notices']} notices × site contrôlées ({len(controle)} sites) → {SORTIE}")
    print(f"  quartiers : {tot['q_ok']} cohérents, {tot['q_ko']} incohérents")
    print(f"  rues : {tot['r_ok']} proches, {tot['r_eloignee']} éloignées, {tot['r_loin']} loin")
    if inconnus:
        print(f"  libellés indexés absents du référentiel : {len(inconnus)} ({', '.join(sorted(inconnus)[:8])}…)")


if __name__ == "__main__":
    main()
