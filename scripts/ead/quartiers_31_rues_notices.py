"""
Indexation par quartier, à 31 quartiers : quartiers possibles des rues, et
notices qu'on peut déjà rattacher à un quartier sans ambiguïté.

Le découpage à 31 quartiers est retenu pour le moment (2026-11-14).

Quartiers d'une rue (à 31) :
  - rue sûre (95 % ou plus de sa longueur dans un quartier) : ce quartier ;
  - rue ambiguë : retenu_31 de data/geo/rues_rattachement.csv (saisie
    validée ; à défaut, quartiers traversés à 5 % ou plus).

Quartier d'une notice, dans l'ordre de priorité décidé le 2026-11-13 :
  1. point : site validé lié à la notice (propositions_sites_20261112),
     point du référentiel indexé (bnr_geo_rbx_sites) ou adresse « n° + voie »
     géocodée sur une rue indexée → quartier(s) du ou des points, toujours ;
  2. rues : toutes les rues indexées ont un seul quartier → ces quartiers ;
  3. croisement : les rues indexées n'ont qu'un quartier en commun → celui-là ;
  4. sinon, ambiguïté à lever.
Une notice qui cite plusieurs lieux sans ambiguïté peut avoir plusieurs
quartiers : ce n'est pas une ambiguïté.

Analyse dérivée : ni le référentiel, ni la table de rattachement, ni les IR
ne sont modifiés.

Sorties (results/ead/indexation/) :
  - rues_quartiers_31.xlsx : une ligne par couple rue / quartier possible ;
  - notices_quartiers_31.xlsx : onglets sans_ambiguite, a_lever, synthese.

À lancer depuis la racine du dépôt, après rues_rattachement.py :
    python scripts/ead/quartiers_31_rues_notices.py
"""
import sys
from collections import Counter, defaultdict
from glob import glob
from os.path import basename, dirname, join

from lxml import etree
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

sys.path.insert(0, dirname(__file__))
from rues_rattachement import (DECISIONS_SITES, IR, LIENS_SITES, REPARTITION, SEUIL_SURE, SITES,  # noqa: E402
                               SITES_NOTICES, SORTIE as RATTACHEMENT, candidats, charger, lire_csv,
                               quartiers_adresses, texte)

IR_ACTUELS = join("results", "ead", "ead_cor", "bnr2mnesys")   # IR tels qu'importés dans Mnesys
SORTIE_DIR = join("results", "ead", "indexation")
SORTIE_RUES = join(SORTIE_DIR, "rues_quartiers_31.xlsx")
SORTIE_NOTICES = join(SORTIE_DIR, "notices_quartiers_31.xlsx")


def quartiers_rues():
    """{rue: [quartiers à 31]} pour les rues de Roubaix ayant un tracé réparti par quartier."""
    retenu = {r["libelle"]: [q for q in r["retenu_31"].split(" | ") if q] for r in lire_csv(RATTACHEMENT)}
    out = {}
    for r in lire_csv(REPARTITION):
        if not r["q31_repartition"]:
            continue
        lib = r["libelle"]
        if float(r["q31_pct_principal"]) >= SEUIL_SURE:
            out[lib] = [r["q31_principal"]]
        else:
            out[lib] = retenu.get(lib) or candidats(r["q31_repartition"])
    return out


def indexation_actuelle():
    """{(ir, c_id): {"quartier": [...], "rue": [...]}} : geogname des IR actuels (bnr2mnesys), tels quels."""
    out = {}
    for f in sorted(glob(join(IR_ACTUELS, "*.xml"))):
        ir = basename(f).replace("FR595126101_", "").replace(".xml", "")
        for c in etree.parse(f).iter("c"):
            d = {"quartier": [], "rue": []}
            for g in c.findall("controlaccess/geogname"):
                if g.get("source") in d:
                    d[g.get("source")].append(texte(g))
            out[(ir, c.get("id"))] = {k: list(dict.fromkeys(v)) for k, v in d.items()}
    return out


def ark_previous(c):
    """Lien ARK de la notice : dao ou daogrp/daoloc de rôle publication:previous, propre au <c>."""
    for d in c.findall("dao") + c.findall("daogrp/daoloc"):
        if d.get("role") == "publication:previous" and "ark:/" in (d.get("href") or ""):
            return d.get("href")
    return ""


def ecrire(wb, titre, entetes, lignes, premier=False):
    ws = wb.active if premier else wb.create_sheet()
    ws.title = titre
    ws.append(entetes)
    for c in ws[1]:
        c.font = Font(bold=True)
    for li in lignes:
        ws.append([li.get(e, "") for e in entetes])
        if li.get("ark"):
            cell = ws.cell(ws.max_row, entetes.index("ark") + 1)
            cell.hyperlink, cell.style = li["ark"], "Hyperlink"
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    for i, e in enumerate(entetes, 1):
        larg = max([len(str(e))] + [len(str(li.get(e, ""))) for li in lignes[:2000]])
        ws.column_dimensions[get_column_letter(i)].width = min(max(10, larg + 2), 70)


def main():
    rues, traces, q31, points, du13, cles_rue, ban, _ = charger()
    qr = quartiers_rues()
    actuel = indexation_actuelle()

    # 1. Rues → quartiers possibles.
    wb = Workbook()
    ecrire(wb, "rues_quartiers_31", ["rue", "quartier"],
           [{"rue": r, "quartier": q} for r in sorted(qr) for q in qr[r]], premier=True)
    wb.save(SORTIE_RUES)

    # Sites validés liés aux notices (nom + quartier).
    sites = {r["site_cle"]: r for r in lire_csv(SITES) if r["decision"] in DECISIONS_SITES and r["quartier_point"]}
    sites_notice = defaultdict(dict)
    for r in lire_csv(SITES_NOTICES):
        s = sites.get(r["site_cle"])
        if r["lien"] in LIENS_SITES and s:
            sites_notice[(r["ir"], r["c_id"])][s.get("libelle_propose") or s["forme"]] = s["quartier_point"]
    quartier_point = {lib: next((q for q, g in q31.items() if g.contains(pt)), None) for lib, pt in points.items()}

    # 2. Notices.
    sures, a_lever = [], []
    bilan = Counter()
    for f in sorted(glob(join(IR, "*.xml"))):
        ir = basename(f).replace("FR595126101_", "").replace(".xml", "")
        for c in etree.parse(f).iter("c"):
            geos = c.findall("controlaccess/geogname")
            voies = [texte(g) for g in geos if "bnr_geo_rbx_voies" in (g.get("source") or "")]
            voies = list(dict.fromkeys(v for v in voies if v in qr))
            pts_ref = {texte(g): quartier_point.get(texte(g)) for g in geos
                       if "bnr_geo_rbx_sites" in (g.get("source") or "") and quartier_point.get(texte(g))}
            sn = sites_notice.get((ir, c.get("id")), {})
            if not voies and not sn and not pts_ref:
                continue
            bilan["notices avec rue ou site"] += 1
            did = c.find("did")
            act = actuel.get((ir, c.get("id")), {})
            if not act:
                bilan["(notice absente des IR actuels)"] += 1
            base = {"ir": ir, "c_id": c.get("id"), "ark": ark_previous(c),
                    "index_actuel_quartier": " | ".join(act.get("quartier", [])),
                    "index_actuel_rue": " | ".join(act.get("rue", [])),
                    "unitid": texte(did.find("unitid")) if did is not None and did.find("unitid") is not None else "",
                    "unittitle": texte(did.find("unittitle"))
                    if did is not None and did.find("unittitle") is not None else "",
                    "rues_indexees": " | ".join(voies),
                    "quartiers_deja_indexes": " | ".join(dict.fromkeys(
                        texte(g) for g in geos if "bnr_geo_rbx_quartiers" in (g.get("source") or "")))}
            adr = quartiers_adresses(c, voies, traces, q31, cles_rue, ban) if voies else set()
            pts = set(sn.values()) | set(pts_ref.values()) | adr
            if pts:
                fond = [x for x, ok in (("site", sn or pts_ref), ("adresse", adr)) if ok]
                detail = [f"{s} ({q})" for s, q in {**pts_ref, **sn}.items()]
                if adr:
                    detail.append("adresse(s) : " + " | ".join(sorted(adr)))
                contre = [v for v in voies if len(qr[v]) == 1 and qr[v][0] not in pts]
                sures.append({**base, "quartiers_31": " | ".join(sorted(pts)), "fondement": " + ".join(fond),
                              "detail": " ; ".join(detail),
                              "rue_sure_hors_quartier_du_point": " | ".join(f"{v} ({qr[v][0]})" for v in contre)})
                bilan["point : " + " + ".join(fond)] += 1
                continue
            cands = {v: set(qr[v]) for v in voies}
            if all(len(s) == 1 for s in cands.values()):
                qs = sorted(set().union(*cands.values()))
                sures.append({**base, "quartiers_31": " | ".join(qs), "fondement": "rue(s)",
                              "detail": " ; ".join(f"{v} ({qr[v][0]})" for v in voies)})
                bilan["rue(s)"] += 1
                continue
            commun = set.intersection(*cands.values())
            if len(commun) == 1:
                sures.append({**base, "quartiers_31": next(iter(commun)), "fondement": "croisement de rues",
                              "detail": " ; ".join(f"{v} ({' | '.join(qr[v])})" for v in voies)})
                bilan["croisement de rues"] += 1
                continue
            amb = [v for v in voies if len(cands[v]) > 1]
            connus = sorted({qr[v][0] for v in voies if len(cands[v]) == 1})
            a_lever.append({**base,
                            "rues_ambigues": " ; ".join(f"{v} ({' | '.join(qr[v])})" for v in amb),
                            "quartiers_possibles": " | ".join(sorted(set().union(*(cands[v] for v in amb)))),
                            "quartiers_surs_autres_rues": " | ".join(connus)})
            bilan["à lever"] += 1

    cle = lambda x: (x["ir"], x["unitid"], x["c_id"])  # noqa: E731
    sures.sort(key=cle)
    a_lever.sort(key=cle)
    wb = Workbook()
    ecrire(wb, "sans_ambiguite", ["ir", "unitid", "c_id", "ark", "unittitle", "quartiers_31", "fondement", "detail",
                                  "rues_indexees", "rue_sure_hors_quartier_du_point", "quartiers_deja_indexes",
                                  "index_actuel_quartier", "index_actuel_rue"],
           sures, premier=True)
    ecrire(wb, "a_lever", ["ir", "unitid", "c_id", "ark", "unittitle", "rues_ambigues", "quartiers_possibles",
                           "quartiers_surs_autres_rues", "rues_indexees", "quartiers_deja_indexes",
                           "index_actuel_quartier", "index_actuel_rue"], a_lever)
    synth = [{"categorie": k, "notices": v} for k, v in bilan.most_common()]
    par_ir = Counter(x["ir"] for x in sures)
    par_ir_l = Counter(x["ir"] for x in a_lever)
    synth += [{}] + [{"categorie": f"IR {ir}", "notices": "", "sans_ambiguite": par_ir[ir], "a_lever": par_ir_l[ir]}
                     for ir in sorted(set(par_ir) | set(par_ir_l))]
    ecrire(wb, "synthese", ["categorie", "notices", "sans_ambiguite", "a_lever"], synth)
    wb.save(SORTIE_NOTICES)

    print(f"{len(qr)} rues, {sum(len(v) for v in qr.values())} couples rue / quartier → {SORTIE_RUES}")
    print(f"{len(sures)} notices sans ambiguïté, {len(a_lever)} à lever → {SORTIE_NOTICES}")
    for k, v in bilan.most_common():
        print(f"  {k} : {v}")


if __name__ == "__main__":
    main()
