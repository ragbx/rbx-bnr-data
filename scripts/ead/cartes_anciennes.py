"""Liste des cartes et plans de la BN-R (Médiathèque et Archives municipales), d'après les IR.

IR lus dans data/ead/bnr (IR d'origine, tels qu'en ligne sur la BN-R) :
  - FR595129901_MED_23 : Collection des cartes et plans de la médiathèque (séries PLA_A/B/C) ;
  - FR595129901_AMR_008 : Collection des cartes et plans des Archives (série PLA_1) ;
  - FR595129901_AMR_006 : Plans cadastraux (1804, 1819, 1826, 1847, 1884) ;
  - FR595129901_AMR_001 : Pièces remarquables des Archives, composants de genre « Cartes et
    plans » ou dont le titre contient plan/carte (surtout des plans de bâtiments) : onglet à part.

Une ligne par composant de niveau « file ». Les zones HTML échappées des IR (physdesc,
scopecontent, acqinfo) sont remises en texte. Colonnes déduites : cote d'origine (« Cote : »,
« Cote Archives : »), échelle, numérisé (dao / daoloc, hors « non-numerise »), mention de
Roubaix, « plan de ville » (titre plan/carte/cadastre + Roubaix, ou série cadastre) = candidat
à un géoréférencement pour la carte Leaflet.

Sortie : results/ead/indexation/cartes_anciennes_bnr.xlsx

    python scripts/ead/cartes_anciennes.py
"""
import html
import re
from os.path import dirname, join, realpath

import pandas as pd
from lxml import etree

RACINE = dirname(dirname(dirname(realpath(__file__))))
IR = join(RACINE, "data/ead/bnr")
SORTIE = join(RACINE, "results/ead/indexation/cartes_anciennes_bnr.xlsx")
FONDS = {
    "FR595129901_MED_23": "MED – Cartes et plans de la médiathèque",
    "FR595129901_AMR_008": "AMR – Cartes et plans des Archives",
    "FR595129901_AMR_006": "AMR – Plans cadastraux",
}
PIECES = ("FR595129901_AMR_001", "AMR – Pièces remarquables")
PLAN = re.compile(r"\b(plan|plans|carte|cartes|cadastre|cadastral|atlas)\b", re.I)


def texte(e):
    """Élément (ou None) → texte : HTML échappé déballé, balises retirées, espaces normalisés."""
    if e is None:
        return ""
    t = html.unescape(" ".join(e.itertext()))
    t = html.unescape(re.sub(r"<[^>]+>", " ", t))
    return " ".join(t.split())


def images(c):
    """Fichiers images du composant (dao et daoloc), sans « non-numerise »."""
    fichiers = [d.get("href", "") for d in c.findall("{*}dao") + c.findall("{*}daogrp/{*}daoloc")]
    return [f for f in fichiers if f and "non-numerise" not in f]


def ligne(c, fonds, serie, fichier_ir):
    did = c.find("{*}did")
    titre = texte(did.find("{*}unittitle"))
    physdesc = texte(did.find("{*}physdesc"))
    description = texte(c.find("{*}scopecontent"))
    date = did.find("{*}unitdate")
    normal = date.get("normal", "") if date is not None else ""
    annee = re.match(r"\d{4}", normal)
    cote = re.search(r"Cote(?: Archives)?\s*:\s*([^.;]+)", physdesc)
    echelle = re.search(r"[ée]chelle[^.;]*", physdesc + " " + description, re.I)
    imgs = images(c)
    roubaix = "Roubaix" in titre or "Roubaix" in description
    return {
        "IR": fichier_ir,
        "fonds": fonds,
        "série": serie,
        "cote BN-R": did.findtext("{*}unitid") or "",
        "cote d'origine": cote.group(1).strip() if cote else "",
        "titre": titre,
        "date": texte(date),
        "année": int(annee.group()) if annee else None,
        "auteur(s)": " ; ".join(f"{texte(n)} ({n.get('role')})" if n.get("role") else texte(n)
                                for n in did.findall("{*}origination/{*}name")),
        "échelle": echelle.group().strip() if echelle else "",
        "description matérielle": physdesc,
        "présentation": description,
        "numérisé": "oui" if imgs else "non",
        "nb images": len(imgs),
        "première image": imgs[0] if imgs else "",
        "Roubaix": "oui" if roubaix else "",
        "plan de ville": "oui" if (serie.startswith("CAD") or (PLAN.search(titre) and roubaix)) else "",
    }


def composants(fichier, fonds, filtre=None):
    racine = etree.parse(join(IR, fichier + ".xml")).getroot()
    lignes = []
    for c in racine.iter("{*}c"):
        if c.get("level") != "file":
            continue
        parent = c.getparent()
        serie = (parent.findtext("{*}did/{*}unitid") or "") if parent is not None and parent.get("level") == "series" else ""
        if filtre and not filtre(c):
            continue
        lignes.append(ligne(c, fonds, serie, fichier))
    return lignes


def est_plan(c):
    genres = " ".join(texte(g) for g in c.iter("{*}genreform"))
    return bool(re.search(r"carte|plan|cadastr", genres, re.I) or PLAN.search(texte(c.find("{*}did/{*}unittitle"))))


def main():
    cartes = pd.DataFrame([l for f, nom in FONDS.items() for l in composants(f, nom)])
    pieces = pd.DataFrame(composants(PIECES[0], PIECES[1], est_plan))
    cartes = cartes.sort_values(["année", "fonds", "cote BN-R"], na_position="last")
    pieces = pieces.sort_values(["année", "cote BN-R"], na_position="last")

    synthese = (cartes.assign(siecle=cartes["année"].map(lambda a: f"{(int(a) - 1) // 100 + 1}e s." if pd.notna(a) else "sans date"))
                .groupby(["fonds", "siecle"]).agg(documents=("titre", "size"),
                                                  numérisés=("numérisé", lambda s: (s == "oui").sum()),
                                                  plans_de_ville=("plan de ville", lambda s: (s == "oui").sum()))
                .reset_index())

    with pd.ExcelWriter(SORTIE, engine="openpyxl") as x:
        cartes.to_excel(x, sheet_name="Cartes et plans", index=False)
        pieces.to_excel(x, sheet_name="Plans AMR_001", index=False)
        synthese.to_excel(x, sheet_name="Synthèse", index=False)
        for nom, df in (("Cartes et plans", cartes), ("Plans AMR_001", pieces), ("Synthèse", synthese)):
            ws = x.sheets[nom]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            for i, col in enumerate(df.columns, 1):
                larg = min(60, max(10, int(df[col].astype(str).str.len().quantile(0.9)) + 2, len(col) + 2))
                ws.column_dimensions[ws.cell(1, i).column_letter].width = larg

    print(f"→ {SORTIE}")
    print(f"  cartes et plans : {len(cartes)} (numérisés {sum(cartes['numérisé'] == 'oui')}, "
          f"plans de ville {sum(cartes['plan de ville'] == 'oui')}) ; plans AMR_001 : {len(pieces)}")
    print(synthese.to_string(index=False))


if __name__ == "__main__":
    main()
