"""Termes RAMEAU des IR actuels qui ressemblent à un lieu → results/ead/indexation/rameau_lieux.xlsx.
Classement à la main (liste relue des 928 termes, 14/11/2026). Lecture seule."""
import csv
import re
import unicodedata
from collections import Counter, defaultdict
from glob import glob

from lxml import etree
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

# Vedettes de lieu : terme → type.
LIEUX = {}
def ajoute(type_, termes):
    for t in termes.strip().split("\n"):
        LIEUX[t.strip()] = type_

ajoute("commune / ville", """
Aix-en-Provence (Bouches-du-Rhône)
Alger (Algérie)
Amiens (Somme)
Armentières (Nord)
Arras (Pas-de-Calais)
Auchy-les-Mines (Pas-de-Calais)
Barcelone (Espagne)
Berlin (Allemagne)
Bischwiller (Bas-Rhin)
Blankenberge (Belgique)
Bordeaux (Gironde)
Bourbonne-les-Bains (Haute-Marne)
Brescia (Italie)
Bruxelles (Belgique)
Calais (Pas-de-Calais)
Cambrai (Nord)
Casablanca (Maroc)
Cassel (Nord)
Chéreng (Nord)
Coblence (Allemagne)
Couzon-au-Mont-d'Or (Rhône)
Croix (Nord)
Dinant (Belgique)
Dohem (Pas-de-Calais)
Douai (Nord)
Douaumont (Meuse)
Dunkerque (Nord)
Enghien (Belgique)
Esquermes (Nord)
Folkestone (GB)
Francfort (Allemagne)
Guebwiller (Haut-Rhin)
Ham (Somme)
Heidelberg (Allemagne)
Hendaye (Pyrénées-Atlantiques)
Jérusalem
Jouy-en-Josas (Yvelines)
Lannoy (Nord)
Las Vegas (Nev.)
Le Mont-Dore (Puy-de-Dôme)
Leers (Nord)
Lens (Pas-de-Calais)
Les Andelys (Eure)
Lille (Nord)
Lisors (Eure)
Londres (GB)
Luchon (Haute-Garonne)
Lys-lez-Lannoy (Nord)
Manchester (GB)
Marseille (Bouches-du-Rhône)
Menton (Alpes-Maritimes)
Messines (Belgique)
Mons (Belgique)
Nice (Alpes-Maritimes)
Nogent-sur-Marne (Val-de-Marne)
Oissel (Seine-Maritime)
Ostende (Belgique)
Palaiseau (Essonne)
Paris (France)
Peterhof (Russie)
Pierrefitte (Seine-Saint-Denis)
Plombières-les-Bains (Vosges)
Pont-de-l'Arche (Eure)
Puchay (Eure)
Roncq (Nord)
Rosendaël (Nord)
Roubaix (Nord)
Rouen (Seine-Maritime)
Saint-Aubert (Nord)
Saint-Malo (Ille-et-Vilaine)
Saint-Mihiel (Meuse)
Saint-Pierre-du-Vauvray (Eure)
Saint-Pétersbourg (Russie)
Saint-Quentin (Aisne)
Salonique (Grèce)
Saussay-La-Vache (Eure)
Saussay-la-Vache (Eure)
Spa (Belgique)
Tourcoing (Nord)
Tournai (Belgique)
Trente (Italie)
Trieste (Italie)
Troyes (Aube)
Trèves (Allemagne)
Valenciennes (Nord)
Venise (Italie)
Verdun (Meuse)
Vernon (Eure)
Vichy (Allier)
Ville-sur-Cousances (Meuse)
Wattrelos (Nord)
""")
ajoute("département / région", """
Alpes-Maritimes (France)
Aveyron (France)
Bas-Rhin (France)
Cantal (France)
Champagne (France)
Charente (France)
Cher (France)
Côte-d'Or (France)
Dordogne (France)
Eure (France)
Eure-et-Loir (France)
Gard (France)
Gironde (France)
Haut-Rhin (France)
Haute-Garonne (France)
Haute-Savoie (France)
Hautes-Pyrénées (France)
Indre-et-Loire (France)
Lot (France)
Lozère (France)
Manche (France)
Marne (France)
Modane (Savoie ; région)
Moselle (France)
Nord (France)
Nord (département)
Nord-Pas-de-Calais (France)
Orne (France)
Picardie (France)
Puy-de-Dôme (France)
Pyrénées-Atlantiques (France)
Pyrénées-Orientales (France)
Périgord (France)
Somme (France)
Vosges (France)
Communauté urbaine de Lille
""")
ajoute("pays / ensemble", """
Algérie
Allemagne
Amérique du Sud
Angleterre (GB)
Autriche
Belgique
Bulgarie
Cuba
Espagne
Etats-Unis
États-Unis
Europe
Europe Centrale
France
Italie
Luxembourg
Maroc
Martinique
Monaco
Moyen-Orient
Proche-Orient
Prusse
Roumanie
Scandinavie
Suisse
""")
ajoute("site naturel", """
Léman, Lac
Mer de Glace (Haute-Savoie)
""")
ajoute("édifice / site nommé", """
Commercy (Meuse) -- Château
Douaumont, Fort de (Meuse)
Grand Théâtre (Lille, Nord)
Kursaal (Nord)
La Piscine-Musée d'art et d'industrie André Diligent (Roubaix, Nord)
Lille (Nord) -- Préfecture
Musée Weerts (Roubaix, Nord)
Paris (France) -- Arc de triomphe de l'Étoile
Paris (France) -- Hôtel Matignon
Paris (France) -- Place de la Concorde
Paris (France) -- Vélodrome d'hiver
Roubaix (Nord) -- Quartier de l'Alma-Gare
Roubaix, Canal de (Nord)
Théâtre Louis Richard
Versailles (Yvelines) -- Château -- Jardins
""")
ajoute("lieu + subdivision", """
Algérie--Histoire--Autonomie et mouvements indépendantistes
France -- 1914-1918 (Occupation allemande)
""")
# Limites (entreprise / institution qui est aussi un lieu) : à trancher.
DOUTEUX = {
    "Cercle de l'Industrie (Roubaix, Nord)": "cercle (institution) ; son siège est un bâtiment",
    "La Lainière de Roubaix": "entreprise ; son usine est un lieu",
    "Société des Nations (Genève)": "organisation internationale, siège à Genève",
}
# Sujets localisés : sujet + lieu entre parenthèses ou en subdivision (le lieu n'est pas la vedette).
SUJET_LOC = re.compile(r"\(([^()]*(?:Nord|Belgique|Paris|Genève|Pas-de-Calais|France|Espagne|Algérie|Maroc))\)\s*$"
                       r"|--\s*(Roubaix \(Nord\)|France|Amérique)\s*$")
# Faux amis : parenthèse de qualification, pas un lieu.
PAS_SUJET_LOC = {"Front national (France ; 1972-....)", "Grève des chemins de fer (France ; 1920)",
                 "Jeunesse ouvrière chrétienne (France)", "Mouvement ouvrier (France)",
                 "Mouvement républicain populaire (France)", "Organisation armée secrète (France)",
                 "Parti socialiste (France)", "Parti socialiste SFIO (France)", "Partis politiques (France)",
                 "Elections (France)", "Socialisme (France)", "Mouvement pour le triomphe des libertés démocratiques (Algérie)",
                 "L'Istiqlal (Maroc)", "Independence Day (États-Unis)", "Mineurs (Borinage, Belgique)"} - {"Mineurs (Borinage, Belgique)"}


def norm(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", s).split())


def main():
    occ, irs, notices = Counter(), defaultdict(Counter), defaultdict(set)
    for f in sorted(glob("results/ead/ead_cor/bnr2mnesys/*.xml")):
        ir = f.split("FR595126101_")[1][:-4]
        for c in etree.parse(f).iter("c"):
            for e in c.findall("controlaccess/subject"):
                if "bnr_rameau" in (e.get("source") or ""):
                    t = " ".join("".join(e.itertext()).split())
                    occ[t] += 1
                    irs[t][ir] += 1
                    notices[t].add((ir, c.get("id")))
        # archdesc
        for e in etree.parse(f).getroot().findall("archdesc/controlaccess/subject"):
            if "bnr_rameau" in (e.get("source") or ""):
                t = " ".join("".join(e.itertext()).split())
                occ[t] += 1
                irs[t][ir] += 1
    absents = [t for t in list(LIEUX) + list(DOUTEUX) if t not in occ]
    assert not absents, absents

    territoires = {norm(r["libelle"]): r["libelle"] for r in csv.DictReader(open("data/geo/referentiel_communes_pays.csv", encoding="utf-8"))}
    for r in csv.DictReader(open("data/geo/referentiel_communes_pays.csv", encoding="utf-8")):
        for fo in (r["formes_ir"] or "").split("|"):
            if fo.strip():
                territoires.setdefault(norm(fo), r["libelle"])
    lieux_rbx = {}
    for r in csv.DictReader(open("data/geo/referentiel_lieux_roubaix.csv", encoding="utf-8")):
        lieux_rbx[norm(r["libelle_manuel"] or r["libelle"])] = f"{r['libelle_manuel'] or r['libelle']} ({r['thesaurus']})"

    MANUEL = {"Roubaix (Nord) -- Quartier de l'Alma-Gare": "lieux Roubaix : Alma Gare (quartiers)",
              "Roubaix, Canal de (Nord)": "absent du référentiel (aucun lieu « canal »)",
              "La Piscine-Musée d'art et d'industrie André Diligent (Roubaix, Nord)":
                  "point BD TOPO écarté du référentiel (site validé « La Piscine »)"}

    def corresp(t):
        if t in MANUEL:
            return MANUEL[t]
        base = t.split("--")[0].strip()
        for cand in (t, base):
            if norm(cand) in territoires:
                return "territoires : " + territoires[norm(cand)]
        cle = norm(re.sub(r"\s*\([^)]*\)\s*$", "", t))
        if "canal de" in norm(t):
            return "à voir : canal de Roubaix (absent du référentiel ?)"
        if cle in lieux_rbx:
            return "lieux Roubaix : " + lieux_rbx[cle]
        return ""

    def ligne(t, type_, rem=""):
        return {"terme": t, "type": type_, "occurrences": occ[t], "notices": len(notices[t]),
                "ir": " | ".join(f"{i} {n}" for i, n in irs[t].most_common()),
                "dans_referentiel": corresp(t), "remarque": rem}

    lieux = [ligne(t, ty) for t, ty in LIEUX.items()] + [ligne(t, "à trancher", r) for t, r in DOUTEUX.items()]
    ordre = ["édifice / site nommé", "commune / ville", "département / région", "pays / ensemble", "site naturel",
             "lieu + subdivision", "à trancher"]
    lieux.sort(key=lambda x: (ordre.index(x["type"]), x["terme"]))
    loc = []
    for t in sorted(occ):
        if t in LIEUX or t in DOUTEUX or t in PAS_SUJET_LOC:
            continue
        m = SUJET_LOC.search(t)
        if m:
            lieu = (m.group(1) or m.group(2)).split(";")[-1].strip()
            sujet = t[:m.start()].strip(" -")
            loc.append({"terme": t, "sujet": sujet, "lieu": lieu, "occurrences": occ[t], "notices": len(notices[t]),
                        "ir": " | ".join(f"{i} {n}" for i, n in irs[t].most_common())})

    wb = Workbook()
    def feuille(titre, entetes, lignes, premier=False):
        ws = wb.active if premier else wb.create_sheet()
        ws.title = titre
        ws.append(entetes)
        for c in ws[1]:
            c.font = Font(bold=True)
        for li in lignes:
            ws.append([li.get(e, "") for e in entetes])
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
        for i, e in enumerate(entetes, 1):
            ws.column_dimensions[get_column_letter(i)].width = min(max([len(e)] + [len(str(li.get(e, ""))) for li in lignes]) + 2, 70)
    feuille("lieux", ["terme", "type", "occurrences", "notices", "ir", "dans_referentiel", "remarque"], lieux, True)
    feuille("sujets_localises", ["terme", "sujet", "lieu", "occurrences", "notices", "ir"], loc)
    wb.save("results/ead/indexation/rameau_lieux.xlsx")
    print(len(lieux), "lieux,", sum(x["occurrences"] for x in lieux), "occ. ;", len(loc), "sujets localisés")
    print(Counter(x["type"] for x in lieux))
    print("sans correspondance :", [x["terme"] for x in lieux if not x["dans_referentiel"]])


if __name__ == "__main__":
    main()
