"""KML des lieux indexés dans les IR réécrits (results/ead/ead_cor/bnr2mnesysgeo/).

Lit les <geogname> des thésaurus géographiques (quartiers, voies, sites, territoires)
et leur @coord (WKT), tels qu'écrits par geogname_applique_concordance.py, et produit un
repère par lieu (thésaurus + libellé) : géométrie d'après le @coord (point, tracé,
tracé multiple ; contour pour les quartiers), nombre d'occurrences, répartition par
IR et liste des documents (cote, titre, date), dans un dossier par thésaurus.

Les <geogname> sans @coord (lieu sans géométrie ou forme sans lieu) ne peuvent pas
être placés : ils sont comptés en fin d'exécution.

Usage : python scripts/ead/geogname_ir2kml.py [--ir-dir DIR] [--sortie FICHIER.kml]
"""
import argparse, re
from collections import Counter, defaultdict
from glob import glob
from html import escape
from os.path import basename, dirname, join, realpath

from lxml import etree
from shapely import wkt

RACINE = dirname(dirname(dirname(realpath(__file__))))
IR_DIR = join(RACINE, "results/ead/ead_cor/bnr2mnesysgeo")
SORTIE = join(RACINE, "results/ead/indexation/bnr2mnesysgeo_lieux.kml")
THESAURUS = {  # fichier @source → (clé, titre du dossier, couleur KML aabbggrr)
    "bnr_geo_rbx_quartiers.xml": ("quartiers", "Quartiers", "ff8e24aa"),
    "bnr_geo_rbx_voies.xml": ("rues", "Voies", "ff3935e5"),
    "bnr_geo_rbx_sites.xml": ("points", "Sites", "ff00a5ff"),
    "bnr_geo_territoires.xml": ("communes_pays", "Territoires", "ffe5881e"),
}
MAX_DOCUMENTS = 30
KML = "http://www.opengis.net/kml/2.2"
# Couleurs des quartiers (aabbggrr, sans l'opacité) : deux quartiers qui se touchent
# n'ont jamais la même (coloriage glouton DSATUR du graphe de voisinage).
COULEURS_QUARTIERS = ["e5881e", "3935e5", "43a047", "00a5ff", "aa248e", "8bb800", "6d4c79"]
VOISINAGE_M = 1e-5  # tolérance de contact entre contours (degrés, environ 1 m)


def texte(e):
    return re.sub(r"\s+", " ", "".join(e.itertext())).strip() if e is not None else ""


def notice(g):
    """(cote, titre, date) de la notice <c> qui porte le geogname."""
    c = g.getparent()
    while c is not None and not re.fullmatch(r"(\{.*\})?c(\d\d)?", c.tag):
        c = c.getparent()
    if c is None:
        return ("", "(niveau IR)", "")
    did = c.find("{*}did")
    if did is None:
        return ("", "", "")
    return texte(did.find("{*}unitid")), texte(did.find("{*}unittitle")), texte(did.find("{*}unitdate"))


def coordonnees(suite):
    """[(lon, lat), …] → texte KML « lon,lat lon,lat … »."""
    return " ".join(f"{lon},{lat}" for lon, lat in suite)


def ligne_kml(ligne):
    return f"<LineString><tessellate>1</tessellate><coordinates>{coordonnees(ligne.coords)}</coordinates></LineString>"


def geometrie(coord):
    """@coord (WKT, lon lat) → élément de géométrie KML (texte)."""
    geo = wkt.loads(coord)
    if geo.geom_type == "Point":
        return f"<Point><coordinates>{geo.x},{geo.y}</coordinates></Point>"
    if geo.geom_type == "Polygon":
        return ("<Polygon><outerBoundaryIs><LinearRing><coordinates>"
                f"{coordonnees(geo.exterior.coords)}</coordinates></LinearRing></outerBoundaryIs></Polygon>")
    if geo.geom_type == "LineString":
        return ligne_kml(geo)
    if geo.geom_type == "MultiLineString":
        return f"<MultiGeometry>{''.join(ligne_kml(l) for l in geo.geoms)}</MultiGeometry>"
    raise ValueError(f"géométrie non prévue : {geo.geom_type}")


def colorier_quartiers(contours):
    """{libellé: @coord du contour} → {libellé: n° de couleur}, deux voisins jamais de même couleur (DSATUR)."""
    polygones = {lib: wkt.loads(c).buffer(VOISINAGE_M)
                 for lib, c in contours.items()}
    voisins = {a: {b for b in polygones if b != a and polygones[a].intersects(polygones[b])} for a in polygones}
    couleur = {}
    while len(couleur) < len(polygones):
        # quartier le plus contraint : le plus de couleurs voisines distinctes, puis le plus de voisins
        lib = max((l for l in polygones if l not in couleur),
                  key=lambda l: (len({couleur[v] for v in voisins[l] if v in couleur}), len(voisins[l]), l))
        prises = {couleur[v] for v in voisins[lib] if v in couleur}
        couleur[lib] = next(i for i in range(len(polygones)) if i not in prises)
    if max(couleur.values()) >= len(COULEURS_QUARTIERS):
        raise SystemExit(f"{max(couleur.values()) + 1} couleurs nécessaires : compléter COULEURS_QUARTIERS")
    return couleur


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--ir-dir", default=IR_DIR)
    parser.add_argument("--sortie", default=SORTIE)
    args = parser.parse_args()

    lieux = {}  # (thésaurus, libellé) → données
    sans_coord = Counter()
    fichiers = sorted(glob(join(args.ir_dir, "*.xml")))
    for fichier in fichiers:
        ir = re.sub(r"^FR\d+_", "", basename(fichier))[:-4]
        for g in etree.parse(fichier).iter("{*}geogname"):
            source = g.get("source", "").replace("--SLASH--", "/").split("/")[-1]
            if source not in THESAURUS:
                if source in ("rue", ""):
                    sans_coord["forme sans lieu"] += 1
                continue
            thesaurus = THESAURUS[source][0]
            libelle, coord = texte(g), g.get("coord", "")
            if not coord:
                sans_coord[f"{thesaurus} sans géométrie"] += 1
                continue
            l = lieux.setdefault((thesaurus, libelle), {"coord": coord, "par_ir": Counter(), "documents": []})
            l["par_ir"][ir] += 1
            l["documents"].append((ir,) + notice(g))

    kml = ['<?xml version="1.0" encoding="UTF-8"?>', f'<kml xmlns="{KML}"><Document>',
           "<name>Lieux indexés dans les IR de la BnR (bnr2mnesysgeo)</name>",
           "<description>Un repère par lieu des thésaurus géographiques, d'après les &lt;geogname&gt; et leur @coord "
           "dans les IR réécrits ; nombre d'occurrences et documents en description.</description>"]
    for _, (cle, titre, couleur) in THESAURUS.items():
        kml.append(f'<Style id="{cle}"><IconStyle><color>{couleur}</color><scale>0.8</scale></IconStyle>'
                   f"<LineStyle><color>{couleur}</color><width>3</width></LineStyle>"
                   f"<PolyStyle><color>40{couleur[2:]}</color></PolyStyle></Style>")
    couleur_quartier = colorier_quartiers({lib: l["coord"] for (th, lib), l in lieux.items() if th == "quartiers"})
    for i, c in enumerate(COULEURS_QUARTIERS):
        kml.append(f'<Style id="quartiers_{i}"><LineStyle><color>ff{c}</color><width>2</width></LineStyle>'
                   f"<PolyStyle><color>80{c}</color></PolyStyle></Style>")
    for _, (cle, titre, _) in THESAURUS.items():
        du_thesaurus = sorted(((lib, l) for (th, lib), l in lieux.items() if th == cle), key=lambda x: x[0].lower())
        kml.append(f"<Folder><name>{escape(titre)} ({len(du_thesaurus)})</name>")
        for lib, l in du_thesaurus:
            n = sum(l["par_ir"].values())
            par_ir = ", ".join(f"{ir} {k}" for ir, k in l["par_ir"].most_common())
            docs = "".join(f"<li>{escape(ir)} {escape(cote)} — {escape(titre_doc)}{' (' + escape(date) + ')' if date else ''}</li>"
                           for ir, cote, titre_doc, date in l["documents"][:MAX_DOCUMENTS])
            reste = n - min(n, MAX_DOCUMENTS)
            description = (f"<p><b>{n} occurrence{'s' if n > 1 else ''}</b> ({escape(par_ir)})</p><ul>{docs}</ul>"
                           + (f"<p>… et {reste} autre{'s' if reste > 1 else ''}</p>" if reste else ""))
            style = f"quartiers_{couleur_quartier[lib]}" if cle == "quartiers" else cle
            kml.append(f"<Placemark><name>{escape(lib)} ({n})</name><styleUrl>#{style}</styleUrl>"
                       f"<description><![CDATA[{description}]]></description>"
                       f'<ExtendedData><Data name="libelle"><value>{escape(lib)}</value></Data>'
                       f'<Data name="thesaurus"><value>{cle}</value></Data>'
                       f'<Data name="occurrences"><value>{n}</value></Data>'
                       f'<Data name="ir"><value>{escape(par_ir)}</value></Data>'
                       + (f'<Data name="couleur"><value>{couleur_quartier[lib]}</value></Data>' if cle == "quartiers" else "")
                       + "</ExtendedData>"
                       f"{geometrie(l['coord'])}</Placemark>")
        kml.append("</Folder>")
    kml.append("</Document></kml>")
    with open(args.sortie, "w", encoding="utf-8") as f:
        f.write("\n".join(kml))

    print(f"{len(fichiers)} IR → {args.sortie}")
    for _, (cle, titre, _) in THESAURUS.items():
        du = [l for (th, _), l in lieux.items() if th == cle]
        print(f"  {titre} : {len(du)} lieux, {sum(sum(l['par_ir'].values()) for l in du)} occurrences")
    print("  non placés :", dict(sans_coord))


if __name__ == "__main__":
    main()
