"""KML des lieux indexés dans les IR réécrits (results/ead/ead_cor/bnr2mnesysgeo/).

Lit les <geogname> des thésaurus géographiques de Roubaix (quartiers, voies, sites ;
les territoires, communes et pays, sont laissés hors du KML depuis le 2026-11-08) et leur @coord (WKT), tels qu'écrits par geogname_applique_concordance.py, et produit un
repère par lieu (thésaurus + libellé) : géométrie d'après le @coord (point, tracé,
tracé multiple ; contour pour les quartiers), dans un dossier par thésaurus. La fiche
ne montre que le libellé, le nombre d'occurrences et un lien vers la recherche de la
BN-R (2026-11-08).

Lien BN-R (format du 2026-11-14) : la BN-R indexe les IR d'origine (data/ead/bnr), pas
les IR réécrits. Voies et quartiers : filtre de facettes (fq) sur les formes d'origine du
lieu prises dans la concordance (forme + @source → lieu), chacune dans l'index de son
@source (INDEX_BNR : rue, quartier, adresse ; sujet_geo pour les formes sans @source),
reliées par OR (lien_bnr). Sites : pas d'index « site » sur la BN-R, recherche plein texte
des noms du site entre guillemets, avec dropFQ=1 (lien_bnr_site, noms_sites). Un seul
lien par lieu.

Les voies ont en plus, dans un dernier dossier « Voies : zones de clic » replié, un
repère de même nom et même description dont la géométrie est un tampon invisible de
TAMPON_M mètres : il rend la voie cliquable autour de la ligne. Les voies elles-mêmes
restent de simples lignes (lisibles comme telles dans QGIS).

uMap ne lit pas les styles KML (import par togeojson, tout dans la couleur par défaut,
tampons des voies visibles) : les mêmes lieux sont donc aussi écrits au format natif
de uMap (.umap, à côté du KML), un calque par thésaurus, couleurs dans les options
uMap des calques et des quartiers, descriptions dans la syntaxe de uMap (2026-11-08) ;
depuis le 2026-11-14, avec un dernier calque « Voies : zones de clic » (tampons quasi
transparents, UMAP_CLIC).

Les <geogname> sans @coord (lieu sans géométrie ou forme sans lieu) ne peuvent pas
être placés : ils sont comptés en fin d'exécution.

Usage : python scripts/ead/geogname_ir2kml.py [--ir-dir DIR] [--sortie FICHIER.kml]
        [--sortie-umap FICHIER.umap] [--concordance FICHIER.csv]
"""
import argparse, csv, json, re, unicodedata
from collections import Counter, defaultdict
from glob import glob
from html import escape
from os.path import dirname, exists, join, realpath
from urllib.parse import urlencode

from lxml import etree
from pyproj import Transformer
from shapely import set_precision, wkt
from shapely.geometry import mapping
from shapely.ops import transform

RACINE = dirname(dirname(dirname(realpath(__file__))))
IR_DIR = join(RACINE, "results/ead/ead_cor/bnr2mnesysgeo")
SORTIE = join(RACINE, "results/ead/indexation/bnr2mnesysgeo_lieux.kml")
CONCORDANCE = join(RACINE, "data/geo/concordance_geogname_lieux.csv")
SITES = join(RACINE, "data/geo/propositions_sites_20261112.csv")  # noms des sites validés (liens plein texte)
# Lieux dont le filtre fq ne trouve rien sur la BN-R (leurs <geogname> n'y sont pas indexés en facette) :
# lien plein texte sur leurs formes (mode « plein texte ») ou pas de lien (mode « sans lien » : rien sur la
# BN-R). Colonnes thesaurus, libelle, mode, motif ; tenu d'après verifier_liens_bnr.py.
REPLI = join(RACINE, "data/geo/liens_bnr_repli.csv")
def kml_bgr(rvb):
    """Couleur « #RRVVBB » → « bbggrr » (ordre KML, sans l'opacité)."""
    rvb = rvb.lstrip("#")
    return (rvb[4:6] + rvb[2:4] + rvb[0:2]).lower()


# Palette de la BnR (2026-11-06) : noir #000000, blanc #FFFFFF, turquoise #2FAAAF,
# jaune #E9B400, violet #9460A4, gris #ADA796.
NOIR, TURQUOISE, JAUNE, VIOLET, GRIS = "#000000", "#2FAAAF", "#E9B400", "#9460A4", "#ADA796"
# Rouge des sites / points (14/11/2026), hors palette mais accordé à elle : teinte opposée au
# turquoise, même saturation (58 %), un peu plus clair (47 %) ; partagé avec quartiers2kml.py.
ROUGE = "#C03730"
THESAURUS = {  # fichier @source → (clé, titre du dossier, couleur KML aabbggrr)
    "bnr_geo_rbx_quartiers.xml": ("quartiers", "Quartiers", "ff" + kml_bgr(VIOLET)),
    "bnr_geo_rbx_voies.xml": ("rues", "Voies", "ff" + kml_bgr(NOIR)),
    "bnr_geo_rbx_sites.xml": ("points", "Sites", "ff" + kml_bgr(ROUGE)),
}
CLES = {cle for cle, _, _ in THESAURUS.values()}
HORS_KML = {"bnr_geo_territoires.xml"}  # territoires (communes, pays) : retirés du KML le 2026-11-08
# Recherche avancée de la BN-R (2026-11-08) : index par @source de la forme d'origine (vérifié sur
# le site : « Mascara, rue de » 2 résultats en rue, « Hutin » 1 en quartier ; les formes sans
# @source ne sont trouvées que par sujet_geo). Recherche par texte seulement, pas par coordonnées.
BNR_RECHERCHE = "https://www.bn-r.fr/resultat.php"
INDEX_BNR = {"rue": "rue", "quartier": "quartier", "adresse": "adresse", "": "sujet_geo"}
KML = "http://www.opengis.net/kml/2.2"
# Couleurs des quartiers (bbggrr, sans l'opacité), les 4 couleurs de la palette hors noir et
# blanc : deux quartiers qui se touchent n'ont jamais la même (coloriage glouton DSATUR du
# graphe de voisinage ; 4 couleurs sont nécessaires et suffisent).
QUARTIERS_RVB = (TURQUOISE, JAUNE, VIOLET, GRIS)
COULEURS_QUARTIERS = [kml_bgr(c) for c in QUARTIERS_RVB]
# Calques uMap (même ordre que les dossiers du KML) : options de style uMap par thésaurus.
UMAP_CALQUES = {
    "quartiers": {"weight": 2, "opacity": 1, "fillOpacity": 0.5},
    "rues": {"color": NOIR, "weight": 3, "opacity": 1},
    "points": {"color": ROUGE, "iconClass": "Circle"},
}
CENTRE_UMAP = (3.1746, 50.6942)  # Roubaix (lon, lat)
VOISINAGE_M = 1e-5  # tolérance de contact entre contours (degrés, environ 1 m)
# Tampon invisible des voies : une ligne fine (ou les squares, lignes de 2 m) est difficile à
# viser ; le polygone transparent autour d'elle rend la voie cliquable à TAMPON_M mètres près.
TAMPON_M = 30  # 15 m le 2026-11-06, porté à 30 m le même jour
VERS_L93 = Transformer.from_crs(4326, 2154, always_xy=True).transform
VERS_WGS84 = Transformer.from_crs(2154, 4326, always_xy=True).transform


def texte(e):
    return re.sub(r"\s+", " ", "".join(e.itertext())).strip() if e is not None else ""


def formes_origine(chemin):
    """Concordance → {(thésaurus, libellé retenu): [(@source, forme d'origine), …]}, formes les
    plus fréquentes d'abord."""
    formes = defaultdict(list)
    with open(chemin, encoding="utf-8") as f:
        lignes = [r for r in csv.DictReader(f) if r["lieu_id"] and r["thesaurus"] in CLES]
    for r in sorted(lignes, key=lambda r: -int(r["occurrences"] or 0)):
        cle = (r["source"], r["forme"])
        if r["source"] in INDEX_BNR and cle not in formes[(r["thesaurus"], r["libelle_retenu"])]:
            formes[(r["thesaurus"], r["libelle_retenu"])].append(cle)
    return formes


def guillemets(valeurs):
    """["A", "B"] → '"A" OR "B"' (guillemets doubles internes retirés)."""
    return " OR ".join('"' + v.replace('"', "") + '"' for v in valeurs)


def variantes_bnr(forme):
    """Valeurs de facette possibles d'une forme d'origine (constaté sur la BN-R le 2026-11-14) :
    la BN-R retire l'antislash (« Esperance, rue de l\\' » indexé « Esperance, rue de l' ») et
    garde parfois une espace finale que l'extraction des IR a perdue (« Corneille, rue  »,
    « Fraternité  ») : forme sans antislash, avec et sans espace finale."""
    v = forme.replace("\\", "").strip()
    return [v, v + " "]


def lien_bnr(formes):
    """Voies et quartiers (2026-11-14) : filtre de facettes de la BN-R, chaque forme d'origine dans
    l'index de son @source, exactement telle qu'écrite (le filtre distingue casse et tirets :
    « Grand place » 680 résultats, « Grand Place » 11), index reliés par OR :
        fq=rue:("Jean Lebas, avenue" OR "Gare, rue de la") OR sujet_geo:("Gare, rue de la")
    Un lien fq remplace les filtres que la BN-R garde en session."""
    par_index = defaultdict(list)
    for source, forme in formes:
        for v in variantes_bnr(forme):
            if v not in par_index[INDEX_BNR[source]]:
                par_index[INDEX_BNR[source]].append(v)
    fq = " OR ".join(f"{index}:({guillemets(f)})" for index, f in par_index.items())
    return BNR_RECHERCHE + "?" + urlencode(
        [("q", "deleted:(0)"), ("spec_expand", ""), ("sort_define", "score"), ("sort_order", "1"),
         ("rows", "9"), ("fq", fq)])


def lien_bnr_site(phrases):
    """Sites (2026-11-14) : pas d'index « site » sur la BN-R, recherche plein texte des noms du
    site entre guillemets (expressions exactes, accents et casse indifférents), reliés par OR.
    dropFQ=1 vide les filtres gardés en session (sans lui, un filtre rue ou quartier posé
    auparavant donne 0 résultat)."""
    return BNR_RECHERCHE + "?" + urlencode(
        [("type_rech", "rs"), ("index[]", "fulltext"), ("bool[]", ""), ("reset", "1"), ("nr", "1"),
         ("dropFQ", "1"), ("value[]", guillemets(phrases))])


def noms_sites(chemin, origine):
    """{libellé du site: [noms à chercher]} : libellé, puis formes des sites validés
    (propositions_sites, proposition « validé »), sans doublon d'accents ou de casse et sans
    nom d'un seul mot (« mairie », « gare » : trop larges en plein texte)."""
    noms = defaultdict(list)
    if chemin and exists(chemin):
        with open(chemin, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r["proposition"] == "validé" and r["libelle_propose"]:
                    noms[r["libelle_propose"]].append(r["forme"])
    sortie = {}
    for (th, lib) in [k for k in origine if k[0] == "points"] + [("points", l) for l in noms]:
        vus, liste = set(), []
        for n in [lib, *noms.get(lib, [])]:
            cle = unicodedata.normalize("NFD", n.lower()).encode("ascii", "ignore").decode()
            cle = re.sub(r"\W+", " ", cle).strip()
            if cle and cle not in vus and len(cle.split()) > 1:
                vus.add(cle)
                liste.append(n)
        sortie[lib] = liste
    return sortie


def coordonnees(suite):
    """[(lon, lat), …] → texte KML « lon,lat lon,lat … »."""
    return " ".join(f"{lon},{lat}" for lon, lat in suite)


def ligne_kml(ligne):
    return f"<LineString><tessellate>1</tessellate><coordinates>{coordonnees(ligne.coords)}</coordinates></LineString>"


def polygone_kml(poly):
    anneaux = "".join(f"<innerBoundaryIs><LinearRing><coordinates>{coordonnees(a.coords)}</coordinates></LinearRing></innerBoundaryIs>"
                      for a in poly.interiors)
    return ("<Polygon><outerBoundaryIs><LinearRing><coordinates>"
            f"{coordonnees(poly.exterior.coords)}</coordinates></LinearRing></outerBoundaryIs>{anneaux}</Polygon>")


def tampon(coord):
    """Tampon de TAMPON_M mètres autour d'une voie (calculé en Lambert 93, simplifié à 1 m,
    arrondi à 6 décimales) → géométrie shapely en WGS84."""
    return set_precision(transform(VERS_WGS84, transform(VERS_L93, wkt.loads(coord)).buffer(TAMPON_M).simplify(1)), 1e-6)


# Calque uMap des zones de clic (2026-11-14) : polygones sans contour, remplissage quasi
# transparent (0,01 plutôt que 0, au cas où uMap prendrait 0 pour « non renseigné ») ; un clic
# à TAMPON_M mètres d'une voie ouvre sa fiche. Les points (marqueurs) restent au-dessus.
UMAP_CLIC = {"stroke": False, "fill": True, "color": NOIR, "fillColor": NOIR, "fillOpacity": 0.01}


def tampon_kml(coord):
    """Tampon de TAMPON_M mètres autour d'une voie → élément de géométrie KML (texte)."""
    zone = tampon(coord)
    if zone.geom_type == "Polygon":
        return polygone_kml(zone)
    return f"<MultiGeometry>{''.join(polygone_kml(p) for p in zone.geoms)}</MultiGeometry>"


def geometrie(coord):
    """@coord (WKT, lon lat) → élément de géométrie KML (texte)."""
    geo = wkt.loads(coord)
    if geo.geom_type == "Point":
        return f"<Point><coordinates>{geo.x},{geo.y}</coordinates></Point>"
    if geo.geom_type == "Polygon":
        return polygone_kml(geo)
    if geo.geom_type == "LineString":
        return ligne_kml(geo)
    if geo.geom_type == "MultiLineString":
        return f"<MultiGeometry>{''.join(ligne_kml(l) for l in geo.geoms)}</MultiGeometry>"
    raise ValueError(f"géométrie non prévue : {geo.geom_type}")


def feature_umap(lib, l, n, lien, couleur):
    """Feature GeoJSON du format uMap : nom = libellé, description dans la syntaxe de uMap
    (occurrences, lien [[url|texte]]), couleur du quartier dans ses options uMap (les autres
    prennent celle du calque)."""
    description = f"{n} occurrence{'s' if n > 1 else ''}" + (f"\n[[{lien}|Voir sur la BN-R]]" if lien else "")
    proprietes = {"name": lib, "description": description}
    if couleur is not None:
        proprietes["_umap_options"] = {"color": QUARTIERS_RVB[couleur], "fillColor": QUARTIERS_RVB[couleur]}
    return {"type": "Feature", "geometry": mapping(wkt.loads(l["coord"])), "properties": proprietes}


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
    parser.add_argument("--sortie-umap", help="défaut : le KML en .umap")
    parser.add_argument("--concordance", default=CONCORDANCE)
    parser.add_argument("--sites", default=SITES, help="propositions de sites validées (noms des sites)")
    parser.add_argument("--repli", default=REPLI, help="lieux à lier en plein texte (filtre fq sans résultat)")
    args = parser.parse_args()
    args.sortie_umap = args.sortie_umap or re.sub(r"\.kml$", "", args.sortie) + ".umap"

    lieux = {}  # (thésaurus, libellé) → données
    sans_coord = Counter()
    hors_kml = 0
    fichiers = sorted(glob(join(args.ir_dir, "*.xml")))
    for fichier in fichiers:
        for g in etree.parse(fichier).iter("{*}geogname"):
            source = g.get("source", "").replace("--SLASH--", "/").split("/")[-1]
            if source not in THESAURUS:
                if source in HORS_KML:
                    hors_kml += 1
                elif source in ("rue", ""):
                    sans_coord["forme sans lieu"] += 1
                continue
            thesaurus = THESAURUS[source][0]
            libelle, coord = texte(g), g.get("coord", "")
            if not coord:
                sans_coord[f"{thesaurus} sans géométrie"] += 1
                continue
            l = lieux.setdefault((thesaurus, libelle), {"coord": coord, "n": 0})
            l["n"] += 1

    kml = ['<?xml version="1.0" encoding="UTF-8"?>', f'<kml xmlns="{KML}"><Document>',
           "<name>Lieux de Roubaix dans les archives de la BN-R</name>",
           "<description>Un repère par lieu des thésaurus géographiques, d'après les &lt;geogname&gt; et leur @coord "
           "dans les IR réécrits ; nombre d'occurrences et lien vers la recherche de la BN-R.</description>"]
    for _, (cle, titre, couleur) in THESAURUS.items():
        kml.append(f'<Style id="{cle}"><IconStyle><color>{couleur}</color><scale>0.8</scale></IconStyle>'
                   f"<LineStyle><color>{couleur}</color><width>3</width></LineStyle>"
                   f"<PolyStyle><color>40{couleur[2:]}</color></PolyStyle></Style>")
    # Tampons des voies : remplis mais entièrement transparents (alpha 00), sans contour ; dossier
    # replié dans la liste (checkHideChildren).
    kml.append('<Style id="rues_clic"><LineStyle><color>00ffffff</color><width>0</width></LineStyle>'
               "<PolyStyle><color>00ffffff</color><fill>1</fill><outline>0</outline></PolyStyle></Style>"
               '<Style id="dossier_clic"><ListStyle><listItemType>checkHideChildren</listItemType></ListStyle></Style>')
    couleur_quartier = colorier_quartiers({lib: l["coord"] for (th, lib), l in lieux.items() if th == "quartiers"})
    for i, c in enumerate(COULEURS_QUARTIERS):
        kml.append(f'<Style id="quartiers_{i}"><LineStyle><color>ff{c}</color><width>2</width></LineStyle>'
                   f"<PolyStyle><color>80{c}</color></PolyStyle></Style>")
    clic = []  # repères des tampons des voies, écrits dans un dossier à part, en dernier
    origine = formes_origine(args.concordance)
    sites = noms_sites(args.sites, origine)
    repli = set()
    if exists(args.repli):
        with open(args.repli, encoding="utf-8") as f:
            repli = {(r["thesaurus"], r["libelle"]): r.get("mode") or "plein texte" for r in csv.DictReader(f)}
    sans_lien = []
    umap_features = defaultdict(list)  # thésaurus → features uMap
    for _, (cle, titre, _) in THESAURUS.items():
        du_thesaurus = sorted(((lib, l) for (th, lib), l in lieux.items() if th == cle), key=lambda x: x[0].lower())
        kml.append(f"<Folder><name>{escape(titre)} ({len(du_thesaurus)})</name>")
        for lib, l in du_thesaurus:
            n = l["n"]
            formes = origine.get((cle, lib))
            if cle == "points":
                lien = lien_bnr_site(sites[lib]) if sites.get(lib) else ""
            elif repli.get((cle, lib)) == "sans lien":
                lien = ""  # aucune notice trouvée sur la BN-R (ni filtre ni plein texte), cf. liens_bnr_repli.csv
            elif (cle, lib) in repli and formes:
                lien = lien_bnr_site(list(dict.fromkeys(f.replace("\\", "").strip() for _, f in formes)))
            else:
                lien = lien_bnr(formes) if formes else ""
            if not lien:
                sans_lien.append(f"{titre} : {lib}")
            umap_features[cle].append(feature_umap(lib, l, n, lien,
                                                   couleur_quartier.get(lib) if cle == "quartiers" else None))
            description = (f"<p>{n} occurrence{'s' if n > 1 else ''}</p>"
                           + (f'<p><a href="{escape(lien)}">Voir sur la BN-R</a></p>' if lien else ""))
            donnees = (f'<ExtendedData><Data name="libelle"><value>{escape(lib)}</value></Data>'
                       f'<Data name="occurrences"><value>{n}</value></Data>'
                       f'<Data name="lien"><value>{escape(lien)}</value></Data></ExtendedData>')
            style = f"quartiers_{couleur_quartier[lib]}" if cle == "quartiers" else cle
            kml.append(f"<Placemark><name>{escape(lib)}</name><styleUrl>#{style}</styleUrl>"
                       f"<description><![CDATA[{description}]]></description>{donnees}"
                       f"{geometrie(l['coord'])}</Placemark>")
            if cle == "rues":
                clic.append(f"<Placemark><name>{escape(lib)}</name><styleUrl>#rues_clic</styleUrl>"
                            f"<description><![CDATA[{description}]]></description>{donnees}"
                            f"{tampon_kml(l['coord'])}</Placemark>")
        kml.append("</Folder>")
    # Dernier dossier, pour ne pas décaler le numéro des couches précédentes (projet QGIS : layerid).
    kml.append(f"<Folder><name>Voies : zones de clic</name><styleUrl>#dossier_clic</styleUrl>{''.join(clic)}</Folder>")
    kml.append("</Document></kml>")
    with open(args.sortie, "w", encoding="utf-8") as f:
        f.write("\n".join(kml))
    umap = {"type": "umap", "geometry": {"type": "Point", "coordinates": list(CENTRE_UMAP)},
            "properties": {"name": "Lieux de Roubaix dans les archives de la BN-R", "zoom": 14},
            "layers": [{"type": "FeatureCollection", "features": umap_features[cle],
                        # options du calque dans _umap_options : seule clé lue par uMap en
                        # service (2026-11-08) ; la version en développement la lit encore
                        # (rétrocompatibilité, son nouveau format met « properties »)
                        "_umap_options": {"name": titre, **UMAP_CALQUES[cle]}}
                       for _, (cle, titre, _) in THESAURUS.items()]}
    # Zones de clic des voies, dernier calque (2026-11-14) : même nom et même description que la voie.
    umap["layers"].append({"type": "FeatureCollection",
                           "features": [{"type": "Feature", "properties": f["properties"],
                                         "geometry": mapping(tampon(lieux[("rues", f["properties"]["name"])]["coord"]))}
                                        for f in umap_features["rues"]],
                           "_umap_options": {"name": "Voies : zones de clic", **UMAP_CLIC}})
    with open(args.sortie_umap, "w", encoding="utf-8") as f:
        json.dump(umap, f, ensure_ascii=False)

    print(f"{len(fichiers)} IR → {args.sortie}, {args.sortie_umap}")
    for _, (cle, titre, _) in THESAURUS.items():
        du = [l for (th, _), l in lieux.items() if th == cle]
        print(f"  {titre} : {len(du)} lieux, {sum(l['n'] for l in du)} occurrences")
    print("  non placés :", dict(sans_coord))
    print(f"  territoires laissés hors du KML : {hors_kml} occurrences")
    print(f"  sans lien BN-R (aucune forme d'origine dans la concordance) : {len(sans_lien)}", *sans_lien[:20], sep="\n    ")


if __name__ == "__main__":
    main()
