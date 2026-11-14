"""KML et uMap des quartiers, rues et points de Roubaix ; deux quartiers voisins de couleurs différentes.

Source : data/geo/referentiel_lieux_roubaix.csv (thesaurus = quartiers, rues, points ;
géométrie coord_manuel sinon coord_wkt, libellé libelle_manuel sinon libelle, comme
geogname2skos.py). Trois calques / dossiers : Quartiers (31), Rues, Points (en rouge).

Quartiers : palette et coloriage repris de geogname_ir2kml.py, 4 couleurs de la palette de
la BnR (turquoise, jaune, violet, gris), coloriage glouton DSATUR du graphe de voisinage
(contours en contact à environ 1 m près). Contrôle en fin d'exécution : aucune paire de
voisins de même couleur. Rues en noir (comme geogname_ir2kml.py), points en rouge (ROUGE).

Fiche d'une rue : catégorie et quartier(s) de rattachement, même règle que
rues_quartiers2skos.py (retenu_31 de data/geo/rues_rattachement.csv pour les rues
ambiguës, sinon q31_principal de data/geo/rues_quartiers_13_repartition.csv). Fiche d'un
point : catégorie, quartier (pas de dates, 14/11/2026). Les rues sans tracé ne sont pas placées.

Carte « pros » (la carte « public », à partir des IR, est faite par geogname_ir2kml.py).
Sorties : results/ead/indexation/roubaix_lieux_referentiel.kml et .umap (format natif uMap, à
importer dans une carte neuve avec le format « umap »).

    python scripts/ead/quartiers2kml.py
"""
import csv
import json
import sys
from html import escape
from os.path import dirname, join, realpath

from shapely import wkt
from shapely.geometry import mapping

sys.path.insert(0, dirname(realpath(__file__)))
from geogname_ir2kml import (CENTRE_UMAP, COULEURS_QUARTIERS, KML, NOIR, QUARTIERS_RVB, ROUGE,  # noqa: E402
                             TAMPON_M, UMAP_CLIC, VOISINAGE_M, colorier_quartiers, geometrie,
                             kml_bgr, tampon, tampon_kml)

RACINE = dirname(dirname(dirname(realpath(__file__))))
REFERENTIEL = join(RACINE, "data/geo/referentiel_lieux_roubaix.csv")
RATTACHEMENT = join(RACINE, "data/geo/rues_rattachement.csv")
REPARTITION = join(RACINE, "data/geo/rues_quartiers_13_repartition.csv")
SORTIE_KML = join(RACINE, "results/ead/indexation/roubaix_lieux_referentiel.kml")
SORTIE_UMAP = join(RACINE, "results/ead/indexation/roubaix_lieux_referentiel.umap")
TITRE = "Lieux de Roubaix : référentiel de la BnR (quartiers, rues, points)"
NOMS_COULEURS = ("turquoise", "jaune", "violet", "gris")
NOIR_KML = "ff" + kml_bgr(NOIR)
ROUGE_KML = "ff" + kml_bgr(ROUGE)
CALQUES = (  # clé du référentiel, titre, options uMap du calque
    ("quartiers", "Quartiers", {"weight": 2, "opacity": 1, "fillOpacity": 0.5}),
    ("rues", "Rues", {"color": NOIR, "weight": 3, "opacity": 1}),
    ("points", "Points", {"color": ROUGE, "iconClass": "Circle"}),
)


def lire_csv(chemin):
    with open(chemin, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def lire_lieux():
    """{thesaurus: [lieu]} ; lieu = dict avec libelle, coord et champs du référentiel."""
    lieux = {cle: [] for cle, _, _ in CALQUES}
    sans_geo = []
    for r in lire_csv(REFERENTIEL):
        if r["thesaurus"] not in lieux:
            continue
        r["libelle"] = r["libelle_manuel"].strip() or r["libelle"]
        r["coord"] = r["coord_manuel"].strip() or r["coord_wkt"].strip()
        if r["coord"]:
            lieux[r["thesaurus"]].append(r)
        else:
            sans_geo.append(f"{r['thesaurus']} : {r['libelle']}")
    for liste in lieux.values():
        liste.sort(key=lambda r: r["libelle"].lower())
    return lieux, sans_geo


def quartiers_des_rues():
    """{lieu_id: [quartiers]} : rattachement validé (rue ambiguë) sinon quartier principal."""
    q = {r["lieu_id"]: [r["q31_principal"]] for r in lire_csv(REPARTITION) if r["q31_principal"]}
    for r in lire_csv(RATTACHEMENT):
        q[r["lieu_id"]] = [n.strip() for n in r["retenu_31"].split("|") if n.strip()]
    return q


def lignes_fiche(lieu, cle, q_rues):
    """Lignes de texte de la fiche (description)."""
    lignes = []
    if cle != "quartiers" and lieu["categorie"]:
        lignes.append(lieu["categorie"][:1].upper() + lieu["categorie"][1:])
    quartiers = q_rues.get(lieu["lieu_id"], []) if cle == "rues" else (
        [lieu["quartiers"]] if cle == "points" and lieu["quartiers"] else [])
    if quartiers:
        lignes.append("Quartier(s) : " + ", ".join(quartiers))
    if lieu["nom_ancien"]:
        lignes.append("Ancien(s) nom(s) : " + lieu["nom_ancien"].replace(" | ", ", "))
    return lignes


def main():
    lieux, sans_geo = lire_lieux()
    q_rues = quartiers_des_rues()
    contours = {q["libelle"]: q["coord"] for q in lieux["quartiers"]}
    couleur = colorier_quartiers(contours)

    # Contrôle : deux quartiers qui se touchent n'ont jamais la même couleur.
    poly = {lib: wkt.loads(c).buffer(VOISINAGE_M) for lib, c in contours.items()}
    paires = [(a, b) for a in poly for b in poly if a < b and poly[a].intersects(poly[b])]
    conflits = [(a, b) for a, b in paires if couleur[a] == couleur[b]]
    if conflits:
        raise SystemExit(f"voisins de même couleur : {conflits}")

    kml = ['<?xml version="1.0" encoding="UTF-8"?>', f'<kml xmlns="{KML}"><Document>',
           f"<name>{TITRE}</name>",
           "<description>Les 31 quartiers de Roubaix (quartiers MEL), deux quartiers voisins toujours "
           "de couleurs différentes ; rues et points du référentiel des lieux de la BnR.</description>"]
    for i, c in enumerate(COULEURS_QUARTIERS):
        kml.append(f'<Style id="quartiers_{i}"><LineStyle><color>ff{c}</color><width>2</width></LineStyle>'
                   f"<PolyStyle><color>80{c}</color></PolyStyle></Style>")
    kml.append(f'<Style id="rues"><LineStyle><color>{NOIR_KML}</color><width>3</width></LineStyle></Style>'
               f'<Style id="points"><IconStyle><color>{ROUGE_KML}</color><scale>1</scale></IconStyle></Style>')
    # Zones de clic des rues (« fantômes ») : tampons entièrement transparents, dossier replié.
    kml.append('<Style id="rues_clic"><LineStyle><color>00ffffff</color><width>0</width></LineStyle>'
               "<PolyStyle><color>00ffffff</color><fill>1</fill><outline>0</outline></PolyStyle></Style>"
               '<Style id="dossier_clic"><ListStyle><listItemType>checkHideChildren</listItemType></ListStyle></Style>')

    calques_umap = []
    clic_kml, clic_umap = [], []
    for cle, titre, options in CALQUES:
        kml.append(f"<Folder><name>{titre} ({len(lieux[cle])})</name>")
        features = []
        for lieu in lieux[cle]:
            lib, fiche = lieu["libelle"], lignes_fiche(lieu, cle, q_rues)
            style = f"quartiers_{couleur[lib]}" if cle == "quartiers" else cle
            donnees = f'<Data name="libelle"><value>{escape(lib)}</value></Data>'
            if cle == "quartiers":
                donnees += f'<Data name="couleur"><value>{NOMS_COULEURS[couleur[lib]]}</value></Data>'
            description = ("<description><![CDATA[" + "".join(f"<p>{escape(l)}</p>" for l in fiche)
                           + "]]></description>") if fiche else ""
            kml.append(f"<Placemark><name>{escape(lib)}</name><styleUrl>#{style}</styleUrl>{description}"
                       f"<ExtendedData>{donnees}</ExtendedData>{geometrie(lieu['coord'])}</Placemark>")
            proprietes = {"name": lib}
            if fiche:
                proprietes["description"] = "\n".join(fiche)
            if cle == "quartiers":
                proprietes["_umap_options"] = {"color": QUARTIERS_RVB[couleur[lib]],
                                               "fillColor": QUARTIERS_RVB[couleur[lib]]}
            features.append({"type": "Feature", "geometry": mapping(wkt.loads(lieu["coord"])),
                             "properties": proprietes})
            if cle == "rues":
                clic_kml.append(f"<Placemark><name>{escape(lib)}</name><styleUrl>#rues_clic</styleUrl>{description}"
                                f"<ExtendedData>{donnees}</ExtendedData>{tampon_kml(lieu['coord'])}</Placemark>")
                clic_umap.append({"type": "Feature", "geometry": mapping(tampon(lieu["coord"])),
                                  "properties": proprietes})
        kml.append("</Folder>")
        calques_umap.append({"type": "FeatureCollection", "features": features,
                             "_umap_options": {"name": titre, **options}})
    # Dernier dossier / calque : zones de clic des rues (même nom, même fiche que la rue).
    kml.append(f"<Folder><name>Rues : zones de clic</name><styleUrl>#dossier_clic</styleUrl>{''.join(clic_kml)}</Folder>")
    calques_umap.append({"type": "FeatureCollection", "features": clic_umap,
                         "_umap_options": {"name": "Rues : zones de clic", **UMAP_CLIC}})
    kml.append("</Document></kml>")
    with open(SORTIE_KML, "w", encoding="utf-8") as f:
        f.write("\n".join(kml))

    umap = {"type": "umap", "geometry": {"type": "Point", "coordinates": list(CENTRE_UMAP)},
            "properties": {"name": TITRE, "zoom": 14}, "layers": calques_umap}
    with open(SORTIE_UMAP, "w", encoding="utf-8") as f:
        json.dump(umap, f, ensure_ascii=False)

    print(f"→ {SORTIE_KML}, {SORTIE_UMAP}")
    for cle, titre, _ in CALQUES:
        print(f"  {titre} : {len(lieux[cle])}")
    print(f"  zones de clic des rues : {len(clic_umap)} tampons de {TAMPON_M} m")
    print(f"  sans géométrie, non placés : {len(sans_geo)}")
    print(f"  {len(paires)} paires de quartiers voisins, aucune de même couleur")
    for i, nom in enumerate(NOMS_COULEURS):
        print(f"  {nom} ({QUARTIERS_RVB[i]}) : {', '.join(l for l in sorted(couleur) if couleur[l] == i)}")


if __name__ == "__main__":
    main()
