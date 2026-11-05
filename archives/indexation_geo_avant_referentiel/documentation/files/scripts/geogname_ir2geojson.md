# Script : geogname_ir2geojson.py

**Emplacement :** `scripts/ead/geogname_ir2geojson.py`

Exporte en **GeoJSON** les `<geogname>` géoréférencés d'un ou plusieurs IR,
pour les contrôler dans un SIG (QGIS…). Outil de contrôle de
l'[indexation géographique](indexation_geographique.md), en particulier des IR
produits par [geogname_applique_referentiel.py](geogname_applique_referentiel.md).

---

## Traitement

Une **entité par toponyme distinct** (même texte, même `@source`, même
`@coord`), qui regroupe toutes ses occurrences. Le `@coord` des IR, en
`[lat, lon]`, est retourné en `[lon, lat]` (ordre GeoJSON, WGS84 /
EPSG:4326) :

| `@coord` | Géométrie |
|---|---|
| `[lat, lon]` | `Point` |
| `[[lat, lon], …]` | `Polygon` pour un quartier (contour refermé au besoin), `LineString` sinon — une rue en boucle reste une ligne |
| `[[[lat, lon], …], …]` | `MultiLineString` |

Propriétés : `libelle`, `source` (`@source` sans le préfixe
`thesaurus--SLASH--`), `thesaurus` (`quartiers`, `rues`, `points`,
`communes_pays`, ou vide si le `<geogname>` n'est pas encore rattaché à un
thésaurus), `occurrences`, `unitid` (cotes des composants, séparées par
` | `), `ir`.

Les `<geogname>` sans `@coord` (communes et pays…) ou au `@coord` illisible
ne sont pas exportés ; ils sont comptés à l'écran.

Dans QGIS, un fichier à géométries mixtes s'ouvre en proposant **une couche
par type de géométrie** (points, lignes, polygones). Filtrer sur `thesaurus`
vide pour voir les `<geogname>` encore à l'ancienne.

## Utilisation

Depuis la racine du dépôt :

    python scripts/ead/geogname_ir2geojson.py <IR.xml> [<IR.xml> …] -o <sortie.geojson>

## Premier export (2026-10-27)

Version « sans réserve » de MED_PHO :

    python scripts/ead/geogname_ir2geojson.py results/ead/ead_cor/bnr2mnesysgeo_sans_reserve/FR595126101_MED_PHO.xml \
        -o results/ead/indexation/MED_PHO_sans_reserve.geojson

193 entités (934 `<geogname>`) : 29 polygones, 90 lignes, 38 multi-lignes,
36 points ; 147 rattachées à un thésaurus (rues 129, quartiers 17, points 1),
46 encore à l'ancienne. Non exportés : 82 sans `@coord` (34 communes et pays,
48 non rattachés) et 1 `@coord` illisible (`Hutin`, polygone tronqué dans
l'IR). Lu par GDAL (`ogrinfo`) : EPSG:4326, emprise de Roubaix. Le polygone
MEL du quartier **Hommelet**, qui se recoupait (défaut de la donnée source),
est depuis réparé en amont, dans la concordance : plus aucune géométrie
invalide.

Après retrait des rues réduites à un point (`--sans-rues-ponctuelles`,
cf. [geogname_applique_referentiel](geogname_applique_referentiel.md)) :
**158 entités** (871 `<geogname>`) — 29 polygones, 90 lignes, 38
multi-lignes, **1 seul point** (Parc Barbieux) ; 146 rattachées à un
thésaurus, 12 encore à l'ancienne (contours de quartier d'origine). Puis,
après retrait de tous les `<geogname>` non appliqués : **146 entités** (669
`<geogname>`) — 17 polygones, 90 lignes, 38 multi-lignes, 1 point —, toutes
rattachées à un thésaurus.
