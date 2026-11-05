# Script : geogname_ir2kml.py

**Emplacement :** `scripts/ead/geogname_ir2kml.py`

Produit un fichier KML (Google Earth, QGIS, uMap…) des lieux indexés dans
les IR réécrits par [geogname_applique_concordance.py](geogname_applique_concordance.md) :
**un repère par lieu** (thésaurus + libellé), placé d'après le `@coord` des
`<geogname>`, avec le nombre d'occurrences, leur répartition par IR et la
liste des documents qui le citent.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/bnr2mnesysgeo/*.xml` | IR de test, concordance appliquée : `<geogname>` des thésaurus géographiques avec leur `@coord` |

Seuls les `<geogname>` dont le `@source` désigne un thésaurus géographique
sont lus : `bnr_geo_rbx_quartiers`, `bnr_geo_rbx_voies`, `bnr_geo_rbx_sites`,
`bnr_geo_territoires`.

## Résultat

`results/ead/indexation/bnr2mnesysgeo_lieux.kml`, un dossier par thésaurus
(Quartiers, Voies, Sites, Territoires), un style de couleur par dossier.

| Élément du repère | Contenu |
|---|---|
| `name` | Libellé du lieu, tel qu'écrit dans les IR, suivi du nombre d'occurrences entre parenthèses : « Rue Henri Carrette (16) » |
| géométrie | D'après `@coord` (WKT, depuis le 2026-11-05) : point ; tracé (`LineString`) ; tracé multiple (`MultiGeometry`) ; contour refermé (`Polygon`) pour les quartiers |
| `description` | Nombre d'occurrences et répartition par IR, puis les documents (IR, cote, titre, date), limités aux 30 premiers |
| `ExtendedData` | `libelle` (sans le nombre), `thesaurus`, `occurrences`, `ir` ; pour les quartiers, `couleur` (n° de couleur) |

**Couleurs des quartiers :** deux quartiers qui se touchent n'ont jamais la même couleur
(coloriage glouton DSATUR du graphe de voisinage, contours en contact à 1 m près ;
4 couleurs suffisent au 2026-10-30). Chaque quartier porte son style dans le KML ;
dans QGIS, choisir la symbologie « Symboles intégrés » pour le voir, ou catégoriser
sur le champ `couleur`.

Les `<geogname>` sans `@coord` (rue du référentiel sans géométrie, forme
« sans lieu » restée en `@source="rue"`) ne peuvent pas être placés : ils
sont seulement comptés à la fin de l'exécution.

## Utilisation

```bash
conda run -n rbx-bnr-data python scripts/ead/geogname_ir2kml.py
```

Options : `--ir-dir` (dossier des IR, défaut `results/ead/ead_cor/bnr2mnesysgeo`),
`--sortie` (fichier KML).

À relancer après chaque application de la concordance aux IR.

## État au 2026-10-30

37 IR : 732 lieux placés (31 quartiers, 514 rues, 4 points, 183 communes et
pays) pour 12 171 occurrences ; non placés : 21 `<geogname>` de rues sans
géométrie, 18 formes sans lieu.
