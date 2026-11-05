# Script : geogname_applique_concordance.py

**Emplacement :** `scripts/ead/geogname_applique_concordance.py`

Applique la [concordance des formes avec les lieux](geogname_concordance_lieux.md)
aux `<geogname>` des IR, **à titre de test** : les IR de
`results/ead/ead_cor/bnr2mnesys/` ne sont pas modifiés, les IR corrigés sont
écrits dans `results/ead/ead_cor/bnr2mnesysgeo/`. C'est l'étape 3 de
l'[indexation géographique](indexation_geographique.md).

Écrit le 2026-10-29. Remplace `geogname_applique_referentiel.py` (méthode
d'avant le référentiel, archivé dans `archives/indexation_geo_avant_referentiel/`).

---

## Utilisation

```bash
# après geogname_concordance_lieux.py
conda run -n rbx-bnr-data python scripts/ead/geogname_applique_concordance.py
conda run -n rbx-bnr-data python scripts/ead/geogname_applique_concordance.py --ir MED_PHO MED_CP
```

| Option | Rôle |
|---|---|
| `--ir` | IR à traiter (code sans `FR595126101_`) ; par défaut tous |
| `--sortie` | Dossier des IR corrigés (défaut `bnr2mnesysgeo`, recréé à chaque exécution ; ne peut pas être `bnr2mnesys`) |

## Entrées

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/bnr2mnesys/*.xml` | IR transformés (lus seulement) |
| `data/geo/concordance_geogname_lieux.csv` | Forme → statut et lieu |
| `data/geo/referentiel_lieux_roubaix.csv` | Lieu → libellé, thésaurus, géométrie |
| `data/geo/referentiel_communes_pays.csv` | Idem pour les lieux hors de Roubaix (thésaurus communes_pays) |

## Méthode

La forme d'un `<geogname>` est retrouvée dans la concordance par son texte
et son `@source`, le texte étant normalisé des deux côtés (`\'` → `'`,
espaces réduits : la concordance garde les formes d'origine, les IR ont
été nettoyés depuis).

| Statut de la forme | Traitement |
|---|---|
| aligné | réécrit : texte = libellé du lieu ; `@source` = thésaurus du lieu ; `@coord` = géométrie du lieu |
| hors référentiel, sans lieu | laissé tel quel, sauf l'`@coord` éventuel, converti de l'ancien JSON en WKT |
| à retirer | `<geogname>` retiré (et son `<controlaccess>` s'il en est vidé) |
| absente de la concordance (corrigée dans l'IR après l'extraction du 2026-09-26), texte = libellé exact d'un lieu | réécrit sur ce lieu, comme une forme « aligné » |
| vide, `@source="chrono"`, introuvable | laissé tel quel |

`@source` : `thesaurus--SLASH--bnr_geo_rbx_quartiers.xml`,
`bnr_geo_rbx_voies.xml`, `bnr_geo_rbx_sites.xml` ou
`bnr_geo_territoires.xml`.
Noms décidés le 2026-11-05 (anciens : `bnr_geo_rbx_rues`, `bnr_geo_rbx_points`,
`bnr_geo_communes_pays`) : *voies* parce que le thésaurus compte aussi cours,
allées, places, impasses, squares, cités, quais… ; *sites* parce que *points*
nommait la forme de la géométrie et non le contenu (églises, hôpitaux,
squares, parc, gare, mairie…) ; *territoires* parce que le thésaurus va du
lieu-dit au continent. La colonne `thesaurus` des référentiels garde ses
valeurs internes (`rues`, `points`, `communes_pays`), traduites par `SOURCES`.

`@coord` : géométrie complète du lieu, `coord_manuel` sinon `coord_wkt`,
**en WKT** (longitude puis latitude, WGS84, 6 décimales, environ 0,1 m) :

| Géométrie | `@coord` |
|---|---|
| point (points, communes_pays) | `POINT (lon lat)` |
| tracé | `LINESTRING (lon lat, …)` |
| tracé en plusieurs tronçons | `MULTILINESTRING ((lon lat, …), (…))` |
| quartier | `POLYGON ((lon lat, …))`, contour refermé |

Format décidé le 2026-11-05, à la place de l'ancien JSON `[lat, lon]` des IR
d'origine (rues : un point géocodé ; quartiers : contour non refermé, dont
3 illisibles, Oran Cartigny et Hutin ×2). Raisons : `@coord` n'existe pas
dans la DTD EAD 2002, c'est une convention maison qu'aucun logiciel ne lit
(Mnesys ne gère pas l'indexation géographique) ; le WKT est standard, dit
son type de géométrie (le JSON ne distinguait un tracé d'un contour que par
le thésaurus) et se lit directement dans QGIS, shapely ou PostGIS. Le tracé
complet est recopié à chaque occurrence du lieu.

Un lieu sans géométrie fait **retirer** l'`@coord` existant : les `@coord`
des IR ne font pas foi.

Les autres attributs (`role`, `normal`…) sont conservés. Deux formes d'un
même lieu dans un même `<controlaccess>` (ancien et nouveau nom d'une voie,
même texte avec et sans `@source`) donnent deux `<geogname>` identiques :
ils sont **gardés** (décision du 2026-10-27) et signalés dans le rapport.

## Sorties

- `results/ead/ead_cor/bnr2mnesysgeo/*.xml` : IR corrigés ;
- `results/ead/indexation/bnr2mnesysgeo_rapport.csv` : une ligne par
  `<geogname>` — `ir`, `c_id`, `texte_avant`, `source_avant`, `coord_avant`
  (type : point, tracé ou contour, tracé multiple), `statut_forme`,
  `lieu_id`, `thesaurus`, `texte_apres`, `source_apres`, `coord_apres`,
  `action` (réécrit, réécrit sans géométrie, laissé, laissé (@coord converti
  en WKT), retiré, ignoré),
  `doublon`.

## Résultat du 2026-10-29

37 IR, 12 212 `<geogname>` :

| Action | `<geogname>` |
|---|---|
| réécrit | 10 923 (rues 9 999, quartiers 656, points 330 — sans géométrie compris) |
| réécrit sans géométrie | 62 (13 lieux : quais d'Anvers / de Calais / de Gravelines, rue Saint-Vincent-de-Paul, squares Pierre Catteau / des Prés / de l'Armistice, rues Christophe Colomb, Centrale, Wagram, Vaucanson, Rossini, Malesherbes) |
| laissé (hors référentiel) | 1 204 |
| laissé (sans lieu) | 21 |
| retiré | 1 (« Pas de mention d'adresse », MED_LET) |
| ignoré (chrono) | 1 |

Contrôles : aucune ligne modifiée hors `<geogname>` ; les 10 924 `@coord`
se lisent en JSON. **203 doublons** dans un même `<controlaccess>` (MED_PAR
118, MED_CP 48…), dont 10 déjà présents tels quels dans les IR source.
IR : 52 Mo → 64 Mo (tracés).

### Avec le thésaurus communes_pays (2026-10-29, soir)

Après le lot D (cf. [geogname_concordance_lieux](geogname_concordance_lieux.md)) :
37 IR, 12 214 `<geogname>`. Il y en a deux de plus qu'avant, parce que deux
`<geogname>` ont été scindés dans les IR source : « France; Belgique » dans MED_VDM,
et « Champagne » dans MED_AFF, remplacé par « Ardennes (France) » et
« Marne (France) ».

| Action | `<geogname>` |
|---|---|
| réécrit | 12 096 |
| réécrit sans géométrie | 63 (les 62 de Roubaix + 1 Quai d'Anvers corrigé dans MED_PAR) |
| laissé (hors référentiel) | 32 (7 formes non tranchées : Flandre, Ostrebosc, « Centre », La Vaux-Marie, Laussage, Ravin de la Hayette, Tolbecq) |
| laissé (sans lieu) | 21 |
| retiré | 1 |
| ignoré (chrono) | 1 |

Réécrits par thésaurus : rues 10 000, communes_pays 1 173, quartiers 655,
points 331. 223 doublons dans un même `<controlaccess>` (« Roubaix » et
« Roubaix (Nord) », par exemple). Contrôle : aucune ligne modifiée hors
`<geogname>`.

### Après le reliquat du lot D (2026-10-29)

12 214 `<geogname>` :
- réécrits : 12 126 ;
- réécrits sans géométrie : 63 ;
- laissés : 21 (sans lieu) ;
- retirés : 3 (« Pas de mention d'adresse », Laussage, Tolbecq) ;
- ignoré : 1.

Réécrits par thésaurus : rues 10 000, communes_pays 1 202, quartiers 656,
points 331. Il reste 223 doublons. Contrôle : aucune ligne modifiée hors `<geogname>`.

Après le lot C (2026-10-29) : 12 128 réécrits (rues 10 002), 19 laissés (sans lieu).

### Passage au WKT (2026-11-05)

12 214 `<geogname>` : 12 171 réécrits, 21 réécrits sans géométrie, 17 laissés
(sans lieu), 1 laissé avec son `@coord` converti en WKT (« Paraboles, avenue
des »), 3 retirés, 1 ignoré. Réécrits par thésaurus : rues 9 996,
communes_pays 1 203, quartiers 656, points 337.
`@coord` : rues 9 256 MULTILINESTRING + 719 LINESTRING, quartiers 656 POLYGON,
points 337 et communes_pays 1 203 POINT. Contrôles : tous les `@coord` se
lisent en WKT et sont valides (OGC) ; tous égaux à la géométrie du
référentiel à 2·10⁻⁶ près ; textes, `@source` et lieux identiques à
l'exécution précédente ; aucune ligne modifiée hors `<geogname>` (sauf les
3 retraits). IR : 52 Mo → 62 Mo.

## Questions ouvertes

- Remplacer `bnr2mnesys` par `bnr2mnesysgeo` : après les SKOS des
  thésaurus. Mnesys ne lit ni ne gère l'indexation géographique (précisé le
  2026-11-05) : rien à attendre de l'éditeur.
- Dédoublonner les `<geogname>` identiques d'un même `<controlaccess>` ?
