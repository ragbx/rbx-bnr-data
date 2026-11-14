# Script : geogname2skos.py

**Emplacement :** `scripts/ead/geogname2skos.py`

Génère les **quatre thésaurus SKOS géographiques** de la BnR à partir des
référentiels, et non des IR : tous les lieux y figurent, cités ou non dans
les IR, avec leur géométrie, leurs anciens noms, les formes rencontrées dans
les IR et, pour les territoires, leur hiérarchie. Modèle décidé le 2026-11-06.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `data/geo/referentiel_lieux_roubaix.csv` | Quartiers, voies (`thesaurus` = `rues`) et sites (`thesaurus` = `points`) ; les lieux écartés (`_ecartes.csv`) ne sont pas exportés |
| `data/geo/referentiel_communes_pays.csv` | Territoires (`thesaurus` = `communes_pays`) |
| `data/geo/concordance_geogname_lieux.csv` | Formes des IR alignées sur chaque lieu (`skos:hiddenLabel`) |

## Sorties

`results/ead/indexation/thesaurus/`, en RDF/XML, à côté des thésaurus produits
par [controlaccess2skos.py](controlaccess2skos.md) et avec la même convention
d'URI (`https://www.bn-r.fr/thesaurus/<nom>#<libellé normalisé>`) :

| Fichier | Schéma | Concepts au 2026-11-06 |
|---|---|---|
| `bnr_geo_rbx_quartiers.xml` | `…/thesaurus/geo_rbx_quartiers` | 31 |
| `bnr_geo_rbx_voies.xml` | `…/thesaurus/geo_rbx_voies` | 938 (2 squares venus des sites le 2026-11-06) |
| `bnr_geo_rbx_sites.xml` | `…/thesaurus/geo_rbx_sites` | 3 (Parc Barbieux, Hôtel de ville, Gare de Roubaix) |
| `bnr_geo_territoires.xml` | `…/thesaurus/geo_territoires` | 207 |

Les noms de fichier sont ceux du `@source` des `<geogname>` des IR
(`thesaurus--SLASH--bnr_geo_rbx_voies.xml`…), repris de `SOURCES` dans
[geogname_applique_concordance.py](geogname_applique_concordance.md).
Le fichier est trié par URI : deux exécutions sur les mêmes données donnent
un fichier identique.

## Contenu d'un concept

| Propriété | Contenu |
|---|---|
| `skos:prefLabel@fr` | Libellé du lieu, c'est-à-dire le texte des `<geogname>` des IR (seul lien entre l'IR et le concept) |
| `skos:notation` | `lieu_id` (code BAN, clé BD TOPO, id MEL, code INSEE, `dep` + code, QID…) |
| `dct:type@fr` | Catégorie : rue, cour, place, quartier, Eglise, commune, département, pays… |
| `skos:altLabel@fr` | Anciens noms (colonne `nom_ancien`, remplie depuis `data/geo/noms_anciens.csv`) |
| `skos:hiddenLabel@fr` | Formes des IR alignées sur le lieu, différentes du libellé et des anciens noms (« Gare, rue de la », « Jean Lebas, avenue »…) |
| `geo:hasGeometry` → `<concept>_geometrie` / `geo:asWKT` | GeoSPARQL : `coord_manuel` sinon `coord_wkt`, WKT longitude latitude, 6 décimales — exactement le `@coord` des IR. `geo:wktLiteral` sans CRS explicite = CRS84 (WGS84 lon lat) |
| ~~`skos:related`~~ | **Retiré le 2026-11-14** : voies et sites → quartiers traversés. Dans Mnesys, un lien vers un autre thésaurus devient un terme associé vide (constaté à l'import du fichier d'essai) ; remplacé par la `scopeNote` |
| `skos:scopeNote@fr` | Sites : « Quartier(s) : X » (quartier du point, colonne `quartiers`). Voies : « Quartier(s) : A, B » = quartier(s) de **rattachement** (`retenu_31` de `data/geo/rues_rattachement.csv` pour une rue ambiguë, sinon quartier principal ; fonction `quartiers_des_rues` de [quartiers2kml.py](quartiers2kml.md)). Ajouté le 2026-11-14 : Mnesys affiche la `scopeNote` en « note » et ne résout pas un `skos:related` vers un autre thésaurus |
| `skos:historyNote@fr` | Lieux disparus : « Dates : 1877 – années 1990. » |
| `dct:isReplacedBy` | Lieu disparu → voie qui le remplace (`remplacee_par`) |
| `skos:broader` / `skos:narrower` | Territoires : d'après la colonne `parent` (commune → département → France) |
| `skos:exactMatch` | Territoires : Wikidata (`http://www.wikidata.org/entity/Q…`) et data.bnf.fr (`https://data.bnf.fr/ark:/12148/cb…`) |
| `skos:topConceptOf` | Tous les lieux de Roubaix ; les territoires sans parent |

Exemple :

```xml
<rdf:Description rdf:about="https://www.bn-r.fr/thesaurus/geo_rbx_voies#avenue_jean_lebas">
  <rdf:type rdf:resource="http://www.w3.org/2004/02/skos/core#Concept"/>
  <rdf:type rdf:resource="http://www.opengis.net/ont/geosparql#Feature"/>
  <skos:prefLabel xml:lang="fr">Avenue Jean Lebas</skos:prefLabel>
  <skos:notation>59512_2800</skos:notation>
  <dct:type xml:lang="fr">avenue</dct:type>
  <skos:altLabel xml:lang="fr">Rue de la Gare</skos:altLabel>
  <skos:hiddenLabel xml:lang="fr">Gare, rue de la</skos:hiddenLabel>
  <geo:hasGeometry rdf:resource="…#avenue_jean_lebas_geometrie"/>
  <skos:related rdf:resource="https://www.bn-r.fr/thesaurus/geo_rbx_quartiers#esperance"/>
  …
</rdf:Description>
```

## Contrôles du 2026-11-06

- 1 187 concepts (1 179 après la réduction des sites à 3, même jour) ; aucun `broader`, `narrower`, `related` ou `isReplacedBy`
  vers un concept absent ; aucun libellé en double dans un schéma ; aucun
  concept à la fois de tête et rattaché à un parent ; tous les WKT se lisent.
- Les 12 192 `<geogname>` des thésaurus géographiques des IR de test
  (`bnr2mnesysgeo`) ont tous leur concept (même libellé, même schéma), avec
  un `@coord` identique au `geo:asWKT` du concept.
- 29 voies sans géométrie (pas de `geo:hasGeometry`).

Pour compléter la hiérarchie, 23 parents ont été ajoutés le 2026-11-06 au
thésaurus des territoires : 22 départements (code, QID, BnF et point de
Wikidata, libellé contrôlé) et Coudray (Eure), commune d'Ostrebosc ; ils ne
sont pas cités dans les IR.

## Thésaurus des voies pour Mnesys (2026-11-14)

Décision : le thésaurus des rues chargé dans Mnesys **remplace
`bnr_geo_rbx_voies`** (même fichier, même schéma, mêmes URI que celles des
IR), avec les quartiers de rattachement en `skos:scopeNote` (pas de concepts
quartiers dans ce fichier). Le test de [rues_quartiers2skos.py](rues_quartiers2skos.md)
(schéma `geo_rbx_rues_quartiers`) ne sert plus que de trace.

Fichier d'essai : `results/ead/indexation/mnesys_test/voies_note/bnr_geo_rbx_voies.xml`
(`--sortie`), 937 voies, 908 notes (les 29 voies sans tracé n'en ont pas),
mêmes URI et libellés que le thésaurus en service (seul retrait : Parc de la
Potennerie, devenu un site). Il garde la géométrie GeoSPARQL et les
`hiddenLabel`. Premier import (2026-11-14) : les `skos:related` vers les
quartiers traversés apparaissaient comme termes associés vides, « gênant » :
retirés du script le même jour (voies et sites), fichier d'essai régénéré.
Second import (2026-11-14) : **correct** — géométrie GeoSPARQL tolérée,
plus de termes associés vides, note des quartiers affichée. **Décision : un
seul fichier** par thésaurus, le même pour Mnesys et pour l'archive (pas de
version allégée).

## Utilisation

```bash
conda run -n rbx-bnr-data python scripts/ead/geogname2skos.py
conda run -n rbx-bnr-data python scripts/ead/geogname2skos.py --sortie results/ead/indexation/mnesys_test/voies_note   # essai, sans toucher aux thésaurus en service
```

À relancer après toute modification des référentiels ou de la concordance.

## Cohabitation avec controlaccess2skos.py

[controlaccess2skos.py](controlaccess2skos.md) produit un thésaurus par
valeur de `@source` trouvée dans `bnr2mnesys`, dans le même dossier. Depuis le
2026-11-06, il ignore les sources `bnr_geo_*` : les fichiers de ce script ne
sont pas écrasés, même quand `bnr2mnesysgeo` remplacera `bnr2mnesys`.
