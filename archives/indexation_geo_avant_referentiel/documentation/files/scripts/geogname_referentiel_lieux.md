# Script : geogname_referentiel_lieux.py

**Emplacement :** `scripts/ead/geogname_referentiel_lieux.py`

Construit le **référentiel géographique des lieux**
(`data/geo/referentiel_geogname.csv`) : **une ligne par lieu**, futur concept
d'un des quatre thésaurus géographiques de la BnR. C'est l'étage central de
la chaîne de l'[indexation géographique](indexation_geographique.md) :

    formes des IR → concordance formes ↔ lieux → référentiel des lieux → thésaurus SKOS → IR

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `data/geo/concordance_geogname_lieux.csv` | Concordance formes ↔ lieux, produite par [geogname_concordance_lieux.py](geogname_concordance_lieux.md) (à lancer avant) |

## Construction

1. **Thésaurus** de chaque forme, d'après sa catégorie dans la concordance :

   | Catégorie | Thésaurus | Fichier SKOS prévu (et `@source`) |
   |---|---|---|
   | quartier | `quartiers` — quartiers de Roubaix (quartiers historiques MEL) | `bnr_geo_rbx_quartiers.xml` |
   | voie | `rues` — voies de Roubaix, y compris disparues | `bnr_geo_rbx_rues.xml` |
   | espace public, adresse | `points` — espaces publics et adresses de Roubaix | `bnr_geo_rbx_points.xml` |
   | commune, lieu hors Nord, pays, à identifier | `communes_pays` — villes, départements, régions, pays… hors Roubaix (seul thésaurus hiérarchisé, colonne `parent`) | `bnr_geo_communes_pays.xml` |

   Les formes « non toponyme » sont écartées.
2. **Regroupement en lieux** :
   - forme rattachée à une référence : par son identifiant (`ref_id`) — code
     voie TOPO `59512_<code>`, `MEL quartier <id>`, objet BD TOPO, `INSEE
     <code>`… ;
   - forme sans référence (voie disparue, lieu hors Nord, pays, à
     identifier) : par son libellé normalisé, avec un identifiant BnR
     provisoire `bnr:<thésaurus>:<libellé normalisé>` ;
   - une forme sans référence dont le libellé est exactement celui d'un lieu
     rattaché du même thésaurus rejoint ce lieu (signalé).
3. **Libellé** : celui des formes (`libelle_retenu` de la concordance) ; si
   elles divergent, celui de la forme la plus fréquente — ou, si elles ne
   diffèrent que par la casse ou les accents, la variante la mieux écrite.
   **Mnesys relie un `<geogname>` à un concept par le libellé seul** : le
   libellé doit être unique dans chaque thésaurus (contrôlé).
4. **Géométrie** : `coord_manuel` d'une forme s'il y en a un, sinon la `coord`
   de la forme la plus fréquente qui en a une.

## Sortie

`data/geo/referentiel_geogname.csv` — une ligne par lieu, triée par thésaurus
puis libellé :

| Colonne | Contenu |
|---|---|
| `lieu_id` | Identifiant du lieu (référence officielle, sinon `bnr:…` provisoire) ; clé de conservation des colonnes manuelles |
| `thesaurus` | `quartiers`, `rues`, `points` ou `communes_pays` |
| `libelle` | Libellé du concept (futur `prefLabel`), unique dans le thésaurus |
| `a_traiter` | **Ce qui reste à régler** sur le lieu ; vide quand il n'y a rien à faire ou dès que `validation` est remplie |
| `nom_ancien` | Ancien nom (voie renommée) |
| `variantes` | Autres formes du lieu dans les IR, remises à l'endroit (futurs `altLabel`) |
| `categorie` | Catégorie de la concordance |
| `ref_nom`, `ref_origine` | Objet de référence et sa source |
| `geometrie`, `coord` | Provenance et coordonnées `[lat, lon]` (tracé, polygone ou point) |
| `coord_manuel` | **À la main**, prioritaire sur `coord` |
| `coord_wkt` | La géométrie (`coord_manuel`, sinon `coord`) en **WKT**, WGS84, ordre **`lon lat`**, avec son type : `POINT`, `LINESTRING`, `MULTILINESTRING` ou `POLYGON` (cf. ci-dessous) |
| `parent` | **À la main** : lieu englobant (thésaurus `communes_pays` seulement : commune → département → région → pays) |
| `occurrences`, `ir` | Nombre de `<geogname>`, IR concernés |
| `validation`, `remarque` | **À la main** |

Les colonnes `validation`, `remarque`, `coord_manuel` et `parent` sont
conservées d'une exécution à l'autre (clé : `lieu_id`).

## Géométrie en WKT (`coord_wkt`, depuis le 2026-10-27)

`coord` garde le format des `@coord` des IR, lu par Mnesys : `[lat, lon]`,
sans type — une rue et un quartier s'y écrivent de la même façon. `coord_wkt`
donne la même géométrie en **WKT**, en **WGS84**, ordre **`lon lat`**, avec
son **type explicite**. Le type découle du thésaurus et de la structure, pas
de la forme des coordonnées : un quartier est un `POLYGON` ; une rue en boucle
reste un `LINESTRING` ; une rue en plusieurs tronçons est un
`MULTILINESTRING`.

| Thésaurus | `POLYGON` | `LINESTRING` | `MULTILINESTRING` | `POINT` |
|---|---|---|---|---|
| quartiers | 31 | – | – | – |
| rues | – | 393 | 102 | 10 (rues sans tracé : point d'origine) |
| points | – | – | – | 7 |

Les lieux sans géométrie (67 rues, 197 communes et pays) n'ont pas de
`coord_wkt`. Toutes les géométries sont valides et dans l'emprise de Roubaix
(2026-10-27). Exemple : `POLYGON ((3.173608 50.700359, 3.173523 50.700492, …))`
(Alma Gare). Les deux colonnes coexistent tant que le format cible de
`@coord` (ou d'un `<geographiccoordinates>` EAD3) n'est pas arrêté avec
l'éditeur de Mnesys.

## Relecture : la colonne `a_traiter`

Un seul filtre : **`a_traiter` non vide**. Motifs :

| Motif | Lieux (2026-10-27) |
|---|---|
| `n forme(s) à traiter dans la concordance` (renvoi vers la concordance) | 167 |
| `pas de géométrie : saisir coord_manuel` (thésaurus `quartiers`, `rues`, `points`) | 69 |
| `pas de référence actuelle (nom ancien ?)` | 66 |
| `lieu à identifier` | 46 |
| `libellés divergents entre formes : …` | 3 (Alsace, Frasez, Dammartin) |
| `n forme(s) sans référence rattachée(s) au lieu par son seul libellé : vérifier` | 2 |
| `libellé en double dans le thésaurus` | 0 |

## Premier passage (2026-10-27)

**807 lieux** pour 12 210 `<geogname>` :

| Thésaurus | Lieux | `<geogname>` | Avec géométrie | À traiter |
|---|---|---|---|---|
| quartiers | 31 | 652 | 31 | 8 |
| rues | 572 | 10 031 | 505 | 107 |
| points | 7 | 345 | 7 | 6 |
| communes_pays | 197 | 1 182 | – | 46 |

Les 31 quartiers sont exactement les 31 quartiers historiques de la MEL.
Le thésaurus `communes_pays` n'a pas encore de hiérarchie (`parent` vide) ni de
qualificatif uniforme (« Amiens », « Anvers (Pays-Bas) » à revoir).

## Suite

Générer les quatre thésaurus SKOS à partir de ce référentiel, puis réécrire
les `<geogname>` des IR (libellé, `@source`, `@coord`) : scripts à écrire.

## Utilisation

Depuis la racine du dépôt, après [geogname_concordance_lieux.py](geogname_concordance_lieux.md) :

    python scripts/ead/geogname_referentiel_lieux.py
