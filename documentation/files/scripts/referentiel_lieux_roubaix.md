# Script : referentiel_lieux_roubaix.py

**Emplacement :** `scripts/ead/referentiel_lieux_roubaix.py`

Construit le **référentiel des lieux de Roubaix**
(`data/geo/referentiel_lieux_roubaix.csv`) **à partir des seules sources de
référence, sans regarder les instruments de recherche** : une ligne par lieu,
futur concept d'un des thésaurus géographiques de la BnR.

C'est la première des deux étapes de la méthode retenue le 2026-10-27 pour
l'[indexation géographique](indexation_geographique.md) :

1. **construire le référentiel** à partir des sources (ce script) ;
2. **aligner l'existant** (les `<geogname>` des IR) sur ce référentiel — à
   faire.

Il remplace l'ancien référentiel déduit des formes des IR
(`data/geo/referentiel_geogname.csv`, 807 lieux), archivé le 2026-10-29
avec toute l'ancienne chaîne dans `archives/indexation_geo_avant_referentiel/`.
Le script est autonome (plus d'import des anciens scripts de concordance).

---

## Utilisation

```bash
# une fois par édition de la BD TOPO : extrait les objets de Roubaix du GeoPackage du Nord
conda run -n rbx-bnr-data python scripts/ead/referentiel_lieux_roubaix.py --extraire <BDT_3-5_GPKG_LAMB93_D059-…gpkg>

# ensuite, autant de fois que nécessaire
conda run -n rbx-bnr-data python scripts/ead/referentiel_lieux_roubaix.py
```

Le GeoPackage de la BD TOPO du Nord (4,4 Go décompressé) n'est pas dans le
dépôt. Le 2026-10-27, il a été décompressé dans
`~/.cache/rbx-bnr-data/bdtopo/` depuis l'archive IGN
`BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D059_2026-06-15.7z`. Seul l'extrait
`data/geo/bdtopo_roubaix_extrait.csv` est versionné.

Le CSV de sortie est souvent ouvert dans LibreOffice : le script s'arrête si
le fichier de verrou `.~lock.referentiel_lieux_roubaix.csv#` (ou celui du
fichier des écartés) existe.

## Sources (`data/geo/`)

| Fichier | Source | Rôle |
|---|---|---|
| `ban_roubaix.csv` | Base Adresse Nationale, lignes `code_insee = 59512` du fichier `adresses-59.csv.gz` (adresse.data.gouv.fr, mis à jour le 2026-10-27) : 26 398 adresses, 773 voies | **Pivot des voies** : identifiant BAN, nom certifié par la commune, nombre et position des adresses |
| `bdtopo_roubaix_extrait.csv` | BD TOPO IGN, édition 2026-06-15 ; produit par `--extraire` | Tracés des voies nommées (917), zones d'activité ou d'intérêt (322), zones d'habitation (181), équipements de transport hors parkings (22), ponts et écluses, cours d'eau, contour de la commune ; WKT Lambert 93, 2D |
| `topo_roubaix.csv` | Fichier TOPO de la DGFiP (ex-FANTOIR) ; copie figée (téléchargée par l'ancien `geogname_concordance.py`, archivé) | Liste officielle des voies et lieux-dits (1 623), sans géométrie ; dates de création |
| `filaire_voies_roubaix.geojson` | Filaire des voies de la ville (MEL) | Tracés de secours et autres graphies |
| `quartiers_historiques_mel.geojson` | Quartiers historiques (MEL) | Les 31 quartiers |
| `osm_roubaix_noms.json` | OpenStreetMap ; copie figée (téléchargée par l'ancien `geogname_concordance.py`, archivé) | `old_name` → `nom_ancien`, `alt_name` → `variantes` |

## Construction

### Quartiers (`quartiers`)

Les 31 **quartiers historiques de la MEL**, et eux seuls : les anciens
quartiers fusionnés par la MEL (Fresnoy, Mackellerie, Sartel, Hutin…) ne sont
pas des lieux distincts (décision du 2026-10-27). Une zone d'habitation BD
TOPO de nature « Quartier » portant le même nom, sans l'article initial
(« l'Épeule »), leur est rattachée : elle donne la graphie du libellé, avec
les accents et la casse, sans article. `ALIAS_QUARTIERS` rattache aussi
« Sainte-Élizabeth » (orthographe de la BD TOPO) au quartier STE ELISABETH.

Les cinq quartiers sans zone BD TOPO qui donne leur graphie ont un libellé
fixé dans `LIBELLES_QUARTIERS`, d'après les voies de même nom dans la BAN
(décision du 2026-10-27). Ce libellé l'emporte aussi sur celui de la BD TOPO :

| Nom MEL | Libellé |
|---|---|
| CHEMIN NEUF | Chemin Neuf |
| CROUY | Crouy |
| EDOUARD VAILLANT | Édouard Vaillant |
| LINNE | Linné |
| STE ELISABETH | Sainte-Élisabeth |

### Rues (`rues`)

1. **BAN** : une voie par identifiant BAN (`59512_<code FANTOIR>` ou
   identifiant propre à la BAN, `59512_9rc416`) ; l'identifiant FANTOIR de
   ses adresses la relie à TOPO.
2. **Voies nommées BD TOPO** : rattachées par `identifiant_voie_ban`, sinon par
   le nom (au plus proche des homonymes), sinon nouvelles voies.
3. **Filaire MEL** : rattaché par le nom ; à défaut, **par le tracé** s'il
   s'écrit autrement (`noms_proches` : même type de voie et nom semblable —
   coquille, abréviation, prénom en plus — ou type interchangeable, comme
   « Avenue » / « Rue du Onze Novembre » ; un pont ou une place reste
   distinct). On retient la voie dont le tracé BD TOPO, avec une marge de 15 m,
   recouvre au moins 60 % du tronçon, sinon la plus proche à moins de 50 m.
   Sinon, nouvelle voie « connue du filaire MEL seul ».
4. **OSM** : `old_name` et `alt_name` des voies au nom identique.
5. **TOPO** : chaque entrée est rattachée par identifiant, sinon par le nom
   à une voie ou à un point. À défaut :
   - **ensemble d'habitation** (courée, cité, résidence, pavillon, groupe,
     bâtiment, fort, rangée), reconnu par sa nature (`CRS`, `VC`, `CITE`,
     `RES`, `CR`…) ou par son libellé (numéro, voie citée : « DELBAR RUE
     MARCEAU », « RUE DE TUNIS COUR SEYS ») : **écarté s'il n'est attesté ni
     par la BAN ni par la BD TOPO** (décision du 2026-10-27), soit 877 entrées.
     Une date dans le libellé (« SQ DU 8 MAI 1945 ») ne compte pas comme un
     numéro (`DATE_LIBELLE`, correctif du 2026-10-29 : le Square du 8 Mai 1945
     était écarté à tort) ;
   - autre voie : gardée comme « connue de TOPO seul », sans géométrie
     (voies disparues ou jamais ouvertes : Rue Wagram, Rue Christophe Colomb,
     Quai d'Anvers…).
6. **Hors Roubaix** : une voie inconnue de la BAN et de TOPO est retirée si
   plus de la moitié de son tracé sort de la commune ou si son nom cite une
   commune voisine (« Bd Gambetta (Tourcoing) »).

Une place, un square ou une résidence de la BD TOPO (zone) qui porte le nom
d'une voie située à moins de 50 m est rattaché à cette voie (`cleabs_bdtopo`,
`variantes`) au lieu de devenir un point.

### Points (`points`)

Les autres lieux nommés de la BD TOPO : espaces publics sans voie de même
nom, équipements (écoles, églises, hôpitaux, mairie…), stations de métro et
de tramway, gare, ponts, écluses, cours d'eau (coupés au contour de la
commune), zones d'habitation qui ne sont pas des quartiers MEL. Les objets de
même nom distants de moins de 100 m forment un seul lieu (bâtiments d'un même
établissement). Les zones d'habitation qui ne sont pas des quartiers MEL
sont classées `résidence` si leur nom commence par Résidence, Pavillon,
Tour…, sinon `ensemble d'habitation` (les Aloès, la Grive, Georges
Feydeau…), sans signalement (décision du 2026-10-27).

### Libellé

Nom de la BAN, sinon de la BD TOPO, du filaire MEL, de TOPO. Un nom qui
n'existe qu'en majuscules (TOPO, certains noms de la BD TOPO) est remis en
forme (`mettre_en_forme` : « QUAI D ANVERS » → « Quai d'Anvers », « R BLIDA » →
« Rue Blida ») et signalé, car il n'a pas d'accents. `libelle_manuel`, si
rempli, l'emporte. Un `<geogname>` n'est relié à un concept que par son
libellé : un libellé en double dans un thésaurus est signalé.

### Géométrie

Une forme par thésaurus (décision du 2026-10-28) : **quartiers = polygones,
rues = lignes ou multilignes, points = points**.

Tracé BD TOPO, sinon filaire MEL, sinon centre des adresses BAN (voies) ;
points : un point pris dans l'emprise BD TOPO (`point_on_surface`, toujours
sur l'objet, contrairement au centroïde), l'emprise elle-même étant gardée
dans `emprise_wkt` ; contour MEL (quartiers ; réparé par `make_valid`,
dont seules les parties surfaciques sont gardées, cf. Hommelet). Calculs en
Lambert 93 ; `coord_wkt` en WKT, **WGS84, ordre lon lat**, 6 décimales.
`coord_manuel` (même convention), si rempli, la remplace dans le GeoJSON.

**Rattachement aux quartiers** (colonne `quartiers`) : quartiers MEL contenant
au moins 10 % du lieu (longueur, surface), sinon le principal.

### Doublons fusionnés

Table `FUSIONS` du script (lieu absorbé → lieu gardé, décision du
2026-10-28) : le lieu gardé est de préférence celui qui a un identifiant
officiel (BAN, TOPO) ; il reçoit les identifiants, sources, noms et tracés
du lieu absorbé. **Les places et squares restent des rues (lignes)** : d'un
espace public BD TOPO absorbé, on ne garde que l'identifiant et le nom (en
variante), jamais l'emprise.

| Absorbé | Gardé |
|---|---|
| Place des Martyrs de la Résistance (MEL) | Place des Martyrs Resis (TOPO) → libellé MEL |
| Square de la Savonnerie Victor Vaissier (MEL) | Square Victor Vaissier (TOPO) → libellé MEL |
| Square Jean Pennel Rés. les Muséal (TOPO) | Square Jean Pennel (BAN) |
| Parvis St François Rés. la Bruyère (TOPO) | Parvis Saint François (TOPO) |
| Square Louis Suzanne et Andrée Herbeaux (espace public BD TOPO) | Square Louis Suzanne An Herbeaux (TOPO, sans tracé) |
| Place des 3 Ponts (MEL), Place des Trois Ponts (espace public BD TOPO) | Place des trois Ponts (BAN) |
| Place Saint-Élisabeth (espace public BD TOPO) | Place Ste Elisabeth (MEL) |
| Square Armistice 8 Mai 1945 (espace public BD TOPO, 2026-10-29) | Square du 8 Mai 1945 (TOPO, sans tracé) → libellé manuel « Square de l'Armistice du 8 Mai 1945 » |

Un square connu de la seule BD TOPO, sans voie dans aucune source, peut être
classé en rue sans géométrie par la table `ESPACES_EN_RUES` (cleabs ; règle
« les squares sont des rues », rappelée le 2026-10-29) ; elle est vide depuis
le 2026-10-30.

**Squares en points (décision du 2026-10-30).** Les squares sans tracé de
voie dans aucune source sont des points, pas des rues : une rue est
toujours une ligne, et ces squares n'en ont pas. Table `ESPACES_EN_POINTS`
(lieu gardé par `FUSIONS`) : la voie TOPO garde son identifiant, passe dans
le thésaurus des points et reçoit l'emprise de l'espace public BD TOPO
qu'elle absorbe (point dans l'emprise, emprise en `emprise_wkt`) : Square
Louis Suzanne et Andrée Herbeaux (59512_3203), Square de l'Armistice du
8 Mai 1945 (59512_2686). Square Pierre Catteau, connu de la seule BD TOPO,
y est un espace public ordinaire (sorti d'`ESPACES_EN_RUES`).

**Squares cités par les IR : retour dans les rues (décision du 2026-11-06).**
Square Pierre Catteau (retour dans `ESPACES_EN_RUES`) et Square de l'Armistice
du 8 Mai 1945 (retiré d'`ESPACES_EN_POINTS`) redeviennent des rues. Faute de
tracé de voie, leur point est transformé en **ligne minuscule** : segment de
2 m est-ouest centré sur l'ancien point (pris dans l'emprise BD TOPO), saisi
dans `coord_manuel` (« les points deviennent des lignes, aussi petites
soient-elles »). Seul Square Louis Suzanne et Andrée Herbeaux, non cité dans
les IR, reste un point. Comme pour toute géométrie seulement manuelle,
`quartiers` et `source_geometrie` restent vides pour ces deux squares.
Référentiel : 31 quartiers, 938 rues, 11 points ; backups
`*_backup_20261106_avant_squares`.

**Points réduits aux sites utiles (décision du 2026-11-06).** Le thésaurus
des points (sites) ne garde que Parc Barbieux et Hôtel de ville, cités dans
les IR, et Gare de Roubaix, gardée bien que non citée. Les 8 autres (3 églises,
2 hôpitaux, théâtre, conservatoire, Square Louis Suzanne et Andrée Herbeaux)
sont validés « ne pas conserver » et passent dans `_ecartes.csv`.
Référentiel : **972 lieux (31 quartiers, 938 rues, 3 points)**, 495 écartés ;
backups `*_backup_20261106_avant_sites`.

### Couche historique (2026-10-29)

Deux tables tenues à la main, relues à chaque exécution (après les fusions) :

- **`data/geo/noms_anciens.csv`** (`lieu_id`, `libelle`, `nom_ancien`,
  `date_debut`, `date_fin`, `source`, `remarque`) : anciens noms d'une voie
  actuelle, ajoutés à sa colonne `nom_ancien` (avec ceux d'OpenStreetMap).
  23 au 2026-10-29 (Rue de la Gare → Avenue Jean Lebas, Rue du Moulin → Rue
  Jean Moulin, Rue Pauvrée → Rue Jean Monnet…), chacun justifié par une
  notice des IR ;
- **`data/geo/lieux_disparus.csv`** (`lieu_id`, `libelle`, `categorie`,
  `date_debut`, `date_fin`, `remplacee_par`, `trace_wkt`, `source_trace`,
  `source`, `remarque`) : voies disparues. Une voie connue d'une source (TOPO)
  y reçoit ses dates, sa voie de remplacement et son tracé ; une voie connue
  des seuls IR y est créée (`bnr:<nom normalisé>`, source « historique »).
  Le tracé historique (WGS84, WKT) prime sur les autres géométries
  (`source_geometrie` = « tracé historique »). 8 au 2026-10-29 : Rue des
  Longues Haies (créée, tracé saisi à la main), Rues Notre-Dame, Darbo et
  Cadeau (tracés OpenHistoricalMap), Rue Saint-Vincent-de-Paul et Quais
  d'Anvers, de Calais et de Gravelines (sans tracé, remplacés par l'Avenue des
  Nations Unies et la Promenade au Fil de l'Eau). 11 au 2026-10-30, après
  validation dans QGIS (`data/geo/validation_historique_20261030.qgz`) :
  tracés ajoutés aux trois quais (OpenHistoricalMap pour Anvers, alias
  « QU DE CALAIS » des tronçons BD TOPO pour Calais, OpenStreetMap pour
  Gravelines) ; Rues de l'Arc, Saint-Honoré et de la Clef ajoutées (tracés
  OpenHistoricalMap d'après le cadastre de 1845, disparues avec l'Avenue des
  Nations Unies). Saint-Vincent-de-Paul reste sans tracé.

**Règle (décision du 2026-10-29) : un concept par voie physique.** Un simple
changement de nom (même tracé) reste un seul concept, libellé au nom
actuel, l'ancien nom dans `noms_anciens.csv`. Une voie détruite ou réunie
dans un aménagement plus large garde son propre concept, dans
`lieux_disparus.csv`, relié à sa remplaçante par `remplacee_par`.

Les décisions viennent pour l'essentiel de l'ancien dépôt `~/dev/rbx-geo2`
(validées les 27-30/09/2026) ; fichier de reprise
`data/geo/reprise_rbx_geo2_20261029.csv`. Les tracés corrigés de deux voies
actuelles sont en `coord_manuel` : Rue de Menin (OpenHistoricalMap, 341 m au
lieu de 53) et Avenue de Jussieu (OpenStreetMap ; la BD TOPO la place à
850 m).

### Catégories fixées à la main

Table `CATEGORIES_FORCEES` du script (`lieu_id` → catégorie), pour les lieux
dont ni le type de voie BD TOPO ni le premier mot du libellé ne donnent la
bonne catégorie (décisions du 2026-10-28) : Grand Place → `place` ;
Grande Rue, La Closeraie, Le Frome, Rue Fléchier → `rue` ; Deuxième Cour Jean
Ferret → `cour` ; Contre allée quai de Boulogne → `allée` ; Rond Point de la
Limite → `rond-point` ; Gare de Roubaix → `Gare`. Les changements de
catégorie se notent dans le script, pas dans `a_traiter` (recalculée à
chaque exécution).

## Sorties

Le référentiel est toujours produit en **deux formats, CSV et GeoPackage**,
réécrits ensemble à chaque exécution.

- `data/geo/referentiel_lieux_roubaix.csv` — une ligne par lieu, triée par
  thésaurus puis libellé ; c'est là (et dans le CSV des écartés) que se
  saisissent les colonnes manuelles ;
- `data/geo/referentiel_lieux_roubaix.gpkg` — les mêmes lieux et colonnes,
  en WGS84 (EPSG:4326), une couche par thésaurus : `quartiers`
  (MultiPolygon), `rues` (MultiLineString), `points` (MultiPoint), plus
  `ecartes_rues` et `ecartes_points` pour les lieux écartés. La géométrie est
  `coord_manuel` si remplie, sinon `coord_wkt` ; un lieu sans géométrie
  (voies connues de TOPO seul) y figure avec une géométrie vide. Fichier
  réécrit à chaque exécution : **ne pas y saisir**, les modifications
  seraient perdues ;
- `data/geo/referentiel_lieux_roubaix_ecartes.csv` — les lieux validés
  **« ne pas conserver »**, mis de côté avec toutes leurs colonnes (mêmes
  colonnes que le référentiel). Ils sont recalculés depuis les sources à
  chaque exécution ; leurs colonnes manuelles sont relues dans ce fichier :
  changer leur `validation` les fait revenir dans le référentiel.

| Colonne | Contenu |
|---|---|
| `lieu_id` | Identifiant : identifiant BAN (`59512_…`), code TOPO, `bdtopo:<cleabs>`, `mel:<nom normalisé>`, `mel_quartier:<id>` ; clé de conservation des colonnes manuelles |
| `thesaurus` | `quartiers`, `rues` ou `points` |
| `categorie` | Type de voie BD TOPO (rue, cour, cité…), nature BD TOPO (École primaire, Église, Square…), `résidence`, `ensemble d'habitation`, `quartier` |
| `libelle` | Libellé du concept (futur `prefLabel`) |
| `a_traiter` | **Ce qui reste à régler** ; vide dès que `validation` est remplie |
| `variantes` | Autres graphies des sources (BD TOPO, MEL, nom collaboratif, `alt_name` OSM, zone rattachée) |
| `nom_ancien` | `old_name` OSM |
| `date_debut`, `date_fin`, `remplacee_par` | Voie disparue (`lieux_disparus.csv`) : dates d'existence (texte libre) et `lieu_id` de la voie qui l'a remplacée |
| `quartiers` | Quartiers MEL du lieu |
| `sources` | BAN, BD TOPO, TOPO, MEL, OSM |
| `nom_ban`, `nom_bdtopo`, `nom_mel`, `nom_topo` | Nom dans chaque source |
| `id_ban`, `id_topo`, `cleabs_bdtopo`, `id_mel` | Identifiants dans chaque source (séparés par « \| ») |
| `nb_adresses` | Nombre d'adresses BAN |
| `date_creation_topo` | Date de création de l'article TOPO, si postérieure à la création initiale du fichier (1987-01-01) |
| `source_geometrie`, `geometrie_type`, `coord_wkt` | Géométrie (voir plus haut) |
| `emprise_wkt` | Points seulement : emprise BD TOPO d'où est tiré le point (WKT, WGS84) |
| `libelle_manuel`, `coord_manuel`, `validation`, `remarque` | **Tenues à la main**, conservées d'une exécution à l'autre ; `validation` = « ne pas conserver » écarte le lieu |

Motifs de `a_traiter` : libellé reconstitué d'un nom en majuscules ; pas de
géométrie ; connue de TOPO seul / du filaire
MEL seul / de la BD TOPO seule ; noms différents selon les sources (dont
les rattachements par le tracé, à vérifier) ; abréviation dans le libellé ;
libellé en double dans le thésaurus (calculé sur les lieux conservés, après
`libelle_manuel`).

## État au 2026-10-30

**980 lieux** (31 quartiers, 936 rues, 13 points), 0 à traiter ; 487
« ne pas conserver ». Couche historique : 23 anciens noms, 11 voies
disparues, 10 lieux à tracé historique. Validation QGIS des lieux sans tracé
(`validation_historique_20261030.qgz`) : tracés historiques (voir
`lieux_disparus.csv`) ; `coord_manuel` (tracés OpenStreetMap ou BD TOPO) pour
les Rues du Gros Saule, Jean-Baptiste Glorieux, du Pétrole (limitrophe de
Wattrelos), des Messagers et le Square des Prés ; trois squares passés en
points ; Rues Frédéric Chopin et Gabriel Fauré « ne pas conserver » (voies de
Wattrelos connues du seul TOPO de Roubaix, citées par aucun IR). Restent
29 rues sans géométrie, dont 7 citées par les IR (Saint-Vincent-de-Paul,
Christophe Colomb, Centrale, Wagram, Malesherbes, Rossini, Vaucanson).

## État au 2026-10-29

**982 lieux** (31 quartiers, 941 rues, 10 points), 0 à traiter ; 485
« ne pas conserver ». Couche historique : 23 anciens noms, 8 voies disparues
(dont la Rue des Longues Haies, créée). Changements du jour : point « Mairie de Roubaix »
renommé « Hôtel de ville » (`libelle_manuel`) ; Square de l'Armistice du
8 Mai 1945 et Square Pierre Catteau ajoutés aux rues (sans tracé, validés).

## État au 2026-10-28

Après la revue des points puis d'une partie des rues par l'utilisateur :
**472 lieux « ne pas conserver »** mis de côté (460 points, 12 rues : Gare de
Debord, La Closeraie, Le Frome, Laterale Gauche Bd du General de Gaulle,
Liaison Bd de Reims, Liason Braille Matisse, Ligne de Somain à Halluin,
Lotissement la Closeraie, Parking du Colisée, Pavillon Stendhal, Résidence la
Roseraie, Résidence Le Lafargue), et 8 doublons fusionnés (voir plus haut).
Restent **1 027 lieux : 31 quartiers, 986 rues, 10 points** (Conservatoire,
Gare de Roubaix, Hospital Victor Provo, Hôpital de la Fraternité, Mairie,
Parc Barbieux, Théâtre Louis Richard, églises Saint-Joseph, Saint-Martin et
Saint-Michel) ; 220 à traiter, tous des rues. Géométries conformes à la
règle : 31 polygones, 923 lignes ou multilignes (63 rues sans géométrie),
10 points.

## État au 2026-10-27 (avant la revue des points)

1 507 lieux : **31 quartiers, 1 003 rues, 473 points** ; 236 à traiter,
tous des rues.

| Motif | Lieux |
|---|---|
| libellé reconstitué d'un nom en majuscules | 123 |
| connue de TOPO seul (sans géométrie) | 70 |
| connue du filaire MEL seul | 56 |
| noms différents selon les sources | 43 |
| connue de la BD TOPO seule | 37 |
| abréviation dans le libellé | 13 |

Rues : 594 lieux sont attestés à la fois par la BAN, la BD TOPO, TOPO et
la MEL ; les 773 voies de la BAN sont toutes présentes.

## Hors du périmètre (pour l'instant)

- **Couche historique** (voies disparues, anciens noms, dates) : seulement
  les voies « TOPO seul » et les `old_name` d'OSM ; d'autres sources sont à
  choisir.
- **Lieux hors Roubaix** : thésaurus `communes_pays`, tenu à part dans
  `data/geo/referentiel_communes_pays.csv` (cf.
  [geogname_concordance_lieux](geogname_concordance_lieux.md#thésaurus-communes_pays--lot-d-2026-10-29)).
- **Étape 2** : aligner les formes des IR sur ce référentiel, puis l'appliquer
  aux IR.
