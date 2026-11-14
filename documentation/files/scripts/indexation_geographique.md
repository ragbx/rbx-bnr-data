# Indexation géographique : état des travaux et reprise

Page de reprise du chantier « indexation géographique » des `<geogname>`
des IR (état au **2026-11-14** ; version précédente : 2026-11-06).

---

## Règle de travail

**Le référentiel des lieux est la seule source** des lieux de Roubaix :
`data/geo/referentiel_lieux_roubaix.csv`, construit par
[referentiel_lieux_roubaix.py](referentiel_lieux_roubaix.md) à partir des
seules sources de référence (BAN, BD TOPO, TOPO, filaire et quartiers MEL,
OpenStreetMap). Les sources brutes ne se consultent plus que pour discuter
des cas complexes. Les lieux hors de Roubaix sont dans le thésaurus des
territoires (`data/geo/referentiel_communes_pays.csv`).

L'alignement des IR se fait **sur le texte des `<geogname>`** (chaîne de
caractères du libellé), comparé aux libellés du référentiel ; les `@coord`
existants des IR sont ignorés.

## État au 2026-11-14

| Brique | État | Page |
|---|---|---|
| Référentiel des lieux | 31 quartiers, 937 rues (29 sans tracé, dont 7 cités), **6 points** (Gare de Roubaix, Hôtel de ville, Parc Barbieux, Hôtel des Pompiers et Usine Cavrois-Mahieu, ajoutés les 13-14/11, Parc de la Potennerie, passé des rues aux points le 14/11 : **les parcs sont des points, les squares des rues**) ; 495 lieux écartés (466 points BD TOPO, 29 rues) ; couche historique : 23 noms anciens, 13 lieux disparus | [referentiel_lieux_roubaix](referentiel_lieux_roubaix.md) |
| Thésaurus des territoires | 207 concepts (communes, départements, pays ; libellés RAMEAU, hiérarchie) | [geogname_concordance_lieux](geogname_concordance_lieux.md#thésaurus-communes_pays--lot-d-2026-10-29) |
| Concordance formes ↔ lieux | 1 273 formes : 1 255 alignées, 13 sans lieu, 3 à retirer, 2 hors référentiel (fichier du 2026-11-06) | [geogname_concordance_lieux](geogname_concordance_lieux.md) |
| IR de test | `results/ead/ead_cor/bnr2mnesysgeo/` (37 IR, concordance appliquée, `@coord` en WKT) ; **`bnr2mnesys` non remplacé** ; sortie du 2026-11-06 | [geogname_applique_concordance](geogname_applique_concordance.md) |
| Thésaurus SKOS `bnr_geo_*` | 4 thésaurus, 1 179 concepts ; sortie du 2026-11-06 (**sites : 3 concepts, le référentiel en a 5**) | [geogname2skos](geogname2skos.md) |
| Sites (bâtiments) | 950 candidats, 106 validés dans QGIS le 2026-11-12 : 86 validés (sur 60 points : 4 en service, **56 encore parmi les écartés**), 18 refusés, 7 à revoir, Institut Turgot sans décision ; ≈ 2 280 notices reliées à un point | [sites_candidats](sites_candidats.md) |
| Sites sans point | 306 sites candidats sans point avec lien retenu (940 liens) : propositions (OSM, adresse des notes, référentiel, milieu de la rue co-indexée), équivalences `MEMES_SITES` (Grand'Plage = Médiathèque de Roubaix, Château du Congo = Château Vaissier) ; **exception : Château Vaissier (Tourcoing) sera un point du référentiel** ; projet QGIS de validation `validation_sites_sans_point_20261114.qgz`, report `--reporter` | [sites_sans_point](sites_sans_point.md) |
| Rattachement des rues aux quartiers | 31 quartiers : 780 rues sûres (≥ 95 % dans un quartier), 129 ambiguës tranchées (119 « oui », 10 corrigées) ; découpage à 13 quartiers construit en réserve | [rues_rattachement](rues_rattachement.md), [quartiers_13](quartiers_13.md) |
| Quartier des notices (31) | 9 464 notices avec rue ou site : point 4 363 (site 2 310, adresse 2 026, les deux 27), rues 2 559, croisement 254, **à lever 2 288** (MED_PAR 852, MED_AFF 325, MED_CP 269, MED_LET 236…) | [quartiers_31_rues_notices](quartiers_31_rues_notices.md) |
| RAMEAU | 928 vedettes relues à la main ; vedettes de lieu et sujets localisés repérés | [rameau_lieux](rameau_lieux.md) |
| Mnesys | **Décision du 2026-11-14 : le thésaurus des rues pour Mnesys remplace `bnr_geo_rbx_voies`** (mêmes URI, quartiers de rattachement en `scopeNote`, produit par `geogname2skos.py`) . **Plus de `skos:related` vers les quartiers** (voies et sites) : à l'import, ils donnaient des termes associés vides ; sites aussi en `scopeNote`. Fichier d'essai `mnesys_test/voies_note/bnr_geo_rbx_voies.xml` importé avec succès (géométrie tolérée) : **un seul fichier par thésaurus**, pour Mnesys et pour l'archive ; à charger en production après la regénération de la chaîne. **Import SKOS testé le 2026-11-14** : `skos:related` → terme associé (TA), résolu seulement vers un terme du même fichier ; `altLabel` → EP ; `hiddenLabel` et `editorialNote` perdus ; `scopeNote` affichée en « note ». Variantes : rues + quartiers liés, deux branches, quartiers en note | [rues_quartiers2skos](rues_quartiers2skos.md) |
| Cartes | pros (référentiel) et public (IR) en uMap/KML ; prototype Leaflet public (`carte_bnr/index.html`, fonds anciens IGN), à publier sur `medrbx/carte-bnr` ; liste des cartes et plans anciens de la BN-R et copie sur clé USB | [quartiers2kml](quartiers2kml.md), [geogname_ir2kml](geogname_ir2kml.md), [carte_leaflet](carte_leaflet.md), [cartes_anciennes](cartes_anciennes.md), [cartes_vers_usb](cartes_vers_usb.md) |

Le 2026-11-05, il avait été noté que Mnesys ne lit ni ne gère l'indexation
géographique. Le test du 2026-11-14 nuance ce point : Mnesys importe un
thésaurus SKOS de rues avec leurs quartiers (lien ou note). Il ne lit
toujours pas les `@coord` des IR.

## Reste à faire

1. **Intégrer les sites validés au référentiel** (pas encore fait), sauvegarde
   puis `referentiel_lieux_roubaix.py` et comparaison avant/après :
   - points validés encore écartés (56 du 12/11, et les « point écarté » des
     sites sans point) : `validation` « ne pas conserver » → « validé » dans
     `referentiel_lieux_roubaix_ecartes.csv` (le lieu revient aux points) ;
   - squares validés (Carihem, de la Tour, Camille Claudel TOPO `59512_0942`) :
     **rues**, `ESPACES_EN_RUES` ou voie TOPO + ligne de 2 m en `coord_manuel`
     (attention aux homonymes Rue de Carihem, Rue de la Tour) ;
   - sites sans point validés (OSM, adresse, position saisie, Château Vaissier) :
     lieux créés à géométrie POINT, chargés comme la couche historique ;
     `lieux_disparus.csv` pour les sites disparus, **`lieux_ajoutes.csv`** pour
     les sites encore debout (décision du 2026-11-14, table prête) ; un groupe
     `MEMES_SITES` = un lieu ;
   - Château Vaissier (Tourcoing) : lieu créé, `retirer_hors_roubaix` (rues
     seulement) ne le concerne pas ; pas de quartier ;
   - puis médiathèque dans `LIEU_SI_RUE`, relance de `sites_candidats.py` ;
   - trancher les 7 « à revoir » et l'Institut Turgot (sites du 12/11).
2. **Regénérer la chaîne** une fois les sites intégrés, dans l'ordre :
   concordance → IR de test (`geogname_applique_concordance.py`) → SKOS
   (`geogname2skos.py`) → cartes (`geogname_ir2kml.py`, `quartiers2kml.py`,
   `carte_leaflet.py`). Toutes ces sorties datent du 2026-11-06 ou d'avant
   les derniers ajouts.
3. **Notices « à lever » (2 288)** : choisir une règle par défaut (convention
   de rattachement des rues ambiguës, cf. [rues_rattachement](rues_rattachement.md))
   ou un traitement à la main des IR les plus touchés.
4. **Couche historique** : 13 formes sans lieu (Balance, Saint-André ×2,
   Union ×2, Chapelle Carette ×2, Dunkerque, Jeanne d'Arc, Latérale,
   Pergolèse, Paraboles, Téléphone ; pistes en remarque de la concordance) ;
   29 rues sans tracé (7 citées) ; 3 formes « à retirer » à appliquer.
5. **Passage en production** : remplacer `bnr2mnesys` par `bnr2mnesysgeo`
   (IR) ; choisir le modèle SKOS à charger dans Mnesys (rues + quartiers
   liés, ou rues avec les quartiers en note).
6. **Découpage** : confirmer 31 quartiers (retenu pour le moment) ou 13.
7. **Carte publique** : avant diffusion, titre définitif et nombre de
   mentions compté sur les 52 IR de la BN-R (aujourd'hui : 37 IR de test).

## Fichiers en service

| Fichier | Rôle |
|---|---|
| `data/geo/referentiel_lieux_roubaix.csv` / `_ecartes.csv` / `.gpkg` | Référentiel des lieux (colonnes manuelles saisies dans les CSV) |
| `data/geo/noms_anciens.csv`, `data/geo/lieux_disparus.csv` | Couche historique du référentiel |
| `data/geo/ban_roubaix.csv`, `bdtopo_roubaix_extrait.csv`, `topo_roubaix.csv`, `filaire_voies_roubaix.geojson`, `quartiers_historiques_mel.geojson`, `osm_roubaix_noms.json` | Sources du référentiel |
| `data/geo/referentiel_communes_pays.csv` / `.gpkg` | Thésaurus des territoires (lieux hors de Roubaix), tenu à la main |
| `results/ead/indexation/rbx-bnr_geogname_origine.csv` | Formes des `<geogname>` des IR avant corrections (extraction figée du 2026-09-26, `c_id` réalignés sur Mnesys par [ead_ids_mnesys.py](ead_ids_mnesys.md)) : base de l'alignement ; **ne pas régénérer** |
| `data/geo/concordance_geogname_lieux.csv` | Concordance des formes des IR avec les lieux |
| `results/ead/ead_cor/bnr2mnesysgeo/` + `results/ead/indexation/bnr2mnesysgeo_rapport.csv` | IR de test avec la concordance appliquée |
| `results/ead/indexation/thesaurus/bnr_geo_*.xml` | Thésaurus SKOS géographiques |
| `data/geo/propositions_sites_20261112.csv` / `_notices.csv`, `validation_sites_20261112.qgz` | Sites candidats et leur validation (colonnes `decision`, `lieu_manuel`, `remarque`) |
| `data/geo/quartiers_13.csv` / `.gpkg` / `.qgz`, `rues_quartiers_13_repartition.csv` | Découpage à 13 quartiers, répartition des rues à 13 et 31 |
| `data/geo/rues_rattachement.csv` / `.gpkg`, `rues_rattachement_conflits_points.csv` | Rattachement des rues ambiguës (saisies `decision`, `quartier_31_manuel`, `quartier_13_manuel`) |
| `results/ead/indexation/notices_quartiers_31.xlsx`, `rues_quartiers_31.xlsx` | Quartier des notices à 31 |
| `results/ead/indexation/rameau_lieux.xlsx` | Vedettes RAMEAU de lieu |
| `results/ead/indexation/mnesys_test/` | Fichiers SKOS des tests d'import Mnesys et exports Mnesys (`FRAM59100_THES_*.xml`) |
| `results/ead/indexation/roubaix_lieux_referentiel.{kml,umap}`, `bnr2mnesysgeo_lieux.{kml,umap}`, `carte_bnr/index.html` | Cartes pros, public, prototype Leaflet |
| `results/ead/indexation/cartes_anciennes_bnr.xlsx` | Cartes et plans de la BN-R (MED_23, AMR_008, AMR_006, AMR_001) |

Les nombreux fichiers `*.backup_AAAAMMJJ_avant_*` de `data/geo/` sont des
sauvegardes prises avant chaque report de saisie.

## Historique

### Ancienne méthode et validation du référentiel (jusqu'au 2026-10-28)

Tout ce qui a précédé le référentiel (méthode « depuis les IR », jusqu'au
2026-10-27) est rangé dans `archives/indexation_geo_avant_referentiel/`,
chemins d'origine conservés (ancienne concordance, ancien référentiel,
extraits BD TOPO de septembre, exports, IR de test, scripts
`geogname2geo`, `geogname_concordance`, `geogname_referentiel_lieux`,
`geogname_applique_referentiel`, `geogname_ir2geojson`, `geogname_brut`…, et
l'ancienne version de cette page). Ces scripts ne sont plus exécutables tels
quels depuis l'archive.

Les fichiers de travail de la validation du référentiel (2026-10-28) sont
dans `archives/indexation_geo_validation_20261028/data/geo/` (propositions,
projets QGIS, sauvegardes) ; toutes leurs décisions sont reportées dans le
référentiel.

### Concordance (2026-10-29)

Après report des propositions « sûr » et « probable »
(`data/geo/propositions_concordance_20261029.csv`) : 1 047 formes alignées
(10 985 `<geogname>`). Lots validés dans QGIS le même jour :

- lots A et B (23 formes alignées autrement que par le libellé exact ou avec
  une `@source` d'un autre thésaurus) : toutes validées ; « Barbieux » [rue]
  rattaché au Parc Barbieux ; libellé corrigé en « Avenue du Onze Novembre » ;
- lot E (sondage de 64 formes alignées par libellé exact, 3 292
  `<geogname>`) : aucune erreur ;
- lot D (communes_pays) : 201 formes → 178 concepts, puis reliquat tranché
  (Laussage, Tolbecq retirés) : 182 concepts. Conventions : libellé RAMEAU
  (« Lille (Nord) », « Nord (France) », « Belgique ») ; un point (centre de
  commune geo.api.gouv.fr, sinon Wikidata) ; « Roubaix (Nord) » est un
  concept des territoires ; voies et quartiers hors Roubaix rattachés à leur
  commune ; communes disparues rattachées à la commune actuelle ;
- lot C : 2 formes alignées (Rue Nabuchodonosor, Place Verte), 14 laissées
  sans lieu pour la couche historique.

Deux `<geogname>` corrigés dans les IR source `bnr2mnesys` : MED_PAR « Le
quai d'Anvers » (« Anvers » → « Anvers, quai d' ») ; MED_VDM « France;
Belgique » scindé.

Thésaurus des territoires : 183 concepts le 2026-10-30 (Mouvaux, pour la Rue
des Duriez), 184 le 2026-11-06 (Armentières (Nord) : la forme sans `@source`
de MED_LBI et MED_LET désignait la ville, alignée par erreur sur le quartier).
Renommé `bnr_geo_territoires` dans les IR le 2026-11-05.

### Application aux IR de test et SKOS (2026-10-29 → 2026-11-06)

- 2026-10-29 : [geogname_applique_concordance.py](geogname_applique_concordance.md)
  → `results/ead/ead_cor/bnr2mnesysgeo/` (10 985 `<geogname>` réécrits,
  62 sans géométrie ; 203 doublons gardés).
- 2026-11-05 : `@coord` en **WKT** (géométrie complète, lon lat, 6
  décimales) au lieu de l'ancien JSON `[lat, lon]`.
- 2026-11-06 : SKOS des quatre thésaurus par [geogname2skos.py](geogname2skos.md)
  (1 187 concepts, 1 179 après la réduction des sites à 3 ; géométrie
  GeoSPARQL, anciens noms, formes des IR, hiérarchie des territoires).

### Couche historique (2026-10-29 → 2026-10-30)

Commencée le 2026-10-29 (`noms_anciens.csv`, `lieux_disparus.csv`, règle
« un concept par voie physique ») ; lieux sans tracé validés dans QGIS le
2026-10-30 (14 géométries ajoutées, 2 voies de Wattrelos écartées) ; formes
sans lieu revues le 2026-10-30 (`validation_sans_lieu_20261030.qgz`) :
« Duriez, rue des » → Mouvaux (Nord), 4 sans piste, 9 à revoir.

### Sites, quartiers, Mnesys, cartes (2026-11-12 → 2026-11-14)

- 2026-11-12 : méthode « sites » ([sites_candidats](sites_candidats.md)),
  validation QGIS de 106 sites, correctif de la gare sans nom.
- 2026-11-13 : découpage à 13 quartiers ([quartiers_13](quartiers_13.md)),
  rattachement des rues ambiguës ([rues_rattachement](rues_rattachement.md)) ;
  règles « éponyme » et « majoritaire » retirées ; priorité point > rue sûre
  > convention. Points ajoutés au référentiel : Hôtel des Pompiers (13/11),
  Usine Cavrois-Mahieu (14/11).
- 2026-11-14 : 31 quartiers retenus pour le moment
  ([quartiers_31_rues_notices](quartiers_31_rues_notices.md)) ; RAMEAU relu
  ([rameau_lieux](rameau_lieux.md)) ; tests d'import SKOS dans Mnesys
  ([rues_quartiers2skos](rues_quartiers2skos.md)) ; cartes pros / public,
  prototype Leaflet, cartes anciennes de la BN-R.
