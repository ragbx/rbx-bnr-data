# Indexation géographique : état des travaux et reprise

Page de reprise du chantier « indexation géographique » des `<geogname>`
des IR (état au **2026-10-29**).

---

## Règle de travail

**Le référentiel des lieux est désormais la seule source** des lieux de
Roubaix : `data/geo/referentiel_lieux_roubaix.csv` (980 lieux validés : 31
quartiers, 936 rues, 13 points ; 487 lieux écartés dans
`referentiel_lieux_roubaix_ecartes.csv`), construit par
[referentiel_lieux_roubaix.py](referentiel_lieux_roubaix.md) à partir des
seules sources de référence (BAN, BD TOPO, TOPO, filaire et quartiers MEL,
OpenStreetMap). Les sources brutes ne se consultent plus que pour discuter
des cas complexes.

L'alignement des IR se fait **sur le texte des `<geogname>`** (chaîne de
caractères du libellé), comparé aux libellés du référentiel ; les
`@coord` existants des IR sont ignorés.

## Fichiers en service

| Fichier | Rôle |
|---|---|
| `data/geo/referentiel_lieux_roubaix.csv` / `_ecartes.csv` / `.gpkg` | Référentiel des lieux (colonnes manuelles saisies dans les CSV) |
| `data/geo/ban_roubaix.csv`, `bdtopo_roubaix_extrait.csv`, `topo_roubaix.csv`, `filaire_voies_roubaix.geojson`, `quartiers_historiques_mel.geojson`, `osm_roubaix_noms.json` | Sources du référentiel |
| `results/ead/indexation/rbx-bnr_geogname_origine.csv` | Formes des `<geogname>` des IR dans leur état d'avant corrections (extraction figée du 2026-09-26, `c_id` réalignés sur Mnesys par [ead_ids_mnesys.py](ead_ids_mnesys.md)) : base de l'alignement |
| `data/geo/referentiel_communes_pays.csv` / `.gpkg` | Thésaurus communes_pays : lieux hors de Roubaix (communes, départements, pays…), tenu à la main |
| `data/geo/concordance_geogname_lieux.csv` | Concordance des formes des IR avec les lieux, cf. [geogname_concordance_lieux](geogname_concordance_lieux.md) |
| `results/ead/ead_cor/bnr2mnesysgeo/` + `results/ead/indexation/bnr2mnesysgeo_rapport.csv` | IR de test avec la concordance appliquée, cf. [geogname_applique_concordance](geogname_applique_concordance.md) |
| `results/ead/indexation/bnr2mnesysgeo_lieux.kml` | KML des lieux indexés dans les IR de test (un repère par lieu, occurrences et documents), cf. [geogname_ir2kml](geogname_ir2kml.md) |
| `scripts/ead/geogname2csv.py` | Script d'extraction des `<geogname>` (fonction `extraire`) ; cf. [geogname2csv](geogname2csv.md) |

## Archive de l'ancienne méthode

Tout ce qui a précédé le référentiel (méthode « depuis les IR », jusqu'au
2026-10-27) est rangé dans
`archives/indexation_geo_avant_referentiel/`, chemins d'origine conservés :
concordance formes ↔ lieux (`concordance_geogname_lieux.csv`), ancien
référentiel (`referentiel_geogname.csv`), concordance des renommages,
extraits BD TOPO de septembre, exports GeoJSON/CSV, IR de test
`bnr2mnesysgeo*`, scripts `geogname2geo`, `geogname_concordance`,
`geogname_concordance_lieux`, `geogname_referentiel_lieux`,
`geogname_applique_referentiel`, `geogname_ir2geojson`, `geogname_brut`, et
leur documentation (dont l'ancienne version de cette page). Ces scripts ne
sont plus exécutables tels quels depuis l'archive.

Les fichiers de travail de la validation du référentiel (2026-10-28) sont
rangés dans `archives/indexation_geo_validation_20261028/data/geo/` :
fichiers de propositions (`propositions_*_20261028.csv`), projets QGIS de
validation (`validation_*_20261028.qgz` + `.gpkg`, `symbology-style.db`) et
sauvegardes successives du référentiel (`referentiel_lieux_roubaix*_avant_*`,
`*.backup_20261028_*`). Toutes les décisions qu'ils portent sont reportées
dans le référentiel.

## Reste à faire

1. **Traiter la concordance des formes avec les lieux**
   (`data/geo/concordance_geogname_lieux.csv`, produite par
   [geogname_concordance_lieux.py](geogname_concordance_lieux.md), alignement
   par le texte) : au 2026-10-29, après report des propositions « sûr » et
   « probable » (`data/geo/propositions_concordance_20261029.csv`), 1 047
   formes alignées (10 985 `<geogname>`), 16 sans lieu (21) après la reprise
   de la couche historique de rbx-geo2 ; 205 formes hors référentiel
   (communes, pays), 1 à retirer.
   Le 2026-10-29, les 23 formes alignées autrement que par le libellé exact
   ou avec une `@source` d'un autre thésaurus ont été validées dans QGIS
   (`data/geo/propositions_concordance_20261029_lots_AB.csv`, projet
   `validation_concordance_AB_20261029.qgz`) : toutes « validé », « Barbieux »
   [rue] rattaché au Parc Barbieux ; libellé du référentiel corrigé en
   « Avenue du Onze Novembre ». Le même jour, le sondage des alignements
   par libellé exact (lot E : 64 formes, 3 292 `<geogname>`,
   `data/geo/propositions_concordance_20261029_lot_E.csv`, projet
   `validation_concordance_E_20261029.qgz`) a donné 64 « validé », aucune
   erreur. Restent : 16 formes sans lieu, les formes hors référentiel
   (thésaurus communes_pays).
   **Lot D (communes_pays), validé et reporté le 2026-10-29** : 201 formes
   rattachées à 178 concepts (`data/geo/referentiel_communes_pays.csv`) ;
   reliquat de 7 formes tranché le même jour (Flandre (France), Ostrebosc,
   Centre-Ville, La Vaux-Marie, Ravin de la Hayette ; Laussage et Tolbecq
   retirés) : 182 concepts ; cf.
   [geogname_concordance_lieux](geogname_concordance_lieux.md#thésaurus-communes_pays--lot-d-2026-10-29).
   Propositions :
   `data/geo/propositions_concordance_20261029_lot_D.csv` (209 formes →
   184 concepts), projet `validation_concordance_D_20261029.qgz`.
   Conventions décidées le 2026-10-29 :
   - libellé RAMEAU (« Lille (Nord) », « Nord (France) »,
     « Bruxelles (Belgique) », « Belgique ») ;
   - un point (centre de commune geo.api.gouv.fr, sinon Wikidata) ;
   - « Roubaix (Nord) » est un concept de communes_pays ;
   - voies et quartiers hors Roubaix rattachés à leur commune ;
   - communes disparues (Ascq, Lomme, Malo-les-Bains…) rattachées à la
     commune actuelle.
   Deux `<geogname>` corrigés dans les IR source `bnr2mnesys` :
   - MED_PAR, « Le quai d'Anvers » (1999) : « Anvers » → « Anvers, quai d' » ;
   - MED_VDM, contrebande 1936-1937 : « France; Belgique » scindé en
     « France » + « Belgique ».
   **Application aux IR en sortie de test faite le 2026-10-29** :
   [geogname_applique_concordance.py](geogname_applique_concordance.md) →
   `results/ead/ead_cor/bnr2mnesysgeo/` (10 985 `<geogname>` réécrits, dont
   62 sans géométrie ; 203 doublons gardés) ; `bnr2mnesys` non remplacé.
   Le 2026-11-05, `@coord` passe en **WKT** (géométrie complète du lieu,
   lon lat, 6 décimales) à la place de l'ancien JSON `[lat, lon]` ;
   textes et `@source` inchangés, cf.
   [geogname_applique_concordance](geogname_applique_concordance.md).
2. SKOS des thésaurus quartiers / rues / points. Mnesys ne lit ni ne gère
   l'indexation géographique (précisé le 2026-11-05) : aucune question à
   l'éditeur, aucune contrainte Mnesys sur `@coord` ni sur les libellés.
3. Thésaurus communes_pays : construit le 2026-10-29 (182 concepts), toutes les
   formes hors référentiel tranchées ; 183 concepts depuis l'ajout de Mouvaux
   (Nord) le 2026-10-30 (Rue des Duriez).
   Lot C revu le 2026-10-29 : 2 formes alignées (Rue Nabuchodonosor, Place Verte),
   14 laissées sans lieu (19 `<geogname>`) avec pistes en remarque, pour la couche historique.
4. Couche historique : commencée le 2026-10-29 (`noms_anciens.csv`,
   `lieux_disparus.csv`, règle « un concept par voie physique », cf.
   [referentiel_lieux_roubaix](referentiel_lieux_roubaix.md)) ; lieux sans
   tracé validés dans QGIS le 2026-10-30 (14 géométries ajoutées, 2 voies
   de Wattrelos écartées) ; 14 formes sans lieu revues dans QGIS le
   2026-10-30 (`validation_sans_lieu_20261030.qgz`) : « Duriez, rue des » →
   Mouvaux (Nord) ; 4 laissées sans lieu faute de piste (Balance, Latérale,
   Pergolèse, Paraboles) ; 9 à revoir (Saint-André ×2, Union ×2, Chapelle
   Carette ×2, Dunkerque, Jeanne d'Arc, Téléphone), pistes en remarque de la
   concordance. Restent 13 formes sans lieu (18 `<geogname>`) et 29 rues sans
   géométrie (7 citées par les IR).
