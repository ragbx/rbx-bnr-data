# Indexation géographique : état des travaux et reprise

Page de reprise du chantier « indexation géographique » des `<geogname>`
des IR (état au **2026-10-27**). Elle résume l'objectif, la chaîne de
scripts, ce qui a été fait, les décisions prises et ce qui reste à faire,
et indique comment reprendre le travail sur une autre machine.

---

## 1. Objectif

Fiabiliser les `<geogname>` des IR transformés
(`results/ead/ead_cor/bnr2mnesys/*.xml`) : bonne `@source` (`rue`,
`quartier`, `adresse`…), géométrie (`@coord`) cohérente, et rattachement de
chaque toponyme à un **objet de référence actuel** (voie, espace public,
quartier, commune) doté d'un identifiant pérenne.

Démarche retenue le 2026-09-26 : **décider en amont, dans des tables
validées à la main, dont découleront les corrections des IR** — au lieu de
corrections ponctuelles appliquées directement aux IR.

**Vocabulaire et chaîne (2026-10-27)** :

- **concordance** formes ↔ lieux (`data/geo/concordance_geogname_lieux.csv`,
  une ligne par forme trouvée dans les IR ; nommée `referentiel_geogname.csv`
  jusqu'au 2026-10-27) : relie ce que l'on trouve dans les IR aux lieux ;
  c'est un outil pour nourrir le référentiel, entre autres ;
- **référentiel** = **table des lieux** (`data/geo/referentiel_geogname.csv`,
  construit par [geogname_referentiel_lieux.py](geogname_referentiel_lieux.md))
  : une ligne par lieu, futur concept des thésaurus ;
- **thésaurus géographiques** SKOS générés depuis le référentiel, puis
  réécriture des IR.

Formes des IR → concordance → référentiel des lieux → thésaurus SKOS → IR.

**Nouvelle méthode (2026-10-27, soir)** : on reprend depuis le départ, en
deux temps — **1. construire le référentiel des lieux de Roubaix à partir des
seules sources de référence** (BAN, BD TOPO, TOPO, MEL, OSM), sans regarder
les IR ; **2. aligner l'existant** (les formes des IR) sur ce référentiel.
L'étape 1 est faite par
[referentiel_lieux_roubaix.py](referentiel_lieux_roubaix.md)
(`data/geo/referentiel_lieux_roubaix.csv`, 1 507 lieux) ; l'étape 2 reste à
écrire. L'ancien référentiel déduit des IR (`referentiel_geogname.csv`, 807
lieux, sans aucune saisie manuelle) reste en place tant que l'étape 2 n'est
pas écrite.

## 2. Reprendre sur une autre machine

1. **Récupérer le dépôt à jour**, *après* avoir commité et poussé depuis la
   machine d'origine (voir § 7 la liste des fichiers à ne pas oublier).
2. **Git LFS est indispensable** : tous les `*.csv` sont en LFS
   (`.gitattributes`). Sans lui, les CSV ne sont que des pointeurs de
   trois lignes.

        git lfs install
        git lfs pull

3. **Environnement Python** : `environment.yml` (conda-forge) contient ce
   qu'il faut, y compris `shapely` et `pyproj` ajoutés pour ce chantier.

        conda env create -f environment.yml      # ou : conda env update -f environment.yml

   Sur le poste d'origine, l'env `rbx-bnr-data` n'existait pas : les
   scripts ont été lancés avec l'env `ds` (pandas, lxml, shapely, pyproj).
4. **Accès réseau** : seulement pour retélécharger les sources (option
   `--maj` de `geogname_concordance.py`). Les copies locales sont dans
   `data/geo/` : sans `--maj`, tout fonctionne hors ligne.
5. **BD TOPO** : pas nécessaire. Les extraits utiles sont dans `data/geo/`.
   Le GeoPackage complet (`BDT_3-5_GPKG_LAMB93_D059-ED2026-06-15.gpkg`,
   4,4 Go, BD TOPO 3.5 IGN du Nord, édition 2026-06-15) ne sert qu'à
   réextraire, avec `geogname_concordance_lieux.py --bdtopo <chemin>`.
6. **`ogr2ogr`** (GDAL) n'est requis que par `geogname2geo.py` (reprojection
   du GeoJSON en EPSG:27561). Il n'était pas installé sur le poste
   d'origine.

Tous les scripts se lancent **depuis la racine du dépôt**.

## 3. Chaîne des scripts

| Ordre | Script | Rôle | Fiche |
|---|---|---|---|
| 1 | `scripts/ead/geogname2csv.py` | Extraction des `<geogname>` des IR, une ligne par élément, avec l'état d'origine | [geogname2csv](geogname2csv.md) |
| 2 | `scripts/ead/geogname_concordance.py [--maj]` | Sources TOPO + OSM, concordance des anciens noms, appariements anachroniques | [geogname_concordance](geogname_concordance.md) |
| 3 | `scripts/ead/geogname_concordance_lieux.py [--bdtopo <gpkg>]` | Concordance formes ↔ lieux | [geogname_concordance_lieux](geogname_concordance_lieux.md) |
| 4 | `scripts/ead/geogname_referentiel_lieux.py` | Référentiel des lieux (une ligne par lieu, 4 thésaurus) | [geogname_referentiel_lieux](geogname_referentiel_lieux.md) |
| – | `scripts/ead/geogname_ir2geojson.py` | Contrôle : export GeoJSON (QGIS) des `<geogname>` géoréférencés d'IR | [geogname_ir2geojson](geogname_ir2geojson.md) |
| 5 | `scripts/ead/geogname_applique_referentiel.py` | **Test** d'application aux IR : `bnr2mnesys/` → `bnr2mnesysgeo/` (texte, `@source`, `@coord`) | [geogname_applique_referentiel](geogname_applique_referentiel.md) |
| – | `scripts/ead/geogname2geo.py` | Export GeoJSON / CSV des toponymes géoréférencés (antérieur ; **pas relancé** depuis les corrections) | [geogname2geo](geogname2geo.md) |
| **1** (nouvelle méthode) | `scripts/ead/referentiel_lieux_roubaix.py [--extraire <gpkg>]` | **Référentiel des lieux de Roubaix construit depuis les sources**, sans les IR (quartiers, rues, points) | [referentiel_lieux_roubaix](referentiel_lieux_roubaix.md) |

### Fichiers de données (`data/geo/`)

| Fichier | Nature | Produit par |
|---|---|---|
| `filaire_voies_roubaix.geojson` | Filaire des voies MEL (WFS, 2026-09-22) | téléchargé à la main (cf. geogname2geo) |
| `topo_roubaix.csv` | TOPO DGFiP, Roubaix | `geogname_concordance.py` |
| `osm_roubaix_noms.json` | Noms de voies OSM (ODbL) | `geogname_concordance.py` |
| `bdtopo_roubaix_voies.geojson`, `bdtopo_roubaix_zones.geojson`, `bdtopo_nord_communes.csv` | Extraits BD TOPO (WGS84) | `geogname_concordance_lieux.py --bdtopo` |
| `concordance_renommages.csv` | **Tenu à la main** (le script n'ajoute que des lignes) | `geogname_concordance.py` + édition manuelle |
| `concordance_geogname_lieux.csv` | Concordance formes ↔ lieux ; colonnes `validation`, `remarque`, `coord_manuel` **tenues à la main** et conservées | `geogname_concordance_lieux.py` |
| `referentiel_geogname.csv` | **Référentiel des lieux** (une ligne par lieu) ; colonnes `validation`, `remarque`, `coord_manuel`, `parent` **tenues à la main** et conservées | `geogname_referentiel_lieux.py` |
| `quartiers_historiques_mel.geojson` | Quartiers historiques de Roubaix (31), open data MEL (couche WFS `ville_roubaix:quartiers_historiques`, 2026-10-27) | copie manuelle |
| `ban_roubaix.csv` | Base Adresse Nationale, lignes de Roubaix du fichier `adresses-59.csv.gz` (adresse.data.gouv.fr, 2026-10-27) | téléchargé puis filtré à la main |
| `bdtopo_roubaix_extrait.csv` | Objets nommés BD TOPO touchant Roubaix (WKT Lambert 93, 2D) | `referentiel_lieux_roubaix.py --extraire` |
| `referentiel_lieux_roubaix.csv` / `.geojson` | **Référentiel des lieux de Roubaix construit depuis les sources** ; colonnes `libelle_manuel`, `coord_manuel`, `validation`, `remarque` **tenues à la main** et conservées | `referentiel_lieux_roubaix.py` |

### Résultats (`results/ead/indexation/`)

| Fichier | Contenu |
|---|---|
| `rbx-bnr_geogname.csv` | Tous les `<geogname>` des IR actuels + valeurs d'origine + `nom_officiel` / `filaire_id` (filaire MEL) |
| `rbx-bnr_geogname_origine.csv` | **État de référence figé** (IR du commit `d6ea6ad`) — **ne jamais régénérer** |
| `rbx-bnr_appariements_a_verifier.csv` | Formes au rattachement potentiellement anachronique |

## 4. Ce qui a été fait le 2026-09-26

Le détail est dans l'historique de la fiche [geogname2csv](geogname2csv.md).

**Directement dans les IR bnr2mnesys** (22 IR modifiés, non commités à la
date de rédaction) :

1. Corrections manuelles : suppression des 55 `<geogname>` vides de
   `MED_LET` ; `1946-1959` de `MED_CP` passé de `<geogname source="chrono">`
   en `<subject>`.
2. 3 328 `<geogname>` de voie sans `@source` passés en `source="rue"`
   (statuts 1-2-3 : forme déjà en `rue` dans le corpus, forme voisine, ou
   présente dans le filaire MEL).
3. `@coord` de toutes les rues appariées au filaire MEL (9 365) remplacé par
   le **tracé** de la voie, au format `[[lat, lon], …]` (un tronçon) ou
   `[[[lat, lon], …], …]` (plusieurs) ; `geogname2geo.py` sait les lire.
4. `CORRECTIONS_RUE` (dans `geogname2geo.py`) complété de 10 graphies.
5. Parc Barbieux (329 `<geogname>`) : `source="adresse"`, point
   `[50.67722273760695, 3.1625329346302813]` ; le `Barbieux` isolé de
   `MED_CP` renommé `Parc Barbieux` (seule modification de libellé).

**Ces corrections ont été annulées le 2026-09-30** (commit `4d50819`), sauf la
suppression des 55 `<geogname>` vides de `MED_LET` : les IR ne contiennent
donc ni les tracés, ni les 3 328 `source="rue"`, ni le parc Barbieux en
`adresse`. Les scripts, la documentation et `data/geo/` du 26/09 ont été
gardés.

Ces corrections avaient été faites par des scripts ponctuels, non versionnés :
elles sont documentées mais **pas rejouables**. Et comme les IR BnR sources
(`data/ead/bnr/`) n'ont pas été corrigés, **relancer `ead_bnr2mnesys.py`
les effacerait toutes** : c'est l'une des raisons du passage à des tables en amont (concordance, référentiel).

**Sources explorées** (et verdict) :

- **Filaire MEL** : les voies actuelles, sans parcs ni quartiers ; les
  identifiants (`filaire_des_voies_de_la_ville_de_roubaix.N`) sont des
  numéros d'export, peu pérennes.
- **BD TOPO IGN** : voies (917 objets à Roubaix, dont des morceaux « sans
  BAN » qui complètent la voie BAN de même nom — aucun homonyme), espaces
  publics, quartiers, communes. Retenue comme base géométrique.
- **TOPO DGFiP** (successeur de FANTOIR) : 1 624 entrées pour Roubaix, sans
  géométrie ; donne l'identifiant **`59512_<code voie>`**, commun à la BAN
  et à la BD TOPO, et conserve des voies disparues du terrain (quai
  d'Anvers, rue Darbo…). Aucune voie annulée dans l'export.
- **OSM** : 4 renommages utiles (Boulevard de Paris → Boulevard du Général
  de Gaulle, Rue Neuve → Rue du Maréchal Foch, Rue de l'Alma → Rue Tela
  Tchaï, Rue Émile Moreau → Rue de la Redoute) et quelques variantes.

## 4 bis. Ce qui a été fait le 2026-10-27

Tout dans la concordance formes ↔ lieux, par
[geogname_concordance_lieux.py](geogname_concordance_lieux.md) ; **rien
n'est encore appliqué aux IR**.

1. **Vocabulaire et renommage** : `referentiel_geogname.csv` →
   `concordance_geogname_lieux.csv`, `geogname_referentiel.py` →
   `geogname_concordance_lieux.py` (cf. § 1) ; alerte « absente des sources
   actuelles ».
2. **Libellés** (`libelle_retenu`) : ordre naturel, graphie de référence,
   noms complets, nom actuel pour les renommages (ancien nom dans la nouvelle
   colonne `nom_ancien`), quartiers sans article, communes « (Nord) » ; un
   seul libellé par voie (54 voies à plusieurs libellés → 3). Règles dans la
   fiche du script.
3. **Colonne `a_traiter`** (première colonne) : seul filtre de relecture,
   206 lignes ; se vide quand `validation` est remplie.
4. **Coordonnées** : colonne `coord` (tracés BD TOPO / MEL, contours de
   quartier, points d'espaces publics ; 979 lignes) et `coord_manuel`
   (prioritaire, à la main). Bug d'étiquetage point / polygone corrigé.
5. **Id des IR alignés sur Mnesys** ([ead_ids_mnesys.py](ead_ids_mnesys.md)) :
   les `c_id` de `rbx-bnr_geogname_origine.csv` ont été traduits en id Mnesys
   (seule modification de cet état figé), et `rbx-bnr_geogname.csv` régénéré
   par `geogname2csv.py` (ses colonnes `nom_officiel` / `filaire_id` sont
   désormais vides : les tracés du 26/09 ne sont plus dans les IR).
6. `geogname_concordance.py` : `appariements_a_verifier` réparée (elle
   lisait une colonne disparue le 26/09) ; mêmes 25 formes signalées.
7. **Quartiers** rattachés aux **31 quartiers historiques de la MEL**
   (`data/geo/quartiers_historiques_mel.geojson`), en priorité pour les
   formes indexées comme quartier : les 31 quartiers des IR sont exactement
   ceux de la MEL (Hutin et Oran Cartigny → Hutin Oran Cartigny ; `Gare, place
   de la`, indexé quartier, est une voie). Une forme indexée comme adresse
   (`17 Grand'Place`) reste une adresse.
8. **Référentiel des lieux construit**
   ([geogname_referentiel_lieux.py](geogname_referentiel_lieux.md),
   `data/geo/referentiel_geogname.csv`) : 807 lieux — 31 quartiers, 572
   rues, 7 points, 197 communes et pays (`communes_pays`) —, libellés uniques par thésaurus,
   colonne `a_traiter`.
9. **Test d'application aux IR**
   ([geogname_applique_referentiel.py](geogname_applique_referentiel.md)) :
   IR corrigés dans `results/ead/ead_cor/bnr2mnesysgeo/` (`bnr2mnesys/`
   intact) ; 12 210 `<geogname>` sur 12 212 réécrits, rien d'autre ne
   change ; 72 doublons créés (ancien + nouveau nom d'une même voie dans un
   composant).

## 5. Décisions prises

**2026-10-27 — le référentiel devient le thésaurus géographique**, en
quatre thésaurus : trois pour Roubaix (**quartiers**, **rues**, **points** :
adresses et espaces publics) et un pour les **lieux hors Roubaix** (`communes_pays` : villes,
régions, pays…). Noms : `quartiers`, `rues`, `points`, `communes_pays` ;
fichiers SKOS et `@source` prévus : `bnr_geo_rbx_quartiers.xml`,
`bnr_geo_rbx_rues.xml`, `bnr_geo_rbx_points.xml`, `bnr_geo_communes_pays.xml`.
Dans `@coord` : polygone pour un quartier, **tracé** pour une
rue (y compris en plusieurs tronçons), point pour une adresse ou un espace
public. Mnesys importe les thésaurus SKOS et relie un `<geogname>` à un
concept **par le libellé seul** : le libellé doit être unique dans chaque
thésaurus. Une hiérarchie (`skos:broader` : commune → département → région →
pays) n'est prévue que pour le thésaurus des lieux hors Roubaix (`communes_pays`).

**2026-10-27 — libellés** (décisions de l'utilisateur) : graphie de la
référence (accents, casse, articles, nom officiel complet) ; forme de
`CORRECTIONS_RUE` pour les fautes ; **nom actuel** pour une voie renommée,
avec trace de l'ancien nom ; quartiers **sans article** (`Épeule`) ;
communes uniformisées avec leur département (« (Nord) », « (Pas-de-Calais) »…) ; voies dont le type diffère de la
référence (Alsace, Halluin, Malplaquet, Frasez) : **non corrigées**,
signalées comme cas douteux.

**2026-10-27 — quartiers** : ils sont pris dans la **BD TOPO** ou dans le
**référentiel open data de la MEL**. La MEL publie les 31 quartiers
historiques de Roubaix (couche WFS `ville_roubaix:quartiers_historiques` sur
data.lillemetropole.fr : nom, identifiant, polygone). Comparaison faite le
27/10 : la couche MEL couvre toutes les formes de quartier de la BnR (25 à
l'identique, 5 par inclusion du nom : `Centre` → CENTRE VILLE, `Anseele` →
ANSEELE MOTTE BOSSUT, `Hutin` et `Oran Cartigny` → HUTIN - ORAN - CARTIGNY,
`Linne` → LINNE) ; la BD TOPO en manque 3 (Sainte Elisabeth, Crouy,
Edouard Vaillant) et mêle quartiers et résidences (Nouveau Roubaix : 0 % de
recouvrement avec la MEL, Hauts Champs 44 %, Espérance 63 %). Décision
prise ensuite : les quartiers sont rattachés aux **quartiers
historiques MEL** (`data/geo/quartiers_historiques_mel.geojson`), la BD TOPO
ne fournissant plus que la graphie du nom quand elle existe.

- La concordance ne couvre **pas** les `<geogname>` vides ni le chrono.
- Ordre des coordonnées dans les IR : **`[lat, lon]`** (l'inverse du
  GeoJSON) ; toute valeur fournie au format GeoJSON est à inverser.
- Pour une rue appariée, `@coord` = **tracé** de la voie, pas un point.
- Parcs : `source="adresse"` (parc Barbieux, avec un point fourni à la main).
- Groupes A et B des rues non appariées (variantes de graphie et d'article)
  : validés. Groupe C (type de voie différent : `Fosse aux Chênes, rue de
  la` → Avenue…, `Union, rue de l'` → Avenue…, etc.) : **écarté**.
- Anciennes valeurs toujours conservées (colonnes `_origine` du CSV).
- Scripts Python pour toute étape, par reproductibilité.

## 6. Reste à faire

1. **Relire et valider la concordance** (`validation`, `remarque`), en
   filtrant sur la colonne `a_traiter` :
   - 85 voies absentes des sources actuelles (427 `<geogname>`) : noms
     anciens — rue de la Gare, rue du Moulin, rue Saint-Georges, rue
     Pauvrée, rue des Longues Haies, rue de la Fosse aux Chênes… ;
   - voies actuelles plus récentes que les documents, renommages partiels
     (rue de l'Alma, rue Émile Moreau), rue de la Redoute (voie créée en
     2024, documents de 1884-1929), rue Neuve ;
   - 7 quartiers non trouvés dans la BD TOPO, tous présents dans les
     quartiers historiques MEL (cf. § 5 et point 2 ci-dessous) ;
   - 46 formes « à identifier » (départements, régions, communes anciennes
     ou fusionnées : `Lomme`, `Annappes`, `Ascq`…).
2. **Quartiers** : confirmer les rattachements par inclusion du nom
   (`Centre`, `Anseele`, `Hutin`, `Oran Cartigny`) et les 5 libellés
   construits d'après le nom MEL (`Crouy`, `Linne`, `Sainte Elisabeth`…).
   La convention des renommages est, elle, tranchée (nom actuel + ancien
   nom, cf. § 5).
3. **Compléter la concordance des renommages** (`data/geo/concordance_renommages.csv`)
   pour les noms anciens : pistes — l'ancien fichier FANTOIR (avril 2023,
   134 Mo, sur data.economie.gouv.fr, garde les voies annulées), les
   Archives municipales de Roubaix, et surtout **le géocodage d'origine**
   des `coord` (fait en amont, dans Mnesys / l'index « Plan de Roubaix » de
   bn-r.fr, y compris pour des rues disparues).
4. **Thésaurus SKOS** : générer les quatre thésaurus à partir du référentiel
   des lieux (construit le 2026-10-27) ; pour le thésaurus `communes_pays`, remplir
   la hiérarchie (`parent`) et uniformiser les qualificatifs.
5. **Application aux IR** : un script de test existe
   ([geogname_applique_referentiel.py](geogname_applique_referentiel.md),
   sortie dans `bnr2mnesysgeo/`). Restent à décider : dédoublonner les
   `<geogname>` identiques d'un même composant, n'appliquer que les lieux
   validés ou non, et quand remplacer `bnr2mnesys/`.
6. Faire lire à `geogname2geo.py` le référentiel plutôt que
   `CORRECTIONS_RUE`, puis le relancer (GeoJSON, CSV).
7. Vérifier sur bn-r.fr le lien de recherche du parc Barbieux, désormais
   dans l'index « adresse ».

## 7. Pièges connus

- **Ne jamais régénérer** `results/ead/indexation/rbx-bnr_geogname_origine.csv`.
  Seule sa colonne `c_id` suit les id des IR : elle est traduite par
  [ead_ids_mnesys.py](ead_ids_mnesys.md) à chaque réalignement sur Mnesys
  (sinon les années des documents ne sont plus retrouvées). Après un
  réalignement, relancer `geogname2csv.py`.
- **Mnesys réattribue les id des IR à chaque import** : relancer
  `ead_ids_mnesys.py` sur un export frais après tout (ré)import.
- **Git LFS** pour les CSV (cf. § 2).
- **`rbx-bnr_geogname_origine.csv` et les nouveaux fichiers de `data/geo/`
  étaient non suivis** au moment de la rédaction : penser à les ajouter au
  commit, avec les deux nouveaux scripts et leurs fiches.
- `python` n'est pas dans le PATH de Git Bash sur le poste d'origine :
  appeler l'interpréteur de l'env conda explicitement.
- L'API **Overpass** renvoie souvent des erreurs 504 : le script réessaie et
  bascule sur un miroir.
- `curl` (Git Bash) échoue en TLS sur Overpass : les téléchargements passent
  par Python (`urllib`).
- Rattacher un **nom ancien** à la voie **actuelle** homonyme peut être
  faux (rue Neuve, rue de la Redoute) : voir les alertes.
