# Indexation géographique : état des travaux et reprise

Page de reprise du chantier « indexation géographique » des `<geogname>`
des IR (état au **2026-09-26**). Elle résume l'objectif, la chaîne de
scripts, ce qui a été fait, les décisions prises et ce qui reste à faire,
et indique comment reprendre le travail sur une autre machine.

---

## 1. Objectif

Fiabiliser les `<geogname>` des IR transformés
(`results/ead/ead_cor/bnr2mnesys/*.xml`) : bonne `@source` (`rue`,
`quartier`, `adresse`…), géométrie (`@coord`) cohérente, et rattachement de
chaque toponyme à un **objet de référence actuel** (voie, espace public,
quartier, commune) doté d'un identifiant pérenne.

Démarche retenue en fin de journée : **un référentiel en amont**
(`data/geo/referentiel_geogname.csv`, une ligne par forme BnR), validé à la
main, dont découleront les corrections des IR — au lieu de corrections
ponctuelles appliquées directement aux IR.

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
   réextraire, avec `geogname_referentiel.py --bdtopo <chemin>`.
6. **`ogr2ogr`** (GDAL) n'est requis que par `geogname2geo.py` (reprojection
   du GeoJSON en EPSG:27561). Il n'était pas installé sur le poste
   d'origine.

Tous les scripts se lancent **depuis la racine du dépôt**.

## 3. Chaîne des scripts

| Ordre | Script | Rôle | Fiche |
|---|---|---|---|
| 1 | `scripts/ead/geogname2csv.py` | Extraction des `<geogname>` des IR, une ligne par élément, avec l'état d'origine | [geogname2csv](geogname2csv.md) |
| 2 | `scripts/ead/geogname_concordance.py [--maj]` | Sources TOPO + OSM, concordance des anciens noms, appariements anachroniques | [geogname_concordance](geogname_concordance.md) |
| 3 | `scripts/ead/geogname_referentiel.py [--bdtopo <gpkg>]` | Proposition du référentiel des formes | [geogname_referentiel](geogname_referentiel.md) |
| – | `scripts/ead/geogname2geo.py` | Export GeoJSON / CSV des toponymes géoréférencés (antérieur ; **pas relancé** depuis les corrections) | [geogname2geo](geogname2geo.md) |

### Fichiers de données (`data/geo/`)

| Fichier | Nature | Produit par |
|---|---|---|
| `filaire_voies_roubaix.geojson` | Filaire des voies MEL (WFS, 2026-09-22) | téléchargé à la main (cf. geogname2geo) |
| `topo_roubaix.csv` | TOPO DGFiP, Roubaix | `geogname_concordance.py` |
| `osm_roubaix_noms.json` | Noms de voies OSM (ODbL) | `geogname_concordance.py` |
| `bdtopo_roubaix_voies.geojson`, `bdtopo_roubaix_zones.geojson`, `bdtopo_nord_communes.csv` | Extraits BD TOPO (WGS84) | `geogname_referentiel.py --bdtopo` |
| `concordance_renommages.csv` | **Tenu à la main** (le script n'ajoute que des lignes) | `geogname_concordance.py` + édition manuelle |
| `referentiel_geogname.csv` | Proposition ; colonnes `validation` / `remarque` **tenues à la main** et conservées | `geogname_referentiel.py` |

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

Ces corrections ont été faites par des scripts ponctuels, non versionnés :
elles sont documentées mais **pas rejouables**. Et comme les IR BnR sources
(`data/ead/bnr/`) n'ont pas été corrigés, **relancer `ead_bnr2mnesys.py`
les effacerait toutes** : c'est l'une des raisons du passage au référentiel.

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

## 5. Décisions prises

- Le référentiel ne couvre **pas** les `<geogname>` vides ni le chrono.
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

1. **Relire et valider le référentiel** (`validation`, `remarque`), en
   commençant par la colonne `alerte` :
   - 85 voies absentes des référentiels actuels (427 `<geogname>`) : noms
     anciens — rue de la Gare, rue du Moulin, rue Saint-Georges, rue
     Pauvrée, rue des Longues Haies, rue de la Fosse aux Chênes… ;
   - voies actuelles plus récentes que les documents, renommages partiels
     (rue de l'Alma, rue Émile Moreau), rue de la Redoute (voie créée en
     2024, documents de 1884-1929), rue Neuve ;
   - 7 quartiers non trouvés, dont des équivalents évidents : `Centre` →
     Centre Ville, `Sainte Elisabeth` → Sainte-Élizabeth, `Anseele` →
     Anseele Motte-Bossut, `Linne` → Linne Boulevards, `Oran Cartigny` →
     Hutin Oran Cartigny ;
   - 46 formes « à identifier » (départements, régions, communes anciennes
     ou fusionnées : `Lomme`, `Annappes`, `Ascq`…).
2. **Établir la convention d'indexation de la BnR** : l'index emploie
   parfois le nom actuel pour des documents anciens (rond-point de l'Europe
   sur des documents de 1910) ; cela conditionne le traitement des
   renommages.
3. **Compléter la concordance** (`data/geo/concordance_renommages.csv`)
   pour les noms anciens : pistes — l'ancien fichier FANTOIR (avril 2023,
   134 Mo, sur data.economie.gouv.fr, garde les voies annulées), les
   Archives municipales de Roubaix, et surtout **le géocodage d'origine**
   des `coord` (fait en amont, dans Mnesys / l'index « Plan de Roubaix » de
   bn-r.fr, y compris pour des rues disparues).
4. **Écrire le script d'application** du référentiel aux IR (libellé,
   `@source`, `@coord` depuis la géométrie de référence), rejouable après
   `ead_bnr2mnesys.py`.
5. Faire lire à `geogname2geo.py` les appariements du référentiel plutôt que
   `CORRECTIONS_RUE`, puis le relancer (GeoJSON, CSV).
6. Vérifier sur bn-r.fr le lien de recherche du parc Barbieux, désormais
   dans l'index « adresse ».

## 7. Pièges connus

- **Ne jamais régénérer** `results/ead/indexation/rbx-bnr_geogname_origine.csv`.
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
