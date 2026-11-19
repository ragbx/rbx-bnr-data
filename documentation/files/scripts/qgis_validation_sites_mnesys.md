# Script : qgis_validation_sites_mnesys.py

**Emplacement :** `scripts/ead/qgis_validation_sites_mnesys.py`

Projet QGIS de validation des **sites des Archives municipales** (IR Mnesys), **notice par notice** : une entité
par notice et par site, un champ de décision, navigation en vue formulaire. Le modèle est le projet des vélodromes.

```bash
python3 scripts/ead/qgis_validation_sites_mnesys.py --top 37    # Python de QGIS : génère le projet
python3 scripts/ead/qgis_validation_sites_mnesys.py --reporter  # saisies → CSV
```

## Entrées

- `results/ead/indexation/sites_mnesys_priorites_20261119.csv` : les sites à valider, classés par nombre de
  notices (sans les sites déjà validés de la carte ni les exclus). `--top` garde les N premiers rangs.
- `data/geo/propositions_sites_mnesys_20261119.csv` et `_notices.csv`, produits par
  [sites_candidats](sites_candidats.md) `--prefixe propositions_sites_mnesys`. Les liens « sujet » et « mention »
  sont repris ; les liens « exclu » sont laissés de côté.
- Les IR d'origine `data/ead/mnesys/` : cote, date, note, indexation.

## Sorties (data/geo/)

`validation_sites_mnesys_20261119.gpkg` contient deux couches :
- **notices** : un point par notice × site. Le point est placé sur le lieu proposé (point du référentiel), sinon
  sur l'adresse trouvée dans les notes, sinon dans l'aire d'attente à l'ouest de la ville. Colonnes :
  - pour situer : rang, site, lieu proposé, titre, cote, date ;
  - à saisir : **decision**, **lieu_manuel**, **remarque** ;
  - pour lire : motif (proposition, état côté BnR, alertes), lien (sujet ou mention), note, indexation, position.
- **lieux_proposes** : un point par site qui a un lieu proposé.

`validation_sites_mnesys_20261119.qgz` reprend le projet du 12/11 (fonds de carte, référentiel) et les plans
anciens, masqués.

Valeurs de `decision` :
- « oui, comme proposé » ;
- « oui, autre lieu », avec `lieu_manuel` = `lieu_id` du référentiel ou `POINT (lon lat)` ;
- « non : ne porte pas sur ce site » ;
- « à revoir ».

Couleurs : turquoise = sujet à valider, jaune = mention à valider, violet = décidé. L'ordre de navigation suit le
rang du site, puis met les sujets avant les mentions, puis trie par date.

## Report

`--reporter` écrit les saisies dans `data/geo/sites_mnesys_notices_manuel.csv` (ir, c_id, site_cle, decision,
lieu_manuel, remarque), avec une sauvegarde avant écriture. Une régénération commence par un report si le
GeoPackage existe, pour ne pas perdre les saisies. **Ce CSV n'est pas encore lu par la chaîne** (carte, IR) :
c'est la prochaine étape, une fois la validation avancée.

## Premier lot (2026-11-19)

Les 37 premiers sites, ceux qui ont au moins 10 notices : **676 notices × site**. Placement : 399 sur le lieu
proposé, 162 sur une adresse des notes, 115 dans l'aire d'attente.
