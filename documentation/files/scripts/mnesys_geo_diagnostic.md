# Script : mnesys_geo_diagnostic.py

**Emplacement :** `scripts/ead/mnesys_geo_diagnostic.py`

Diagnostic en **lecture seule** de l'alignement des `<geogname>` de **tous** les
inventaires Mnesys (`data/ead/mnesys/FRAM59100_*.xml`). Il généralise
`fi_geogname_alignement.py` (limité aux séries Fi) et y ajoute les décisions déjà
prises, le thésaurus des territoires et la nature des formes restant à traiter.
Étape 2 du [plan](../plan_geogname_mnesys_20261118.md).

```bash
python scripts/ead/mnesys_geo_diagnostic.py [--date AAAAMMJJ]
```

## Catégories

Chaque forme (texte du `<geogname>`) reçoit la première catégorie qui s'applique :

| # | Catégorie | Règle |
|---|---|---|
| 1 | référentiel | alignée par le texte sur un lieu du référentiel (`aligner()`, sans `@source`) |
| 2 | décision validée | validée dans `<série>_<famille>_propositions_<date>.csv` (`decision` = « oui », ou méthode « automatique » sans « non ») |
| 3 | concordance BnR | forme exacte de `concordance_geogname_lieux.csv` avec un lieu retenu |
| 4 | territoires | libellé ou forme connue d'un concept de `referentiel_communes_pays.csv` (casse et ponctuation finale ignorées) |
| 5 | à traiter | le reste, avec sa **nature** : commune ou pays, voie, site, quartier, courée, lieu écarté |

« Résolue » veut dire qu'un lieu est connu pour la forme : **rien n'est encore réécrit
dans les IR**, sauf les 389 `<geogname>` des Fi appliqués par [mnesys_geo_applique](mnesys_geo_applique.md).

## Sorties (`results/ead/indexation/`)

- `mnesys_geo_diagnostic_formes_<date>.csv` : une ligne par forme (occurrences, IR,
  `@source`, catégorie, nature, lieu, thésaurus du référentiel) ;
- `mnesys_geo_diagnostic_ir_<date>.csv` : une ligne par IR (occurrences par catégorie,
  taux déjà résolu).

## Résultat du 2026-11-18

29 IR, 14 281 `<geogname>`, 1 776 formes (17 fichiers illisibles : esperluettes non
échappées, aucun `<geogname>`).

| Catégorie | Formes | Occ. | % |
|---|---|---|---|
| référentiel | 859 | 11 458 | 80,2 |
| décision validée | 141 | 1 110 | 7,8 |
| concordance BnR | 22 | 69 | 0,5 |
| territoires | 33 | 155 | 1,1 |
| **à traiter** | **721** | **1 489** | **10,4** |

Nature de ce qui reste : **communes et pays 225 formes (528 occ.)**, voies 291
(499), sites 157 (353), quartiers 17 (59), courées 23 (33), lieux écartés 8 (17).
Les 75 formes à traiter présentes dans plusieurs IR pèsent 455 occurrences ; **101
formes couvrent la moitié des occurrences à traiter, 424 en couvrent 80 %** (sur
721).

IR les plus concernés (occurrences à traiter) : 3D130-165 (337 sur 735, des
communes), 3Fi (218), 7C (207 sur 372), 507W (172), 1Ob-voirie (135), 310W (95).
Formes les plus fréquentes : *Ville de Roubaix* (35), *Trois Ponts, quartier des*
(35, 7 IR), *Tournai (Belgique)* (27), *Vélodrome* (25), *Mouscron (Belgique)* (25),
*Jeanne d'Arc, rue* (23, 5 IR), *Parc des Sports* (22), *Université de Lille 3* (20).
