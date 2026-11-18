# Script : mnesys_geo_quartiers.py

**Emplacement :** `scripts/ead/mnesys_geo_quartiers.py`

Alignement des `<geogname>` « quartier » des inventaires Mnesys des séries Fi
(`data/ead/mnesys/FRAM59100_*Fi*.xml`) sur le thésaurus des quartiers
(`bnr_geo_rbx_quartiers`). Piste 1 du diagnostic `fi_geogname_alignement.py`
(note [reprise_geo_20261118](../reprise_geo_20261118.md), section 2).

**Étape 1 (ce script, 2026-11-18) : propositions, lecture seule.** L'étape 2
(écriture des IR corrigés) est faite par
[mnesys_geo_applique](mnesys_geo_applique.md) (qui applique aussi les propositions de
[mnesys_geo_reprise](mnesys_geo_reprise.md)).

## Principe

Les formes visées sont « X, quartier de… » et « Quartier X ». Le mot
« quartier » lève l'homonymie avec la rue : on le retire et on aligne le reste
sur le référentiel avec `aligner()` de `geogname_concordance_lieux.py`
(source « quartier »).

| Méthode | Formes | Occurrences | Règle |
|---|---|---|---|
| automatique | 21 | 66 | le reste s'aligne sur un lieu du thésaurus `quartiers` |
| manuelle | 7 | 15 | variante évidente listée dans `MANUELLES` (Centre → Centre-Ville, Alma → Alma Gare, 3 Ponts → Trois Ponts, Saint-Elisabeth → Sainte-Élisabeth, Hutin → Hutin Oran Cartigny, Nouveaux Roubaix → Nouveau Roubaix) : **à valider** |
| aucune | 1 | 1 | « Alouette, quartier de l' » : pas de quartier Alouette parmi les 31 (seulement la rue de l'Alouette) ; laissé tel quel |

Série `mnesys` (tous les IR ; `mnesys_quartiers_propositions_20261118.csv`) : 17 formes, 59
`<geogname>` (500W 12, 1N 12, 310W 10, 7C 8…) ; automatique 9 formes (46 occ.), dont
*Trois Ponts, quartier des* (35 occ., 7 IR) ; manuelle 3 (3) ajoutées à `MANUELLES` : *Alouette,
quartier de* → Espérance (même décision que les Fi), *Fosse aux Chêne, quartier de la* →
Fosse-aux-Chênes (faute de saisie), *Oran, quartier de* → Hutin Oran Cartigny ; aucune 5
(10) : *Edouard Anseele* (5+1), *Nord*, *Laennec*, *Pont Rouge*, sans quartier du même nom.

Soit 28 formes et 81 geogname sur 4 644 (3Fi 67, 5Fi 6, 9Fi 5, 4Fi 3), 18 quartiers
sur 31. Taux d'alignement des Fi : 86,1 % → 87,8 %. Après validation du
2026-11-18, Alouette a été rattaché à Espérance à la main : **29 formes, 82
geogname, 19 quartiers** (87,9 %).

## Décision du 2026-11-18

La forme « Fraternité, quartier de la » (1 occurrence sur 2) porte déjà un
`@source` Mnesys (`thesaurus/index_Lieux.xml`) : **il est remplacé** par celui
du thésaurus des quartiers (colonne `remplace_source`).

## Sortie

`results/ead/indexation/fi_quartiers_propositions_<date>.csv`, une ligne par
forme, les lignes à valider d'abord : `forme`, `occurrences`, `ir`,
`sources_mnesys`, `methode`, `quartier_propose`, `lieu_id`, `remplace_source`,
`candidats` (formes sans proposition), `decision` et `remarque` (à remplir ;
« non » exclura la ligne de l'étape 2).

```bash
python scripts/ead/mnesys_geo_quartiers.py [--serie fi|mnesys] [--date AAAAMMJJ]
```

Rien n'est écrit dans les IR, le référentiel ni la concordance.

## Étape 2

[mnesys_geo_applique](mnesys_geo_applique.md) réécrit les `<geogname>`
retenus (convention BnR : libellé, `@source`, `@coord`, décision du
2026-11-18) dans `results/ead/ead_cor/fi_geo/`. Les formes **manuelles** ne
sont appliquées que si `decision` vaut « oui » dans le CSV des propositions.
