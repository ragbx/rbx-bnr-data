# Script : mnesys_geo_reprise.py

**Emplacement :** `scripts/ead/mnesys_geo_reprise.py`

Propose un lieu pour les `<geogname>` des inventaires Mnesys des séries Fi que
le référentiel des lieux n'aligne pas, mais qui se résolvent **sans nouveau
travail de fond** (pistes A et B du diagnostic
[fi_geogname_alignement](../reprise_geo_20261118.md)). Lecture seule ; les
propositions sont appliquées par [mnesys_geo_applique](mnesys_geo_applique.md).

```bash
python scripts/ead/mnesys_geo_reprise.py [--serie fi|mnesys] [--date AAAAMMJJ] [--exclure CSV ...]
```

## Origines

| Origine | Règle |
|---|---|
| concordance BnR | la forme exacte est dans `data/geo/concordance_geogname_lieux.csv` avec un lieu retenu (statut « aligné ») : décision déjà prise côté BN-R (« Roubaix » → Roubaix (Nord), « Henri Carette, rue » → Rue Henri Carrette). Lignes sans `@source` préférées ; plusieurs lieux → rien de proposé (« conflit ») |
| territoires | la forme (ponctuation finale et casse ignorées : « Tourcoing, (Nord) ») est le libellé ou une forme connue d'un concept de `data/geo/referentiel_communes_pays.csv` ; si seule la précision diffère (« Paris (Île de France) » / Paris (France)), proposition « manuelle » |

Sont exclues les formes déjà alignées par le référentiel et celles des
propositions de quartiers ([mnesys_geo_quartiers](mnesys_geo_quartiers.md)).

## Méthodes

- **automatique** : appliquée sauf `decision` = « non » ;
- **manuelle** : appliquée seulement si `decision` = « oui » ; ici, une voie
  que la BN-R rattache à une commune (*Verte, rue* → Croix (Nord), *Constantin
  Descat, boulevard* → Tourcoing (Nord) : à confirmer pour les Fi) ou une
  précision différente (*Paris (Île de France)*) ;
- **aucune** : conflit, rien de proposé (`lieu_id` peut être saisi à la main,
  puis `decision` = « oui »).

La colonne `remplace_source` vaut « oui » quand une occurrence porte déjà un
`@source` Mnesys, qui sera remplacé.

## Sortie

`results/ead/indexation/fi_reprise_propositions_<date>.csv`, les lignes à
valider d'abord : `forme`, `occurrences`, `ir`, `sources_mnesys`, `origine`,
`methode`, `motif`, `lieu_propose`, `lieu_id`, `thesaurus`, `remplace_source`,
`decision` et `remarque` (à remplir).

## Résultat du 2026-11-18

39 formes, 164 `<geogname>` : concordance BnR 24 formes (112 automatiques + 4
manuelles), territoires 15 formes (45 automatiques + 3 manuelles). Taux
d'alignement des Fi : 87,9 % → 91,2 % avec les automatiques, **91,4 % avec les
manuelles**. Les 39 formes sont validées (`decision` = « oui » partout), les 7
« manuelles » comprises : *Verte, rue* → Croix (Nord), *Constantin Descat,
boulevard* → Tourcoing (Nord), *Paris (Île de France)* → Paris (France).
Cinq formes (Roubaix, Paris (France), Bruxelles (Belgique), Belgique, Lille ;
7 occurrences) remplacent un `@source` `thesaurus/index_Lieux.xml`.
