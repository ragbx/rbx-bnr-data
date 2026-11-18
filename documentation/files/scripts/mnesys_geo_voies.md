# Script : mnesys_geo_voies.py

**Emplacement :** `scripts/ead/mnesys_geo_voies.py`

Propose un lieu du référentiel pour les `<geogname>` de voies des inventaires
Mnesys des séries Fi que le référentiel n'aligne pas (statut « sans lieu » de
`aligner()`) : fautes de saisie (« Baurepaire, boulevard »), noms abrégés
(« Foch, rue »), type de voie différent (« Carnot, rue » / Place Carnot).
Piste C du diagnostic [fi_geogname_alignement](../reprise_geo_20261118.md).
Lecture seule ; les propositions validées sont appliquées par
[mnesys_geo_applique](mnesys_geo_applique.md).

```bash
python scripts/ead/mnesys_geo_voies.py [--serie fi|mnesys] [--date AAAAMMJJ] [--exclure CSV ...]
```

## Principe

Pour chaque forme, la meilleure des candidates de `aligner()` est choisie par
ressemblance du **nom seul** : type de voie, articles et accents ignorés, mots
dans le désordre (`difflib`).

| Colonne | Contenu |
|---|---|
| `score` | ressemblance du nom, de 0 à 1 ; au moins 0,94 si les mots de l'un sont inclus dans l'autre (« Foch » / Maréchal Foch) ; à classement égal, la candidate de même type de voie passe devant |
| `motif` | « même nom », « nom complété ou abrégé (titre, prénom) » ou « faute de saisie probable », suivi de « autre type de voie (rue → place) » si les types diffèrent, et de « hésitation avec … » si une 2ᵉ candidate est à moins de 0,05 |
| `confiance` | **haute** : score ≥ 0,9, type compatible, pas d'hésitation ; **moyenne** : score ≥ 0,8 |
| `autres_candidats` | les autres candidates, avec leur identifiant : pour changer de lieu, copier l'identifiant dans `lieu_id` |
| `concordance_bnr` | statut de la même forme dans la concordance BN-R quand elle y est laissée sans lieu (couche historique) |

## Méthodes

- **manuelle** : toute proposition ; appliquée seulement si `decision` = « oui » ;
- **aucune** : score < 0,8, aucune candidate, ou forme déjà laissée « sans lieu »
  côté BN-R (« Jeanne d'Arc, rue », « Union, rue de l' », rues disparues de la
  couche historique) ; rien n'est proposé. Un `lieu_id` peut être saisi à la main,
  puis `decision` = « oui ».

La colonne `remplace_source` vaut « oui » quand une occurrence porte déjà un
`@source` Mnesys, qui sera remplacé.

## Sortie

`results/ead/indexation/fi_voies_propositions_<date>.csv`, classé par méthode,
confiance puis occurrences. Sont exclues les formes alignées par le référentiel
et celles des propositions de quartiers et de reprise.

## Résultat du 2026-11-18

86 formes, 157 `<geogname>` :

| Méthode | Confiance | Formes | Occ. |
|---|---|---|---|
| manuelle | haute | 40 | 80 |
| manuelle | moyenne | 28 | 42 |
| aucune | — | 18 | 35 |

**Décisions du 2026-11-18 :** 62 propositions validées sur 68 (6 « non » : *Saint
Pierre, rue*, *Balzac, cité*, *Ecole, rue de l'*, *Ecole, rue des*, *Halles, place
des*, *Union, pont de l'*) ; parmi les 18 « aucune », 2 lieux saisis à la main
(*Martyrs, place aux* → Place des Martyrs de la Résistance, *Union, rue de l'* →
Avenue de l'Union, malgré la décision BN-R « sans lieu »), 1 « oui » sans `lieu_id`
(*Vincent Van der Meersch, avenue* : non appliquée) et 15 « non » (dont *Jeanne
d'Arc, rue*, « tracé connu, à voir plus tard », et *Saint George, rue*, « voie
historique, connue »). **117 `<geogname>` réécrits** : alignement 91,4 % → **93,9 %**
(si tout avait été validé : 94,0 %). Quatre formes remplacent un `@source` Mnesys
(*Parc des sport, avenue du*, *Grand Rue*, *Brame, rue de*, *Trois Ponts, rue
des*). Les 18 « aucune » sont surtout des voies absentes du référentiel
(*Maurice Ravel, square*, *Planche Trouée*, *Paul Gauguin*, *Viaduc*…), à traiter
avec la couche historique (`lieux_disparus.csv`) ou en créant le lieu.
Limites de l'appariement : sur un nom d'un seul mot contenu dans un nom plus
long, la candidate « complétée » peut passer devant une vraie faute de saisie
(*Grand Rue* → Rue de la Grand Mère plutôt que Grande Rue) : c'est le rôle de
la mention « hésitation ».
