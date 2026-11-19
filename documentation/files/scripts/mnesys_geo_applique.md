# Script : mnesys_geo_applique.py

**Emplacement :** `scripts/ead/mnesys_geo_applique.py`

Applique aux inventaires Mnesys des séries Fi
(`data/ead/mnesys/FRAM59100_*Fi*.xml`, **non modifiés**) les alignements
validés des `<geogname>`, et écrit les IR corrigés dans
`results/ead/ead_cor/fi_geo/` (dossier recréé à chaque exécution, **seuls les
IR modifiés** y sont écrits). Remplace `fi_applique_quartiers.py` (même
fonctionnement, limité aux quartiers).

```bash
python scripts/ead/mnesys_geo_applique.py [--serie fi|mnesys] [--propositions CSV ...] [--ir 3Fi_20210427_import 5Fi]
```

## Table de décisions

La table cumulative `data/geo/concordance_mnesys_lieux.csv`, alimentée par
[mnesys_geo_verse](mnesys_geo_verse.md), est lue en premier et **l'emporte** sur les fichiers de propositions
(méthode « table » dans le rapport). `--sans-table` permet de l'ignorer.

## Propositions lues

Le plus récent de chaque famille dans `results/ead/indexation/` (ou
`--propositions`) :

| Fichier | Produit par |
|---|---|
| `fi_quartiers_propositions_<date>.csv` | [mnesys_geo_quartiers](mnesys_geo_quartiers.md) |
| `fi_reprise_propositions_<date>.csv` | [mnesys_geo_reprise](mnesys_geo_reprise.md) |
| `fi_voies_propositions_<date>.csv` | [mnesys_geo_voies](mnesys_geo_voies.md) |
| `fi_sites_propositions_<date>.csv` | [mnesys_geo_sites](mnesys_geo_sites.md) |

Un `lieu_id` absent du référentiel (lieu écarté à réintégrer, lieu OSM à créer :
voir [mnesys_geo_sites](mnesys_geo_sites.md)) laisse la forme **en
attente** : action « laissé (lieu absent du référentiel : à créer ou
réintégrer) » dans le rapport et message « ATTENTION » à l'écran.

Une forme ne doit figurer que dans un fichier. Il faut un `lieu_id` (un lieu du
référentiel des lieux ou du thésaurus des territoires) : la colonne peut être
corrigée ou saisie à la main ; les colonnes de libellé sont indicatives. Un
identifiant copié sans son préfixe (`EQ_RESEA0000000056480057` pour
`bdtopo:EQ_RESEA0000000056480057`) est complété s'il est unique.

| Méthode | `decision` vide | `oui` | `non` |
|---|---|---|---|
| automatique | appliquée | appliquée | exclue |
| manuelle | non appliquée | appliquée | exclue |
| aucune (sans `lieu_id` proposé) | non appliquée | appliquée si un `lieu_id` a été saisi | exclue |

## Réécriture (convention BnR, décision du 2026-11-18)

Comme [geogname_applique_concordance](geogname_applique_concordance.md) :

- texte = libellé du lieu (« Épeule », « Roubaix (Nord) ») ;
- `@source` = thésaurus du lieu : `thesaurus--SLASH--bnr_geo_rbx_quartiers.xml`,
  `…_voies.xml`, `…_sites.xml` ou `thesaurus--SLASH--bnr_geo_territoires.xml` ;
  un `@source` Mnesys existant (`thesaurus/index_Lieux.xml`) est **remplacé**
  (décision du 2026-11-18 pour *Fraternité, quartier de la*, étendue aux
  formes reprises par mnesys_geo_reprise : 8 `<geogname>` au total) ;
- `@coord` = géométrie du lieu en WKT (lon lat, 6 décimales) : contour d'un
  quartier, tracé d'une voie, point d'un site ou d'une commune.

Les autres attributs sont gardés. Le fichier est modifié **au niveau du texte,
`<geogname>` par `<geogname>`** : le reste de l'IR (DOCTYPE, fins de ligne,
entités, indentation) est conservé à l'octet près, ce qu'une réécriture par
l'arbre XML ne garantit pas. Les `<geogname>` sont repérés par expression
régulière puis recoupés avec l'arbre (même nombre, même texte) ; les fichiers
écrits sont relus pour contrôle.

## Sortie

- `results/ead/ead_cor/fi_geo/FRAM59100_<IR>.xml` ;
- `results/ead/indexation/fi_geo_rapport.csv` : une ligne par `<geogname>`
  concerné par une proposition (IR, `c_id`, avant / après, méthode, thésaurus,
  action, doublon).

## Résultat du 2026-11-18

Quatre fichiers de propositions lus : quartiers (29 formes, 82 occ., tout validé),
reprise (39 formes, 164 occ., tout validé), voies (86 formes : 65 « oui », 21 « non »)
et sites (125 formes : 9 « oui », 3 « non », 113 non encore décidées).
**389 `<geogname>` réécrits** dans 6 IR (3Fi 311, 9Fi 42, 4Fi 22, 5Fi 11, 10Fi 2,
1AEFi 1) : 82 quartiers, 155 communes et pays, 130 voies, 22 sites (*Gare* →
Gare de Roubaix 15, *Musée de la Piscine* 7). Aucun doublon dans un même
`<controlaccess>`. 16 `@source` Mnesys remplacés. Une voie n'a pas de `@coord` :
« Rue Centrale » (`59512_1030`), voie sans tracé du référentiel.

**En attente** (8 `<geogname>`, 4 formes validées dont le lieu n'est pas au référentiel) :
*Monument Nadaud* (3), *Palais de justice* (2), *Monument aux martyrs de la
Résistance.* (1) — lieux OSM à créer ; *Couteaux, pont des* (2) — lieu écarté à
réintégrer. Laissés : 44 « à valider », 11 « non », 194 sans proposition.

Contrôle : hors `<geogname>`, les IR corrigés sont identiques aux originaux ; les
363 `<geogname>` réécrits aux étapes précédentes sont inchangés.

Taux d'alignement des Fi (diagnostic) : 87,9 % (référentiel + quartiers) → 91,4 %
(reprise) → 93,9 % (voies) → **94,5 %** (sites ; 4 387 occurrences sur 4 644).
Les 3 998 formes déjà alignées par le référentiel ne sont pas réécrites dans les
IR : elles n'ont pas encore de fichier de propositions.

Le `@source` ne sera exploitable dans Mnesys qu'une fois les thésaurus
`bnr_geo_*` chargés en production.

## Séries et premier passage sur tous les IR (2026-11-18)

`--serie fi` (défaut) traite les séries Fi ; `--serie mnesys` traite les 29 IR qui ont des `<geogname>`
(`results/ead/ead_cor/mnesys_geo/`, rapport `mnesys_geo_rapport.csv`). Les décisions sont globales : celles
des Fi s'appliquent à tous les IR. Les `<geogname/>` vides sont acceptés et les 17 IR illisibles ignorés.

Premier passage `--serie mnesys`, avec les seules décisions Fi : **1 098 `<geogname>` réécrits** dans 23 IR
(3Fi 311, 3D130-165 262, 7C 133, 507W 67, 310W 63…), dont 657 communes et pays, 227 voies, 184 quartiers,
30 sites ; 11 doublons dans un même `<controlaccess>` (gardés et signalés) ; hors `<geogname>`, tous les IR
écrits sont identiques aux originaux. Ce résultat est provisoire : le dossier est recréé à chaque exécution.
