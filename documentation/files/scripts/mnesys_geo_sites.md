# Script : mnesys_geo_sites.py

**Emplacement :** `scripts/ead/mnesys_geo_sites.py`

Propose un lieu pour les `<geogname>` de sites des inventaires Mnesys des séries
Fi que le référentiel n'aligne pas (statut « hors référentiel » ou « lieu
écarté ») : bâtiments, monuments, courées, noms de personnes. Piste E2 du
diagnostic [fi_geogname_alignement](../reprise_geo_20261118.md). Lecture seule ;
les propositions validées sont appliquées par [mnesys_geo_applique](mnesys_geo_applique.md).

```bash
python scripts/ead/mnesys_geo_sites.py [--serie fi|mnesys] [--date AAAAMMJJ] [--exclure CSV ...]
```

## Principe

Les formes qui sont des **communes ou des pays** (« Gand (Belgique) », « Hem
(Nord) ») ne sont **plus proposées ici** depuis le 2026-11-18 : elles relèvent de la
famille « territoires » ([mnesys_geo_territoires](mnesys_geo_territoires.md)). Les fichiers
`fi_sites` déjà validés les listent encore (`nature` = « commune ou pays », sans
proposition) : ces lignes sont inertes, la famille « territoires » les reprend. Exception : « Ville de Roubaix »,
« Ville de Lille » (…) désignent la commune ; si le reste de la forme est une commune
ou un pays du thésaurus des territoires, c'est elle qui est proposée (origine
« territoires », confiance haute, thésaurus `bnr_geo_territoires`), sans chercher de
lieu dont le nom contiendrait « Roubaix » (avant cette règle, *Ville de Roubaix*
tombait sur une mairie de quartier OSM). Les autres sont comparées, par
leur **nom seul** (type de voie, articles et accents ignorés, mots dans le
désordre, chiffres comptés), aux noms de trois fonds :

| Origine | Fond | `action_referentiel` |
|---|---|---|
| référentiel | lieux du référentiel (points, rues ; quartiers exclus) : le `lieu_id` s'applique tel quel | — |
| écarté | lieux « ne pas conserver » (points BD TOPO écartés) | réintégrer (écarté) |
| OSM | éléments nommés de `data/geo/osm_roubaix_lieux.json` (hors voies), `lieu_id` = `osm:<type>/<id>` | créer (OSM) |

À ressemblance égale, le plus sûr passe devant (référentiel, puis écarté, puis
OSM). Un mot inclus dans un nom plus long ne vaut 0,94 que s'il y a au moins
deux mots ou si le reste est un qualificatif (« Veuve ») ; un seul mot (« Gare »,
« Marché », « Fort ») ne donne que 0,80 : la candidate est listée, jamais
proposée. Seuil de proposition : 0,85.

| Colonne | Contenu |
|---|---|
| `nature` | site ou courée (« commune ou pays » seulement dans les `fi_sites` d'origine) |
| `thesaurus_bnr_geo` | **thésaurus bnr_geo dont le `@source` sera écrit** si la proposition est appliquée (voir ci-dessous) |
| `type_osm` | type de l'objet dans OpenStreetMap (`monument`, `memorial`, `parking`…) : information seulement, pour les lieux OSM |
| `confiance` | **haute** : score ≥ 0,9, la candidate couvre tous les mots de la forme, type compatible, pas d'hésitation ; **moyenne** : sinon |
| `motif` | « même nom », « nom complété ou abrégé », « faute de saisie probable », « autre type de voie ou de lieu », « hésitation avec … » |
| `nom_correspondant` | variante de nom qui a servi à l'appariement, si elle diffère du libellé |
| `autres_candidats` | jusqu'à 6 candidates « libellé (id <identifiant> ; origine ; score) → thésaurus bnr_geo » : copier l'**identifiant complet, préfixe compris** (`bdtopo:EQ_RESEA…`, `osm:node/…`) dans `lieu_id` pour en choisir une. Les colonnes `lieu_propose`, `origine`, `type_osm` et `thesaurus_bnr_geo` décrivent la proposition d'origine et ne sont pas recalculées |

Toutes les propositions sont « manuelles » (appliquées seulement si `decision` =
« oui »).

## Thésaurus bnr_geo utilisé

Le `@source` écrit dans l'IR est celui du thésaurus du lieu choisi, comme dans
[geogname_applique_concordance](geogname_applique_concordance.md) :

| Thésaurus du référentiel | `thesaurus_bnr_geo` | `@source` |
|---|---|---|
| rues | `bnr_geo_rbx_voies` | `thesaurus--SLASH--bnr_geo_rbx_voies.xml` |
| points (sites, bâtiments) | `bnr_geo_rbx_sites` | `thesaurus--SLASH--bnr_geo_rbx_sites.xml` |
| quartiers | `bnr_geo_rbx_quartiers` | `thesaurus--SLASH--bnr_geo_rbx_quartiers.xml` |
| communes_pays | `bnr_geo_territoires` | `thesaurus--SLASH--bnr_geo_territoires.xml` |

Un lieu OSM à créer serait un site (`bnr_geo_rbx_sites`). Une courée ou une
voie écartée retrouvée comme « Cour… » ou « Rue… » va dans `bnr_geo_rbx_voies`.
La colonne donne le thésaurus de la **proposition** ; chaque candidate de
`autres_candidats` porte le sien après la flèche. Si tu changes `lieu_id`, la
colonne n'est pas recalculée : le thésaurus effectivement utilisé figure dans
`fi_geo_rapport.csv` (colonnes `thesaurus` et `source_apres`) après
`mnesys_geo_applique.py`.

Les 39 propositions : `bnr_geo_rbx_voies` 24 formes (32 occ.), `bnr_geo_rbx_sites` 15
formes (49 occ.).

## Lieux à réintégrer ou à créer

Un `lieu_id` « écarté » ou « OSM » n'est pas encore au référentiel :
`mnesys_geo_applique.py` laisse la forme **en attente** (rapport + message à l'écran).
Réintégrer un écarté (`validation` dans `referentiel_lieux_roubaix_ecartes.csv`)
ou créer un lieu OSM (`lieux_ajoutes.csv`, géométrie d'OSM) modifie le référentiel :
sauvegarde, `referentiel_lieux_roubaix.py` et comparaison avant / après, **sur
demande de l'utilisateur** (règle de travail).

## Résultat du 2026-11-18, série mnesys (tous les IR)

`mnesys_sites_propositions_20261118.csv` : 138 formes, 235 `<geogname>` (formes des
Fi, des quartiers, de la reprise et des voies exclues ; communes et pays renvoyés à la
famille « territoires », qui en compte 154 de plus) : site 135 formes (232 occ.),
courée 3 (3). Propositions : haute 20 formes (66 occ.), moyenne 17 (21) ; 101 formes
(148 occ.) sans proposition. Par origine : référentiel 16 formes (18 occ.), écarté 7
(12), OSM 12 (21), territoires 2 (36, *Ville de Roubaix* 35 et *Ville de Lille* 1).

## Résultat du 2026-11-18, série fi

125 formes, 243 `<geogname>` :

| Nature | Formes | Occ. |
|---|---|---|
| site | 41 | 128 |
| courée | 20 | 25 |
| commune ou pays (E1) | 64 | 90 |

Propositions : haute 11 formes (22 occ.), moyenne 28 formes (59 occ.) ; 86 formes
(162 occ.) sans proposition, dont les 64 communes. Par origine : référentiel 26
formes (50 occ., applicables tout de suite), écarté 5 (6 occ.), OSM 8 (25 occ.).
Alignement des Fi : 93,9 % → 95,0 % avec les seules propositions du référentiel
(95,7 % avec les écartés et OSM, une fois les lieux réintégrés ou créés).

Les formes sans proposition sont des sites sans lieu au référentiel (*Ecole de
plein air* 18, *Monument aux morts* 7, *Marché* 7, *Halles centrales* 5, la
courée *Bayart* 4, *Incinérateur* 3…) : les candidates listées sont éloignées ;
un `lieu_id` peut être saisi, ou le lieu créé au référentiel. Les 5 formes
« lieu écarté » (piste D) sont comprises dans ce fichier.

**Décisions du 2026-11-18 (saisie en cours) :** 9 « oui » (34 occ.), 3 « non »
(*Débord, gare de*, *Gabriel Fauré, rue*, *Le Fresnoy*), 113 formes non encore
décidées. Appliquées : *Gare* → Gare de Roubaix (15), *Musée de la Piscine* (7),
*Nations Unies, avenues des* (3), *Henri Dunant* (1). En attente (lieu à créer ou
à réintégrer) : *Monument Nadaud* (3), *Palais de justice* (2), *Monument aux
martyrs de la Résistance.* (1), *Couteaux, pont des* (2). Alignement des Fi :
93,9 % → 94,5 %.
