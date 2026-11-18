# Plan — geogname de tous les IR Mnesys (2026-11-18)

Suite du travail sur les `<geogname>` des inventaires Fi : extension à l'ensemble
des IR de `data/ead/mnesys/` (voir aussi [reprise_geo_20261118](reprise_geo_20261118.md)
pour l'état du chantier et les règles de travail).

## Inventaire (lecture seule, 2026-11-18)

- `data/ead/mnesys/` : 634 fichiers ; **29 IR ont des `<geogname>` non vides**
  (14 281 `<geogname>`, 1 776 formes) ; 25 autres n'ont que des `<geogname>` vides.
  17 fichiers ne s'analysent pas (`&` non échappés, ex. 3O208-357, 397W) : **aucun
  `<geogname>`** même en analyse tolérante, hors sujet.
- Les Fi font 4 644 `<geogname>` ; les dix plus gros IR couvrent 86 %
  (3Fi 3 415, 507W 2 569, 1N 1 480, 500W 968, 3D130-165 735, 9Fi 733,
  1Ob-voirie 708, 310W 625, 419W 591, 410W 496).
- 74 % des `<geogname>` n'ont pas de `@source` ; les autres citent
  `thesaurus/index_Lieux.xml` (1 957, dont une variante `--SLASH--`) ou
  `thesaurus/FRAM59100_THES_000004.xml` (1 714), thésaurus Mnesys absents du dépôt.
- Situation des occurrences : 80,2 % déjà alignées par le référentiel (texte),
  7,8 % par une décision Fi, 0,5 % par la concordance BN-R, **11,5 % à traiter**
  (753 formes, dont 660 aussi hors des Fi).

## Décisions de l'utilisateur (2026-11-18)

1. **Périmètre** : les 29 IR de `data/ead/mnesys/` (Archives municipales), avec une
   table de décisions partagée avec les IR de la BN-R (qui restent sur la chaîne
   `bnr2mnesysgeo`).
2. **`@source` Mnesys existants** (`index_Lieux.xml`, `FRAM59100_THES_000004.xml`) :
   **remplacés partout** par le `@source` du thésaurus `bnr_geo_*`, comme pour les Fi.
3. **4 lieux en attente des Fi** (3 lieux OSM à créer : *Monument Nadaud*, *Palais de
   justice*, *Monument aux martyrs de la Résistance.* ; 1 écarté à réintégrer :
   *Pont des Couteaux*) : traités **en un seul lot après l'étape 4**, avec les
   nouveaux sites validés sur tous les IR (une sauvegarde, une régénération du
   référentiel).
4. **Forme d'origine** : conservée **dans le rapport seulement** (par `c_id`), pas
   en variante SKOS.

## Étapes

| # | Étape | Livrable | État |
|---|---|---|---|
| 1 | Généraliser les scripts `fi_*` (chemin des IR en paramètre) | scripts `mnesys_geo_*` + fiches | **fait le 2026-11-18** (voir ci-dessous) |
| 2 | Diagnostic sur les 29 IR, lecture seule | [mnesys_geo_diagnostic](scripts/mnesys_geo_diagnostic.md) | **fait le 2026-11-18** : 89,6 % des occurrences déjà résolues ; reste 721 formes / 1 489 occ. (communes 528, voies 499, sites 353, quartiers 59) |
| 3 | Table de décisions unique `data/geo/concordance_mnesys_lieux.csv` (forme → lieu), amorcée avec le référentiel, les Fi validés et la concordance BN-R | table | à faire |
| 4 | Propositions pour les formes nouvelles, par familles (quartiers, concordance et territoires, voies, sites, communes absentes du thésaurus) ; validation dans les CSV | CSV | **propositions générées le 2026-11-18** pour quartiers, reprise, voies et sites (486 formes, voir ci-dessous) ; validation par l'utilisateur à faire ; famille « territoires » (communes absentes du thésaurus, E1) : script écrit, **CSV à générer** quand geo.api.gouv.fr répond |
| 5 | Application dans `results/ead/ead_cor/mnesys_geo/` (originaux intacts), y compris les formes déjà alignées ; contrôles identiques aux Fi | IR + rapport | à faire |
| 6 | Aval : SKOS, cartes, chargement des thésaurus `bnr_geo_*` dans Mnesys, réimport des IR | à cadrer avec Mnesys | à faire |

## Organisation des scripts (étape 1)

Un module commun, [mnesys_geo_commun.py](../../scripts/ead/mnesys_geo_commun.py), définit les **séries**
(`--serie`, défaut `fi`) : `fi` = `FRAM59100_*Fi*.xml` → `results/ead/ead_cor/fi_geo/` ; `mnesys` =
`FRAM59100_*.xml` → `results/ead/ead_cor/mnesys_geo/`. Les propositions sont écrites dans
`results/ead/indexation/<série>_<famille>_propositions_<date>.csv`, familles `quartiers`, `reprise`, `voies`,
`sites` : chaque famille exclut les formes des familles précédentes **et de toutes les séries précédentes**
(fi avant mnesys), donc une forme n'est proposée qu'une fois. Les décisions sont **globales** :
`mnesys_geo_applique.py` lit le plus récent fichier de chaque série et famille et applique chaque décision à tous
les IR de la série où la forme apparaît.

| Script | Rôle |
|---|---|
| `mnesys_geo_diagnostic.py` | diagnostic de tous les IR (lecture seule) |
| `mnesys_geo_quartiers.py`, `_reprise.py`, `_voies.py`, `_sites.py` | propositions par famille (lecture seule) |
| `mnesys_geo_applique.py` | écrit les IR corrigés et le rapport |
| `mnesys_geo_commun.py` | séries, chemins, chaîne des familles |

Les scripts ont été renommés (`fi_quartiers`, `fi_propositions_*`, `fi_applique` → `mnesys_geo_*`) ; les fichiers
de résultats des Fi gardent leur nom (`fi_*_propositions_20261118.csv`, `fi_geo/`, `fi_geo_rapport.csv`).
Contrôles : propositions Fi régénérées sous une date d'essai = mêmes formes que les fichiers validés ; application
`--serie fi` = IR et rapport identiques (389 réécrits). L'application accepte désormais les `<geogname/>` vides et
ignore les 17 IR illisibles.

Le travail déjà fait sur les Fi est décrit dans [mnesys_geo_quartiers](scripts/mnesys_geo_quartiers.md),
[mnesys_geo_reprise](scripts/mnesys_geo_reprise.md),
[mnesys_geo_voies](scripts/mnesys_geo_voies.md),
[mnesys_geo_sites](scripts/mnesys_geo_sites.md) et
[mnesys_geo_applique](scripts/mnesys_geo_applique.md).

## Étape 4 : propositions pour les autres IR (2026-11-18)

Générées avec `--serie mnesys`, dans cet ordre (chaque famille exclut les précédentes et
tous les fichiers `fi_*`) ; **486 formes, 949 `<geogname>`** (hors territoires), aucune saisie encore :

| Fichier (`results/ead/indexation/`) | Formes | Occ. | À valider | Sans proposition |
|---|---|---|---|---|
| `mnesys_quartiers_propositions_20261118.csv` | 17 | 59 | 3 manuelles (Alouette, Fosse aux Chêne, Oran) | 5 |
| `mnesys_reprise_propositions_20261118.csv` | 57 | 229 | 3 manuelles (*Paris (Île-de-France)* ×2, *Pont-Sainte-Maxence (Oise)*) | 0 |
| `mnesys_voies_propositions_20261118.csv` | 274 | 426 | 171 (82 confiance haute, 89 moyenne) | 103 |
| `mnesys_sites_propositions_20261118.csv` | 138 | 235 | 37 (20 haute, 17 moyenne) | 101 |
| `mnesys_territoires_propositions_<date>.csv` | 216 attendues | 516 | à générer (voir ci-dessous) | — |

Les propositions **automatiques** (quartiers 9 formes / 46 occ. ; reprise 54 / 223) sont
appliquées sans décision ; **toutes les autres** attendent `decision` = « oui » (le `lieu_id`
est modifiable). Une règle a été ajoutée au passage dans `mnesys_geo_sites.py` : « Ville de
Roubaix » (35 occ., 7C) et « Ville de Lille » sont proposées sur la commune des territoires
(elles tombaient sur une mairie de quartier OSM).

Essai d'application (provisoire, `results/ead/ead_cor/mnesys_geo/` recréé à chaque exécution) :
1 367 `<geogname>` réécrits avec les décisions des Fi et les automatiques de cette étape
(1 098 avant), 421 « à valider », 653 sans proposition (les 516 occurrences de communes et pays
n'apparaissent dans aucun fichier tant que la famille « territoires » n'est pas générée).

**Communes et pays absents du thésaurus des territoires** (216 formes, 516 occ. : 154 formes hors Fi ;
*Tournai (Belgique)* 27, *Mouscron (Belgique)* 25, *Hazebrouck (Nord)* 21, *Bourbourg (Nord)* 16,
*Arleux (Pas-de-Calais)* 15…, plus 64 formes des Fi) : famille « territoires »,
[mnesys_geo_territoires](scripts/mnesys_geo_territoires.md). Elles sortent du fichier `sites`. Le script
propose, pour chaque forme, le concept à créer dans `referentiel_communes_pays.csv` : code INSEE et
centre via geo.api.gouv.fr pour les communes françaises, QID et coordonnées Wikidata pour les villes
étrangères, rattachements de la concordance BnR (Lomme → Lille…). Les créer modifie le référentiel :
c'est dans le lot unique prévu, avec les 3 lieux OSM, le lieu écarté et les sites validés.

**État de cette famille (2026-11-18, 14 h)** : script écrit et essayé ; une première version du CSV
(216 formes : 119 automatiques, 64 manuelles, 33 sans proposition ; 153 concepts à créer, 23 départements
ou pays parents à créer aussi) a été jetée à cause de défauts de recherche depuis corrigés (libellés
Wikidata « mul », gares et sections de commune, fautes de saisie, communes par département). La régénération
a échoué : **geo.api.gouv.fr était injoignable** (connexion refusée) depuis 13 h 30. Pour la relancer, une fois le
service revenu :

```bash
python scripts/ead/mnesys_geo_territoires.py --serie mnesys --date AAAAMMJJ
```

Le fichier `mnesys_territoires_propositions_<date>.csv` n'existe donc pas encore ; ne pas valider une
version antérieure.
