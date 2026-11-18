# Script : mnesys_geo_territoires.py

**Emplacement :** `scripts/ead/mnesys_geo_territoires.py`

Propose des **concepts à créer dans le thésaurus des territoires**
(`data/geo/referentiel_communes_pays.csv`) pour les `<geogname>` de communes et de
pays que ce thésaurus ne connaît pas (« Hazebrouck (Nord) », « Tournai (Belgique) »).
Piste E1 du [plan](../plan_geogname_mnesys_20261118.md). Lecture seule : ni les IR,
ni le référentiel ne sont modifiés.

```bash
python scripts/ead/mnesys_geo_territoires.py [--serie fi|mnesys] [--date AAAAMMJJ] [--hors-ligne]
```

Sortie : `results/ead/indexation/<série>_territoires_propositions_<date>.csv`, et les
réponses des deux services dans `<série>_territoires_sources_<date>.json` (le script
rejoué avec ce fichier ne contacte plus personne ; `--hors-ligne` l'interdit). Seuls des
noms de communes et de pays sont envoyés.

## Formes retenues

Les formes que le référentiel n'aligne pas (« hors référentiel » ou « lieu écarté »)
de nature « commune ou pays » (`mnesys_geo_sites.nature` : « Nom (précision) »), absentes
des fichiers des autres familles. Les lignes que les fichiers `sites` ont listées sans
proposition pour ces communes (64 formes des `fi_sites` validés) sont **reprises ici** :
elles sont inertes (pas de `lieu_id`, pas de décision), et `mnesys_geo_applique.py`
accepte qu'une forme figure dans deux fichiers si l'une des lignes n'a pas de `lieu_id`.
Le script « sites » ne propose plus de communes.

## Méthode

On extrait le nom et la précision, puis :

| Précision | Source | `lieu_id` | Libellé, parent, point |
|---|---|---|---|
| département français (fautes tolérées) | [geo.api.gouv.fr](https://geo.api.gouv.fr), commune du département | code INSEE | « Nom officiel (Département) », parent « Département (France) », centre de la commune |
| ancien département (Seine, Seine-et-Oise) | idem, dans les départements actuels | code INSEE | idem |
| pays (Belgique, Allemagne, Italie, Angleterre, Grèce, Portugal, Canada, États-Unis…) | Wikidata, restreint au pays (propriété P17, point P625) | QID | « Nom français Wikidata (Pays) », parent = le pays, point Wikidata |
| autre (continent, ancien État, précision inconnue) | — | — | aucune proposition |

Mêmes conventions que le lot D de la concordance BnR (2026-10-29) : libellé RAMEAU,
un point par concept, commune disparue ou quartier **rattaché à la commune actuelle**
(table `MANUELLES` : Lomme → Lille, Malo-les-Bains → Dunkerque, Mézières →
Charleville-Mézières, Lille Fives et Lille Saint-Maurice-Pellevoisin → Lille, Mont Cassel →
Cassel…). Ces lignes sont toujours « manuelles ».

## Colonnes

| Colonne | Contenu |
|---|---|
| `forme`, `occurrences`, `ir`, `sources_mnesys` | la forme du `<geogname>`, ses occurrences et IR |
| `nom_extrait`, `precision` | découpage « Nom (précision) » |
| `methode` | **automatique** : nom identique à la source, département ou pays confirmé, une seule candidate ; **manuelle** : le reste ; **aucune** : rien de proposé |
| `confiance`, `motif` | haute ou moyenne ; ce qui a été corrigé (« département corrigé : Aude → Aube », « nom approché »…) |
| `lieu_propose`, `lieu_id`, `categorie`, `parent`, `insee`, `wikidata`, `coord_wkt`, `source_donnees` | le concept proposé (ligne du futur CSV des territoires) |
| `concept_au_referentiel` | **oui** : le concept existe déjà, la forme est une variante à rattacher, applicable tout de suite ; **non** : concept à créer |
| `parent_au_referentiel` | « non » : le département ou le pays est à créer aussi |
| `nb_formes_concept` | nombre de formes du fichier qui visent le même concept (« Strasbourg (Bas-Rhin) » et « Strasbourg, (Bas-Rhin) ») |
| `autres_candidats` | « libellé (id ; score ; point) » : copier l'`id` dans `lieu_id` pour choisir une autre candidate |
| `remplace_source` | « oui » si la forme porte un `@source` Mnesys (remplacé par `bnr_geo_territoires`) |
| `decision`, `remarque` | à remplir |

## Règle d'application

Celle des autres familles : `decision` « oui » applique, « non » exclut, vide applique
seulement les « automatique ». Un concept à créer **ne peut pas être appliqué** tant
qu'il n'est pas dans `referentiel_communes_pays.csv` : `mnesys_geo_applique.py` signale
la forme « en attente (lieu absent du référentiel) ». La création des concepts
(sauvegarde, parents, SKOS, GeoPackage) est une étape à part, avec l'accord de
l'utilisateur, dans le lot unique prévu par le plan.

## État (2026-11-18)

Script écrit et essayé sur Wikidata (réseau réel) avec une liste de communes de substitution pour la
partie française : **le CSV n'a pas été produit**, `geo.api.gouv.fr` refusant les connexions depuis 13 h 30.
Une première version (216 formes, 153 concepts à créer, 23 parents) a été jetée : défauts de recherche
corrigés depuis (libellés Wikidata « mul » ; gares, cimetières et sections de commune écartés ou classés
après les villes ; communes cherchées dans la liste du département, ce qui tolère les fautes de saisie ;
anciens États et pays courants ajoutés). Le cache Wikidata (`<série>_territoires_sources_<date>.json`, 650 entités) est gardé : la relance ne
interroge plus que geo.api.gouv.fr. À relancer quand le service répond ; contrôler alors les lignes
« manuelles » et « aucune », puis valider comme les autres familles.
