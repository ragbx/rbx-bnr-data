# Script : mnesys_geo_verse.py

**Emplacement :** `scripts/ead/mnesys_geo_verse.py`

Verse les décisions saisies dans les fichiers de propositions (`decision` = `oui` / `non`) dans la **table de
décisions cumulative** `data/geo/concordance_mnesys_lieux.csv` (forme → lieu). La validation se fait en
plusieurs fois : on lance le versement après chaque séance, autant de fois que nécessaire.

```bash
python scripts/ead/mnesys_geo_verse.py [--propositions CSV ...] [--remplacer] [--essai]
```

Par défaut, il lit le fichier de propositions le plus récent de chaque série (fi, mnesys) et de chaque famille,
comme [mnesys_geo_applique](mnesys_geo_applique.md). Les fichiers de propositions ne sont pas modifiés.

## Règles

- Seules les lignes **décidées** sont versées. Les lignes sans décision restent dans les fichiers de
  propositions. Pour les lignes « automatiques », elles continuent d'y être appliquées.
- `oui` exige un `lieu_id`, sinon la ligne n'est pas versée et elle est signalée. Un identifiant copié sans son
  préfixe est complété s'il est unique.
- `non` : `lieu_id` reste vide et le lieu proposé et refusé va dans `lieu_rejete`. La forme est laissée telle
  quelle dans les IR.
- La table **n'est jamais reconstruite**. Une forme nouvelle est ajoutée. Une forme déjà versée avec la même
  décision et le même lieu est gardée : sa remarque, son libellé et son thésaurus sont mis à jour s'ils ont
  changé.
- Une forme déjà versée avec une autre décision ou un autre lieu est un **CONFLIT**. Il est signalé et la table
  garde sa valeur, sauf avec `--remplacer`. On peut aussi corriger une décision directement dans la table.
- Une même forme décidée différemment dans deux fichiers arrête le script.

## Effet sur les autres scripts

- [mnesys_geo_applique](mnesys_geo_applique.md) lit la table en premier. Elle l'emporte sur les fichiers de
  propositions, et ses lignes sont notées « table » dans la colonne `methode` du rapport. `--sans-table`
  permet de l'ignorer.
- Les scripts de propositions (`mnesys_geo_quartiers`, `_reprise`, `_voies`, `_sites`, `_territoires`)
  excluent les formes de la table. On peut donc **régénérer des propositions**, après un réimport Mnesys par
  exemple : le nouveau fichier ne contient que les formes encore à décider, nouvelles formes comprises, et
  aucune décision versée n'est perdue. Une remarque saisie sur une ligne **non décidée** serait en revanche
  perdue : il faut la verser (décision) avant de régénérer.
- [mnesys_geo_diagnostic](mnesys_geo_diagnostic.md) tient compte de la table.

## Colonnes de la table

| Colonne | Contenu |
|---|---|
| `forme` | texte du `<geogname>`, clé de la table |
| `decision` | `oui` ou `non` |
| `lieu_id` | lieu du référentiel des lieux ou du thésaurus des territoires (vide pour `non`) |
| `libelle`, `thesaurus` | indicatifs, d'après le référentiel ; vides si le lieu est à créer |
| `lieu_rejete` | pour `non` : le lieu proposé et refusé |
| `remarque` | remarque de la saisie |
| `validation` | `explicite` |
| `origine` | fichier de propositions d'où vient la décision |
| `date_versement`, `date_maj` | dates ISO |

## Premier versement (2026-11-19)

Versement des fichiers Fi (validés) et de la validation partielle des voies Mnesys : **336 formes**
(240 `oui`, 96 `non`). 428 lignes restent à décider. Un `oui` n'a pas été versé faute de `lieu_id` :
*Vincent Van der Meersch, avenue* (fi_voies). Quatre formes validées attendent leur lieu au référentiel :
Monument Nadaud, Palais de justice, Monument aux martyrs de la Résistance., Couteaux (pont des).

Contrôle : l'application avec la table produit des IR **identiques à l'octet près** à l'application sans la
table (séries fi et mnesys). Un essai de régénération des propositions de voies ne redonne que les 103 formes
non décidées.
