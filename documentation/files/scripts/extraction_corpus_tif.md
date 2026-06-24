# Script : extraction_corpus_tif.py

**Emplacement :** `scripts/img/extraction_corpus_tif.py`

Construit, à partir du [fichier de référence](../donnees/fichier_ref.md), trois
corpus d'images TIFF servant à la mise au point de la diffusion (voir la
[chaîne images de diffusion](images_diffusion.md)). Chaque corpus est écrit sous
forme de **manifeste** CSV : la liste des fichiers retenus, avec toutes les
colonnes du référentiel.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ref/_ref_files_{date}.csv.gz` | Fichier de référence (la date se règle via `DATE_REF` en tête de script) |

## Résultat

`results/corpus/corpus_{nom}_{date}.csv.gz` — un fichier par corpus, daté du jour
d'exécution (`presse`, `iconographie`, `manuscrits_plans`).

---

## Utilisation

Depuis la racine du projet :

    conda run -n rbx-bnr-data python scripts/img/extraction_corpus_tif.py [--seed N]

| Option | Description |
|---|---|
| `--seed` | Graine du tirage aléatoire (défaut : 42). Une graine différente produit un **nouveau corpus** à contraintes égales (autres TIFF tirés). |

Pour générer un corpus de test supplémentaire avec les mêmes contraintes, relancer
avec une autre graine, par exemple :

    conda run -n rbx-bnr-data python scripts/img/extraction_corpus_tif.py --seed 43

Le nombre de documents par corpus reste le même (il ne dépend que des effectifs et
des plafonds, pas de la graine) ; seuls les fichiers tirés changent. Les manifestes
étant datés du jour, ils ne s'écrasent pas s'ils sont produits un autre jour ; pour
deux tirages le **même jour**, le second écrase le premier.

---

## Définition des trois corpus

| Slug | Définition (corpus_code) |
|---|---|
| `presse` | tous les `PRA_*` |
| `iconographie` | `MED_CP`, `MED_IMA`, `MED_AFF`, `AMR_AFF`, `AMR_DEL` |
| `manuscrits_plans` | `MED_MS`, `MED_PLA`, `MED_MON` |

La définition se règle dans le dictionnaire `CORPORA` en tête de script.

## Règles de sélection

- On ne retient que les **TIFF** (`extension == .tif`) effectivement **versés
  sur S3** (`s3_uploaded = True`), dédoublonnés par `uuid` (le référentiel
  contient des lignes en double).
- **Exception `MED_PLA`** (aucun dépôt S3) : on garde ses TIFF dont la source
  est Azraël (`source2s3 = az`).
- **Échantillon équilibré** : chaque `corpus_code` fournit le même nombre `N` de
  documents (1 document = 1 TIFF unique). `N` est le plus grand entier tel que :
  - `N` ≤ effectif du plus petit `corpus_code`,
  - `N × nombre total de corpus_code` ≤ `MAX_DOCS_TOTAL` (600 documents),
  - le volume cumulé des trois corpus reste ≤ `MAX_SIZE_GO` (200 Go).
- Le tirage des `N` documents de chaque `corpus_code` est **aléatoire mais
  reproductible** : à graine donnée, le résultat est identique d'une exécution à
  l'autre. La graine se règle via l'option `--seed` (défaut `SEED = 42`).

Les plafonds se règlent en tête de script (`MAX_DOCS_TOTAL`, `MAX_SIZE_GO`) ; la
graine par défaut (`SEED`) aussi, mais elle se surcharge à l'exécution avec
`--seed`.

Le script affiche le détail par corpus et par `corpus_code` (nombre de documents
et volume), ainsi que le total des trois corpus.
