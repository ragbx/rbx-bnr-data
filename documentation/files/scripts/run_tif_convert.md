# Script : run_tif_convert.sh

**Emplacement :** `scripts/img/run_tif_convert.sh`

Lanceur de **tests** pour [tif_convert.py](tif_convert.md). Il balaie automatiquement
plusieurs combinaisons de qualité et de seuil de résolution, sur plusieurs dossiers
d'entrée et plusieurs types de documents, afin de comparer le compromis
poids / qualité avant de figer les paramètres de diffusion. Ce n'est pas un script de
production : c'est un outil de mise au point, à éditer pour ajuster les jeux de
paramètres à tester.

Les résultats (récapitulatifs CSV par essai) sont ensuite agrégés et analysés avec
`scripts/img/concat_csv.py` et `scripts/img/poids_par_parametres.py`.

---

## Environnement

À lancer avec l'environnement unifié du projet **`rbx-bnr-data`** : le script appelle
lui-même `conda run -n rbx-bnr-data python …`, il suffit donc de l'exécuter depuis un
shell où `conda` est disponible. C'est cet environnement qui fournit la `libvips`
compilée avec le support JPEG2000 (`.jp2`) requis par `tif_convert.py`.

---

## Arborescence attendue

Chaque dossier d'entrée est un **corpus**. Le lanceur convertit récursivement tout le
sous-arbre `conservation/` et le reproduit à l'identique sous `diffusion/` :

```
<corpus>/conservation/...   →   <corpus>/diffusion/...
```

Le **type de document** (donc le facteur de réduction) est **déduit du nom du dossier
de corpus** : ce nom doit contenir l'une des clés de `FACTEUR_PAR_TYPE`. Ainsi
`corpus_presse_20260502_1` est traité comme `presse` (facteur 0,50). Cela correspond à
la sortie de [telechargement_corpus.py](telechargement_corpus.md), qui range les TIFF
selon l'arborescence d'archives source (ex. `conservation/BNR_SAUV/MED/MED_PRA/PRA_*.tif`)
et non par type.

Sont ignorés (message sur la sortie d'erreur) : un dossier d'entrée introuvable, un
corpus sans sous-dossier `conservation/`, ou un corpus dont le nom ne contient aucune
clé de type connue.

---

## Utilisation

    ./scripts/img/run_tif_convert.sh

Le script ne prend **aucun argument** : toute la configuration est en tête de fichier,
dans le bloc « Configuration — à adapter ». Variables à éditer :

| Variable | Rôle |
|---|---|
| `INPUT_DIRS` | Liste des dossiers d'entrée à traiter (chacun contient un `conservation/`). |
| `SOURCE_SUBDIR` | Sous-dossier source des TIFF (défaut : `conservation`). |
| `OUT_SUBDIR` | Sous-dossier de sortie, en miroir (défaut : `diffusion`). |
| `FACTEUR_PAR_TYPE` | Facteur de réduction `f` par type de document (le type est repéré comme clé contenue dans le nom du dossier de corpus). Défaut : `manuscrits_plans`=0,80 ; `iconographie`=0,65 ; `presse`=0,50 (cf. les niveaux haut/moyen/bas de `tif_convert.py`). |
| `FORMAT` | Format de sortie : `jpeg` (défaut), `jp2` ou `ptiff`. |
| `QUALITIES` | Liste des qualités Q à tester (défaut : 80 85 90). |
| `RESMINS` | Liste des seuils de résolution minimale, en px de largeur (défaut : 2000 2500 3000), passés à `--resolution-min`. |
| `WORKERS` | Nombre de processus parallèles (vide = nombre de cœurs). |
| `CONDA_ENV` | Environnement conda utilisé (défaut : `rbx-bnr-data`). |

---

## Comportement

- **Balayage combinatoire** : pour chaque corpus × chaque qualité × chaque seuil de
  résolution, un appel à `tif_convert.py` est lancé. Le **facteur** de réduction n'est
  pas balayé : il est fixé par le type déduit du nom de corpus via `FACTEUR_PAR_TYPE`.
  Le nombre total d'essais est annoncé au démarrage et chaque essai est numéroté
  `[n/total]`.
- **Cohabitation des essais** : qualité, facteur et seuil sont inscrits dans le nom
  de sortie par `tif_convert.py` (ex. `page001_q85_f65_rmin2000.jpg`). Toutes les
  combinaisons coexistent donc dans le même dossier de sortie sans s'écraser.
- **Reprenable** : `tif_convert.py` saute les fichiers déjà produits ; relancer le
  lanceur ne refait que ce qui manque.
- **Robuste** : un essai en erreur n'interrompt pas le balayage (message sur la
  sortie d'erreur, on passe au suivant). Le script tourne sous `set -euo pipefail`,
  mais l'appel de conversion est protégé pour ne pas tuer le lot.
- **Récapitulatif CSV** : un CSV horodaté à la milliseconde est écrit par essai à la
  racine du dossier `diffusion/` du corpus (`recap_q<Q>_rmin<seuil>_<horodatage>.csv`),
  pour éviter tout écrasement entre essais partageant le même dossier.

---

## Voir aussi

- [tif_convert.py](tif_convert.md) — le convertisseur appelé pour chaque essai.
- [Chaîne images de diffusion](images_diffusion.md) — vue d'ensemble.
</content>
</invoke>
