# Script : telechargement_corpus.py

**Emplacement :** `scripts/img/telechargement_corpus.py`

Copie sur disque dur les fichiers TIFF des trois corpus d'images, à partir des
manifestes produits par [extraction_corpus_tif.py](extraction_corpus_tif.md).
Les fichiers sont récupérés depuis le **stockage d'origine** (et non depuis S3) :
chaque fichier se trouve à `<source>/<path>/<name>`.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/corpus/corpus_{nom}_{date}.csv.gz` | Manifeste de chaque corpus (`presse`, `iconographie`, `manuscrits_plans`) |

Pour chaque corpus, le manifeste le plus récent est utilisé par défaut, ou celui
de la date passée en option.

## Résultat

- Les fichiers copiés, rangés en `<dest>/<corpus>/<corpus_code>/<name>`.
- Un journal CSV par exécution : `results/corpus/telechargement_{horodatage}.csv`
  (colonnes `corpus`, `corpus_code`, `name`, `statut`, `src`, `dst`, `erreur`).
  Statuts possibles : `copie`, `deja_present`, `absent`, `erreur`.

---

## Utilisation

Depuis la racine du projet :

    conda run -n ds python scripts/img/telechargement_corpus.py \
        --source <racine_source> --dest <disque_dur> [--date AAAAMMJJ]

| Option | Description |
|---|---|
| `--source` | Racine du stockage source (les `path` + `name` du référentiel s'y résolvent). **Obligatoire.** |
| `--dest` | Racine de destination sur le disque dur. **Obligatoire.** |
| `--date` | Date des manifestes (AAAAMMJJ). Par défaut : le manifeste le plus récent de chaque corpus. |

Exemple sous Windows (partage réseau de la bn-r) :

    conda run -n ds python scripts\img\telechargement_corpus.py ^
        --source \\srvbnr.ntrbx.local\BNR --dest E:\corpus

---

## Comportement

- **Copie reprenable** : un fichier déjà présent à destination, à la même taille,
  est sauté (`deja_present`). On peut donc relancer une copie interrompue.
- **Multi-plateforme** : les séparateurs `/` du référentiel sont convertis vers
  ceux du système ; sous Windows, les chemins longs (> 260 caractères) sont gérés
  via le préfixe `\\?\` (chemins locaux et UNC). Le script fonctionne aussi bien
  sous Windows que sous Linux.
- **Progression** : barre `tqdm` si le paquet est installé (optionnel).
- À la fin, un bilan affiche le nombre de copies, de fichiers déjà présents,
  absents et en erreur.
