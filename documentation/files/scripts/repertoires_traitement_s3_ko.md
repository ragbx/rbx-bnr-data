# Script : repertoires_traitement_s3_ko.py

**Emplacement :** `scripts/s3/repertoires_traitement_s3_ko.py`

Liste les répertoires contenant des fichiers dont le traitement S3 n'est pas
réalisé, pour identifier ce qu'il reste à verser ou à statuer dans le chantier
de transfert vers le stockage S3.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ref/_ref_files_{date}.csv.gz` | Fichier de référence (la date se règle via `ref_date` en tête de script) |

## Résultat

`results/s3/repertoires_traitement_s3_ko.csv` — régénéré en place à chaque
exécution (non daté), une ligne par répertoire, classée par répertoire.

| Colonne | Description |
|---|---|
| `répertoire` | Chemin du répertoire sur le support source (colonne `path` du fichier de référence) |
| `nb_fichiers` | Nombre de fichiers non traités dans ce répertoire |
| `conservation_statut` | Répartition de leurs statuts, sous la forme `valeur (n) ; valeur (n)` |

---

## Utilisation

Depuis la racine du projet :

    python scripts/s3/repertoires_traitement_s3_ko.py

---

## Règle de calcul

Un fichier est considéré en traitement S3 **KO** si sa valeur
`conservation_statut` ne contient aucune des chaînes de caractères
`TRANSFERT_S3_OK`, `CORBEILLE`, `SUPPRIMER`, `NE PAS GARDER` — c'est la règle
inverse de la colonne `traitement_s3` du [suivi des corpus](suivi_corpus.md).
Les statuts concernés sont donc `INCONNU`, `À TRANSFERER` et
`EN LIGNE - À TRANSFERER ?`.
