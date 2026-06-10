# Script : suivi_corpus.py

**Emplacement :** `scripts/corpus_liste/suivi_corpus.py`

Génère le tableau de bord de suivi des corpus : pour chaque corpus, l'état
d'avancement des chantiers (traitement S3, publication) et la composition des
fichiers, calculés depuis le fichier de référence.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ref/_ref_files_{date}.csv.gz` | Fichier de référence (la date se règle via `ref_date` en tête de script) |
| `data/corpus_liste/bnr_corpus.xlsx` | Liste descriptive des corpus (intitulés, rattachement archivistique) |

## Résultat

`results/corpus_liste/suivi_corpus.xlsx` — régénéré en place à chaque exécution
(non daté), une ligne par corpus plus une ligne `TOTAL` en fin de tableau.

---

## Utilisation

Depuis la racine du projet :

    python scripts/corpus_liste/suivi_corpus.py

---

## Colonnes calculées

| Colonne | Description |
|---|---|
| `fichiers` | Nombre de fichiers du corpus |
| `volume_go` | Volume total en Go |
| `conservation_statut` | Répartition des statuts de conservation, sous la forme `valeur (n) ; valeur (n)` |
| `traitement_s3` | Part (en %) des fichiers dont le traitement S3 est réalisé |
| `fichiers_publies` | Nombre de fichiers avec `publication_statut = oui` |
| `jpeg`, `tiff`, `pdf`, … | Nombre de fichiers par type (une colonne par valeur de `file_type`) |

Les colonnes descriptives de `bnr_corpus.xlsx` (intitulé, typologie, série, cote)
sont reprises telles quelles.

### Règle de calcul de `traitement_s3`

Un fichier est compté comme traité si sa valeur `conservation_statut` **contient**
l'une des chaînes de caractères suivantes (toutes variantes incluses) :
`TRANSFERT_S3_OK`, `CORBEILLE`, `SUPPRIMER`, `NE PAS GARDER`.
Les statuts `INCONNU`, `À TRANSFERER` et `EN LIGNE - À TRANSFERER ?` sont donc
les seuls comptés comme non traités.

---

## Mise en forme

Le fichier Excel est mis en forme automatiquement (openpyxl) :
en-tête en gras sur fond gris et ligne figée, ligne `TOTAL` en gras avec bordure
supérieure, échelle de couleurs rouge → jaune → vert sur `traitement_s3`,
séparateurs de milliers sur les décomptes, largeurs de colonnes ajustées.
