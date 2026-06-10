# Corpus numérisés

Un corpus est un ensemble documentaire cohérent ayant fait l'objet d'une campagne
de numérisation (une série d'archives, un titre de presse, un fonds photographique…).
Chaque corpus est identifié par un `corpus_code` (ex. `AMR_2I`, `VAH_PUB`), que l'on
retrouve dans les chemins des fichiers numérisés et dans le
[fichier de référence](fichier_ref.md).

---

## Liste des corpus

`data/corpus_liste/bnr_corpus.xlsx`

Fichier de référence des corpus : il en recense une centaine, avec leur rattachement
archivistique et le nombre de fichiers associés.

| Colonne | Description |
|---|---|
| `corpus_code` | Code du corpus |
| `corpus` | Intitulé du corpus |
| `archives typologie` | Typologie archivistique (Archives modernes, Documents figurés…) |
| `archives série` | Série archivistique (I, W, PER…) |
| `archives cote` | Cote ou tranche de cotes |
| `fichiers` | Nombre de fichiers du corpus dans le fichier de référence |

### Mise à jour

Le script `scripts/corpus_liste/maj_liste.py` recalcule le nombre de fichiers par
corpus à partir du fichier de référence (`results/ref/_ref_files_{date}.csv.gz`)
et regénère le fichier Excel. Les dates des fichiers de référence à utiliser sont
définies en tête de script.

---

## Extractions par corpus

`results/corpus/`

Pour préparer la publication d'un corpus, on en extrait les données depuis les
sources consolidées. Exemple pour le corpus `VAH_PUB`
(script `scripts/corpus/extraction_vah_pub.py`) :

| Fichier | Contenu |
|---|---|
| `vah_pub_files_{date}.csv.gz` | Fichiers du corpus, extraits du fichier de référence (filtre sur `corpus_code`) |
| `vah_pub_dao_{date}.csv.gz` | Liens `<dao>` du corpus, extraits de la liste à plat des dao (`results/dao/liste_dao_flat_{date}.csv.gz`) |
