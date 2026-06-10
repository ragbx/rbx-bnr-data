# Documentation — Métadonnées et fichiers de la bn-r

Ce dépôt rassemble les travaux menés sur les métadonnées et les fichiers numériques
de la [Bibliothèque numérique de Roubaix](https://www.bn-r.fr) dans le cadre de sa
refonte. Il couvre à la fois les descriptions archivistiques encodées en EAD et les
fichiers issus des campagnes de numérisation.

Pour le vocabulaire employé, voir le [Glossaire](files/glossaire.md).

---

## Accès rapides

- [Suivi des corpus](../data/corpus_liste/bnr_corpus.xlsx) — `data/corpus_liste/bnr_corpus.xlsx`
- [Fichier de référence (dernière version)](../results/ref/_ref_files_20260502.csv.gz) — `results/ref/_ref_files_20260502.csv.gz`
- [IR prêts pour Mnesys](../results/ead_cor/bnr2mnesys/) — `results/ead_cor/bnr2mnesys/`

---

## Chantiers

### 1. Cartographier les corpus
Établir une vue d'ensemble des corpus numérisés : liste des ensembles documentaires,
état des fichiers, fichier de référence consolidé.

→ [Données : corpus](files/donnees/corpus.md) · [Fichier de référence](files/donnees/fichier_ref.md)

### 2. Transférer les fichiers vers le stockage S3
Verser les fichiers de conservation sur le stockage objet S3 et en assurer le suivi.

→ [Données : fichiers de conservation](files/donnees/fichiers.md) · [Scripts S3](files/scripts/s3.md)

### 3. Transférer les instruments de recherche vers Mnesys
Transformer les fichiers EAD produits par la bn-r pour les importer dans le logiciel
d'archivistique Mnesys.

→ [Sources de données](files/donnees/sources.md) · [Script ead_bnr2mnesys](files/scripts/ead_bnr2mnesys.md)

### 4. Préparer la publication des corpus
Préparer les données en vue de leur publication en ligne.

→ [Données : corpus](files/donnees/corpus.md)
