# Documentation — Métadonnées et fichiers de la bn-r

Ce dépôt rassemble les travaux menés sur les métadonnées et les fichiers numériques
de la [Bibliothèque numérique de Roubaix](https://www.bn-r.fr) dans le cadre de sa
refonte. Il couvre à la fois les instruments de recherche encodés en EAD et les
fichiers issus des campagnes de numérisation.

Pour le vocabulaire employé, voir le [Glossaire](files/glossaire.md).

---

## Accès rapides

- [Suivi des corpus](../results/corpus_liste/suivi_corpus.xlsx?raw=true) — `results/corpus_liste/suivi_corpus.xlsx`, généré par [suivi_corpus.py](files/scripts/suivi_corpus.md)
- [Fichier de référence (dernière version)](../results/ref/_ref_files_20260502.csv.gz?raw=true) — `results/ref/_ref_files_20260502.csv.gz`
- [IR prêts pour Mnesys](../results/ead/ead_cor/bnr2mnesys/) — `results/ead/ead_cor/bnr2mnesys/`
- [Répertoires en traitement S3 KO](../results/s3/repertoires_traitement_s3_ko.xlsx?raw=true) — `results/s3/repertoires_traitement_s3_ko.xlsx`, généré par [repertoires_traitement_s3_ko.py](files/scripts/repertoires_traitement_s3_ko.md)
- [Stats DAO par IR](../results/dao/) — `results/dao/dao_stats_ir_{date}.xlsx`, généré par [dao_stats_ir.py](files/scripts/dao_stats_ir.md)

---

## Chantiers

### 1. Cartographier les corpus
Établir une vue d'ensemble des corpus numérisés : liste des ensembles documentaires,
état des fichiers, fichier de référence consolidé.

→ [Données : corpus](files/donnees/corpus.md) · [Fichier de référence](files/donnees/fichier_ref.md)

→ **[Enrichissement du référentiel](files/scripts/enrichissement_ref.md)** — maillon s3/dao/oai entre la chaîne azrael et la fusion finale

### 2. Transférer les fichiers vers le stockage S3
Verser les fichiers de conservation sur le stockage S3 et en assurer le suivi.

→ [Données : fichiers de conservation](files/donnees/fichiers.md) · [Scripts S3](files/scripts/s3.md)

### 3. Transférer les instruments de recherche vers Mnesys
Transformer les fichiers EAD produits par la bn-r pour les importer dans le logiciel
d'archivistique Mnesys.

→ [Les liens DAO : structures et cas de figure](files/donnees/dao_daogrp.md) — `<dao>` / `<daogrp>` dans les IR

→ **[Chaîne d'appariement des DAO](files/scripts/dao_appariement.md)** — vue d'ensemble, flux et enchaînement des scripts

Détail par script :
→ [Liste des IR à traiter](files/scripts/ead_liste_ir.md) · [ead_bnr2mnesys](files/scripts/ead_bnr2mnesys.md) (id Mnesys : [mnesys_id](files/scripts/mnesys_id.md)) · [Liens sans conservation](files/scripts/dao_sans_conservation.md) · [Appariement des orphelins](files/scripts/dao_appariement_conservation.md) · [Développement des plages](files/scripts/dao_first_last_developpe.md) · [Vérification dans le référentiel](files/scripts/dao_first_last_verif_ref.md) · [Plages non contiguës](files/scripts/dao_first_last_plages_lacunaires.md) · [Accès sans conservation](files/scripts/dao_first_last_access_sans_conservation.md)

**Indexation** — à partir des `<controlaccess>` des IR transformés :
→ [Génération des thésaurus SKOS](files/scripts/controlaccess2skos.md) · [Stats DAO par IR](files/scripts/dao_stats_ir.md)

### 4. Préparer la publication des corpus
Préparer les données en vue de leur publication en ligne.

→ [Données : corpus](files/donnees/corpus.md) · [Génération des EAD de corpus océrisés](files/scripts/corpusocr2ead.md)

→ **[Chaîne images de diffusion](files/scripts/images_diffusion.md)** — constitution des corpus d'images, copie des fichiers maîtres et conversion pour le web

Détail par script :
→ [Extraction des corpus TIFF](files/scripts/extraction_corpus_tif.md) · [Téléchargement des corpus](files/scripts/telechargement_corpus.md) · [Conversion TIFF → JP2](files/scripts/tif_convert.md)


## TODO
Les tâches à réaliser sur ce dépôt sont listées dans le fichier [TODO](files/todo.md)
