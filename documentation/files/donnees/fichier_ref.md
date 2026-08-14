# Fichier de référence des fichiers numérisés

`results/ref/_ref_files_{date}.csv.gz`

Fichier central du projet : recense l'ensemble des fichiers numérisés présents
sur les supports de stockage, avec leurs métadonnées et leur statut dans les
différents chantiers. Il est mis à jour à chaque nouvelle extraction d'Azrael.

## Sources de données

Le projet s'appuie sur trois types de sources complémentaires.

---

### Fichiers EAD

Les instruments de recherche (IR) de la bn-r sont encodés en EAD.

- **Origine** : export ponctuel depuis l'outil bn-r
- **Emplacement** : `data/ead/bnr/`
- **Usage** : transformation pour import dans Mnesys

---

### Fichiers numérisés

Les fichiers issus des campagnes de numérisation sont répartis sur plusieurs supports :
- le serveur de stockage actuel, dit "Azraël",
- des disques durs externes,
- le serveur S3.

Le chemin et le nom de chaque fichier sont porteurs d'information : ils encodent
le corpus d'appartenance, la cote et parfois la séquence de numérisation.
Les métadonnées techniques (format, poids, dimensions) sont extraites et consolidées
dans le [fichier de référence](fichier_ref.md) `results/ref/_ref_files_{date}.csv.gz`.

→ Scripts d'inventaire : `scripts/azrael/`, `scripts/s3/`

---

### Serveur OAI de la bn-r

Le serveur OAI de la bn-r expose les notices des documents numérisés. Il permet
notamment de faire le lien entre les identifiants de notice bn-r (`osiros_id`) et
les balises `<unitid>` des instruments de recherche EAD.

- **Usage** : construction des anciens ARK BnR dans les fichiers EAD transformés
- **Script de moissonnage** : `scripts/oai/bnr_moissonnage.py`
- **Résultat** : `data/oai/oai_records_{date}.csv.gz`

---

## Création et description du fichier de référence

### Création

La construction se fait en deux temps, à partir de la dernière extraction Azrael
et du ref précédent (dont on récupère `uuid` + `checksum_md5` sans relire les
fichiers autant que possible).

1. **Chaîne d'appariement azrael** (`scripts/azrael/`, étapes `10` → `55`) —
   apparie l'extraction courante au ref précédent (métadonnées, taille, puis MD5
   pour le résiduel) et réinjecte la partie non-azrael. Détail, convention de
   nommage et lancement : **`scripts/azrael/README.md`**.
2. **Maillon d'enrichissement s3/dao/oai** — attache l'état S3, la cote DAO et les
   identifiants OAI : voir **[Enrichissement du référentiel](../scripts/enrichissement_ref.md)**.
3. **Fusion finale** (`scripts/azrael/60_merge_new_old_ref.py`) — coalesce les
   valeurs neuves avec l'ancien ref pour produire `_ref_files_{date}.csv.gz`.

Les dates (`OLD_REF_DATE`, `NEW_REF_DATE`) sont centralisées dans
`scripts/azrael/_pipeline.py` — à modifier à chaque nouveau ref.

Vérification de l'intégrité (optionnel) : `scripts/azrael/04_ref_integrité.ipynb`

---

### Description des colonnes

#### Identification
| Colonne | Description |
|---|---|
| `name` | Nom du fichier |
| `path` | Chemin du fichier sur le support source |
| `uuid` | Identifiant unique stable du fichier |
| `checksum_md5` | Empreinte MD5 |

#### Métadonnées de base
| Colonne | Description |
|---|---|
| `size` | Taille en octets |
| `last_content_modification_date` | Date de dernière modification du contenu |
| `last_metadata_modification_date` | Date de dernière modification des métadonnées |
| `extension` | Extension |
| `file_type` | Type de fichier |
| `source2s3` | Support source pour le versement S3 (`az` = serveur Azrael ; `AMR_CADN_*` = disques durs externes ; `UnivLille`, `AMR_ARCHIPOP` = autres sources) |

#### Métadonnées techniques MIX (images)
| Colonne | Description |
|---|---|
| `mix_objectIdentifierValue` | Identifiant MIX |
| `mix_fileSize` | Taille selon MIX |
| `mix_dateTimeCreated` | Date de création |
| `mix_formatName` | Format |
| `mix_formatVersion` | Version du format |
| `mix_byteOrder` | Ordre des octets |
| `mix_compressionScheme` | Schéma de compression |
| `mix_imageWidth` | Largeur en pixels |
| `mix_imageHeight` | Hauteur en pixels |
| `mix_xSamplingFrequency` | Résolution horizontale |
| `mix_ySamplingFrequency` | Résolution verticale |
| `mix_samplingFrequencyUnit` | Unité de résolution |
| `mix_colorSpace` | Espace colorimétrique |
| `mix_scanningSoftwareName` | Logiciel de numérisation |

#### Appartenance à un corpus et description
| Colonne | Description |
|---|---|
| `corpus_code` | Code du corpus |
| `finding_aid` | Nom du fichier EAD associé |
| `unitid` | Cote (`<unitid>` dans l'EAD) |
| `osiros_id` | Identifiant de notice bn-r |
| `oai_set` | Set OAI d'appartenance |
| `publication_statut` | Statut de publication sur la Bn-r (`oui` / `jamais` / `inconnu`) |

#### Conservation
| Colonne | Description |
|---|---|
| `conservation_statut` | Statut de conservation |

Valeurs possibles (19, harmonisées le 2026-07-11 — la famille `CORBEILLE` et les
libellés `DDE - NE PAS GARDER (*)` ont été repliés dans `À SUPPRIMER (*)`) :

| Valeur | Signification |
|---|---|
| `TRANSFERT_S3_OK` | Versé sur S3 |
| `À TRANSFERER APRES VALIDATION` | À verser sur S3, décision prise mais versement pas encore réalisé (témoin de l'accompli réel : `s3_uploaded`) |
| `EN LIGNE - À TRANSFERER ?` | Accessible en ligne, statut S3 à confirmer |
| `INCONNU` | Statut non déterminé (backlog à trier) |
| `INCONNU FRAD59` | Statut non déterminé, fonds versé par les Archives départementales (FRAD59), à trancher avec elles |
| `INCONNU VOIR MARIE` | Statut non déterminé, à trancher avec Marie |
| `À TRANSFERER VOIR MARIE` | À verser sur S3 sous réserve de l'avis de Marie |
| `S3_KEY À CONSTRUIRE` | Clé S3 cible restant à construire |
| `DOUBLON - À VOIR` | Doublon suspecté, à examiner manuellement |
| `À SUPPRIMER (DIFFUSION)` | Fichier de diffusion à supprimer (inclut l'ancienne famille `CORBEILLE` / `CORBEILLE (DIFFUSION)`) |
| `À SUPPRIMER (DOUBLON)` | Doublon confirmé par checksum |
| `À SUPPRIMER (DOUBLON AZ)` | Doublon au sein d'Azrael |
| `À SUPPRIMER (DOUBLON S3-AZ)` | Doublon entre S3 et Azrael |
| `À SUPPRIMER (DOUBLON SOURCE - À VERIFIER)` | Doublon à la source (zone grise AMR_EC / AMR_GUE) — **alerte** : contrôle documentaire par échantillon requis avant purge |
| `À SUPPRIMER (REMPLACEMENT)` | Remplacé par une version vérifiée |
| `À SUPPRIMER (RENOMMAGE)` | Fichier renommé, ancienne version à supprimer |
| `À SUPPRIMER (FILE_TYPE)` | Type de fichier non conservable |
| `À SUPPRIMER (TESTS)` | Fichier de test |
| `À SUPPRIMER (MED_PAR)` | Suppression spécifique au corpus MED_PAR |

Seuls `TRANSFERT_S3_OK` et `À SUPPRIMER (*)` comptent comme traitement S3 **accompli**
(voir [suivi_corpus.py](../scripts/suivi_corpus.md)) ; `À TRANSFERER*` reste du décidé,
pas de l'accompli.

#### Stockage S3
| Colonne | Description |
|---|---|
| `source2s3` | Support de conservation avant versement S3 |
| `s3_bucket` | Nom du bucket |
| `s3_key` | Clé dans le bucket S3 |
| `s3_uploaded` | Versement effectué (booléen) |
| `s3_uploaded_date` | Date du versement (AAAAMMJJ) |
| `s3_stem_profil` | Profil documentaire du fichier : extensions présentes pour le même document (même *stem* de `s3_key`), normalisées et jointes par `+` (ex. `pdf+tif+txt+xml`). Calculé par `scripts/s3/ref_stem_profil_20260711.py` : rattache aussi, par (`corpus_code`, nom sans extension), les fichiers `À SUPPRIMER (*)` sans `s3_key` au profil de leur document, avec l'extension marquée `(sup)` (ex. `jpg+jpg(sup)+tif`). Vide si le fichier n'a pas de `s3_key` et n'a pas pu être rattaché. Script idempotent, à rejouer après tout ajout/modification de clés |
