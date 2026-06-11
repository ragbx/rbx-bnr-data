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

Depuis la racine du projet, exécuter les scripts dans l'ordre :

```bash
python scripts/azrael/01a_azrael_list.py
python scripts/azrael/01b_azrael_list_detailed.py
python scripts/azrael/add_checksum.py
python scripts/azrael/02_compare_ref_az_it1.py
python scripts/azrael/02_compare_ref_az_it2.py
python scripts/azrael/02_compare_ref_az_it3.py
python scripts/azrael/03_merge_new_old_ref.py
```

Vérification de l'intégrité (optionnel) : `scripts/azrael/04_ref_integrité.ipynb`

Attention, cette séquence est indicative, les scripts et itération sont à adaptér à chaque nouvelle itération.

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

Valeurs possibles :

| Valeur | Signification |
|---|---|
| `TRANSFERT_S3_OK` | Versé sur S3 |
| `À TRANSFERER` | À verser sur S3 |
| `EN LIGNE - À TRANSFERER ?` | Accessible en ligne, statut S3 à confirmer |
| `INCONNU` | Statut non déterminé |
| `CORBEILLE` | À supprimer |
| `CORBEILLE (DIFFUSION)` | Fichier de diffusion à supprimer |
| `À SUPPRIMER` | Suppression planifiée |
| `À SUPPRIMER (remplacement par Verif / Tampon)` | Remplacé par une version vérifiée |
| `À SUPPRIMER (doublons S3 - az)` | Doublon entre S3 et Azrael |
| `À SUPPRIMER (RENOMMAGE)` | Fichier renommé, ancienne version à supprimer |
| `À SUPPRIMER (Tests)` | Fichier de test |
| `À SUPPRIMER (FILE_TYPE)` | Type de fichier non conservable |
| `DDE - NE PAS GARDER (DIFFUSION)` | Fichier de diffusion, non destiné à la conservation |
| `DDE - NE PAS GARDER (FILE_TYPE)` | Type de fichier exclu de la conservation |
| `DDE - NE PAS GARDER (DOUBLONS AZ)` | Doublon dans Azrael |

Valeurs à adapter selon les situations.

#### Stockage S3
| Colonne | Description |
|---|---|
| `source2s3` | Support de conservation avant versement S3 |
| `s3_bucket` | Nom du bucket |
| `s3_key` | Clé dans le bucket S3 |
| `s3_uploaded` | Versement effectué (booléen) |
| `s3_uploaded_date` | Date du versement (AAAAMMJJ) |
