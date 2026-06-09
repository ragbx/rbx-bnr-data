# Sources de données

Le projet s'appuie sur trois types de sources complémentaires.

---

## Fichiers EAD

Les instruments de recherche (IR) de la bn-r sont encodés en EAD.

- **Origine** : export ponctuel depuis l'outil bn-r
- **Emplacement** : `data/ead/bnr/`
- **Usage** : transformation pour import dans Mnesys

---

## Fichiers numérisés

Les fichiers issus des campagnes de numérisation sont répartis sur plusieurs supports :
- le serveur de stockage actuel, dit "Azraël",
- des disques durs externes,
- le serveur S3.
-
Le chemin et le nom de chaque fichier sont porteurs d'information : ils encodent
le corpus d'appartenance, la cote et parfois la séquence de numérisation.
Les métadonnées techniques (format, poids, dimensions) sont extraites et consolidées
dans le [fichier de référence](fichier_ref.md) `results/ref/_ref_files_{date}.csv.gz`.

→ Scripts d'inventaire : `scripts/azrael/`, `scripts/s3/`

---

## Serveur OAI de la bn-r

Le serveur OAI de la bn-r expose les notices des documents numérisés. Il permet
notamment de faire le lien entre les identifiants de notice bn-r (`osiros_id`) et
les balises `<unitid>` des instruments de recherche EAD.

- **Usage** : construction des anciens ARK BnR dans les fichiers EAD transformés
- **Script de moissonnage** : `scripts/oai/bnr_moissonnage.py`
- **Résultat** : `data/oai/oai_records_{date}.csv.gz`
