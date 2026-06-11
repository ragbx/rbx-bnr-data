# Fichiers de conservation

Les fichiers de conservation sont les fichiers maîtres issus des campagnes de
numérisation (TIFF haute définition principalement), par opposition aux fichiers
de diffusion destinés à la consultation en ligne. Le chantier consiste à les
verser sur le stockage objet S3 et à en assurer le suivi.

Sauf exception, les fichiers de diffusion ne sont pas conservés.

L'inventaire complet des fichiers, leur statut de conservation et leur état de
versement sont tenus dans le [fichier de référence](fichier_ref.md)
(colonnes `conservation_statut`, `s3_key`, `s3_bucket`, `s3_uploaded`,
`s3_uploaded_date`).

---

## Supports source

Les fichiers à verser proviennent de plusieurs supports (colonne `source2s3`
du fichier de référence) :

| Valeur | Support |
|---|---|
| `az` | Serveur de stockage actuel, dit "Azraël" |
| `AMR_CADN_*` | Disques durs externes |
| `UnivLille`, `AMR_ARCHIPOP` | Autres sources externes |

---

## Stockage S3

Deux buckets selon la communicabilité des documents :

| Bucket | Contenu |
|---|---|
| `mediatheque-patarch-communicable` | Documents destinées à être librement diffusés sur la Bn-R |
| `mediatheque-patarch-incommunicable` | Documents qui ne seront pas communiqués au public |

Chaque objet versé porte deux tags S3, `uuid` et `checksum_md5`, qui permettent
de le rattacher au fichier de référence et d'en vérifier l'intégrité.

Les versements, listings et suppressions sont effectués avec les
[scripts S3](../scripts/s3.md).

---

## Données de suivi

| Fichier | Contenu |
|---|---|
| `data/s3/s3_listing_{date}.csv.gz` | Listing des objets présents sur S3 (fusionné ensuite avec le fichier de référence |
| `results/s3/repertoires_traitement_s3_ko.xlsx` | Répertoires contenant des fichiers dont le traitement S3 n'est pas réalisé (voir [le script](../scripts/repertoires_traitement_s3_ko.md)) |
