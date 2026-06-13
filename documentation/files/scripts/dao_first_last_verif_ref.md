# Script : dao_first_last_verif_ref.py

**Emplacement :** `scripts/ead/dao_first_last_verif_ref.py`

Vérifie que les fichiers des plages `first`/`last` développées par
[dao_first_last_developpe.py](dao_first_last_developpe.md) existent dans le
fichier de référence, et sont versés sur S3.

Pour chaque fichier développé (bornes et intermédiaires implicites), le script
recherche dans le référentiel le plus récent une ligne de même `name` (nom de
fichier avec extension) et indique s'il y figure, s'il est versé sur S3
(`s3_key` présent), avec son `uuid`, son statut de conservation et son chemin
S3.

L'enjeu porte surtout sur les fichiers de **conservation** (role
`preservation:*`) : ce sont eux qui sont suivis dans le référentiel. Les liens
d'accès (`.jpg` de diffusion) en sont souvent absents, le référentiel ne
recensant pas les dérivés ; un faible taux de présence côté accès n'est donc
pas un signal de manque.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/dao_first_last_developpe.csv` | Fichiers développés des plages, produits par dao_first_last_developpe.py |
| `results/ref/_ref_files_{date}.csv.gz` | Fichier de référence — la version la plus récente est sélectionnée automatiquement |

Le référentiel est dédoublonné par `name` en privilégiant les lignes versées
sur S3 (`s3_key` présent).

## Résultat

`results/ead/ead_cor/dao_first_last_verif_ref.csv` — la liste développée
enrichie, une ligne par fichier.

| Colonne | Description |
|---|---|
| `ir`, `id_composant`, `unitid`, `role`, `href`, `position`, `taille_plage` | Colonnes reprises de dao_first_last_developpe.csv |
| `name` | Nom de fichier (basename de `href`), clé de la jointure |
| `trouve_ref` | Le fichier figure dans le référentiel |
| `dans_s3` | Le fichier est versé sur S3 (`s3_key` présent) |
| `ref_uuid` | uuid du fichier dans le référentiel |
| `ref_conservation_statut` | Statut de conservation |
| `ref_s3_key` | Chemin S3 (vide si non versé) |
| `ref_path` | Chemin du fichier sur le support source |

Le script affiche une synthèse par role (trouvés dans le référentiel, versés
sur S3, manquants) et la répartition par statut de conservation.

---

## Utilisation

Depuis la racine du projet, après dao_first_last_developpe.py :

    python scripts/ead/dao_first_last_verif_ref.py

---

## Suite

Les plages dont une partie des fichiers manque au référentiel sont isolées par
[dao_first_last_plages_lacunaires.py](dao_first_last_plages_lacunaires.md), qui
distingue les numérotations non contiguës des trous réellement à investiguer.
