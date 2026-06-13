# Script : dao_first_last_access_sans_conservation.py

**Emplacement :** `scripts/ead/dao_first_last_access_sans_conservation.py`

Catégorise les liens d'**accès** des plages `first`/`last` qui n'ont pas de
contrepartie de conservation déclarée dans l'EAD.

[dao_first_last_developpe.py](dao_first_last_developpe.md) ne développe un
`access:<média>` que lorsque le groupe ne porte aucun `preservation:<média>`.
Ces images diffusées sans master de conservation *déclaré* peuvent malgré tout
avoir un master dans le référentiel, sous un nom à extension de conservation
(`.tif`/`.jp2`). Le script recherche, pour chaque `access:image` développé, une
image de même radical (nom de base sans extension, casse et tirets normalisés)
dans le référentiel le plus récent, et range le lien en trois catégories.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/dao_first_last_verif_ref.csv` | Fichiers développés et vérifiés, produits par dao_first_last_verif_ref.py (seules les lignes `access:image` sont reprises) |
| `results/ref/_ref_files_{date}.csv.gz` | Fichier de référence — la version la plus récente est sélectionnée automatiquement |

Le rapprochement se fait par radical : le match exact par nom `.jpg` du
référentiel ne suffit pas, car le master de conservation porte une extension
différente. Pour le fonds sonore FLRS, on essaie aussi le radical après
renommage `RBX_MED_FLRS_` → `RBX_MED_` (et `+` → `_`), comme
ead_bnr2mnesys.py, au cas où le master aurait perdu le segment `FLRS` ; le
radical brut reste essayé en premier, les masters FLRS du référentiel
conservant généralement ce segment.

## Résultat

`results/ead/ead_cor/dao_first_last_access_sans_conservation.csv` — une ligne
par lien `access:image` développé.

| Colonne | Description |
|---|---|
| `ir`, `id_composant`, `unitid`, `role`, `href`, `position`, `taille_plage` | Colonnes reprises du lien d'accès |
| `categorie` | `conservation_existe`, `diffusion_seule` ou `absent` (voir plus bas) |
| `ref_name`, `ref_extension` | Image du référentiel rapprochée par radical (vide si absente) |
| `ref_conservation_statut` | Statut de conservation de l'image rapprochée |
| `ref_uuid` | uuid de l'image rapprochée |
| `ref_s3_key` | Chemin S3 (vide si non versé) |

---

## Catégories

| Catégorie | Définition | Lecture |
|---|---|---|
| `conservation_existe` | Un master de conservation (`.tif`/`.tiff`/`.jp2`) existe pour ce radical | Pas un vrai manque : l'EAD n'a simplement pas déclaré le `preservation` |
| `diffusion_seule` | Seule une image de diffusion (`.jpg`/`.png`) existe | Pas de master de conservation |
| `absent` | Aucune image de ce radical dans le référentiel | Aucune trace, ni conservation ni diffusion |

Les catégories `diffusion_seule` et `absent` réunissent les liens d'accès
réellement **sans master de conservation**, qui sont les cas à investiguer.

---

## Utilisation

Depuis la racine du projet, après dao_first_last_verif_ref.py :

    python scripts/ead/dao_first_last_access_sans_conservation.py
