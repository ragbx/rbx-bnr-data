# Script : dao_appariement_conservation.py

**Emplacement :** `scripts/ead/dao_appariement_conservation.py`

Tente d'apparier les liens de diffusion orphelins recensés par
[dao_sans_conservation.py](dao_sans_conservation.md) avec des fichiers de
conservation du fichier de référence, en élargissant progressivement les
critères d'appariement. Contrairement à [ead_bnr2mnesys.py](ead_bnr2mnesys.md),
les fichiers de référence **sans `s3_key`** (pas encore versés sur S3) sont
aussi considérés : le script sert à distinguer les orphelins dont le fichier
de conservation existe (sous un nom légèrement différent, ou non versé) de
ceux qui n'ont vraiment aucune correspondance.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/dao_sans_conservation.csv` | Liens de diffusion orphelins, produits par dao_sans_conservation.py |
| `results/ref/_ref_files_{date}.csv.gz` | Fichier de référence — la version la plus récente est sélectionnée automatiquement |

Le fichier de référence est dédoublonné par nom de fichier (en privilégiant
les lignes avec `s3_key`) et restreint aux extensions des familles de média
connues (image, audio, video, pdf).

## Résultat

`results/ead/ead_cor/dao_appariement_conservation.csv` — régénéré à chaque
exécution, une ligne par couple (orphelin, candidat de conservation), ou une
ligne unique à méthode `aucun` quand rien n'est trouvé. Un orphelin peut donc
produire plusieurs lignes ; quand un candidat tif existe, les candidats jpg —
redondants — sont écartés.

| Colonne | Description |
|---|---|
| `ir`, `id_composant`, `unitid`, `role`, `href` | Colonnes de l'orphelin, reprises de dao_sans_conservation.csv |
| `methode` | Méthode d'appariement retenue : `exacte`, `normalisee`, `padding` ou `aucun` |
| `ref_name` | Nom du fichier de conservation candidat |
| `ref_path` | Chemin du candidat sur le support source |
| `ref_uuid` | uuid du candidat (vide si absent) |
| `ref_conservation_statut` | Statut de conservation du candidat |
| `ref_s3_key` | Chemin S3 du candidat (vide si non versé) |

Le script affiche en outre une synthèse : décompte des orphelins par meilleure
méthode d'appariement (chaque orphelin compté une fois), puis répartition des
candidats par statut de conservation et présence de `s3_key`.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/dao_appariement_conservation.py

---

## Méthodes d'appariement

L'appariement se fait toujours **au sein d'une même famille de média**
(image, audio, video, pdf) : la famille de l'orphelin vient de son `role`
(`access:image` → image), celle du candidat de son extension. Trois méthodes
sont essayées, de la plus stricte à la plus souple ; chaque ligne du résultat
porte la plus stricte qui fonctionne :

### 1. `exacte`
Nom de base sans extension identique, avec les normalisations de
ead_bnr2mnesys.py :

- suffixes de variante de numérisation audio retirés (`_96kHz24B`, `_TI`…) ;
- côté EAD, renommage du fonds sonore FLRS : `RBX_MED_FLRS_` → `RBX_MED_`
  et `+` → `_`.

C'est la règle de [dao_sans_conservation.py](dao_sans_conservation.md), élargie
aux fichiers de référence sans `s3_key` : un appariement `exacte` signale en
général un fichier de conservation existant mais pas encore versé sur S3.

### 2. `normalisee`
En plus des normalisations précédentes :

- casse ignorée ;
- tirets assimilés aux underscores (`-` ↔ `_`) ;
- extensions de média intermédiaires retirées (`x.tif.jpg` → `x`).

### 3. `padding`
En plus, les zéros de tête des nombres sont ignorés (`_001` ↔ `_1`).

### `aucun`
Aucun candidat trouvé, même par la méthode la plus souple.
