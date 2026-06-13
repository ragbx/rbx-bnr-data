# Script : dao_first_last_plages_lacunaires.py

**Emplacement :** `scripts/ead/dao_first_last_plages_lacunaires.py`

Identifie les plages `first`/`last` **non contiguës** : celles dont une partie
des fichiers développés par
[dao_first_last_developpe.py](dao_first_last_developpe.md) est absente du
référentiel, d'après
[dao_first_last_verif_ref.py](dao_first_last_verif_ref.md).

Quand la numérotation réelle des fichiers n'est pas continue entre les deux
bornes (sous-séries regroupées, numéros sautés), le développement linéaire
« sur-génère » des noms inexistants : la plage apparaît lacunaire. Le script
sert à distinguer ces plages — où les manquants sont un artefact du
développement — des trous isolés, qui sont de meilleurs candidats de fichiers
réellement absents.

L'analyse est restreinte aux fichiers de **conservation** (role
`preservation:*`) : seuls ceux-ci sont recensés dans le référentiel, les liens
d'accès (`.jpg` dérivés) en étant structurellement absents.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/dao_first_last_verif_ref.csv` | Fichiers développés et vérifiés contre le référentiel, produits par dao_first_last_verif_ref.py |

Chaque plage est identifiée de façon exacte par son `(ir, id_composant, role)`
et le nom de sa borne `first`.

## Résultat

`results/ead/ead_cor/dao_first_last_plages_lacunaires.csv` — une ligne par
plage dont au moins un fichier manque, triée par nombre de manquants
décroissant.

| Colonne | Description |
|---|---|
| `ir`, `id_composant`, `unitid` | Contexte de la plage |
| `role` | Préfixe de role (`preservation:image`…) |
| `name_first`, `name_last` | Noms des bornes |
| `taille_plage` | Nombre de fichiers développés |
| `presents` | Fichiers retrouvés dans le référentiel |
| `manquants` | `taille_plage − presents` |
| `taux_presence` | Pourcentage de fichiers présents |

---

## Utilisation

Depuis la racine du projet, après dao_first_last_verif_ref.py :

    python scripts/ead/dao_first_last_plages_lacunaires.py

---

## Lecture

Deux natures de plages lacunaires se distinguent par le taux de présence :

- **taux faible sur une grande plage** (ex. 170/598) : numérotation non
  contiguë, le développement sur-génère ; les manquants ne sont pas de vrais
  fichiers perdus ;
- **taux élevé, un ou deux manquants** : trou isolé, meilleur candidat de
  fichier réellement absent du référentiel.
