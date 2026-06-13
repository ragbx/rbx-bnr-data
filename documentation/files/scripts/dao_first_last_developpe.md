# Script : dao_first_last_developpe.py

**Emplacement :** `scripts/ead/dao_first_last_developpe.py`

Développe les plages de liens DAO décrites par leurs seules bornes `*:first` et
`*:last`. Dans un `<daogrp>`, une suite de fichiers peut n'être représentée que
par son premier et son dernier lien ; les fichiers intermédiaires sont
implicites et n'apparaissent nulle part. Ce script les reconstitue, pour
disposer de la liste exhaustive des fichiers réellement concernés.

Exemple, pour les bornes :

    access:image:first = CSV/RBX_CSV_PAL_1858_01.jpg
    access:image:last  = CSV/RBX_CSV_PAL_1858_08.jpg

sont aussi concernés `_02` à `_07`, soit huit fichiers au total.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/bnr2mnesys/*.xml` | IR EAD ; chaque `<daogrp>` peut porter des couples de bornes `*:first` / `*:last` |

Les couples sont regroupés par préfixe de role (`access:image`,
`preservation:image`…) : un couple est constitué d'un `:first` et d'un `:last`
de même préfixe au sein d'un même groupe.

Dans un même groupe, les bornes `access:<média>` et `preservation:<média>`
décrivent les mêmes fichiers. Pour éviter de les compter deux fois, **l'`access`
n'est développé que si le `preservation` de même famille est absent du
groupe** : la conservation prime, et un `access` développé signale donc une
plage sans contrepartie de conservation déclarée.

## Résultat

`results/ead/ead_cor/dao_first_last_developpe.csv` — une ligne par fichier de
chaque plage, bornes comprises.

| Colonne | Description |
|---|---|
| `ir` | Nom du fichier IR |
| `id_composant` | `id` du composant `<c>` portant la borne |
| `unitid` | `unitid` du composant |
| `role` | Préfixe de role de la plage, sans suffixe (`access:image`, `preservation:image`…) |
| `href` | Chemin reconstitué du fichier |
| `position` | `first`, `last` ou `intermediaire` (fichier implicite) |
| `taille_plage` | Nombre de fichiers de la plage |

`results/ead/ead_cor/dao_first_last_ambigus.csv` — couples écartés faute
d'énumération non ambiguë (voir plus bas).

| Colonne | Description |
|---|---|
| `ir`, `id_composant`, `unitid`, `role` | Contexte du couple |
| `href_first`, `href_last` | Les deux bornes telles quelles |

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/dao_first_last_developpe.py

---

## Développement des plages

La borne `first` et la borne `last` ne diffèrent en général que par un unique
segment numérique. Le nom de base est découpé en tokens (suites de chiffres /
suites de non-chiffres) ; l'énumération porte sur l'unique token numérique qui
change, du début (`first`) à la fin (`last`) inclus. Le dossier, l'extension et
le reste du nom sont conservés tels quels, et la largeur du segment de `first`
fixe le zéro de tête (`_01`, `_02`, …).

Un couple est **écarté comme ambigu** et reporté dans
`dao_first_last_ambigus.csv` lorsque les bornes diffèrent par autre chose qu'un
seul segment numérique : nombre de tokens différent (`…010` vs `…010.2`),
plusieurs segments qui changent (`…S2_008` vs `…S1_010`), ou différence non
numérique. Ces cas demandent une relecture manuelle.
