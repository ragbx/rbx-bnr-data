# Script : dao_sans_conservation.py

**Emplacement :** `scripts/ead/dao_sans_conservation.py`

Liste, pour chaque IR transformé par [ead_bnr2mnesys.py](ead_bnr2mnesys.md), les
liens de diffusion (`role="access:*"`) auxquels aucun fichier de conservation
(`role="preservation:*"`) n'a été apparié. C'est l'outil de diagnostic des liens
« orphelins » restés sans chemin S3 après la transformation.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/bnr2mnesys/*.xml` | Fichiers EAD transformés par ead_bnr2mnesys.py |

## Résultat

`results/ead/ead_cor/dao_sans_conservation.csv` — régénéré à chaque exécution,
une ligne par lien de diffusion orphelin.

| Colonne | Description |
|---|---|
| `ir` | Nom du fichier EAD |
| `id_composant` | Attribut `id` du composant `<c>` (ou `<archdesc>`) contenant le lien |
| `unitid` | Cote (`<did>/<unitid>`) du composant |
| `role` | Rôle du lien (`access:image`, `access:audio`, `access:video`, `access:pdf`) |
| `href` | Cible du lien de diffusion |

Le script affiche en outre un décompte d'orphelins par IR (tous les IR sont
listés, y compris ceux à zéro) et un total.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/dao_sans_conservation.py

---

## Règle d'appariement

L'appariement reprend la logique de l'étape 8 de
[ead_bnr2mnesys.py](ead_bnr2mnesys.md) : au sein d'un même `<daogrp>` (les
`<dao>` isolés formant chacun leur propre groupe), un lien `access:*` est
considéré comme couvert s'il existe un lien `preservation:*` :

- de la **même famille de média** (image, audio, video, pdf — déduite de la
  partie du `role` après `access:`/`preservation:`) ;
- de **même nom de base sans extension**, après les mêmes normalisations que
  ead_bnr2mnesys.py :
  - suffixes de variante de numérisation audio retirés (`_96kHz24B`, `_TI`…) ;
  - renommage du fonds sonore FLRS : `RBX_MED_FLRS_` → `RBX_MED_` et `+` → `_`.

Les liens `access:ocr`, dérivés des liens image/pdf, sont ignorés (ils n'ont
pas de fichier de conservation propre).

Les orphelins recensés peuvent ensuite être passés à
[dao_appariement_conservation.py](dao_appariement_conservation.md), qui tente
de leur trouver un fichier de conservation par des appariements plus souples
sur le fichier de référence.
