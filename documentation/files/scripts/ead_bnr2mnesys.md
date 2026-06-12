# Script : ead_bnr2mnesys.py

**Emplacement :** `scripts/ead/ead_bnr2mnesys.py`

Transforme les fichiers EAD des instruments de recherche de la bn-r
en vue de leur import dans Mnesys.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `data/ead/bnr/` | Fichiers EAD source |
| `data/oai/oai_records_{date}.csv.gz` | Correspondances cote (unitid) / notices bn-r (pour liens ark)|
| `results/ead/indexation/controlaccess_extraction_name2.csv` | Noms typés persname / corpname |
| `results/ref/_ref_files_{date}.csv.gz` | Fichiers de conservation et OCR (chemins S3) |
| `results/ir/liste_instruments_recherche_{date}_transfert_mnesys.xlsx` | Liste des IR à traiter |

Seuls les instruments de recherche dont le statut est `TRANSFERER` dans le fichier
liste_instruments_recherche_{date}_transfert_mnesys.xlsx sont traités.

## Résultats

Les fichiers EAD transformés sont stockés dans `results/ead/ead_cor/bnr2mnesys/`.

La concordance ir / unitid / id (cf. étape 4) est tenue dans
`results/ead/ead_cor/concordance_id.csv` : créée à la première exécution, elle est
rechargée aux exécutions suivantes pour réattribuer les mêmes id.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/ead_bnr2mnesys.py

---

## Transformations appliquées

Les transformations sont appliquées dans l'ordre suivant sur chaque fichier EAD :

### 1. Mise à jour des métadonnées de l'instrument de recherche
- `<eadid>` : remplacé par la nouvelle valeur issue du fichier Excel.
- `<archdesc/did/unitid>` : remplacé par le nouveau identifiant de l'IR.
- `<archdesc/did/repository>` : remplacé par le nouveau nom du service versant.

### 2. Nettoyage du contenu textuel
- Décodage des entités HTML (`&amp;`, `&lt;`, etc.) dans tout l'arbre XML.
- Suppression des espaces en début/fin de texte dans les enfants de `<controlaccess>`.

### 3. Réorganisation des accès (indexation)
- `<origination>` : les éléments `<name>` et `<persname>` qu'il contient sont déplacés dans
  `<controlaccess>` (créé si absent). `<origination>` est supprimé s'il devient vide.
  Lors du déplacement, les `<name>` sont reclassés en `<persname>` ou `<corpname>` selon la
  liste CSV si une correspondance est trouvée.

### 4. Ajout des attributs `id`
- Chaque `<archdesc>` et `<c>` sans attribut `id` en reçoit un, généré au format Mnesys
  avec le préfixe `m0` (cf. `scripts/ead/mnesys_id.py`).
- Les id générés sont consignés dans la concordance ir / unitid / id
  (`results/ead/ead_cor/concordance_id.csv`). Aux exécutions suivantes, si une entrée
  existe pour (ir, unitid), l'id qu'elle contient est repris : les id restent stables
  d'une itération à l'autre. Les éléments sans `<did>/<unitid>` reçoivent un id nouveau
  à chaque exécution (pas de clé de concordance).

### 5. Fusion des `<daogrp>` multiples
- Quand un même `<archdesc>`/`<c>` contient plusieurs `<daogrp>` directs, leurs contenus
  sont fusionnés dans le premier. Les `<daodesc>` de même texte et les `<daoloc>` de
  même couple (href, role) ne sont pas dupliqués.
- La fusion est réappliquée après l'étape 8 : la conversion d'un `<dao>` isolé en
  `<daogrp>` peut recréer un doublon dans un `<c>` qui possédait déjà un `<daogrp>`.

### 6. Ajout des liens ARK BnR
- Pour chaque `<archdesc>` et `<c>` portant un attribut `id`, l'ARK actuel construit à
  partir de cet id (`https://www.bn-r.fr/ark:/20179/BNR<id>`) est ajouté avec
  `role="publication:current"`.
- Pour chaque `<c>` dont le `<unitid>` figure dans la table de correspondance OAI,
  l'ancien ARK BnR (`https://www.bn-r.fr/ark:/20179/<osiros_id>`) est ajouté avec
  `role="publication:previous"`.
- Les balises sont insérées selon trois cas :
  - `<daogrp>` déjà présent → ajout d'un `<daoloc>` par lien dans le groupe (sans
    doublon de role).
  - `<dao>` présent (sans `<daogrp>`) → transformation en `<daogrp>` contenant un `<daoloc>`
    reprenant les attributs de l'ancienne `<dao>` et un `<daoloc>` par lien.
  - Ni `<dao>` ni `<daogrp>` → création d'un `<dao>` (lien unique) ou d'un `<daogrp>`
    (plusieurs liens).
- Dans les cas 2 et 3, la balise est insérée avant le premier enfant `<c>` ou `<dsc>`
  s'il existe.

### 7. Mise à jour des rôles des `<dao>`
- Pour tous les éléments `<dao>` et `<daoloc>` dont l'attribut `role` commence par `image`
  ou vaut `mp3`, `mp4` ou `pdf`, le préfixe `access:` est ajouté devant la valeur.
  `mp3` est au passage renommé en `audio` et `mp4` en `video` (→ `access:audio`,
  `access:video`, `access:pdf`).

### 8. Ajout des chemins de conservation
- Pour chaque `<dao>`/`<daoloc>` dont le `href` correspond (par basename sans extension) à un
  fichier du CSV de référence (s3_key non null), un nouveau `<daoloc role="preservation:...">`
  est ajouté avec le chemin S3 (`s3_key`). Le `role` est obtenu en remplaçant `access:` par
  `preservation:` dans le `role` de l'élément source.
- L'appariement est contraint par famille de média (`FAMILLES_MEDIA`) : un jpg de
  diffusion ne peut être apparié qu'à un fichier image en conservation (tif de
  préférence), un mp3 qu'à un fichier audio (wav de préférence), etc. Les fichiers
  de conservation hors familles connues (xml, txt…) sont ignorés.
- Cas particulier de l'audio : les fichiers de conservation portent un suffixe de
  variante de numérisation (`_96kHz24B`, `_44kHz24B`, `_TI`), retiré pour l'appariement ;
  toutes les variantes trouvées sont ajoutées en `preservation:audio` (96 puis 44
  puis TI). Pour le fonds sonore FLRS, le nom EAD `RBX_MED_FLRS_*` correspond en
  conservation à `RBX_MED_*` (et `+` y devient `_`).
- Le `href` du lien de diffusion est complété avec le dossier du chemin de conservation :
  `RBX_MED_CP_001.jpg` + s3_key `MED/MED_CP/RBX_MED_CP_001.tiff`
  → `MED/MED_CP/RBX_MED_CP_001.jpg`.
- Les URL absolues de l'ancien site (`http://www.bn-r.fr/musique/…`,
  `http://www.bn-r.fr/video/…`), qui ne font plus sens, sont réécrites de la même
  façon : seul le nom de fichier est conservé, le dossier provient du chemin de
  conservation. Faute de correspondance de conservation, l'URL d'origine est
  laissée telle quelle.
- Exception pour la diffusion audio : quand un mp3 de la variante `TI` existe en
  conservation, le href de diffusion est remplacé par son chemin S3 complet
  (ex. `MED/MED_FLRS/FLR_17_2389/RBX_MED_FLR_17_2389_A_01_TI.mp3`), plutôt que par
  un chemin reconstruit à partir du nom EAD.

> **Limite connue (diffusion audio).** Au 2026-06-12, sur 1 111 liens `access:audio`,
> seuls 444 pointent vers un mp3 `TI` réellement présent dans le bucket. 576 pistes
> ont des masters wav en conservation mais aucun mp3 `TI` : leur href de diffusion
> est un chemin reconstruit à partir du nom EAD
> (ex. `MED/MED_FLRS/FLR_78_0101/RBX_MED_FLRS_FLR_78_0101_A_01.mp3`), qui ne
> correspond probablement à aucun fichier S3 existant. Les 91 restants n'ont aucune
> correspondance de conservation (href d'origine inchangé). Avant l'import Mnesys,
> il faudra soit produire les mp3 de diffusion manquants, soit choisir un repli
> (par exemple pointer la diffusion vers le wav `44kHz24B`).
- Les fichiers OCR (file_type `ocr xml` du CSV) sont appariés de la même façon, par
  nom de base, aux liens des familles image et pdf : un `<daoloc role="access:ocr">`
  pointant vers le s3_key de l'OCR est ajouté dans le même `<daogrp>`.
- Si le `<dao>` est isolé (hors `<daogrp>`), il est converti en `<daogrp>` + `<daoloc>` au préalable.

### 9. Tri des enfants des `<daogrp>`
- Dans chaque `<daogrp>`, `<daodesc>` est placé en premier, puis les `<daoloc>` sont
  réordonnés : `preservation:` d'abord, `access:` ensuite, `publication:` en dernier.

### 10. Reclassement des balises `<name>`
- Dans `<controlaccess>`, les balises `<name>` sont remplacées par `<persname>` ou `<corpname>`
  selon la liste CSV. Les `<name>` sans correspondance sont laissés tels quels.

### 11. Suppression des `<repository>` hors contexte
- Toutes les balises `<repository>` situées en dehors de `<archdesc/did>` sont supprimées.

### 12. Nettoyage final
- Suppression des attributs dont la valeur est une chaîne vide.
- Suppression récursive des éléments XML vides (sans texte, sans attribut, sans enfant non vide).
