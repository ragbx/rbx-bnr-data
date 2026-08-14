# Script : corpusocr2ead.py

**Emplacement :** `scripts/ead/corpusocr2ead.py`

Génère des instruments de recherche EAD **de toutes pièces** pour les corpus
océrisés (presse et registres) à partir du fichier de référence des fichiers
numérisés. Contrairement à [ead_bnr2mnesys.py](ead_bnr2mnesys.md) qui transforme
des EAD existants, ce script construit l'arborescence EAD à partir des seules
métadonnées de référencement.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ref/_ref_files_{date}.csv.gz` | Fichiers numérisés (`.tif` de conservation et `.xml` ALTO d'OCR) ; filtrés par `corpus_code`. Colonne `osiros_id` : ancien identifiant OAI, pour l'ARK `publication:previous` |
| `data/corpus_liste/bnr_corpus.xlsx` | Libellés des corpus (`collection_bnr` → eadid, `corpus` → titre) |

La date du référentiel est figée dans le script (`REF_DATE`). Les corpus traités
sont listés dans `CORPUS_PRESSE` (`PRA_*`) et `CORPUS_REGISTRE`
(`AMR_DEL`, `AMR_RAM`).

## Résultat

Un EAD par corpus dans `results/ead/corpus_ocr/{eadid}.xml`. Les id des `<c>` et
de l'`<archdesc>` sont générés au format Mnesys (préfixe `m0`, cf.
[mnesys_id.py](mnesys_id.md)), et stabilisés d'une exécution à l'autre par une
concordance `(ir, unitid) → id` — même mécanisme que `_add_ids` dans
[ead_bnr2mnesys.py](ead_bnr2mnesys.md) — tenue dans
`results/ead/ead_cor/concordance_id_corpusocr.csv` (créée à la première
exécution). L'`ir` est l'`eadid` du corpus, l'`unitid` celui du `<c
level="file">` ; les `<c level="series"/"subseries">`, sans `unitid`, reçoivent
un id nouveau à chaque exécution.

Cet id sert de base aux liens ARK BnR, ajoutés à chaque `<archdesc>`/`<c>` via
`ajouter_ark` (même logique que `_add_dao_ark` dans
[ead_bnr2mnesys.py](ead_bnr2mnesys.md), cf. [dao_ark.py](dao_ark.md)) :

- l'ARK actuel (`role="publication:current"`), construit à partir de l'id :
  `https://www.bn-r.fr/ark:/20179/BNR{id}` ;
- l'ancien ARK (`role="publication:previous"`), pour les `<c>` dont l'`unitid`
  figure dans la correspondance `unitid → osiros_id` construite par
  `dict_osiros` à partir de la colonne `osiros_id` du ref (déjà préfixée
  `BNR`) : `https://www.bn-r.fr/ark:/20179/{osiros_id}`.

Sur les `<c level="file">`, qui ont déjà un `<daogrp>`, les liens sont ajoutés
sous forme de `<daoloc>` dans ce `<daogrp>` plutôt que d'un `<dao>` séparé.

Deux structures selon le type de corpus :

- **presse** (`PRA_*`) : hiérarchie année (`series`) / mois (`subseries`) /
  numéro (`file`), un `<c level="file">` par `unitid` daté
  (`PRA_XXX_AAAAMMJJ`) ;
- **registres** (`AMR_DEL`, `AMR_RAM`) : `dsc` plat, un `<c level="file">` par
  cote, triée par numéro.

Chaque `<c level="file">` porte un `<daogrp>` avec, pour chaque rôle
(`preservation:image`, `access:image`, `preservation:ocr`, `access:ocr`),
deux `<daoloc>` — `{rôle}:first` et `{rôle}:last` — délimitant la plage de
pages (triées par numéro) plutôt qu'une entrée par page. S'il n'y a qu'une
seule page, une seule `<daoloc role="{rôle}">` est émise (sans suffixe).
L'image d'accès reprend le chemin de conservation avec l'extension remplacée
par `.jpg` ; l'OCR d'accès reprend le même chemin que l'OCR de conservation
(nom de fichier inchangé). Le `href` est le `s3_key` quand il est connu,
sinon le chemin local `path/name`.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/corpusocr2ead.py
