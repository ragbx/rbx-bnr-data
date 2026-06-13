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
| `results/ref/_ref_files_{date}.csv.gz` | Fichiers numérisés (`.tif` de conservation et `.xml` ALTO d'OCR) ; filtrés par `corpus_code` |
| `data/corpus_liste/bnr_corpus.xlsx` | Libellés des corpus (`collection_bnr` → eadid, `corpus` → titre) |

La date du référentiel est figée dans le script (`REF_DATE`). Les corpus traités
sont listés dans `CORPUS_PRESSE` (`PRA_*`) et `CORPUS_REGISTRE`
(`AMR_DEL`, `AMR_RAM`).

## Résultat

Un EAD par corpus dans `results/ead/corpus_ocr/{eadid}.xml`. Les id des `<c>` et
de l'`<archdesc>` sont générés au format Mnesys (préfixe `m0`, cf.
[mnesys_id.py](mnesys_id.md)).

Deux structures selon le type de corpus :

- **presse** (`PRA_*`) : hiérarchie année (`series`) / mois (`subseries`) /
  numéro (`file`), un `<c level="file">` par `unitid` daté
  (`PRA_XXX_AAAAMMJJ`) ;
- **registres** (`AMR_DEL`, `AMR_RAM`) : `dsc` plat, un `<c level="file">` par
  cote, triée par numéro.

Chaque `<c level="file">` porte un `<daogrp>` avec, par page (triées par
numéro), une `<daoloc role="preservation:image">` (le `.tif`) et une
`<daoloc role="preservation:ocr">` (l'ALTO). Le `href` est le `s3_key` quand il
est connu, sinon le chemin local `path/name`.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/corpusocr2ead.py
