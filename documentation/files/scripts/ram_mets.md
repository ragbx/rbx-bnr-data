# Scripts : chaîne rapports du maire (JPEG de diffusion et METS)

**Emplacement :** `scripts/img/img3/ram_mets.py`

Chaîne de production pour les **rapports du maire** (corpus `AMR_RAM`) : chaque TIFF
de conservation donne un **JPEG de diffusion**, et chaque rapport reçoit un **METS**
qui décrit ses fichiers. JPEG et METS sont ensuite déposés sur S3, à côté des
fichiers existants.

C'est la [chaîne presse](presse_mets.md) appliquée aux rapports, avec le METS de la
[chaîne délibérations](del_mets.md) : `ram_mets.py` reprend le traitement de
`presse_mets.py` et la construction du METS de `del_mets.py`, et ne définit que ce
qui est propre aux rapports. Cette page décrit ces différences.

**État au 13 octobre 2026** : le script n'a tourné que sur 6 images de synthèse.
Aucun TIFF réel des rapports n'a été converti, aucun fichier n'a été envoyé sur S3.

---

## Périmètre

| | |
|---|---|
| Clés S3 | `AMR/AMR_RAM/<volume>/` |
| Volumes (répertoires) | 26, de `3D001_1862-1870` à `3D026_1938` |
| Rapports | 59 |
| TIFF | 18 408, en couleur, non compressés, 24 Mo en moyenne, 443 Go au total |
| ALTO | 18 409 |
| PDF | 26, un par volume |

Le PDF de chaque volume (`RBX_AMR_3D_<n>_<années>.pdf`) n'est ni lu ni décrit par
le METS : il porte sur le volume entier, pas sur un rapport.

---

## Rapport et vue

Dans les volumes qui réunissent plusieurs années, la numérotation des vues repart
à `0001` à chaque année. L'unité décrite par un METS est donc **le rapport** : dans
un répertoire, les fichiers de même préfixe `RBX_AMR_RAM_<cote>_<année>`.

| Volume | Fichiers | Rapports |
|---|---|---|
| `3D001_1862-1870` | `RBX_AMR_RAM_3D001_1862_0001` … `RBX_AMR_RAM_3D001_1870_0090` | 9, un par année |
| `3D017_1911` | `RBX_AMR_RAM_3D017_1911_0001` … `_0577` | 1 |
| `3D020_1914-1930` | `RBX_AMR_RAM_3D020_1914-1930_0001` … `_0454` | 1 : la plage n'est pas découpée |

Ce découpage est celui de l'instrument de recherche : l'EAD
`results/ead/corpus_ocr/RBX_AMR_RAM.xml` compte 59 notices, une par rapport, chacune
avec sa cote (« 3 D 1-1 », « 3 D 26 ») et son ARK. Les 59 rapports du fichier de
référence y sont tous retrouvés, par la clé de leur première image.

Un rapport compte de 71 à 609 vues (343 en médiane).

---

## Ce qui change par rapport aux délibérations

| Point | Délibérations (`del_mets.py`) | Rapports du maire (`ram_mets.py`) |
|---|---|---|
| Qualité JPEG | 75 | **80**, comme la presse |
| Unité décrite par un METS | le registre : un répertoire | le rapport : une année dans un répertoire |
| Fichiers décrits par vue | TIFF, JPEG, ALTO pour les tapuscrits | TIFF, JPEG, ALTO |
| Intitulé | « Conseil municipal. Registres des procès-verbaux des délibérations » | « Administration de la commune. Rapports du maire » |
| Dates | dates extrêmes lues dans l'EAD | année ou plage d'années lue dans le nom des fichiers |
| Sections descriptives | `DMD_SERIE`, `DMD_REGISTRE` | `DMD_SERIE`, `DMD_RAPPORT` |
| Récapitulatifs | `del_mets_*.csv`, `del_jpeg_*.csv` | `ram_mets_*.csv`, `ram_jpeg_*.csv` |

L'intitulé est repris de l'instrument de recherche `FRAC512_D_01` (constante
`TITRE` du script). Comme pour les délibérations, le MODS ne porte pas de genre.

---

## Le METS

Un METS par rapport, déposé dans le répertoire du volume :

- `AMR/AMR_RAM/3D001_1862-1870/RBX_AMR_RAM_3D001_1862_mets.xml`
- `AMR/AMR_RAM/3D017_1911/RBX_AMR_RAM_3D017_1911_mets.xml`

Sa forme est celle du [METS des délibérations](del_mets.md#le-mets) : `OBJID` égal
à la clé S3 du METS sans l'extension, uuid en `altRecordID`, deux MODS, un bloc MIX
par TIFF et par JPEG, groupes `master`, `access` et `alto`, uuid de chaque fichier
en `OWNERID`, structure physique volume > page.

---

## Entrées

Les manifestes sont rangés dans `results/amr_ram/` :

| Manifeste | Contenu |
|---|---|
| `manifeste_test_2_rapports.csv` | Lot d'essai : le rapport de 1865 (71 vues, volume de plusieurs années) et celui de 1934 (332 vues, volume d'une année), soit 403 TIFF et 10 Go. |
| `manifeste_AMR_RAM.csv.gz` | Les 59 rapports : 18 408 TIFF et 18 409 ALTO. |

Les fichiers `*_tiff.txt` voisins donnent la liste des TIFF (`path/name`), pour les
copier sous `<source>`.

**Les TIFF** : lus sous `<source>/BNR_VERIF/AMR/AMR_RAM/RAM_TAP/3D001(1862-1870)/…`.
Les noms de dossiers portent des parenthèses.

---

## Utilisation

Les options sont celles de `presse_mets.py`.

Essai sur deux rapports, sans envoi, avec estimation de la durée pour l'ensemble :

```bash
python scripts/img/img3/ram_mets.py results/amr_ram/manifeste_test_2_rapports.csv --source /chemin/racine --out-dir /chemin/sortie --validate --estimer-pages 18408
```

Corpus complet, avec envoi réel sur S3 :

```bash
python scripts/img/img3/ram_mets.py results/amr_ram/manifeste_AMR_RAM.csv.gz --source /chemin/racine --out-dir /chemin/sortie --validate --upload
```

Dans les récapitulatifs et les messages du lot, le mot « fascicule » désigne un
rapport.

---

## Limites

- **Jamais lancé sur de vrais TIFF** : cadence, poids des JPEG et comportement sur
  les TIFF en couleur restent à mesurer sur le lot d'essai.
- **Une vue sans TIFF** : la vue `0329` du rapport de 1934 (`3D022_1934`) a un ALTO
  mais pas de TIFF au fichier de référence. Le METS la décrit avec son seul ALTO,
  et le récapitulatif la signale en page incomplète.
- **Un rapport est traité d'un bloc** : un seul TIFF absent, différent du fichier de
  référence ou illisible met tout le rapport en erreur.
- **PDF des volumes** : absents des METS.
- **Bilan** : `presse_bilan.py` ne connaît pas les rapports. `presse_download.py`,
  lui, sait télécharger leurs JPEG et leurs ALTO (depuis le 15 octobre 2026).

---

## Voir aussi

- [Chaîne presse](presse_mets.md) — le traitement commun, en détail
- [Chaîne délibérations](del_mets.md) — la forme du METS
- [Contrôles après conversion](presse_controles_post_conversion.md)
