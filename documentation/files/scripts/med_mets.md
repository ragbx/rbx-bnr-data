# Scripts : chaîne fonds de la médiathèque (JPEG de diffusion et METS)

**Emplacement :** `scripts/img/img3/med_mets.py`

Chaîne de production pour trois fonds de la médiathèque : les **manuscrits**
(`MED_MS`), le **fonds Maxence Van der Meersch** (`MED_VDM`) et les **collections de
particuliers** (`MED_PAR`). Chaque TIFF de conservation donne un **JPEG de
diffusion**, et chaque notice de l'instrument de recherche reçoit un **METS** qui
décrit ses fichiers. JPEG et METS sont ensuite déposés sur S3, à côté des TIFF.

C'est la [chaîne presse](presse_mets.md) appliquée à ces fonds, avec le METS de la
[chaîne délibérations](del_mets.md) : `med_mets.py` reprend le traitement de
`presse_mets.py` et la construction du METS de `del_mets.py`, et ne définit que ce
qui est propre à ces fonds. Cette page décrit ces différences.

**État au 25 octobre 2026** : le lot complet (`med_lot.sh`, envoi réel sur S3) a tourné
du 24 octobre 15 h 45 au 25 octobre 3 h 03. 77 553 vues sur 79 981 sont converties
(`MED_MS` 32 339 sur 32 410, `MED_VDM` 28 923 sur 30 952, `MED_PAR` 16 291 sur 16 299),
soit 117,5 Go de JPEG pour 2 211 Go de TIFF. 30 notices n'ont pas été traitées
(voir [Relance](#relance)). Le fichier de référence `_ref_files_20261023` précède ce lot
et ne le reflète pas.

---

## Périmètre

Les TIFF versés sur S3, au statut `TRANSFERT_S3_OK`, à publier (`publication_statut`
= `oui`), du fichier de référence `_ref_files_20261023` :

| Fonds | Clés S3 | Notices | TIFF | Volume |
|---|---|---|---|---|
| `MED_MS` | `MED/MED_MS/` | 360 | 32 410 | 1 013 Go |
| `MED_VDM` | `MED/MED_VDM/` (et `VDM_a_traiter/…`) | 548, dont 9 refusées | 31 272 | 802 Go |
| `MED_PAR` | `PAR/PAR_*/`, `AMR/PAR_LAB/` | 3 558 | 16 299 | 480 Go |

Ces fonds n'ont que des TIFF : ni ALTO, ni texte, ni PDF par vue.

---

## Notice et vue

L'unité décrite par un METS est **la notice** de l'instrument de recherche : les TIFF
de même `unitid` au fichier de référence (colonne remplie par le maillon DAO). Une
notice compte de 1 à 934 vues : 42 en médiane pour `MED_MS`, 18 pour `MED_VDM` ;
`MED_PAR` a surtout des notices d'une seule image (2 758 sur 3 558).

Les noms de fichiers sont irréguliers (`_160a`, `_109d`, `_0105_A`, « `_001 .tif` »,
« `_320..tif` ») : les vues sont donc numérotées `0001`, `0002`… dans l'ordre
naturel des clés S3, et l'étiquette de chaque vue (`ORDERLABEL`) reprend la fin de
son nom :

| Fichiers de la notice | Étiquettes |
|---|---|
| `RBX_MED_MS_009_001` … `_070` | `001` … `070` |
| `RBX_MED_PAR_BOH_SPO_109`, `_109a` … `_109d` | `109`, `a` … `d` |
| `RBX_MED_PAR_BOH_SPO_001` (une image) | `001` |

---

## Ce qui change par rapport aux autres chaînes

| Point | Délibérations, rapports du maire | Fonds de la médiathèque (`med_mets.py`) |
|---|---|---|
| Qualité JPEG | 75 / 80 | **80** |
| Unité décrite par un METS | le registre, le rapport | **la notice** (`unitid`) |
| Fichiers décrits par vue | TIFF, JPEG, ALTO | TIFF, JPEG |
| Source des notices | EAD `results/ead/corpus_ocr/` | IR `results/ead/ead_cor/bnr2mnesys/FR595126101_<fonds>.xml` |
| Intitulé du fonds | constante du script | set OAI `RBX_<fonds>` |
| Intitulé de l'unité | — | `unittitle` de la notice |
| Sections descriptives | `DMD_SERIE`, `DMD_REGISTRE` / `DMD_RAPPORT` | `DMD_SERIE`, `DMD_NOTICE` |
| Division de la structure | `volume` | `document` |
| Récapitulatifs | `del_*.csv`, `ram_*.csv` | `med_mets_*.csv`, `med_jpeg_*.csv` |

Un extrait peut mélanger les trois fonds.

---

## Le METS

Un METS par notice, déposé dans le répertoire de ses TIFF, sous le nom
`RBX_<fonds>_<unitid>_mets.xml`. Le premier élément de l'`unitid` est omis quand il
répète le fonds :

| Notice | METS |
|---|---|
| `Ms_009` | `MED/MED_MS/RBX_MED_MS_009_mets.xml` |
| `VDM_ADP1_D01` | `MED/MED_VDM/RBX_MED_VDM_ADP1_D01_mets.xml` |
| `BOH_SPO_001` | `PAR/PAR_BOH/RBX_MED_PAR_BOH_SPO_001_mets.xml` |
| `LAB_PH32_030` | `AMR/PAR_LAB/RBX_MED_PAR_LAB_PH32_030_mets.xml` |

Sa forme est celle du [METS des délibérations](del_mets.md#le-mets) : `OBJID` égal à
la clé S3 du METS sans l'extension, uuid en `altRecordID`, un bloc MIX par TIFF et
par JPEG, groupes `master` et `access`, uuid de chaque fichier en `OWNERID`.

Le MODS de la notice (`DMD_NOTICE`) porte :

- l'intitulé (`unittitle`) et, en `relatedItem type="host"`, l'intitulé du fonds ;
- la date telle qu'écrite dans l'IR, et sa forme normalisée (`unitdate/@normal`) ;
  une notice « sans date » n'a pas de date, même si l'IR lui donne une forme
  normalisée ;
- le nombre de vues, la cote (`unitid`), l'identifiant `RBX_<fonds>_<unitid>` ;
- l'ARK actuel et l'ancien (rôles `publication:current` et `publication:previous`
  de l'IR) ;
- `typeOfResource` `text` `manuscript="yes"` pour `MED_MS` seulement ; pas de langue
  (les manuscrits sont en latin comme en français).

---

## Le JPEG

Le JPEG est déposé à la clé S3 du TIFF, avec l'extension `.jpg`, comme l'attendent
les liens `access:image*` de l'IR. Les espaces et points parasites avant l'extension
sont retirés : `RBX_MED_VDM_PDH1_D01_001 .tif` donne `RBX_MED_VDM_PDH1_D01_001.jpg`,
et `RBX_MED_VDM_LFP4_D07_320..tif` donne `RBX_MED_VDM_LFP4_D07_320.jpg` (76 clés
`MED_VDM`, sans collision).

Ce nettoyage passe par la fonction `jpeg_key` de `presse_mets.py`, que `med_mets.py`
remplace ; pour la presse, les délibérations et les rapports du maire, la clé reste
celle du TIFF, extension `.jpg`.

---

## Entrées

Les manifestes sont des extraits du fichier de référence `_ref_files_20261023`
(toutes colonnes), un par fonds :

| Manifeste | Contenu |
|---|---|
| `results/med_ms/manifeste_MED_MS.csv.gz` | 32 410 TIFF, 360 notices |
| `results/med_vdm/manifeste_MED_VDM.csv.gz` | 31 272 TIFF, 548 notices |
| `results/med_par/manifeste_MED_PAR.csv.gz` | 16 299 TIFF, 3 558 notices |

Les fichiers `*_tiff.txt` voisins donnent la liste des TIFF (`path/name`), pour les
copier sous `<source>`. Les TIFF sont lus sous `<source>/<path>/<name>`, c'est-à-dire
sous `BNR_SAUV/…` ou `BNR_VERIF/…` selon les fichiers.

---

## Utilisation

Les options sont celles de `presse_mets.py`.

Essai, sans envoi, avec estimation de la durée pour le fonds :

```bash
python scripts/img/img3/med_mets.py results/med_ms/manifeste_MED_MS.csv.gz --source /chemin/racine --out-dir /chemin/sortie --validate --estimer-pages 32410
```

Fonds complet, avec envoi réel sur S3 :

```bash
python scripts/img/img3/med_mets.py results/med_ms/manifeste_MED_MS.csv.gz --source /chemin/racine --out-dir /chemin/sortie --validate --upload
```

Les trois fonds à la suite, avec envoi réel (`MED_MS`, puis `MED_VDM`, puis
`MED_PAR` ; environ 12 h avec 4 processus, à lancer dans `tmux` ou `screen`) :

```bash
bash scripts/img/img3/med_lot.sh
```

`scripts/img/img3/med_lot.sh` écrit chaque fonds sous `~/med_ms`, `~/med_vdm`,
`~/med_par`, avec un journal `med_lot_<date>.log`. Un fonds en échec n'arrête pas
les suivants ; une relance reprend là où le lot s'était arrêté. Réglages par
variables d'environnement : `SOURCE`, `OUT`, `WORKERS`, `UPLOAD=0` (sans envoi),
`FONDS` (par exemple `FONDS=MED_PAR`).

Dans les récapitulatifs et les messages du lot, le mot « fascicule » désigne une
notice.

---

## Relance

Le lot du 24-25 octobre 2026 a laissé 30 notices (2 428 vues, sans compter les 320 vues
des notices refusées). `scripts/img/img3/med_relance.py` les relève dans le journal du
lot (`nohup.out`) et les classe par cause :

```bash
python scripts/img/img3/med_relance.py ~/Téléchargements/nohup.out
```

Il écrit sous `results/med_relance/` la liste `notices_a_relancer.csv` (fonds, notice,
vues, volume, cause, correctif, action) et, pour les notices relançables, un extrait
du manifeste à passer à `med_mets.py` (`manifeste_<FONDS>_<action>.csv.gz`).

| Cause | Notices | Vues | Correctif | Action |
|---|---|---|---|---|
| Balises du TIFF > 50 Mo cumulés (limite libtiff) | 7 `MED_VDM` | 1 506 | `load_master` : `unlimited=True` | relancer |
| Image > 179 Mpx (garde anti-bombe de Pillow dans `tiff_mix`) | 5 `MED_PAR` (`NAJ_VRO_001` à `005`) | 5 | `tiff_mix` : `MAX_IMAGE_PIXELS = None` | relancer |
| Un JPEG différent est déjà sur S3 | 3 `MED_PAR` (`LAB_PH02_036`, `PH05_048`, `PH12_002`) | 3 | aucun | décider, puis `--overwrite` |
| Un master est un JPEG nommé `.tif` | 3 `MED_VDM` (`CAR1_D01`, `CAT4_D02`, `COR1_D01`) | 79 | aucun | bloqué |
| TIFF à JPEG old-style (« pseudo-tag 65538 ») | 1 `MED_VDM` (`ALS1_D01`) | 175 | aucun | bloqué |
| Master illisible | `MED_MS` `306`, `MED_VDM` `MDC3_D02` | 340 | aucun | bloqué |
| Refusées d'emblée (original et photocopie) | 9 `MED_VDM` | 320 | aucun | bloqué |

Les deux correctifs ont été vérifiés sur des TIFF de synthèse reproduisant les deux
limites (balise de 60 Mo, image de 266 Mpx, 840 Mo de pic de mémoire) : ils n'ont pas
encore été essayés sur les vrais masters. Commencer par les 5 notices `MED_PAR` puis
une notice `MED_VDM` :

```bash
python scripts/img/img3/med_mets.py results/med_relance/manifeste_MED_PAR_relancer.csv.gz --source /chemin/racine --out-dir /chemin/sortie --validate --upload
```

Le JPEG déjà sur S3 des trois notices `LAB` pèse 1,7 à 2,5 Mo, le nouveau 0,2 à 0,5 Mo :
comparer leur résolution avant d'écraser (`manifeste_MED_PAR_relancer_overwrite.csv.gz`,
avec `--overwrite`).

---

## Limites

- **Cadence mesurée au lot du 24 octobre 2026** (4 processus) : 0,5 à 0,6 s par vue,
  soit 6 000 à 7 000 vues et 180 Go de TIFF à l'heure ; 12 h 16 pour les trois fonds.
  Les JPEG pèsent environ 5 % des TIFF. Pic de mémoire : 926 Mo par processus.
- **9 notices `MED_VDM` refusées** : `VDM_MAI2_D01` à `_D03`, `VDM_MAI5_D01`,
  `_D02`, `_D04`, `VDM_MAI8_D01` à `_D03`, sous `VDM_a_traiter/`. Leurs TIFF sont
  répartis entre un répertoire d'original (`VDM_MAI2`) et un répertoire de
  photocopie (`VDM_MAI2a-photocopie`), avec les mêmes noms de fichiers et des
  contenus différents : un METS par notice y mêlerait deux fois les mêmes vues.
- **2 notices `MED_PAR` sans ARK** : `LAB_PH21_019` et `LAB_PH40_117` figurent deux
  fois dans l'IR ; leur METS n'a ni intitulé propre, ni date, ni ARK.
- **Une notice est traitée d'un bloc** : un seul TIFF absent, différent du fichier
  de référence ou illisible met toute la notice en erreur.
- **Bilan** : `presse_bilan.py` et `presse_download.py` ne connaissent pas ces fonds.

---

## Voir aussi

- [Chaîne presse](presse_mets.md) — le traitement commun, en détail
- [Chaîne délibérations](del_mets.md) — la forme du METS
- [Chaîne rapports du maire](ram_mets.md)
