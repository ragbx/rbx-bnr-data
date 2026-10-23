# Scripts : chaîne délibérations (JPEG de diffusion et METS)

**Emplacement :** `scripts/img/img3/del_mets.py`

Chaîne de production pour les **registres de délibérations du conseil municipal**
(corpus `AMR_DEL`) : chaque TIFF de conservation donne un **JPEG de diffusion**, et
chaque registre reçoit un **METS** qui décrit ses fichiers. JPEG et METS sont ensuite
déposés sur S3, à côté des fichiers existants.

C'est la [chaîne presse](presse_mets.md) appliquée aux registres : `del_mets.py`
reprend tout le traitement de `presse_mets.py` et ne remplace que ce qui est propre
aux délibérations. Cette page décrit ces différences ; pour le reste (encodage et
contrôle du JPEG, envoi sur S3, reprise, mesure des durées), la page de la chaîne
presse fait référence.

**État au 12 octobre 2026** : le script n'a tourné que sur 7 images de synthèse.
Aucun TIFF réel des délibérations n'a été converti, aucun fichier n'a été envoyé
sur S3.

---

## Périmètre

Le corpus `AMR_DEL` compte deux séries pourvues de TIFF, reconnues à leur `s3_key` :

| Série | Clés S3 | Registres | TIFF | ALTO | Volume des TIFF |
|---|---|---|---|---|---|
| Manuscrits (1790-1881) | `AMR/AMR_DEL/DEL_MS/` | 33 | 12 631 | aucun | 653 Go |
| Tapuscrits (1881-1982) | `AMR/AMR_DEL/DEL_TAP/` | 100 | 54 735 | 54 735 | 1 649 Go |
| **Total** | | **133** | **67 366** | | **2 302 Go** |

Les TIFF sont en couleur, non compressés, d'environ 34 Mo chacun.

Les séances de 1983 à 2002 (cotes 499W) sont hors périmètre : elles n'ont pas de
TIFF, seulement des JPEG, des ALTO, des PDF et des textes déjà sur S3.

---

## Ce qui change par rapport à la presse

| Point | Presse (`presse_mets.py`) | Délibérations (`del_mets.py`) |
|---|---|---|
| Qualité JPEG | 80 | **75** |
| Unité décrite par un METS | le fascicule (5 pages en moyenne) | le registre (507 vues en moyenne, de 96 à 917) |
| Fichiers décrits par page | TIFF, JPEG, ALTO, texte, PDF | TIFF, JPEG et, pour les tapuscrits, ALTO |
| Titre | intitulé du journal (set OAI) | intitulé fixe de l'ensemble des registres |
| Dates | date du fascicule, lue dans son nom | dates extrêmes du registre, lues dans l'EAD |
| Genre | terme RAMEAU « Périodique » | aucun |
| Récapitulatifs | `presse_mets_*.csv`, `presse_jpeg_*.csv` | `del_mets_*.csv`, `del_jpeg_*.csv` |

Ce qui ne change pas : la clé du JPEG (celle du TIFF, extension `.jpg`), la trace de
conversion et l'uuid inscrits dans le JPEG, les contrôles, les options de la ligne de
commande, le bucket `mediatheque-patarch-communicable`.

---

## Registre et vue

**Le registre** est le répertoire de la `s3_key`, par exemple `1D144_1889`.

**La vue** est ce qui suit, dans le nom du fichier, le préfixe commun du registre.
Trois formes existent :

| Nom du fichier | Préfixe commun | Vue | Cas |
|---|---|---|---|
| `RBX_AMR_DEL_1D144_1889_0152.tif` | `RBX_AMR_DEL_1D144_1889` | `0152` | tapuscrits : l'année est dans le nom |
| `RBX_AMR_DEL_1D001_0096.tif` | `RBX_AMR_DEL_1D001` | `0096` | manuscrits : pas d'année dans le nom |
| `RBX_AMR_DEL_1D008_0001_0694.tif` | `RBX_AMR_DEL_1D008` | `0001_0694` | registre 1 D 8, numérotation en deux parties (694 vues) |
| `RBX_AMR_DEL_1D225_1972_0022_1.tif` | `RBX_AMR_DEL_1D225_1972` | `0022_1` | une sous-vue, seul cas du corpus |

Une ligne dont le nom ne suit pas ce schéma est écartée et signalée
(« ANOMALIE ref »). Le fichier de référence du 30 juin 2026 n'en contient aucune.

---

## Entrées

**Le manifeste** : un extrait du fichier de référence (`.csv` ou `.csv.gz`), avec ses
colonnes d'origine et toutes les lignes des registres à traiter, TIFF et ALTO.
Seules les lignes `AMR_DEL` des deux séries, hors « À SUPPRIMER », sont retenues.
Les manifestes sont rangés dans `results/amr_del/` :

| Manifeste | Contenu |
|---|---|
| `manifeste_test_2_registres.csv` | Lot d'essai : le plus petit registre de chaque série, 1 D 1 (96 vues) et 1 D 169 (307 vues), soit 403 TIFF et 12,6 Go. |
| `manifeste_DEL_MS.csv.gz` | Les 33 registres manuscrits. |
| `manifeste_DEL_TAP.csv.gz` | Les 100 registres tapuscrits. |

Les fichiers `*_tiff.txt` voisins donnent la liste des TIFF (`path/name`), pour les
copier sous `<source>` ; ce ne sont pas des entrées de la chaîne.

**Les TIFF** : lus sous `<source>/<path>/<name>`, soit
`<source>/BNR_VERIF/AMR/AMR_DEL/DEL_MS/1D001(1790-1795)/…` et
`<source>/BNR_VERIF/AMR/AMR_DEL/DEL_TAP/1D144(1889)/…`. Les noms de dossiers
portent des parenthèses.

**Données du dépôt** :

| Fichier | Usage |
|---|---|
| `results/ead/corpus_ocr/RBX_AMR_DEL.xml` | Cote, dates extrêmes, ARK actuel et ancien de chaque registre. Les 133 registres y figurent. |
| `data/xsd/` | Copies locales des schémas METS, MODS et MIX, pour `--validate`. |

---

## Le METS

Un METS par registre, déposé dans le répertoire du registre et nommé d'après le
préfixe commun de ses fichiers :

- `AMR/AMR_DEL/DEL_TAP/1D144_1889/RBX_AMR_DEL_1D144_1889_mets.xml`
- `AMR/AMR_DEL/DEL_MS/1D001_1790-1795/RBX_AMR_DEL_1D001_mets.xml`

| Section | Contenu |
|---|---|
| racine | `OBJID` : la clé S3 du METS sans l'extension. `LABEL` : intitulé, cote et dates, par exemple « …, 1 D 225 (1972) ». |
| `metsHdr` | Date de création, Médiathèque et Archives de Roubaix, `del_mets.py` et sa version, uuid du METS (`altRecordID`). |
| `dmdSec DMD_SERIE` | MODS de l'ensemble des registres : intitulé, code `AMR_DEL`. |
| `dmdSec DMD_REGISTRE` | MODS du registre : intitulé et cote (`partNumber`), langue, dates extrêmes (`dateCreated`), nombre de vues, cote et préfixe commun en identifiants, ARK actuel et ancien, début de clé S3. |
| `amdSec` | Un bloc MIX par TIFF et par JPEG. |
| `fileSec` | Groupes `master` (TIFF), `access` (JPEG) et, pour les tapuscrits, `alto` ; taille, MD5, clé S3 et uuid (`OWNERID`) de chaque fichier. |
| `structMap` | Physique : volume > page. `ORDERLABEL` donne le numéro de vue sans zéros (`152`) ; une vue composée est reprise telle quelle (`0001_0694`, `0022_1`). |

Précisions :

- **Intitulé** : « Conseil municipal. Registres des procès-verbaux des
  délibérations », repris de l'instrument de recherche `FRAC512_D_01`. C'est une
  constante du script (`TITRE`).
- **Genre** : aucun terme de genre dans le MODS.
- **Cote** : écrite comme dans l'EAD, « 1 D 144 », « 1 D 1 ».
- **Manuscrits** : le type de ressource porte `manuscript="yes"`.
- **Logiciel cité dans le MIX des JPEG** : `presse_mets.py`, qui porte le code de
  conversion commun aux deux chaînes.

Un registre complet donne un METS volumineux : environ 150 lignes par vue pour un
tapuscrit, soit de l'ordre de 70 000 lignes pour 450 vues.

---

## Utilisation

Les options sont celles de `presse_mets.py`.

Essai sur deux registres, sans envoi, avec estimation de la durée pour l'ensemble :

```bash
python scripts/img/img3/del_mets.py results/amr_del/manifeste_test_2_registres.csv --source /chemin/racine --out-dir /chemin/sortie --validate --estimer-pages 67366
```

Lot complet d'une série, avec envoi réel sur S3 :

```bash
python scripts/img/img3/del_mets.py results/amr_del/manifeste_DEL_TAP.csv.gz --source /chemin/racine --out-dir /chemin/sortie --validate --upload
```

| Option | Effet |
|---|---|
| `--source` | Dossier qui contient `BNR_VERIF/AMR/AMR_DEL/…`. |
| `--out-dir` | Dossier de travail et des récapitulatifs. |
| `--validate` | Valide chaque METS contre les schémas. |
| `--upload` | Envoi réel : chaque registre est déposé, contrôlé sur S3, puis supprimé du disque local. |
| `--quality` | Qualité JPEG, 75 par défaut. |
| `--workers` | Nombre de registres traités en parallèle. |
| `--estimer-pages` | Nombre total de vues pour lequel la durée est estimée en fin de lot. |

---

## Sorties

- **JPEG et METS** sous `<out-dir>`, dans l'arborescence des clés S3 ; avec
  `--upload`, ils sont supprimés une fois déposés et contrôlés.
- **`del_mets_<date>.csv`** : une ligne par registre (statut, vues, JPEG créés,
  pages incomplètes, durées).
- **`del_jpeg_<date>.csv`** : une ligne par JPEG et par METS à déposer ou déposés,
  avec uuid et MD5. C'est la seule trace locale de ces valeurs après un envoi : à
  conserver jusqu'à l'inscription au fichier de référence.

Dans ces récapitulatifs et dans les messages du lot, le mot « fascicule » désigne
un registre.

Une vue est signalée incomplète s'il lui manque le TIFF ou, dans un registre qui a
des ALTO, l'ALTO. Le fichier de référence du 30 juin 2026 n'en contient aucune.

---

## Limites

- **Jamais lancé sur de vrais TIFF** : la cadence, le poids des JPEG et le
  comportement sur les TIFF en couleur (avec ou sans profil ICC) restent à mesurer
  sur le lot d'essai.
- **Un registre est traité d'un bloc** : un seul TIFF absent, différent du fichier
  de référence ou illisible met tout le registre en erreur, et rien n'en est
  déposé. Le contrôle MD5 relit tous les TIFF du registre avant la conversion.
- **Avancement** : il n'évolue qu'à la fin de chaque registre.
- **Place disque avec `--upload`** : le disque porte les JPEG des registres en
  cours, soit plusieurs centaines de JPEG par processus.
- **Bilan** : `presse_bilan.py` ne connaît pas les registres. `presse_download.py`,
  lui, sait télécharger leurs JPEG et leurs ALTO (depuis le 15 octobre 2026).

---

## Voir aussi

- [Chaîne presse](presse_mets.md) — le traitement commun, en détail
- [Contrôles après conversion](presse_controles_post_conversion.md) — méthode applicable aux registres
- [Génération des EAD de corpus océrisés](corpusocr2ead.md) — origine de `RBX_AMR_DEL.xml`
