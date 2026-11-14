# cartes_vers_usb.py

[scripts/ead/cartes_vers_usb.py](../../../scripts/ead/cartes_vers_usb.py)

Copie les **images des cartes et plans de la BN-R** depuis Azraël (lecteur
`Z:` = `\\srvbnr.ntrbx.local\BNR`) vers une clé USB, rangées par inventaire
et par document, avec un inventaire CSV. 14/11/2026.

```bash
python scripts/ead/cartes_vers_usb.py E:/cartes_bnr --simulation     # d'abord : volume, place, manquants
python scripts/ead/cartes_vers_usb.py E:/cartes_bnr --md5            # fichiers TRANSFERT_S3_OK, jpg + tif, Azraël
python scripts/ead/cartes_vers_usb.py E:/cartes_bnr --md5 --s3       # + les 120 TIFF présents seulement sur S3
python scripts/ead/cartes_vers_usb.py E:/cartes_bnr --selection plans-de-ville
```

**Décision du 14/11/2026 : ne copier que les fichiers `conservation_statut =
TRANSFERT_S3_OK`** (fichiers de conservation), option `--statut`, par
défaut. Les JPEG de diffusion `À SUPPRIMER (DIFFUSION)` (278, dont les
« VERSION DE CONSULTATION » de la médiathèque) et les doublons sont écartés.

Options : `--s3` (télécharge depuis S3 les fichiers absents d'Azraël ;
`s3_bucket` / `s3_key` du référentiel, compte en lecture seule `user_ro` de
`scripts/s3/conf.yml` via `rbx_s3.Rbx_client`) ; `--cote COTE` (répétable,
essai sur quelques documents) ; `--statut` (défaut `TRANSFERT_S3_OK`, `tous` pour ne pas filtrer) ;
`--format jpg|tif|tous` (défaut tous) ; `--selection tout|plans-de-ville`
(colonne « plan de ville » de la liste) ; `--md5` (vérifie chaque copie) ;
`--source` (défaut `Z:/`) ; `--ref` (défaut : le référentiel le plus récent) ;
`--limite N` (essai sur N fichiers) ; `--simulation`.

## Fonctionnement

1. Documents : `results/ead/indexation/cartes_anciennes_bnr.xlsx`, onglet
   « Cartes et plans » ([cartes_anciennes.py](cartes_anciennes.md), colonne `IR`).
2. Fichiers : référentiel `results/ref/_ref_files_*.csv.gz`, apparié par
   `finding_aid` (`bnr_<IR>.xml`) + `unitid` (cote BN-R), `file_type` jpeg / tiff.
3. Chemin sur Azraël : `<source>/<path>/<name>` ; s'il n'existe pas, le
   chemin sans `_CORBEILLE/BNR_VERIF/`, puis tout fichier que le
   référentiel situe sur Azraël (`source2s3 = az`) sous le même nom, ou sous
   le même nom sans année intercalée (cadastre de 1819 :
   `RBX_AMR_PLA_1G1001_01.jpg` dans le référentiel, `RBX_AMR_PLA_1819_1G1001_01.jpg`
   sur Azraël, dossier `AMR_CAD/AMR_PLA_1819_1G1001`). Hors `_CORBEILLE` d'abord.
4. Contrôle de la place libre (arrêt si insuffisante) ; avertissement pour
   les fichiers de plus de 4 Go (clé FAT32).
5. Copie, contrôle de la taille (et du MD5 avec `--md5` : MD5 du
   référentiel pour le fichier même, sinon MD5 de la source) ; reprise : un
   fichier déjà présent avec la bonne taille n'est pas recopié.

Rangement : `<destination>/<IR court>/<année>_<cote>_<titre>/<fichier>`
(noms sans accents ni caractères interdits), et
`<destination>/inventaire_cartes.csv` (`;`, UTF-8 avec BOM, pour Excel) :
cote, titre, date, IR, fichier, taille, MD5, origine (Azraël / S3), statut,
chemin sur la clé. Un téléchargement S3 s'écrit d'abord en `.partiel`, renommé
une fois complet. Le référentiel est lu par morceaux (près de 3 millions de
lignes : la lecture d'un bloc a manqué de mémoire le 14/11/2026).

## Volumes en TRANSFERT_S3_OK (simulation du 14/11/2026, référentiel 20261026)

| Sélection | Fichiers | Copiables depuis Azraël | Introuvables (S3 seulement) |
|---|---|---|---|
| tout | 727 (93,9 Go) | 607 (52,8 Go : 309 JPEG 8,4 Go, 298 TIFF 44,4 Go) | 120 TIFF (41,1 Go) |
| plans de ville | 454 (55,8 Go) | 361 (27,3 Go) | 93 |

Les 120 introuvables sont les TIFF entrés par le disque externe
`AMR_CADN_165-0500` (cadastre de 1819, 20 fichiers ; plans 6 Fi, 100) :
`TRANSFERT_S3_OK` mais pas sur Azraël. Pour les plans 6 Fi, le JPEG de
conservation est sur Azraël et copié ; **le cadastre de 1819 n'a aucun
fichier copié** (ses JPEG d'Azraël, `AMR_CAD/AMR_PLA_1819_1G1001`, ne sont
pas appariés à la cote dans le référentiel). Tous les autres documents
numérisés ont au moins un fichier copié.

**Option `--s3` (ajoutée le 14/11/2026)** : les 120 TIFF sont dans le
bucket `mediatheque-patarch-communicable`, téléchargeables directement (pas
de restauration à demander : `head_object` sans `StorageClass` ni
`Restore`). Avec `--s3` : 727 fichiers, 93,9 Go (Azraël 607 / 52,8 Go,
S3 120 / 41,1 Go), aucun manquant. Essai : `--s3 --md5 --cote PLA_1_6FI004`
→ JPEG d'Azraël (11 Mo) et TIFF de S3 (193 Mo) copiés, MD5 bons ; relance :
« déjà présent ».

Essai du 14/11/2026 (`--limite 24 --md5`) : 4 JPEG du cadastre de 1826
copiés, contrôle MD5 bon ; 20 TIFF de 1819 introuvables.

## Volumes avant le filtre de statut (pour mémoire, même jour)

| Format | Sélection | Fichiers | Volume | Introuvables sur Azraël |
|---|---|---|---|---|
| jpg | tout | 602 | 12,2 Go | 0 |
| jpg | plans de ville | 356 | 8,9 Go | 0 |
| tif | tout | 423 | 86,2 Go (44,4 Go copiables) | 125 |
| tif | plans de ville | 177 | 48,7 Go (19,4 Go copiables) | 98 |

64 documents n'ont aucun fichier : ce sont les documents non numérisés.
Les **TIFF introuvables** (125) sont ceux entrés par des disques externes
(`AMR_CADN_165-0500`, `AMR_CADN_314-1000`) : cadastre de 1819 et plans
6 Fi en TIFF. Ils ne sont pas sur Azraël (seulement leurs JPEG) mais sur
S3 (`TRANSFERT_S3_OK`). Les TIFF du cadastre de 1826 et de l'atlas de 1884
ne sont pas appariés aux cotes dans le référentiel.

Essai du 14/11/2026 : 4 fichiers copiés (`--limite 4 --md5`) puis relance :
contrôle MD5 bon, reprise correcte (« déjà présent »).
