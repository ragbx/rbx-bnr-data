# Scripts : chaîne presse (JPEG de diffusion et METS)

**Emplacement :** `scripts/img/img3/`

Chaîne de **production** pour la presse ancienne (`PRA_*`) : chaque TIFF de
conservation donne un **JPEG de diffusion**, et chaque fascicule reçoit un **METS**
qui décrit ses cinq types de fichiers (TIFF, JPEG, ALTO, texte, PDF). JPEG et METS
sont ensuite déposés sur S3, à côté des fichiers existants.

Elle succède aux essais de mise au point des dossiers `img1` (voir
[Chaîne images de diffusion](images_diffusion.md)) et `img2`, dont elle fige les
réglages : **JPEG, qualité 80, résolution native**.

---

## Flux

```
manifeste (extrait du fichier de référence)     TIFF sous <source>
        │                                             │
        └──────────────► presse_mets.py ◄─────────────┘
                              │   par fascicule : contrôle MD5 → JPEG → METS → validation
                              │
              ┌───────────────┴────────────────┐
        avec --upload                     sans --upload
   envoi S3 + contrôle + suppression      JPEG et METS restent sous <out-dir>
        des fichiers locaux                     │
              │                           presse_upload.py   → envoi S3 + contrôle
              └───────────────┬────────────────┘
                              │
                       presse_bilan.py   → état de chaque fascicule
```

| Script | Rôle |
|---|---|
| `presse_mets.py` | Script principal : contrôle, conversion, METS, et envoi avec `--upload`. |
| `jpeg_encode.py` | Module d'encodage JPEG (lecture stricte du TIFF, préparation, écriture contrôlée, trace XMP). |
| `jpeg_to_mix.py` | Relit la trace d'un JPEG et la restitue en MIX 2.0 ; utilisable seul. |
| `presse_upload.py` | Envoi sur S3 des JPEG et METS déjà produits, quand `--upload` n'a pas été utilisé. |
| [presse_bilan.py](presse_bilan.md) | Bilan a posteriori : envoyé, en erreur, jamais traité. |
| `presse_download.py` | Récupère depuis S3 les JPEG et les ALTO des fascicules d'un manifeste. |

---

## Entrées

**Le manifeste** : un extrait du fichier de référence (`.csv` ou `.csv.gz`), avec ses
colonnes d'origine et **toutes les lignes** des fascicules à traiter — le TIFF, mais
aussi l'ALTO (`.xml`), le texte (`.txt`) et le PDF de chaque page, que le METS décrit.
Les manifestes sont rangés dans `results/presse/` :

| Manifeste | Contenu |
|---|---|
| `manifeste_10_fascicules.csv`, `manifeste_50_fascicules.csv`, `manifeste_1000_fascicules.csv` | Lots d'essai, tous titres mêlés. |
| `manifeste_PRA_RTG.csv` | Un titre entier, le plus petit : *Roubaix-Tourcoing* (212 fascicules, 848 pages, 17,8 Go de TIFF). |

Les fichiers `*_tiff.txt` voisins donnent la seule liste des TIFF (`path/name`), pour
les copier sous `<source>` ; ce ne sont pas des entrées de la chaîne.

Colonnes lues : `name`, `path`, `s3_key`, `size`, `checksum_md5`, `uuid`,
`corpus_code`, `conservation_statut`, `mix_dateTimeCreated`. Seules les lignes
`PRA_*` pourvues d'une `s3_key` et hors « À SUPPRIMER » sont retenues. Fascicule et
page sont lus dans la `s3_key`, qui fait foi quand elle diffère du nom.

**Les TIFF** : lus sous `<source>/<path>/<name>`, `path` et `name` étant ceux du
fichier de référence.

**Données du dépôt** :

| Fichier | Usage |
|---|---|
| `results/oai/bnr_sets_<date>.csv` | Titre du journal, d'après le set OAI `RBX_<corpus>`. |
| `results/ead/corpus_ocr/RBX_<corpus>.xml` | ARK actuel et ancien du fascicule. |
| `data/xsd/` | Copies locales des schémas METS, MODS et MIX, pour `--validate`. |

---

## Traitement d'un fascicule

1. **Contrôle** : tous ses TIFF sont présents sous `<source>` et leur MD5 est celui du
   fichier de référence. Sinon le fascicule est en erreur, rien n'est écrit.
2. **JPEG** : un par TIFF, déposé sous `<out-dir>` à la même clé que le TIFF, extension
   `.jpg`.
3. **METS** : `<fascicule>_mets.xml`, dans le même répertoire.
4. **Validation** (`--validate`) : le METS est validé contre les XSD METS, MODS et MIX.
5. **Envoi** (`--upload`) : voir [Envoi sur S3](#envoi-sur-s3).

Une erreur est **isolée par fascicule** : le lot continue. Un fascicule qui échoue en
cours de conversion ne laisse rien de la tentative (ses JPEG sont supprimés).

Les fascicules sont traités en parallèle (`--workers`, un processus par fascicule).

---

## Le JPEG de diffusion

| Réglage | Valeur |
|---|---|
| Qualité | Q = 80 (`--quality`) |
| Résolution | native, sans réduction |
| Couleur | conversion vers sRGB si le TIFF couleur porte un profil ICC ; pas de conversion en niveaux de gris |
| Profondeur | 16 bits ramenés à 8 bits |
| Encodage | progressif, tables de Huffman optimisées ; sous-échantillonnage 4:2:0 en couleur |
| Outils | libvips (pyvips) et libjpeg-turbo |

**Lecture stricte.** Par défaut libvips tolère les erreurs de décodage : un TIFF
abîmé donnerait une image fausse, sans erreur. Ici le TIFF est lu en mode strict : au
moindre défaut de décodage, le fascicule est en erreur.

**Écriture contrôlée.** Le JPEG est écrit sous `<nom>.jpg.part`, relu et redécodé en
entier, ses dimensions comparées à celles du TIFF, puis renommé. Un traitement
interrompu ne laisse donc jamais de JPEG partiel sous son nom final.

**Trace de génération.** Chaque JPEG porte, dans son XMP :

- un **uuid** (32 caractères hexadécimaux, comme au fichier de référence), attribué à
  sa création ;
- l'outil et ses versions, la date, et la chaîne complète des opérations, par exemple :
  `libvips 8.18.0 / libjpeg-turbo 3.1.4.1 ; icc_transform sRGB ; jpegsave Q=80,
  subsampling 4:2:0 (auto), optimize_coding, progressive`.

Le XMP et l'IPTC hérités du TIFF sont retirés : ils décrivent le master. C'est cette
trace que `jpeg_to_mix.py` relit pour écrire le MIX du JPEG.

**TIFF couleur sans profil ICC** : ses pixels passent tels quels et le MIX du JPEG
déclare `RGB` et non `sRGB`. Ils sont comptés dans la colonne `couleur_sans_profil`.

---

## Le METS

Un fichier par fascicule, `<répertoire de la s3_key>/<fascicule>_mets.xml`.

| Section | Contenu |
|---|---|
| `metsHdr` | Date de création, établissement, script et sa version git, **uuid du METS**. |
| `dmdSec` | Deux MODS : le **titre de presse** (titre, code du corpus) et le **fascicule** (langue, date d'émission tirée de l'identifiant, nombre de pages, identifiant, ARK actuel et ancien). |
| `amdSec` | Un MIX 2.0 par TIFF (d'après ses tags) et par JPEG (d'après sa trace XMP, avec le TIFF source). Pas de métadonnées techniques pour ALTO, texte et PDF. |
| `fileSec` | Cinq groupes : `master` (TIFF), `access` (JPEG), `alto`, `text`, `pdf`. Pour chaque fichier : `s3_key`, taille, MD5, uuid. |
| `structMap` | Physique : fascicule > pages dans l'ordre, chaque page pointant ses fichiers. |

Un METS régénéré garde l'uuid de sa première création.

**Pages incomplètes.** S'il manque au fichier de référence un ALTO, un texte ou un PDF
pour une page, le METS est produit avec ce qui existe et le statut reste `ok`. Ces
pages sont signalées dans les colonnes `pages_incompletes` et `manques`.

---

## Utilisation

    # conversion et METS, sans envoi
    python scripts/img/img3/presse_mets.py results/presse/manifeste_1000_fascicules.csv \
        --source /chemin/des/tiff --out-dir /chemin/sortie --validate

    # conversion, METS et envoi sur S3 au fil de l'eau (ENVOI RÉEL)
    python scripts/img/img3/presse_mets.py results/presse/manifeste_1000_fascicules.csv \
        --source /chemin/des/tiff --out-dir /chemin/sortie --validate --upload

| Option | Rôle |
|---|---|
| `--source` | Préfixe sous lequel se résolvent les chemins du fichier de référence. |
| `--out-dir` | Racine des JPEG et METS, selon l'arborescence des `s3_key`. |
| `--workers` | Nombre de processus (défaut : nombre de cœurs). |
| `--quality` | Qualité JPEG (défaut : 80). |
| `--overwrite` | Régénère les JPEG déjà présents. |
| `--validate` | Valide chaque METS contre les XSD de `data/xsd`. |
| `--upload` | Envoie chaque fascicule sur S3 dès qu'il est prêt, puis supprime les fichiers locaux. |
| `--keep-mets` | Avec `--upload` : garde les METS sous `--out-dir`. |
| `--max-echecs-envoi` | Avec `--upload` : arrête le lot après ce nombre de fascicules en échec d'envoi (défaut : 5). |
| `--bucket` | Bucket de destination (défaut : `mediatheque-patarch-communicable`). |
| `--estimer-pages` | Nombre total de pages à convertir : la durée en est estimée en fin de lot. |
| `--avancement` | Secondes entre deux lignes d'avancement (défaut : 60). |

---

## Envoi sur S3

Deux façons, au choix.

**Au fil de l'eau : `presse_mets.py --upload`.** Prévu pour une machine à peu d'espace
disque : chaque fascicule va au bout avant le suivant. Ses JPEG partent, puis son
METS ; chaque objet déposé est contrôlé (taille et MD5) ; les fichiers locaux sont
alors supprimés. Le disque ne porte que les fascicules en cours.

**En deux temps : `presse_upload.py`.** Après un lancement sans `--upload`, il envoie
les fichiers décrits par le récapitulatif `presse_jpeg_*.csv` :

    python scripts/img/img3/presse_upload.py sortie/presse_jpeg_AAAAMMJJHHMMSS.csv --source sortie            # simulation
    python scripts/img/img3/presse_upload.py sortie/presse_jpeg_AAAAMMJJHHMMSS.csv --source sortie --execute  # envoi réel

Sans `--execute`, c'est une **simulation** : les fichiers locaux sont contrôlés, aucun
appel n'est fait à S3.

Règles communes aux deux modes :

- **Le METS part en dernier**, et seulement si tous les JPEG du fascicule sont sur S3.
  Sa présence sur S3 vaut donc **fascicule complet**.
- Un objet déjà présent n'est pas écrasé (sauf `--overwrite`) : il doit avoir la
  taille et le MD5 du fichier local, sinon c'est une erreur.
- Chaque objet reçoit les étiquettes `uuid` et `checksum_md5`.
- En cas d'échec d'envoi, rien n'est supprimé : les fichiers restent sous `--out-dir`.

L'accès S3 est celui de `scripts/s3/rbx_s3.py` (`conf.yml`, utilisateur `user_rw`) ;
voir [Scripts S3](s3.md).

---

## Sorties

Sous `<out-dir>`, à chaque lancement :

| Fichier | Contenu |
|---|---|
| `presse_mets_AAAAMMJJHHMMSS.csv` | Une ligne **par fascicule** : statut, pages, JPEG créés, pages incomplètes, volumes, durées, message d'erreur. |
| `presse_jpeg_AAAAMMJJHHMMSS.csv` | Une ligne **par fichier à déposer** (JPEG et METS) des fascicules `ok` : nom, chemin, taille, MD5, uuid, `s3_key`. C'est l'entrée de `presse_upload.py`. |

Les deux sont écrits au fil de l'eau, puis réécrits triés en fin de lot : une
interruption en laisse l'état sur disque.

Statuts du récapitulatif par fascicule :

| Statut | Signification |
|---|---|
| `ok` | JPEG et METS produits (et envoyés, avec `--upload`). |
| `erreur` | Contrôle, conversion ou METS en échec. |
| `invalide` | METS refusé à la validation XSD. |
| `refuse` | Écarté d'emblée : aucun TIFF dans le manifeste, ou titre inconnu. |
| `erreur_envoi` | Envoi en échec ; les fichiers sont restés sous `--out-dir`. |
| `deja_envoye` | METS déjà sur S3 : fascicule sauté. |

**Avec `--upload`, conserver les récapitulatifs** : les fichiers locaux étant
supprimés, ils sont la seule trace locale des uuid et MD5 des JPEG.

En fin de lot, le script affiche la cadence (s/page, pages/h, Go de TIFF/h) et le
temps moyen par étape. Cette cadence ne vaut que pour la même machine, le même
`--workers` et les mêmes options.

---

## Reprise et erreurs

- **Relancer le même manifeste est sans risque.** Avec `--upload`, les fascicules dont
  le METS est déjà sur S3 sont sautés. Sans `--upload`, un JPEG déjà présent est repris
  s'il passe le contrôle et porte la trace à la qualité demandée ; il garde son uuid.
- **« unable to call jpegsave » n'est pas forcément un TIFF abîmé** : lire la suite du
  message dans la colonne `msg`. Jusqu'au 8 octobre 2026, `--upload` pouvait échouer
  sur « unable to open for write … No such file or directory » : un processus
  supprimait un répertoire vide qu'un autre venait de créer. Les répertoires vides ne
  sont plus supprimés qu'en fin de lot. Les fascicules touchés passent à la relance.
- **Après un arrêt brutal**, le METS fait foi : ne déposer que les fascicules au
  statut `ok`.
- Pour savoir où en est un lot après plusieurs lancements, utiliser
  [presse_bilan.py](presse_bilan.md).

---

## Récupérer les JPEG et les ALTO

`presse_download.py` télécharge depuis S3, pour les fascicules d'un ou plusieurs
manifestes, **uniquement** les JPEG de diffusion et les ALTO (ni TIFF, ni texte, ni
PDF, ni METS). Les fichiers sont écrits sous `<out-dir>/<s3_key>`.

    python scripts/img/img3/presse_download.py results/presse/manifeste_PRA_RTG.csv \
        results/presse/manifeste_PRA_CTG.csv --out-dir /chemin/sortie

| Option | Rôle |
|---|---|
| `--types` | `jpeg`, `alto` ou les deux (défaut : les deux). |
| `--simulation` | Liste ce qui serait téléchargé, sans appel à S3. |
| `--overwrite` | Retélécharge les fichiers déjà présents. |
| `--workers` | Téléchargements simultanés (défaut : 8). |

- Chaque fichier est contrôlé avant d'être rangé sous son nom : taille, et empreinte
  (MD5 du fichier de référence pour l'ALTO ; ETag de S3 pour le JPEG, recalculé sur le
  fichier quand il a été envoyé en plusieurs parties). La colonne `controle` dit lequel
  a servi.
- Un fichier déjà présent à la bonne taille est gardé : **relancer reprend** là où le
  téléchargement s'est arrêté.
- Un JPEG absent de S3 (fascicule pas encore converti) est signalé au statut `absent`.
- Le résultat, une ligne par fichier (`telecharge`, `deja_present`, `absent`,
  `erreur`), est écrit dans `<out-dir>/presse_download_AAAAMMJJHHMMSS.csv`.

L'accès S3 se fait en lecture seule (`user_ro`).

---

## Limites

- `PRA_BDR` n'est pas couvert (pas de set OAI ; ALTO, texte et PDF sans `s3_key`).
- Les suffixes d'édition du Journal de Roubaix (`_M`, `_S`, `_ES`) restent dans
  l'identifiant, sans interprétation.
- Un identifiant en plage `AAAAMMJJ-JJ` donne une date de début et une date de fin.

---

## Environnement

Environnement unifié du projet **`rbx-bnr-data`** (`environment.yml`) : `pyvips` avec
sa libvips, `pillow`, `pandas`, `lxml`, `boto3`. La version git de la chaîne est
inscrite dans chaque METS, avec le suffixe `-modifie` si un script du dossier diffère
du dernier commit : lancer la production depuis un dépôt à jour et propre.

---

## Voir aussi

- [presse_bilan.py](presse_bilan.md) — bilan des envois.
- [Chaîne images de diffusion](images_diffusion.md) — les essais de mise au point (`img1`).
- [Génération des EAD de corpus océrisés](corpusocr2ead.md) — les EAD qui attendent ces JPEG.
- [Scripts S3](s3.md) — accès au stockage.
