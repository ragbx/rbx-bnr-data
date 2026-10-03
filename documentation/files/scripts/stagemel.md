# Chaîne `stagemel_*` — recap d'audit par corpus

**Emplacement :** `scripts/corpus/stagemel_extraction_corpus.py`,
`scripts/corpus/stagemel_cas_merge.py`, `scripts/corpus/stagemel_recap.py`,
`scripts/corpus/stagemel_pipeline.sh`

> Scripte la méthodologie manuelle du stage de Mélanie (2026), menée par
> notebook dans un dossier `stage/` (non versionné, cf. `.gitignore`) sur un
> partage réseau ECRIN. Le détail original de la méthode est dans
> `stage/Méthodologie/Méthodologie.docx`.

## Objectif

Produire, pour un corpus donné, un **recap Excel de diagnostic** listant tous
ses fichiers connus (par REF ou par DAO) avec leur statut et ce qu'il reste à
vérifier — **sans trancher** les cas ambigus à la place de l'utilisateur. Le
but est d'identifier les problèmes, pas de les résoudre : contrairement à la
[campagne `s3_key_cible`](s3_key_cible.md), ce recap ne pose pas de
`s3_key_cible` ni ne décide d'une action de transfert/suppression sur les
statuts `INCONNU`.

## Méthode (par corpus)

1. **`stagemel_extraction_corpus.py [<corpus>...] [--date <date>] [--ref-path <ref>]`** — extrait, pour chaque
   corpus, les lignes REF (`corpus_code == <corpus>`) et DAO
   (`nom_fichier_base` commence par `<corpus>`, éventuellement précédé d'un
   seul segment de préfixe — `RBX_`, ou les coquilles `RBx_`, `BX_`… qu'il
   faut garder pour les faire ressortir ; un simple « contient » rattachait
   à tort les `RBX_VAH_PUB_LAI_*` au corpus LAI). Sources toujours auto-détectées :
   le dernier `results/ref/_ref_files_*.csv.gz`, et
   `results/ead/ead_cor/dao_ref_link_brut.csv` (régénéré depuis `data/ead/bnr`
   par [`dao_ref_link.py`](dao_appariement.md)) — rien à éditer après une
   nouvelle version de REF ou une nouvelle notice EAD.
   → `results/corpus/stagemel/<corpus>_files_<date>.csv.gz` /
   `<corpus>_dao_<date>.csv.gz`, et `_ref_<date>.txt` (chemin du ref utilisé,
   relu par le recap pour chercher les masters dans le même ref)

2. **`stagemel_cas_merge.py [<corpus>...] [--date <date>]`** — fusionne REF et
   DAO sur une clé (`key` = nom de fichier sans extension), et répartit en
   trois lots selon la présence croisée `uuid` (REF) / `nom_fichier_base` (DAO) :

   | Lot | Condition | Sens |
   |---|---|---|
   | `cas1` (**APPARIÉ**) | `uuid` et `nom_fichier_base` présents | Fichier dans REF et DAO |
   | `cas2` (**DAO SEUL**) | `uuid` absent | Fichier référencé par une notice EAD mais absent du REF — probablement non numérisé, ou erreur de nommage |
   | `cas3` (**REF SEUL**) | `uuid` présent, `nom_fichier_base` absent | Fichier du REF référencé par aucune notice EAD |

   **Écarts volontaires avec les notebooks du stage** (corrigés le 2026-09-24) :
   - la présence côté DAO se teste sur `nom_fichier_base`, pas sur `key` :
     `key` étant la colonne de jointure, elle n'est jamais vide après le merge
     outer — tester `key` classait tous les fichiers REF sans DAO en APPARIÉ
     (61 514 lignes au 2026-09-24) et laissait REF SEUL toujours vide ;
   - les DAO sont dédoublonnées sur `key` avant le merge : un fichier cité par
     plusieurs notices dupliquait sa ligne REF (1 838 doublons au 2026-09-24,
     dont 1 592 sur MED_FLR).

   `cas3` est toujours écrit, même vide, pour qu'un fichier d'un lancement
   antérieur à la même date ne soit pas relu par le recap.
   → `results/corpus/stagemel/cas/<corpus>_cas1/2/3_<date>.csv.gz`

3. **`stagemel_recap.py <corpus> [--date <date>]`** — construit le recap
   Excel (colonnes `CAS` / `<corpus>` / `UUID` / `STATUT` / `CHEMIN` /
   `PROBLEMES` / `À FAIRE`, + un onglet `pivot` ; guide de lecture :
   [colonnes CAS et STATUT](stagemel_recap_colonnes.md)) :

   | Colonne | APPARIÉ | DAO SEUL | REF SEUL |
   |---|---|---|---|
   | `UUID` | uuid du REF | vide, sauf candidat ERREUR DAO (uuid du cas1/cas3 rapproché) | uuid du REF |
   | `STATUT` | `conservation_statut` du REF, tel quel | `SEUL DAO`, ou `ERREUR DAO (candidat)` si la clé normalisée (casse/ponctuation ignorées) correspond à une clé cas1/cas3 | `conservation_statut` du REF, tel quel |
   | `CHEMIN` | `path` du REF | vide | `path` du REF |
   | `PROBLEMES` | « Pas de format tif » si `.jpg`/`.jpeg` sans `.tif`/`.tiff` de même clé parmi les fichiers REF du corpus (casse ignorée) | « Fichier non retrouvé dans REF » / « Clé proche d'un fichier connu » | « Absent des notices EAD » (+ « Pas de format tif », même règle) |
   | `À FAIRE` | déduit du statut (table `ACTION_SUR`, tous les statuts harmonisés sauf la famille `INCONNU*`) | « À numériser » pour SEUL DAO, vide pour un candidat ERREUR DAO | **toujours vide** : notice à créer ou fichier hors périmètre, décision de l'utilisateur |

   **Priorité anti-« À SUPPRIMER »** : la correspondance exacte cherche d'abord
   parmi les fichiers cas1/cas3 dont le statut n'est **pas** `À SUPPRIMER (*)`,
   et seulement s'il n'y a rien là-dedans, un repli sur les fichiers
   `À SUPPRIMER (*)`. Pointer un DAO SEUL vers un fichier voué à disparaître
   serait trompeur s'il existe un autre candidat plus pertinent. Un repli est
   toujours signalé explicitement : `STATUT` devient « ERREUR DAO (candidat,
   fichier À SUPPRIMER) ».

   Les candidats ERREUR DAO et tout statut `INCONNU*` restent **volontairement
   non résolus** (cellule `À FAIRE` vide) : ce sont des pistes proposées, à
   confirmer par l'utilisateur (notice EAD, contexte métier) — jamais une
   politique transfert/suppression inventée par le script.

   **Pas de rapprochement flou** sur les SEUL DAO sans correspondance exacte
   (« Fichier non retrouvé dans REF ») : une colonne `REF_PROCHE*` (clé la
   plus ressemblante par `difflib`) a existé brièvement mais a été retirée le
   2026-08-22 — sur les corpus à foliotation dense (ex. MED_MS, des milliers
   de folios à 3 chiffres par manuscrit), presque tout folio voisin ressortait
   à une similarité >0.85 sans lien réel : trop de faux positifs pour rester
   une piste utile.

   **Exclusion des `À SUPPRIMER` déjà réglés** : une ligne APPARIÉ ou REF SEUL dont le
   statut commence par `À SUPPRIMER` n'apparaît dans le recap que si son
   document n'a **pas** de master (`.tif`/`.tiff`) déjà `TRANSFERT_S3_OK` —
   recherché sur **tout le ref, tous corpus confondus** (pas seulement celui en
   cours de traitement : cf. le cas MED_MS/AMR_PR ci-dessous, où le master d'un
   fichier étiqueté MED_MS peut être classé sous un tout autre `corpus_code`).
   Rien à trancher sur une suppression déjà sécurisée par un master existant —
   ça n'encombre pas le recap. Le nombre exclu est indiqué dans les
   avertissements affichés en fin d'exécution. Le ref consulté est celui noté
   par l'extraction dans `_ref_<date>.txt` (à défaut, le plus récent).

   Un corpus sans aucun fichier REF ni DAO donne un recap vide avec un
   avertissement, plutôt qu'une erreur qui arrêterait le pipeline.
   → `results/corpus/stagemel/recap/<corpus>_recap_<date>.xlsx` (largeur des colonnes ajustée au texte, filtres actifs sur les en-têtes, ligne d'en-tête figée)

`stagemel_pipeline.sh` enchaîne `dao_ref_link.py` puis les trois scripts
ci-dessus sur l'ensemble des 38 corpus (pas de sélection possible par ce
point d'entrée) — à relancer tel quel après toute nouvelle notice EAD ou
nouveau REF. Toutes les étapes reçoivent la même date et la même liste de
corpus (`CORPUS_CODES`), passées explicitement. L'interpréteur est
`conda run -n rbx-bnr-data python` par défaut, surchargeable par la variable
`PY` si cet environnement n'existe pas sur le poste.

## Utilisation

```bash
conda run -n rbx-bnr-data python scripts/corpus/stagemel_extraction_corpus.py MUS_ARC
conda run -n rbx-bnr-data python scripts/corpus/stagemel_cas_merge.py MUS_ARC
conda run -n rbx-bnr-data python scripts/corpus/stagemel_recap.py MUS_ARC

# ou, pour tous les corpus, en une commande :
bash scripts/corpus/stagemel_pipeline.sh

# avec un autre interpréteur que l'env rbx-bnr-data :
PY=/chemin/vers/python bash scripts/corpus/stagemel_pipeline.sh
```

## Corpus couverts

38 corpus au total (liste `CORPUS_CODES` dans `stagemel_extraction_corpus.py`,
reprise de `stage/Méthodologie/extraction_corpus.py`). Seize d'entre eux sont
déjà traités par le pipeline standard ([campagne `s3_key_cible`](s3_key_cible.md)) —
le recap stagemel y est redondant, utile en double-vérification seulement :
MED_CP, MED_EPH, MED_FOO, MED_IMA, MED_MAR, MED_MON, MED_PAR, MED_PBI, MED_PHD,
MED_PHO, MED_PLA, MED_PUB, AMR_CAD, ARA_CPS, LAR_PUB, OBS_JOU.

Les 21 restants sont la cible prioritaire de ce recap : MED_AFF, MED_AVI,
MED_CHA, MED_DIL, MED_FAN, MED_FLR, MED_JOU (≠ OBS_JOU), MED_LET, MED_MS,
MED_NPT, VAH_PUB, MED_VAI, MED_VDM, MED_VID, LAI, AMR_AFF, AMR_LEB, AMR_OBJ,
CSV_PAL, MDF_MTX, MUS_ARC.

`MED_PER` figure dans `CORPUS_CODES` mais n'est classé dans aucune des deux
listes ci-dessus.

## Limites connues

- **Dossier « Zébulon »** : la méthodologie du stage y fait référence
  (fichiers hors REF et hors DAO), mais aucune source de données n'existe
  dans le dépôt pour ce partage réseau — ces cas ne peuvent pas être détectés.
- **Candidats ERREUR DAO** : le rapprochement par clé normalisée est un
  premier filtre mécanique, pas une confirmation — un vrai faux-positif/négatif
  nécessite de vérifier la notice EAD source (cf. méthodologie du stage).
- **`À FAIRE` sur `INCONNU`** : jamais rempli automatiquement, y compris quand
  le contexte (nom de dossier, extension) suggère fortement une action — la
  politique par corpus reste une décision humaine, cf.
  [`CONFIG`](s3_key_cible.md#script-générique-med_s3_key_ciblepy) de
  `med_s3_key_cible.py` pour le mécanisme équivalent côté résolution.

## Voir aussi

- [Récapitulatif d'audit : colonnes CAS et STATUT](stagemel_recap_colonnes.md) — guide
  de lecture des récapitulatifs (termes, combinaisons, action attendue)
- [Campagne `s3_key_cible`](s3_key_cible.md) — pipeline de résolution (pose une
  clé et une action) pour les corpus déjà couverts
- [Chaîne d'appariement des DAO](dao_appariement.md) — génère
  `dao_ref_link_brut.csv`, source DAO de ce recap
- [Fichier de référence](../donnees/fichier_ref.md) — statuts `conservation_statut`
  à jour
