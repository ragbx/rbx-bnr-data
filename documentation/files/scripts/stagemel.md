# Chaîne `stagemel_*` — recap d'audit par corpus

**Emplacement :** `scripts/corpus/stagemel_extraction_corpus.py`,
`scripts/corpus/stagemel_cas_merge.py`, `scripts/corpus/stagemel_recap_draft.py`,
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

1. **`stagemel_extraction_corpus.py [<corpus>...]`** — extrait, pour chaque
   corpus, les lignes REF (`corpus_code == <corpus>`) et DAO
   (`nom_fichier_base` contient `<corpus>`). Sources toujours auto-détectées :
   le dernier `results/ref/_ref_files_*.csv.gz`, et
   `results/ead/ead_cor/dao_ref_link_brut.csv` (régénéré depuis `data/ead/bnr`
   par [`dao_ref_link.py`](dao_appariement.md)) — rien à éditer après une
   nouvelle version de REF ou une nouvelle notice EAD.
   → `results/corpus/stagemel/<corpus>_files_<date>.csv.gz` /
   `<corpus>_dao_<date>.csv.gz`

2. **`stagemel_cas_merge.py [<corpus>...] [--date <date>]`** — fusionne REF et
   DAO sur une clé (`key` = nom de fichier sans extension), et répartit en
   trois lots selon la présence croisée `uuid` (REF) / `key` (DAO) :

   | Lot | Condition | Sens |
   |---|---|---|
   | `cas1` (**APPARIÉ**) | `uuid` et `key` présents | Fichier dans REF et DAO |
   | `cas2` (**DAO SEUL**) | `uuid` absent, `key` présent | Fichier référencé par une notice EAD mais absent du REF — probablement non numérisé, ou erreur de nommage |
   | `cas3` (**REF SEUL**) | `uuid` présent, `key` absent | Rare — ne peut survenir que si le calcul de `key` échoue d'un côté |

   → `results/corpus/stagemel/cas/<corpus>_cas1/2/3_<date>.csv.gz`

3. **`stagemel_recap_draft.py <corpus> [--date <date>]`** — construit le recap
   Excel (colonnes `CAS` / `<corpus>` / `UUID` / `STATUT` / `CHEMIN` /
   `PROBLEMES` / `À FAIRE`, + un onglet `pivot`) :

   | Colonne | APPARIÉ | DAO SEUL | REF SEUL |
   |---|---|---|---|
   | `UUID` | uuid du REF | vide, sauf candidat ERREUR DAO (uuid du cas1/cas3 rapproché) | uuid du REF |
   | `STATUT` | `conservation_statut` du REF, tel quel | `SEUL DAO`, ou `ERREUR DAO (candidat)` si la clé normalisée (casse/ponctuation ignorées) correspond à une clé cas1/cas3 | vide |
   | `CHEMIN` | `path` du REF | vide | `path` du REF |
   | `PROBLEMES` | « Pas de format tif » si `.jpg` sans `.tif` de même clé dans le corpus | « Fichier non retrouvé dans REF » / « Clé proche d'un fichier connu » | vide |
   | `À FAIRE` | déduit du statut (table `ACTION_SUR`, tous les statuts harmonisés sauf la famille `INCONNU*`) | « À numériser » pour SEUL DAO, vide pour un candidat ERREUR DAO | vide |

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

   **Exclusion des `À SUPPRIMER` déjà réglés** : une ligne APPARIÉ dont le
   statut commence par `À SUPPRIMER` n'apparaît dans le recap que si son
   document n'a **pas** de master (`.tif`/`.tiff`) déjà `TRANSFERT_S3_OK` —
   recherché sur **tout le ref, tous corpus confondus** (pas seulement celui en
   cours de traitement : cf. le cas MED_MS/AMR_PR ci-dessous, où le master d'un
   fichier étiqueté MED_MS peut être classé sous un tout autre `corpus_code`).
   Rien à trancher sur une suppression déjà sécurisée par un master existant —
   ça n'encombre pas le recap. Le nombre exclu est indiqué dans les
   avertissements affichés en fin d'exécution.
   → `results/corpus/stagemel/recap/<corpus>_recap_draft_<date>.xlsx`

`stagemel_pipeline.sh` enchaîne `dao_ref_link.py` puis les trois scripts
ci-dessus sur l'ensemble des 37 corpus (pas de sélection possible par ce
point d'entrée) — à relancer tel quel après toute nouvelle notice EAD ou
nouveau REF.

## Utilisation

```bash
conda run -n rbx-bnr-data python scripts/corpus/stagemel_extraction_corpus.py MUS_ARC
conda run -n rbx-bnr-data python scripts/corpus/stagemel_cas_merge.py MUS_ARC
conda run -n rbx-bnr-data python scripts/corpus/stagemel_recap_draft.py MUS_ARC

# ou, pour tous les corpus, en une commande :
bash scripts/corpus/stagemel_pipeline.sh
```

## Corpus couverts

37 corpus au total (liste `CORPUS_CODES` dans `stagemel_extraction_corpus.py`,
reprise de `stage/Méthodologie/extraction_corpus.py`). Seize d'entre eux sont
déjà traités par le pipeline standard ([campagne `s3_key_cible`](s3_key_cible.md)) —
le recap stagemel y est redondant, utile en double-vérification seulement :
MED_CP, MED_EPH, MED_FOO, MED_IMA, MED_MAR, MED_MON, MED_PAR, MED_PBI, MED_PHD,
MED_PHO, MED_PLA, MED_PUB, AMR_CAD, ARA_CPS, LAR_PUB, OBS_JOU.

Les 21 restants sont la cible prioritaire de ce recap : MED_AFF, MED_AVI,
MED_CHA, MED_DIL, MED_FAN, MED_FLR, MED_JOU (≠ OBS_JOU), MED_LET, MED_MS,
MED_NPT, MED_VAH, MED_VAI, MED_VDM, MED_VID, LAI, AMR_AFF, AMR_LEB, AMR_OBJ,
CSV_PAL, MDF_MTX, MUS_ARC.

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

- [Campagne `s3_key_cible`](s3_key_cible.md) — pipeline de résolution (pose une
  clé et une action) pour les corpus déjà couverts
- [Chaîne d'appariement des DAO](dao_appariement.md) — génère
  `dao_ref_link_brut.csv`, source DAO de ce recap
- [Fichier de référence](../donnees/fichier_ref.md) — statuts `conservation_statut`
  à jour
