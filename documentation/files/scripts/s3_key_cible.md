# Campagne `s3_key_cible` (juillet 2026)

**Emplacement :** `scripts/s3/*_s3_key_cible.py`, `scripts/s3/ref_*_20260711.py`

> Contrairement aux autres pages de scripts, celle-ci documente une **campagne
> ponctuelle** (2026-07-07 → 2026-07-11), pas un pipeline générique à rejouer :
> chaque script cible un corpus précis, contient des décisions actées avec
> l'utilisateur à une date donnée, et s'auto-vérifie par des `assert` sur des
> effectifs figés (ex. `assert len(sel) == 840`). Ils ne sont pas conçus pour
> être relancés sur un nouveau ref sans adaptation.

## Objectif

Résorber le stock de fichiers en attente de versement S3 qui échappaient au
pipeline standard (maillon [S3](s3.md) sur `s3_key` réelle + [enrichissement du
référentiel](enrichissement_ref.md)) : des corpus jamais versés, des lots
`INCONNU` à trancher, ou des fichiers déjà en ligne côté bn-r mais sans
correspondance S3 (`EN LIGNE - À TRANSFERER ?`).

Principe commun à tous ces scripts : on calcule une colonne **`s3_key_cible`**
(la clé S3 *prévue*), sans jamais toucher à `s3_key` (qui reste l'invariant
« fichier réellement présent sur S3 », alimenté par le listing S3 — voir
[Fichier de référence](../donnees/fichier_ref.md)). Le témoin du transfert
réel reste `s3_uploaded`, pas `s3_key`/`s3_key_cible`.

`s3_key_cible` a été une colonne **transitoire** : après bascule (voir plus
bas), elle a été absorbée dans `s3_key` et **n'existe plus** dans le
référentiel actuel.

---

## Deux familles de scripts

### A. Construction par corpus (`results/ref/<corpus>.csv` → `<corpus>_fin.csv`)

Chaque script part d'un extrait dédié du ref pour un corpus, pose `s3_key_cible`
et éventuellement révise `conservation_statut` / `publication_statut`, avec un
fichier d'audit en regard (`_<corpus>_cible_audit_20260630.csv`).

| Script | Corpus | Résumé |
|---|---|---|
| `amr_cad_s3_key_cible.py` | AMR_CAD (cadastre) | Convention plate `AMR/AMR_CAD/<name>` ; arbitrage des 267 INCONNU (78 publiés jamais poussés, 189 scans FRAD59 mis de côté) |
| `amr_pla_s3_key_cible.py` | AMR_PLA (plans) | Clé posée sur tout ce qui doit monter (conservation + diffusion) ; 8 INCONNU tranchés |
| `amr_pr_s3_key_cible.py` | AMR_PR | Séries COR/REQ (2 998 masters hors notice) → `À TRANSFERER` avec clé, publication laissée `inconnu` |
| `amr_ec_s3_key_cible.py` | AMR_EC (état civil) | Reconstruction de la clé pour les fichiers `INCONNU` à partir d'une convention cible dédiée (`data/s3/amr_ec_V1.xlsx`), familles non ambiguës seulement |
| `amr_gue_s3_key_cible.py` | AMR_GUE (affiches de guerre 7H) | Regroupe masters (341 tif) + dérivés (678 jpg/ALTO) dans un seul dossier `AMR/AMR_GUE/`, malgré un `corpus_code` d'origine différent pour les masters |
| `amr_pvc_s3_key_cible.py` | AMR_PVC (procès-verbaux CADN) | Versement complet inventé (aucune clé préexistante), schéma calqué sur `DEL_TAP` |
| `amr_del_cadn_s3_key_cible.py` | AMR_DEL (séances CADN) | Tranche 23 956 INCONNU + bascule 23 676 jpg vers `À TRANSFERER`, schéma `DEL_TAP` |
| `ara_cps_s3_key_cible.py` | ARA_CPS (audio) | Convention par analogie `ARA/ARA_CPS/<name>` (jamais versé) ; publie les 28 INCONNU restants |
| `lar_pub_s3_key_cible.py` / `lar_pub_fin.py` | LAR_PUB | Isolé du lot « reste » à cause d'une collision de clé (voir plus bas) ; `lar_pub_fin.py` reproduit la même décision sur l'extrait per-corpus |
| `med_s3_key_cible.py <corpus> [--apply]` | MED_* (générique) | Voir [ci-dessous](#script-générique-med_s3_key_ciblepy) |

### B. Rattrapage direct sur le ref dédupliqué (`_ref_files_20260630_dedup.csv.gz`)

Ces scripts patchent en place le ref (copie de travail dédupliquée), sans
passer par un extrait `<corpus>.csv` intermédiaire — typiquement pour un
backlog `EN LIGNE - À TRANSFERER ?` localisé sur un seul corpus.

| Script | Corpus | Résumé |
|---|---|---|
| `med_eph_s3_key_cible.py` | MED_EPH | 840 masters uniques, aucun doublon → clé plate `MED/MED_EPH/<name>` |
| `med_par_s3_key_cible.py` | MED_PAR | Deux régimes de placement S3 selon le sous-corpus ; dédup par checksum (48 copies « Patch correctif » écartées) ; 6 typos de nom corrigées dans la clé |
| `gue_7h_s3_key_cible.py` + `gue_7h_harmonise_cible.py` + `gue_7h_inconnu_diffusion.py` + `gue_7h_tiff_realign_amr_gue.py` | 7H (affiches de guerre) | Suite de 4 scripts : pose initiale de la clé, harmonisation des noms de masters sur la convention diffusion, résolution de 4 INCONNU restants, puis réalignement `AMR_AFF` → `AMR_GUE` (unification du dossier) |
| `obs_jou_s3_key_cible.py` + `obs_jou_masters_appariement_20260711.py` + `obs_jou_align_dedup_20260711.py` | OBS_JOU | Partition en 3 groupes (A doublons de contenu à supprimer / B masters appariés à un jpg déjà en ligne / R masters uniques) ; les deux derniers scripts reprennent cette logique le 2026-07-11 directement sur le ref (le fichier `_dedup` d'origine ayant été supprimé) |
| `reste_s3_key_cible.py` | 12 corpus restants | Traite en une passe tout ce qui n'a pas son propre script ; règle générale `<PREFIX>/<name>` (ancre S3 réelle si le corpus est déjà versé, sinon convention `<FAMILLE>/<corpus_code>`) ; dédup uniforme par checksum |
| `med_ms_voir_elise.py` | MED_MS | Corpus pollué (mauvais étiquetage, doublons de nom) : pas de décision automatique, statut marqué `EN LIGNE - À TRANSFERER ? VOIR ELISE` pour arbitrage humain |

---

## Script générique `med_s3_key_cible.py`

`python scripts/s3/med_s3_key_cible.py <corpus> [--apply]` — malgré son nom, ne
se limite plus au fonds MED (étendu le 2026-07-11 pour AMR). Paramétré par un
dictionnaire `CONFIG` en tête de script, une entrée par corpus, avec les
options suivantes :

| Option | Rôle |
|---|---|
| `token` | Nom de dossier cible (`<fonds>/<token>/<name>`) |
| `fonds` | Préfixe racine de la clé (`MED` par défaut, `AMR` pour les corpus AMR) |
| `inconnu` | Politique sur les `INCONNU` : `"transfer"` (fichiers RBX propres → `À TRANSFERER` + clé, publication inchangée) ou `"hold"` (backlog laissé en l'état) |
| `inconnu_ext` | Restreint la politique `transfer` à certaines extensions |
| `dedup` | Doublons de nom à checksum identique : `"keep"` (garde la copie dont le path contient `token`) ou `"drop"` (l'inverse) — l'écartée passe `À SUPPRIMER (DOUBLON)` |
| `discard` | Fichiers dont le path contient un `token` donné → statut fixe, sans clé (rebuts) |
| `rename` | Liste `[(regex, remplacement)]` appliquée au nom **dans la clé seulement** (le nom physique `name` ne change pas) — peut aussi injecter un sous-dossier (ex. `<année>/RBX_...`) |
| `strip_double_ext` | Normalise une double extension (`.tif.tif` → `.tif`) dans la clé |

Règles communes à tous les corpus :

- `TRANSFERT_S3_OK` → `s3_key_cible` = `s3_key` réel ;
- `EN LIGNE - À TRANSFERER ?` → clé posée, conservation → `À TRANSFERER` ;
- `À SUPPRIMER (*)` / `CORBEILLE` / doublons → pas de clé, inchangés ;
- garde-fou anti-collision : toute clé cible qui dupliquerait une clé
  existante (réelle ou cible) est **exclue** du keying.

Dix-sept corpus ont été traités avec ce script entre le 09/07 et le 11/07 :

- **MED** (défaut) : `med_eph`, `med_ima`, `med_mar`, `med_foo`, `med_pho`,
  `med_mon`, `med_pla`, `med_pub`, `med_phd`, `med_pbi` ;
- **AMR** (`fonds: "AMR"`) : `amr_pho`, `amr_dia`, `amr_507w`, `amr_vic`,
  `amr_3d`, `amr_pop`, `amr_puv`.

`med_pho` (17 499 lignes) et une partie de `med_foo` sont restés en politique
`hold` (backlog brut non décrit, non traité). `med_cp` n'utilise **pas** ce
script générique : il a son propre script dédié (`med_cp_s3_key_cible.py`,
voir tableau A) pour un arbitrage plus fin de ses INCONNU.

---

## Assemblage final dans le référentiel

Une fois les `<corpus>_fin.csv` produits (famille A) et les patches directs
appliqués (famille B), une suite de scripts intègre le tout dans
`results/ref/_ref_files_20260630.csv.gz` :

| Étape | Script | Rôle |
|---|---|---|
| 1 | `ref_merge_fin_20260711.py` | Fusionne dans le ref global tous les `*_fin.csv` produits les 10 et 11/07, par `uuid`. Les `À TRANSFERER` des fins deviennent `À TRANSFERER APRES VALIDATION` (état pré-upload, à valider) — sauf les corpus du 09/07 déjà réellement versés, intégrés en `TRANSFERT_S3_OK` |
| 2 | `ref_harmonise_statuts_20260711.py` / `ref_harmonise_statuts2_20260711.py` | Harmonisent les libellés `conservation_statut` (casse, fusion des familles `DDE - NE PAS GARDER` / `CORBEILLE` dans `À SUPPRIMER (*)`) — voir le [tableau à jour des statuts](../donnees/fichier_ref.md) |
| 3 | `ref_doublon_source_20260711.py` | Requalifie la zone grise AMR_EC/AMR_GUE en `À SUPPRIMER (DOUBLON SOURCE - À VERIFIER)` — alerte : contrôle documentaire par échantillon requis avant purge |
| 4 | `ref_corbeille_suppr_20260711.py` | Dernier statut `CORBEILLE (DIFFUSION)` replié dans `À SUPPRIMER (DIFFUSION)` : il n'y a plus de famille `CORBEILLE` |
| 5 | `ref_bascule_cible_20260711.py` | **Bascule `s3_key_cible` → `s3_key`** dans le ref, après vérifications anti-collision. `s3_key` cesse d'être strictement « présent sur S3 » : les lignes `À TRANSFERER APRES VALIDATION` portent déjà leur clé avant upload — `s3_uploaded` reste le seul témoin du transfert réel |
| 6 | `ref_stem_profil_20260711.py` | (re)calcule `s3_stem_profil` — voir [Fichier de référence](../donnees/fichier_ref.md) |

Tous ces scripts de fusion suivent le même schéma de sécurité : lecture en
`dtype=str`, vérifications par `assert` avant écriture, sauvegarde du ref
(`_old<n>.csv.gz`) avant toute modification, écriture uniquement avec
`--apply` (dry-run par défaut).

---

## Voir aussi

- [Fichier de référence](../donnees/fichier_ref.md) — statuts et colonnes S3 à jour
- [Scripts S3](s3.md) — inventaire et versement (pipeline standard, hors campagne)
- [Enrichissement du référentiel](enrichissement_ref.md) — jointure `s3_key` réelle dans le pipeline azrael → ref
