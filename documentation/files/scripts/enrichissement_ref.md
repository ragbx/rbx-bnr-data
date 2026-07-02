# Enrichissement du référentiel (maillon s3/dao/oai)

Vue d'ensemble du **maillon d'enrichissement** qui s'intercale, dans la
construction du [fichier de référence](../donnees/fichier_ref.md), entre la
chaîne d'appariement azrael (`scripts/azrael/`, étapes 10 → 55, voir
`scripts/azrael/README.md`) et la fusion finale `60_merge_new_old_ref.py`.

Il part de l'ensemble complet des fichiers (`_az_notaz_ok_all_{date}.csv.gz`,
sortie de l'étape 55 : fichiers azrael résolus + partie non-az réinjectée) et lui
attache, par jointures successives, l'état S3, les liens DAO (cote) et les
identifiants OAI. Chaque étape prend en entrée la sortie de la précédente.

---

## Flux

```
_az_notaz_ok_all_{date}.csv.gz              (sortie étape 55)
│
├── S3   s3_join_ref.py                      → tmp/_ref_files_{date}_tmp_s3.csv.gz
│
├── DAO  dao_ref_link.py                     → ead_cor/dao_ref_link_brut.csv
│         └── dao_ref_apparie.py             → ead_cor/dao_ref_link.csv
│              └── dao_join_ref.py           → tmp/_ref_files_{date}_tmp_s3_dao.csv.gz
│
└── OAI  oai_join_ref.py                     → _ref_files_{date}_tmp_s3_dao_oai.csv.gz
                                               (entrée de 60_merge_new_old_ref.py)
```

L'ordre **s3 → dao → oai** est imposé par les dépendances : `s3_key` doit exister
avant l'indexation DAO, et l'appariement OAI se fait sur la cote (`unitid`)
produite par le DAO.

---

## S3 — état de versement au niveau fichier

| Script | Entrée | Sortie | Rôle |
|---|---|---|---|
| `scripts/s3/s3_join_ref.py` | fichiers + `data/s3/s3_listing_{date}.csv.gz` | `tmp/_ref_files_{date}_tmp_s3.csv.gz` | Reporte l'état S3 sur chaque fichier par **jointure sur `uuid`** |

Le listing du bucket porte, pour chaque objet versé, son `uuid` et son
`checksum_md5` en métadonnées, ainsi que sa clé, sa date et son bucket. Le listing
courant est **autoritaire** : les quatre colonnes S3 sont écrasées (pas d'héritage
de l'ancien ref).

- `s3_key` ← `key` du listing
- `s3_uploaded_date` ← `last_modified` au format **AAAAMMJJ**
- `s3_bucket` ← `s3_bucket` réel du listing (`communicable` / `incommunicable`)
- `s3_uploaded` = `True` si l'`uuid` est dans le listing, sinon `False` (100 % rempli)

Ces colonnes plein-nom alimentent aussi `ead_bnr2mnesys.py`, qui indexe les
fichiers de conservation par `s3_key`.

---

## DAO — cote de diffusion

Objectif : savoir quels fichiers sont reliés à un lien de diffusion (`<dao>`) et
donc à un `unitid`, **sans retransformer les IR**. Trois scripts.

> Ne pas confondre avec la [chaîne de diagnostic DAO](dao_appariement.md)
> (`dao_appariement.sh` : orphelins, plages lacunaires, fichiers manquants), qui
> analyse les IR transformés sans alimenter le ref. Les méthodes « exacte /
> normalisée / padding » y portent les mêmes noms mais pas les mêmes
> définitions : les statistiques des deux chaînes ne sont pas comparables. Le
> développement des plages first/last est partagé (module
> `scripts/ead/dao_plage.py`).

| Script | Entrée | Sortie | Rôle |
|---|---|---|---|
| `scripts/ead/dao_ref_link.py` | `data/ead/bnr/` + `data/ead/mnesys/` + `ead_cor/bnr2mnesys/` (audio) | `ead_cor/dao_ref_link_brut.csv` | **Extraction** : parcourt les IR, développe les plages `first`/`last`, extrait pour chaque lien `href` + `unitid` + `finding_aid` |
| `scripts/ead/dao_ref_apparie.py` | `dao_ref_link_brut.csv` + ref (`_tmp_s3`) | `ead_cor/dao_ref_link.csv` | **Table d'association** fichier ↔ `unitid`, appariée par *stem* (nom sans extension). Un fichier peut avoir plusieurs `unitid` |
| `scripts/ead/dao_join_ref.py` | fichiers (`_tmp_s3`) + `dao_ref_link.csv` | `tmp/_ref_files_{date}_tmp_s3_dao.csv.gz` | **Injection** : n'attache qu'**un seul** couple par fichier (colonnes `dao_finding_aid`, `dao_unitid`) |

Points de conception :

- **Deux formats sources, deux vocabulaires de rôle** — `data/ead/bnr` (natif :
  rôles `image`/`mp3`/…, plages `image:first`/`image:last`) et `data/ead/mnesys`
  (rôle souvent absent, plages `first_image`/`last_image`).
- **`finding_aid` = nom de fichier de l'IR**, pas l'`eadid` (certains IR mnesys ont
  un `eadid` générique / placeholder).
- **Résolution hiérarchique** : pour un même (`href`, IR), on garde le composant le
  plus profond (`unitid` le plus spécifique). Les conflits inter-sources et de
  même profondeur sont conservés (toutes les paires) — souvent des erreurs de
  catalogage assumées.
- **Une seule valeur injectée** : `dao_join_ref.py` priorise la source **bnr** puis
  le premier couple (ordre déterministe).
- **Audio : noms de conservation via les IR transformés** (source `bnr2mnesys`) —
  les IR sources ne portent que le mp3 de diffusion (`RBX_MED_FLRS_X.mp3`) alors
  que la conservation est nommée `RBX_MED_X_{96kHz24B,44kHz24B,TI}.{wav,mp3}` :
  l'appariement par stem échoue (92 % des liens audio perdus, mesuré sur le run
  du 20260630). On extrait donc en plus les liens `preservation:audio` des IR
  transformés (`results/ead/ead_cor/bnr2mnesys/`, où `ead_bnr2mnesys.py` a déjà
  fait l'appariement contre le ref), et rien d'autre (le reste est couvert par
  les sources bnr/mnesys). Le `finding_aid` émis est celui de l'IR **source**,
  résolu via l'Excel de transfert (`results/ir/liste_instruments_recherche_*_
  transfert_mnesys.xlsx`, ex. `FR595126101_MED_FLRS.xml` ← `FR595129901_MED_15.xml`).
  Suppose les IR transformés régénérés contre le ref courant. Effet mesuré :
  couverture `unitid` de l'audio 1,2 % → 44,8 % (3 291/7 347 fichiers, avec
  notice OAI dans la foulée).

> La table `dao_ref_link.csv` (toutes les paires fichier ↔ `unitid`) est conservée
> à part : le référentiel ne reçoit qu'un `unitid` choisi, mais l'association
> complète reste consultable.

---

## OAI — identifiants de notice

| Script | Entrée | Sortie | Rôle |
|---|---|---|---|
| `scripts/oai/oai_join_ref.py` | fichiers (`_tmp_s3_dao`) + `data/oai/oai_records_{date}.csv.gz` | `_ref_files_{date}_tmp_s3_dao_oai.csv.gz` | Reporte `osiros_id` + `setname` sur chaque fichier par jointure sur la **cote** |

Le moissonnage (`scripts/oai/bnr_moissonnage.py`) produit
`data/oai/oai_records_{date}.csv.gz` au niveau **notice** (`identifier`, `cote`,
`title`, `setname`, `osiros_id`). Les fichiers portent une cote dans `unitid` (issue
du DAO) ; après normalisation (`_` → espace, espaces multiples réduits), `unitid`
correspond à la `cote` OAI. Sortie : colonnes `oai_osiros_id` et `oai_setname`.

---

## Raccordement à `60_merge`

La sortie `_ref_files_{date}_tmp_s3_dao_oai.csv.gz` est l'**entrée** de
`60_merge_new_old_ref.py`. Le merge **coalesce** les colonnes DAO/OAI fraîches avec
celles de l'ancien ref (`_old`, appariées par `uuid` + `checksum_md5`) : la valeur
neuve prime, sinon on hérite de l'ancien ref (ce qui couvre notamment la presse).
Les colonnes S3, elles, ne sont pas coalescées — le listing courant fait foi.

---

## Exécution

Depuis la **racine du dépôt**, dans l'ordre du flux (chaque `--input` reprend la
sortie précédente) :

```bash
NEW=20260630

# S3
conda run -n rbx-bnr-data python scripts/s3/s3_join_ref.py \
  --input results/ref/tmp/_az_notaz_ok_all_${NEW}.csv.gz \
  --output results/ref/tmp/_ref_files_${NEW}_tmp_s3.csv.gz --s3-date ${NEW}

# DAO (extraction → table → injection)
conda run -n rbx-bnr-data python scripts/ead/dao_ref_link.py
conda run -n rbx-bnr-data python scripts/ead/dao_ref_apparie.py \
  --ref results/ref/tmp/_ref_files_${NEW}_tmp_s3.csv.gz
conda run -n rbx-bnr-data python scripts/ead/dao_join_ref.py \
  --input results/ref/tmp/_ref_files_${NEW}_tmp_s3.csv.gz \
  --output results/ref/tmp/_ref_files_${NEW}_tmp_s3_dao.csv.gz

# OAI
conda run -n rbx-bnr-data python scripts/oai/oai_join_ref.py \
  --input results/ref/tmp/_ref_files_${NEW}_tmp_s3_dao.csv.gz \
  --output results/ref/_ref_files_${NEW}_tmp_s3_dao_oai.csv.gz \
  --unitid-col dao_unitid --oai-date ${NEW}
```

Les scripts `s3_join_ref.py`, `dao_join_ref.py` et `oai_join_ref.py` sont aussi
**importables** (`ajouter_s3`, `ajouter_dao`, `ajouter_oai`) pour un enchaînement
en notebook.
