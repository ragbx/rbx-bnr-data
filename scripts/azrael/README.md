# Pipeline azrael → ref

Objectif : produire un nouveau référentiel `results/ref/_ref_files_{NEW}.csv.gz` à
partir de la dernière extraction azrael, en récupérant `uuid` + `checksum_md5` depuis
le ref précédent **sans relire les fichiers sur disque autant que possible**.

## Configuration

Toutes les dates sont centralisées dans **`_pipeline.py`** :

```python
OLD_REF_DATE = "20260502"   # ref précédent (source des uuid/checksum)
NEW_REF_DATE = "20260630"   # extraction azrael courante
```

À chaque nouveau ref, **modifier ces deux valeurs uniquement** — aucune date n'est
codée en dur ailleurs. `_pipeline.py` fournit aussi les helpers de chemins
(`ref_file`, `az_file`, `tmp_file`) et la convention de nommage.

## Lancement

Depuis la **racine du dépôt** (les chemins de données sont relatifs à la racine ;
l'import `_pipeline` fonctionne car le script ajoute son propre dossier au path) :

```bash
conda run -n rbx-bnr-data python scripts/azrael/10_match_meta.py
conda run -n rbx-bnr-data python scripts/azrael/20_match_size.py
conda run -n rbx-bnr-data python scripts/azrael/30_compute_checksum.py   # ⚠ lit le disque
conda run -n rbx-bnr-data python scripts/azrael/40_match_checksum.py
conda run -n rbx-bnr-data python scripts/azrael/50_match_checksum_dupl.py
conda run -n rbx-bnr-data python scripts/azrael/55_inject_notaz.py
# … enrichissement s3/dao/oai (hors dépôt) …
conda run -n rbx-bnr-data python scripts/azrael/60_merge_new_old_ref.py
```

## Étapes

| # | Script | Clé d'appariement | Disque | Entrées → sorties |
|---|--------|-------------------|:------:|-------------------|
| 1 | `10_match_meta.py` | name, path, size, **2 dates** | non | ref + az → `s1_meta__ok` / `s1_meta__az` / `s1_meta__ref` / `ref_notaz` |
| 2 | `20_match_size.py` | name, path, size | non | `s1_meta__az`+`s1_meta__ref` → `s2_size__ok` / `s2_size__az` / `s2_size__ref` |
| 3 | `30_compute_checksum.py` | — (calcul MD5) | **oui** | `s2_size__az` → `s3_cs__az` |
| 4 | `40_match_checksum.py` | name, checksum_md5 | non | `s3_cs__az`+`s2_size__ref` → `_ok_cumul_s4` / `s4_cs__*` / `s4_csdupl__*` |
| 5 | `50_match_checksum_dupl.py` | name, path (doublons) | non | `s4_csdupl__*` → **`_az_ok_all`** (az résolus) / `s5_cs__*` |
| 5.5 | `55_inject_notaz.py` | — (concat) | non | `_az_ok_all` + `ref_notaz` → **`_az_notaz_ok_all`** (az + non-az) |
| 6 | `60_merge_new_old_ref.py` | uuid, checksum_md5 | non | `_ref_files_{NEW}_tmp_s3_dao_oai` + ref → `_ref_files_{NEW}` |

Entre l'étape 5.5 et l'étape 6 s'intercale le **maillon d'enrichissement s3/dao/oai
(hors dépôt)** : il consomme `_az_notaz_ok_all` et produit `_ref_files_{NEW}_tmp_s3_dao_oai`.

L'étape **3 est la seule à relire les fichiers** : grâce aux étapes 1 et 2, son volume
est fortement réduit (seuls les fichiers réellement nouveaux ou modifiés y passent).

## Convention de nommage (fichiers intermédiaires, `results/ref/tmp/`)

```
s{n}_{quoi}__{rôle}_{date}.csv[.gz]
   n     : numéro d'étape
   rôle  : ok  → résolus (uuid + checksum_md5 connus)
           az  → à résoudre côté azrael   (ancien « ko_left »)
           ref → à résoudre côté référence (ancien « ko_right »)
```

Fichiers `_`-préfixés = agrégats : `_ok_cumul_s4` (cumul partiel), `_az_ok_all`
(tous les fichiers az résolus = sortie finale de la chaîne de matching).

## Interfaces externes (NE PAS renommer sans reconnecter le maillon)

Ces fichiers sont produits ou consommés **hors de ce dépôt** (enrichissement
s3/dao/oai manuel/notebook) :

- entrées : `data/az/bnr_azrael_{NEW}.csv.gz`, `results/ref/_ref_files_{OLD}.csv.gz`
- sortie chaîne de matching : `results/ref/tmp/_az_ok_all_{NEW}.csv.gz`
- `results/ref/tmp/ref_notaz_{NEW}.csv.gz` — **partie non-az** du ref précédent
  (`source2s3 != 'az'`), hors champ du matching. Réinjectée par `55_inject_notaz.py`
  (concaténée à `_az_ok_all` → `_az_notaz_ok_all`) **avant** le maillon s3/dao/oai,
  pour que celui-ci travaille sur l'ensemble complet (az + non-az).
- entrée du maillon s3/dao/oai : `results/ref/tmp/_az_notaz_ok_all_{NEW}.csv.gz`
- entrée du merge : `results/ref/_ref_files_{NEW}_tmp_s3_dao_oai.csv.gz`
- sortie finale : `results/ref/_ref_files_{NEW}.csv.gz`

> Note : `60_merge` part de `_tmp_s3_dao_oai` (déjà complet) ; le `m2 =
> ref[~ref.uuid.isin(m1.uuid)]` qui y est calculé n'est **pas** utilisé (vestige),
> la réinjection du non-az ayant lieu en amont (étape 5.5).

> ⚠ Le maillon d'enrichissement **s3/dao/oai** (entre `_az_ok_all` et
> `_ref_files_{NEW}_tmp_s3_dao_oai`) n'est pas versionné ici : il doit pointer en
> entrée sur `_az_ok_all_{NEW}.csv.gz` (anciennement `new_ref_az_ok_it3`).

## Correspondance avec l'ancienne nomenclature

| Ancien | Nouveau |
|--------|---------|
| `02_compare_ref_az_it1.py` | `10_match_meta.py` |
| `02_compare_ref_az_it1b.py` | `20_match_size.py` |
| `add_checksum.py` | `30_compute_checksum.py` |
| `02_compare_ref_az_it2.py` | `40_match_checksum.py` |
| `02_compare_ref_az_it3.py` | `50_match_checksum_dupl.py` |
| `03_merge_new_old_ref.py` | `60_merge_new_old_ref.py` |
| `new_ref_az_ok_it1` | `s1_meta__ok` |
| `new_ref_az_ko_left_it1` / `_right_it1` | `s1_meta__az` / `s1_meta__ref` |
| `new_ref_az_ok_it1b` | `s2_size__ok` |
| `new_ref_az_ko_left_it1b` / `_right_it1b` | `s2_size__az` / `s2_size__ref` |
| `bnr_azrael_{d}_nouuid-cs` | `s3_cs__az` |
| `new_ref_az_ok_it2` | `_ok_cumul_s4` |
| `new_ref_az_ko_left_no_dupl_it2` / `_right_` | `s4_cs__az` / `s4_cs__ref` |
| `new_ref_az_ko_left_csdupl_it2` / `_right_` | `s4_csdupl__az` / `s4_csdupl__ref` |
| `new_ref_az_ok_it3` | `_az_ok_all` |

> Les scripts `azrael_compare_01.py` et `azrael_compare_02_withCS.py` sont des
> versions antérieures conservées pour mémoire et ne font pas partie de ce pipeline.
