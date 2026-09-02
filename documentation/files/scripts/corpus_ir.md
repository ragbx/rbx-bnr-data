# Script : corpus_ir.py

**Emplacement :** `scripts/ead/corpus_ir.py`

Matérialise le lien `corpus_code` → instrument(s) de recherche (EAD), qui
n'existe nulle part ailleurs comme configuration explicite dans le dépôt (la
colonne `ir` de [data/corpus_liste/bnr_corpus.xlsx](../donnees/corpus.md) est
quasi vide — 9/102 lignes, toutes des placeholders « à créer par script »).

## Comment le lien est calculé

Aucune association corpus ↔ IR n'est déclarée quelque part : elle est déduite
en filtrant `results/ead/ead_cor/dao_ref_link_brut.csv` (colonne
`nom_fichier_base`) sur les lignes dont le nom de fichier **contient** le
`corpus_code` — la même méthode que
[stagemel_extraction_corpus.py](stagemel.md) pour extraire les DAO d'un
corpus. `dao_ref_link_brut.csv` est lui-même produit depuis
[data/ead/bnr/](../donnees/dao_daogrp.md) par
[dao_ref_link.py](dao_appariement.md) (colonne `ir` = nom du fichier EAD
source) : relancer ce script après tout ajout/modif de notice EAD, puis
`corpus_ir.py`.

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ref/_ref_files_{date}.csv.gz` | Liste des `corpus_code` distincts (dernier ref, auto-détecté) |
| `results/ead/ead_cor/dao_ref_link_brut.csv` | Colonnes `ir` (fichier EAD) et `nom_fichier_base` (pour le filtre par corpus) |

## Résultat

`results/ead/corpus_ir.csv` — colonnes `corpus_code`, `ir`, `chemin`
(`data/ead/bnr/<ir>`), `n_dao` (nombre de lignes DAO liées à ce couple
corpus/IR). Une ligne par IR distinct rattaché à un corpus :
- 0 IR → une ligne avec `ir`/`chemin`/`n_dao` vides (corpus sans notice EAD,
  ex. AMR_OBJ, MED_AVI, MED_PER, MED_NPT, MED_VID) ;
- 1 IR → cas le plus courant ;
- plusieurs IR → ex. AMR_AFF (2 IR), AMR_PLA (3 IR), LAI (2 IR, dont un
  partagé avec VAH).

## Utilisation

    conda run -n rbx-bnr-data python scripts/ead/corpus_ir.py

## Voir aussi

- [Chaîne stagemel](stagemel.md) — consommateur direct de cette même logique de filtrage
- [Chaîne d'appariement des DAO](dao_appariement.md) — génère `dao_ref_link_brut.csv`
