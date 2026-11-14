# quartiers_31_rues_notices.py

Indexation par quartier à **31 quartiers** (découpage retenu pour le moment, 14/11/2026).

```bash
python scripts/ead/rues_rattachement.py          # d'abord
python scripts/ead/quartiers_31_rues_notices.py
```

## Quartiers d'une rue
- rue sûre (≥ 95 % de sa longueur dans un quartier) : ce quartier ;
- rue ambiguë : `retenu_31` de `data/geo/rues_rattachement.csv` (saisie validée).

## Quartier d'une notice (priorité décidée le 13/11/2026)
1. **point** : site validé (`propositions_sites_20261112*.csv`), point du référentiel indexé
   (`bnr_geo_rbx_sites`) ou adresse « n° + voie » géocodée (BAN) → quartier du point, toujours ;
2. **rue(s)** : toutes les rues indexées ont un seul quartier ;
3. **croisement** : les rues indexées n'ont qu'un quartier en commun ;
4. sinon **à lever**.

Plusieurs lieux distincts → plusieurs quartiers : ce n'est pas une ambiguïté.

## Sorties (`results/ead/indexation/`)
- `rues_quartiers_31.xlsx` : une ligne par couple rue / quartier possible ;
- `notices_quartiers_31.xlsx` : onglets `sans_ambiguite`, `a_lever`, `synthese` ; colonnes `index_actuel_quartier` / `index_actuel_rue` = geogname `source="quartier"` / `"rue"` des IR actuels (`results/ead/ead_cor/bnr2mnesys/`, tels qu'importés dans Mnesys), textes tels quels ; colonne `ark` = lien ARK `publication:previous` (dao ou daoloc), cliquable ; vide si la notice n'en a pas.

Analyse dérivée : ni le référentiel, ni la table de rattachement, ni les IR ne sont modifiés.
