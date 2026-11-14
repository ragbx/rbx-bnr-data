# quartiers_13.py

[scripts/ead/quartiers_13.py](../../../scripts/ead/quartiers_13.py)

Construit un **découpage de Roubaix en 13 quartiers**, chacun étant l'union de
quartiers MEL (les 31 « quartiers historiques » du référentiel des lieux),
selon les regroupements de l'ancienne liste des quartiers de la BnR. Répartit
ensuite chaque rue du référentiel entre les 13 et entre les 31 quartiers.

Analyse dérivée (2026-11-13) : le référentiel des lieux n'est pas modifié.

```bash
python scripts/ead/quartiers_13.py
```

## Entrées

- `data/geo/referentiel_lieux_roubaix.csv` : polygones des 31 quartiers MEL,
  tracés des rues ;
- `data/geo/bnr_liste_quartiers_20261112.csv` : ancienne liste des quartiers
  de la BnR (export du 12/11/2026, latin-1, tabulations ; colonnes Catégorie
  = regroupement, Libellé = quartier, Valeur = contour `[lat, lon]`) ;
- `data/geo/bnr_liste_rues_20261112.csv` : ancienne liste des rues de la BnR
  (même format ; Catégorie = l'un des 13 regroupements, Valeur = un point) ;
- `results/ead/ead_cor/bnr2mnesysgeo/*.xml` : pour compter les citations
  (geogname « voies ») de chaque rue.

## Méthode

- Les regroupements sont écrits dans `QUARTIERS_13` (libellés proposés avec
  les graphies du référentiel). Espérance est rangée avec Anseele, Centre,
  Nations Unies, Crouy dans les deux listes, mais ne figure dans le nom que
  dans la liste des rues. Hutin et Oran Cartigny sont un seul quartier MEL.
- Contrôle : chaque polygone de l'ancienne liste recouvre le quartier MEL
  attendu (IoU ≥ 0,95 ; 0,976 à 1,000 constatés) et est rangé dans le même
  regroupement. Les contours de l'ancienne liste sont donc les contours MEL ;
  la géométrie des 13 quartiers est construite depuis le référentiel.
- Union des polygones MEL ; les interstices de moins de 100 m² entre deux
  quartiers voisins sont bouchés (un seul, 2 m², dans Hauts Champs).
- Rues : part de la longueur du tracé dans chaque quartier (sur la longueur
  dans Roubaix) ; un quartier est « traversé » au-delà de 20 m. La rue est
  comparée à l'ancienne liste des rues (libellé « Pierre Motte, rue »
  remis à l'endroit, clé `cle()` du référentiel).

## Sorties (data/geo/)

- `quartiers_13.csv` : `lieu_id` (`bnr:quartiers_13:01` à `13`), `libelle`,
  `libelle_bnr` (noms dans les anciennes listes), `quartiers_31`,
  `nb_quartiers_31`, `surface_ha`, `coord_wkt` (WGS84) ; colonnes manuelles
  `libelle_manuel`, `remarque` conservées d'une exécution à l'autre ;
- `quartiers_13.gpkg` : couches `quartiers_13` et `quartiers_31` (avec
  `quartier_13`, `lieu_id_13`) ;
- `rues_quartiers_13_repartition.csv` : une ligne par rue du référentiel ;
  `q13_*` / `q31_*` = quartier principal, part, nombre de quartiers traversés,
  répartition ; `nb_geogname` = citations dans les IR de test ;
  `liste_bnr_*` et `accord_liste_bnr` (identique / liste incomplète / liste
  en trop / différent / sans quartier) = comparaison avec l'ancienne liste.

## Ancienne liste des rues : comment les rues étaient rangées

Contrôle du 13/11/2026 : la liste range les rues dans les **13**
regroupements (pas dans les 31), et une rue y figure dans **tous les
quartiers qu'elle traverse** (92 rues sur 669 dans 2 à 4 quartiers). Le
point de chaque rue n'est pas son centroïde : il est sur le tracé (écart
médian 1 m, contre 21 m pour le centroïde), vers le milieu de sa longueur,
et c'est le même point pour toutes les lignes d'une rue ; il ne sert donc pas
au rangement. Sur les 596 rues retrouvées, 531 ont exactement les quartiers
traversés (> 20 m), 35 en ont un de trop, 17 un de moins, 5 un autre.

## Bilan (13/11/2026)

| | 31 quartiers | 13 quartiers |
|---|---|---|
| rues à ≥ 95 % dans un seul quartier | 780 / 909 (85,8 %) | 814 / 909 (89,5 %) |
| citations de ces rues dans les IR | 4 607 / 9 981 (46,2 %) | 6 426 / 9 981 (64,4 %) |
