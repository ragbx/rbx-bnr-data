# quartiers2kml.py

[scripts/ead/quartiers2kml.py](../../../scripts/ead/quartiers2kml.py)

Carte **« pros »**, tirée du référentiel ; la carte **« public »**, tirée de
ce que l'on trouve dans les IR (lien vers la BN-R), est produite par
[geogname_ir2kml.py](geogname_ir2kml.md) avec les mêmes couleurs (décision
du 14/11/2026 : deux couples umap / kml).

Produit une carte des **quartiers, rues et points de Roubaix** en KML et au
format natif de uMap ; deux quartiers voisins ont toujours des couleurs
différentes (14/11/2026).

```bash
python scripts/ead/quartiers2kml.py
```

## Entrées

- `data/geo/referentiel_lieux_roubaix.csv` : quartiers, rues et points
  (`thesaurus`) ; géométrie `coord_manuel` sinon `coord_wkt`, libellé
  `libelle_manuel` sinon `libelle`, comme [geogname2skos.py](geogname2skos.md) ;
- `data/geo/rues_rattachement.csv` et `data/geo/rues_quartiers_13_repartition.csv` :
  quartier(s) de rattachement des rues, même règle que
  [rues_quartiers2skos.py](rues_quartiers2skos.md).

Le référentiel est lu directement : au 14/11/2026 il a 5 points, le
thésaurus `bnr_geo_rbx_sites.xml` (non regénéré) seulement 3.

## Calques

| Calque | Nombre | Style | Fiche |
|---|---|---|---|
| Quartiers | 31 | couleur du quartier, contour 2, remplissage 50 % | — |
| Rues | 909 (29 sans tracé non placées) | noir, épaisseur 3 | catégorie, quartier(s), ancien(s) nom(s) |
| Points | 5 | rouge `#C03730`, rond (`Circle` dans uMap) | catégorie, quartier (pas de dates, 14/11/2026) |

**Rouge des points** (14/11/2026) : hors palette de la BnR, mais accordé à
elle — teinte opposée au turquoise `#2FAAAF`, même saturation (58 %), un peu
plus clair (47 %) pour ressortir sur les quartiers.

Exemple de fiche : « Rue / Quartier(s) : Entrepont, Centre-Ville, Hommelet /
Ancien(s) nom(s) : Rue du Galon d'Eau ».

## Couleurs des quartiers

Reprises de [geogname_ir2kml.py](geogname_ir2kml.md) (fonctions importées) :
palette de la BnR hors noir et blanc, **turquoise `#2FAAAF`, jaune `#E9B400`,
violet `#9460A4`, gris `#ADA796`** ; coloriage glouton DSATUR du graphe de
voisinage, contours en contact à environ 1 m près. Le script s'arrête si deux
voisins ont la même couleur.

Au 14/11/2026 : 81 paires de voisins, aucune de même couleur.

| Couleur | Quartiers |
|---|---|
| turquoise | Alma Gare, Centre-Ville, Chemin Neuf, Hutin Oran Cartigny, Sainte-Élisabeth, Sartel Carihem, Trichon, Édouard Vaillant |
| jaune | Armentières, Espérance, Fosse-aux-Chênes, Justice, Moulin, Nouveau Roubaix, Pile, Épeule |
| violet | Fraternité, Hauts Champs, Hommelet, Nations Unies, Potennerie, Vauban |
| gris | Anseele Motte-Bossut, Barbieux, Crouy, Cul-de-Four, Entrepont, Fresnoy-Mackellerie, Linné, Petites Haies, Trois Ponts |

Limite acceptée (décision du 14/11/2026 : on garde 4 couleurs) : Anseele
Motte-Bossut et Crouy, tous deux gris, ne se touchent pas mais sont à 30 m
l'un de l'autre. Une tolérance de voisinage élargie à environ 20 m ou plus
demanderait 5 couleurs ; la palette n'en a que 4 hors noir et blanc.

## Sorties (results/ead/indexation/)

- `roubaix_lieux_referentiel.kml` : trois dossiers « Quartiers (31) », « Rues (909) »,
  « Points (5) » ;
  `ExtendedData` `libelle` (et `couleur` pour les quartiers) ; dans QGIS,
  symbologie « Symboles intégrés » ou catégorisation sur `couleur` ;
- `roubaix_lieux_referentiel.umap` : trois calques ; couleur de chaque quartier dans
  ses `_umap_options` (uMap n'applique pas les styles KML). Importer dans une
  **carte neuve** avec le format « umap » (l'import remplace le nom et le
  centre de la carte).

## Zones de clic des rues (« fantômes », 14/11/2026)

Une ligne fine est difficile à viser : un dernier dossier / calque **« Rues :
zones de clic »** contient, pour chaque rue, un tampon de 30 m autour du tracé
(fonction `tampon` de geogname_ir2kml.py, `TAMPON_M`) avec le même nom et la
même fiche. Un clic jusqu'à 30 m de la ligne ouvre donc la fiche de la rue.

- KML : style `rues_clic` entièrement transparent, dossier replié
  (`checkHideChildren`), comme dans geogname_ir2kml.py ;
- uMap : calque sans contour, rempli à 1 % (`UMAP_CLIC`). Non vérifié dans
  uMap.

Les points (marqueurs) restent au-dessus des zones de clic. Taille : KML
0,9 → 1,6 Mo, uMap 0,6 → 1,25 Mo.
