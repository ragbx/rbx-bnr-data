# Script : geogname_ir2kml.py

**Emplacement :** `scripts/ead/geogname_ir2kml.py`

Produit un fichier KML (Google Earth, QGIS, uMap…) des lieux indexés dans
les IR réécrits par [geogname_applique_concordance.py](geogname_applique_concordance.md) :
**un repère par lieu** (thésaurus + libellé), placé d'après le `@coord` des
`<geogname>`. Depuis le 2026-11-08, la fiche ne montre que le libellé, le
nombre d'occurrences et un lien vers la recherche de la BN-R.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/bnr2mnesysgeo/*.xml` | IR de test, concordance appliquée : `<geogname>` des thésaurus géographiques avec leur `@coord` |
| `data/geo/concordance_geogname_lieux.csv` | Formes d'origine (`forme`, `source`) de chaque lieu (`thesaurus`, `libelle_retenu`), pour le lien BN-R (`--concordance`) |

Seuls les `<geogname>` dont le `@source` désigne un thésaurus géographique
de Roubaix sont placés : `bnr_geo_rbx_quartiers`, `bnr_geo_rbx_voies`,
`bnr_geo_rbx_sites`. **Depuis le 2026-11-08, `bnr_geo_territoires`
(communes et pays) est laissé hors du KML** (`HORS_KML`) : ses occurrences
sont seulement comptées en fin d'exécution.

## Résultat

`results/ead/indexation/bnr2mnesysgeo_lieux.kml`, un dossier par thésaurus
(Quartiers, Voies, Sites), un style de couleur par dossier, puis le dossier
des zones de clic des voies.

| Élément du repère | Contenu |
|---|---|
| `name` | Libellé du lieu, tel qu'écrit dans les IR : « Rue Henri Carrette » (jusqu'au 2026-11-08 suivi du nombre d'occurrences entre parenthèses) |
| géométrie | D'après `@coord` (WKT, depuis le 2026-11-05) : point ; tracé (`LineString`) ; tracé multiple (`MultiGeometry`) ; contour refermé (`Polygon`) pour les quartiers |
| `description` | Nombre d'occurrences (dans les 37 IR de test), puis le lien « Voir sur la BN-R » |
| `ExtendedData` | `libelle`, `occurrences`, `lien` (jusqu'au 2026-11-08 aussi `thesaurus`, `ir` et, pour les quartiers, `couleur`) |

**Lien BN-R (2026-11-08).** La BN-R indexe les IR **d'origine**
(`data/ead/bnr`), pas les IR réécrits : le lien cherche donc les formes
d'origine du lieu, prises dans la concordance (toutes les lignes alignées sur
ce lieu). Recherche **par texte seulement** : chaque forme, entre guillemets,
dans l'index de la recherche avancée qui correspond à son `@source`
(`INDEX_BNR`) :

| `@source` d'origine | Index BN-R |
|---|---|
| `rue` | `rue` |
| `quartier` | `quartier` |
| `adresse` | `adresse` |
| (aucun) | `sujet_geo` (seul index qui trouve ces formes : « André Diligent, avenue » 0 résultat en `rue`, 7 en `sujet_geo`) |

Les critères sont reliés par `OR` (`bool[]=OR`, vérifié : « Mascara, rue de »
en `rue` OU « Hutin » en `quartier` = 2 + 1 = 3 résultats) : **un seul lien
par lieu**, quel que soit son nombre de formes. `decouvrir_par_terme` = libellé
du lieu. Les 549 lieux ont un lien. Les exemples de l'utilisateur
cherchaient les rues et les sites par coordonnées (`coord_rue`,
`coord_adresse`) ; écarté, car 135 rues n'ont aucune forme d'origine avec
`@coord`.

Contrôle sur le site (14 lieux) : tous les liens donnent des résultats. Le
nombre de résultats de la BN-R (documents, sur tous ses IR) dépasse souvent le
nombre d'occurrences du KML (37 IR de test) : Avenue Julien Lagache 52 → 330.
La recherche par texte est moins stricte que par coordonnées : « 17 Grand'Place »
(Hôtel de ville, 2 occurrences) donne 46 résultats en `adresse`, contre 1 par
`coord_adresse`.

**Couleurs (palette de la BnR, 2026-11-06)** : noir `#000000`, blanc `#FFFFFF`,
turquoise `#2FAAAF`, jaune `#E9B400`, violet `#9460A4`, gris `#ADA796`. Voies et
sites en noir ; quartiers dans les 4 couleurs hors noir et
blanc (turquoise, jaune, violet, gris, remplissage à 50 %).

**Couleurs des quartiers :** deux quartiers qui se touchent n'ont jamais la même couleur
(coloriage glouton DSATUR du graphe de voisinage, contours en contact à 1 m près).
**4 couleurs sont nécessaires et suffisent** : Alma Gare, Armentières,
Fresnoy-Mackellerie et Nations Unies se touchent deux à deux (vérifié par
recherche exhaustive le 2026-11-06). Chaque quartier porte son style dans le KML ;
dans QGIS, choisir la symbologie « Symboles intégrés » pour le voir, ou catégoriser
sur le champ `couleur`.

**Tampon invisible des voies (2026-11-06).** Une ligne fine est difficile à
viser, et les squares passés dans les voies n'ont qu'une ligne de 2 m. Un
dernier dossier, **« Voies : zones de clic »**, contient pour chaque voie un
repère de même nom et de même description dont la géométrie est un tampon de
30 m (`TAMPON_M` ; 15 m au départ, porté à 30 m le 2026-11-06 ; calculé en Lambert 93, simplifié à 1 m, arrondi à
6 décimales), rendu entièrement transparent par le style `rues_clic`
(`PolyStyle` `00ffffff`, `fill` 1, `outline` 0). Un clic jusqu'à 30 m de la
ligne ouvre donc la fiche de la voie ; le dossier est replié dans la liste
(`checkHideChildren`). Les voies elles-mêmes restent de simples lignes.
KML : 1,35 → 2,2 Mo (descriptions en double ; 2,23 Mo avec le tampon de 30 m).

Un premier essai mettait le tampon dans la `MultiGeometry` de la voie : QGIS
lisait alors la couche « Voies » comme des `GeometryCollection`, qu'il
n'affiche pas ; d'où le dossier à part. L'invisibilité dépend du
visualiseur : Google Earth applique le style KML, un outil qui impose son
propre style (uMap, Google My Maps…) peut afficher les tampons. Non testé
dans un visualiseur.

## Fichier uMap (2026-11-08)

uMap ne respecte pas les couleurs du KML : il le convertit avec togeojson et
n'applique aucun style KML (tout dans la couleur par défaut, et les tampons des
zones de clic deviennent visibles). Le script écrit donc aussi
`results/ead/indexation/bnr2mnesysgeo_lieux.umap` (option `--sortie-umap`),
au **format natif de uMap** :

| Calque | Options uMap |
|---|---|
| Quartiers (31) | contour 2, remplissage 50 % ; couleur de chaque quartier dans ses `_umap_options` (`color`, `fillColor`), même coloriage que le KML |
| Voies (516) | noir, épaisseur 3 |
| Sites (2) | noir, icône `Circle` |

Pas de zones de clic. Chaque objet ne porte que `name` (libellé) et
`description` : nombre d'occurrences, puis le lien BN-R dans la syntaxe de
uMap (`[[url|Voir sur la BN-R]]`, crochets de l'URL encodés `%5B%5D`). Territoires exclus, comme dans le KML. 0,6 Mo.

Options des calques dans la clé `_umap_options` du calque : un premier essai
les mettait dans `properties` (nouveau format de la version de uMap en
développement) ; umap.openstreetmap.fr les ignorait alors (voies et sites en
bleu, marqueurs par défaut ; seuls les quartiers, stylés objet par objet,
étaient bons). `_umap_options` est lu par les deux versions.

Dans uMap : importer le fichier avec le format « umap ». L'import du format
umap remplace aussi le nom et le centre de la carte (Roubaix, zoom 14) : il
faut l'importer dans une carte neuve. Format lu dans les sources de uMap
(`importRaw`, `createDataLayer`, `Utils.toHTML`). Vérifié le 2026-11-08 par
import d'un extrait (un objet par calque, sans enregistrer la carte) sur
umap.openstreetmap.fr et umap.incubateur.anct.gouv.fr : calques nommés
« Quartiers », « Voies », « Sites », voies et sites noirs, sites en ronds.
Avec le premier essai (options dans `properties`), l'instance de l'ANCT
nommait les calques « Calque 1 » à « Calque 3 », en couleurs par défaut.

## Projet QGIS

`results/ead/indexation/bnr2mnesysgeo_lieux.qgz` (fonds IGN, OSM, cadastre,
état-major, photos aériennes) ouvre le KML par **numéro de couche**
(`bnr2mnesysgeo_lieux.kml|layerid=N` : 0 Quartiers, 1 Voies, 2 Sites ;
la couche 3, zones de clic, n'y est pas chargée). Réparé le
2026-11-06 : il désignait les couches par leur nom de dossier, qui contient
le nombre de lieux (« Rues (514) », « Points (4) »…) et change à chaque
régénération. Backup `bnr2mnesysgeo_lieux.backup_20261106_avant_reparation.qgz`.
Contrôle PyQGIS : les 4 couches valides (Polygon 31, MultiLineString 516,
Point 2, Point 184), rendu sans erreur.

**2026-11-08 : couche « Territoires » retirée du projet**, après le retrait
des territoires du KML (son `layerid=3` désignait désormais le dossier des
zones de clic). Retrait fait dans le `.qgs` (arbre des couches, ordre,
accrochage, `maplayer`), sans réécriture par QGIS, pour garder les chemins
relatifs. Backup `bnr2mnesysgeo_lieux.backup_20261108_avant_territoires.qgz`.
Contrôle PyQGIS : 3 couches KML valides (Polygon 31, MultiLineString 516,
Point 2), rendu sans erreur.

Les `<geogname>` sans `@coord` (rue du référentiel sans géométrie, forme
« sans lieu » restée en `@source="rue"`) ne peuvent pas être placés : ils
sont seulement comptés à la fin de l'exécution.

## Utilisation

```bash
conda run -n rbx-bnr-data python scripts/ead/geogname_ir2kml.py
```

Options : `--ir-dir` (dossier des IR, défaut `results/ead/ead_cor/bnr2mnesysgeo`),
`--sortie` (fichier KML).

À relancer après chaque application de la concordance aux IR.

## État au 2026-11-08

37 IR : 549 lieux placés (31 quartiers, 516 voies, 2 sites) pour 10 965
occurrences ; non placés : 21 `<geogname>` de voies sans géométrie, 18 formes
sans lieu ; 1 206 occurrences de territoires laissées hors du KML. KML 2,05 Mo (fiches réduites, lien BN-R), uMap 0,6 Mo ; 549 liens BN-R.

## État au 2026-10-30

37 IR : 732 lieux placés (31 quartiers, 514 rues, 4 points, 183 communes et
pays) pour 12 171 occurrences ; non placés : 21 `<geogname>` de rues sans
géométrie, 18 formes sans lieu.
