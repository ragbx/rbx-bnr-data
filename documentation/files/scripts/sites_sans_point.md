# sites_sans_point.py

[scripts/ead/sites_sans_point.py](../../../scripts/ead/sites_sans_point.py)

Suite de la méthode « sites » ([sites_candidats.py](sites_candidats.md)) :
propositions de localisation pour les **sites candidats que le référentiel ne
connaît pas** (proposition « sans point »), à valider. 14/11/2026. Lecture
seule des référentiels et des IR.

```bash
python scripts/ead/sites_sans_point.py                 # extrait OSM gardé s'il existe
python scripts/ead/sites_sans_point.py --extraire-osm  # refaire l'extrait OSM
```

## Entrées

- `data/geo/propositions_sites_20261112.csv` et `_notices.csv` : 698 sites
  « sans point », dont **306 avec au moins un lien retenu** (sujet ou lieu,
  940 liens) — seuls traités ;
- `data/geo/referentiel_lieux_roubaix.csv` / `_ecartes.csv` : rues et points ;
- `data/geo/osm_roubaix_lieux.json` : **lieux nommés d'OpenStreetMap** dans
  Roubaix (Overpass, zone `ref:INSEE=59512`, objets nommés hors voies,
  itinéraires et arrêts ; 2 237 lieux le 2026-11-14).

## Méthode

1. **Groupes** : sites de même famille aux mots distinctifs égaux, ou inclus
   si le plus court en garde au moins deux (« usine Motte » n'absorbe pas
   Motte-Porisse ni Rossel-Motte). Groupe par inclusion : alerte.
2. **Candidats**, du plus sûr au moins sûr :
   - rue du référentiel au même nom (squares : « Square Catteau » → Square
     Pierre Catteau, les squares sont des rues) ;
   - point du référentiel ou écarté, et lieu OSM, dont le nom contient les
     mots distinctifs du site et **compatible** (`compatible`) : ni voie, ni
     quartier, ni commerce OSM ; même famille que le site (une église pour une
     église), ou, famille inconnue, au moins deux mots distinctifs ; site sans
     nom propre (« Médiathèque de Roubaix ») : lieu OSM de nom identique ;
   - adresse « n° + voie » des notes géocodée par la BAN (sites_candidats.py) ;
   - **milieu de la rue la plus indexée avec le site** dans ses notices (poids
     2 pour un lien retenu, 1 sinon) : **approximatif**, simple indice.
3. **Mêmes sites sous deux noms** (`MEMES_SITES`, décisions de l'utilisateur du
   2026-11-14) : Médiathèque La Grand'Plage = Médiathèque de Roubaix ; Château
   du Congo (et des Princes du Congo) = Château Vaissier. Un groupe est un seul
   site : chaque membre reprend la meilleure proposition du groupe (« — reprise
   du groupe »), sa propre piste passant en `autres_candidats`.
4. Proposition = premier candidat (à type égal, le plus proche de la rue
   co-indexée). Alertes : groupe par inclusion, proposition à plus de 500 m de
   la rue co-indexée, plusieurs points ou lieux OSM possibles.

## Sorties (data/geo/)

- `propositions_sites_sans_point_<date>.csv` : une ligne par site (groupes
  triés par liens retenus) : groupe, site, famille, forme, notices, liens
  retenus, variantes, rue co-indexée et poids, proposition, libellé et
  identifiant proposés, `coord_wkt` (WGS84), autres candidats, alertes,
  dates des notes, exemple ; colonnes **manuelles conservées** (clé
  `site_cle`) : `decision`, `lieu_manuel`, `coord_manuel`, `remarque` ;
- `propositions_sites_sans_point_<date>.gpkg` : couches `propositions`
  (point proposé, mêmes colonnes) et `osm_candidats` (lieux OSM rapprochés
  d'un site), écrites sans GDAL.

## Validation dans QGIS (2026-11-14)

```bash
"C:/Program Files/QGIS 3.40.6/bin/python-qgis-ltr.bat" scripts/ead/qgis_validation_sites_sans_point.py
python scripts/ead/sites_sans_point.py --reporter        # après la saisie
```

[qgis_validation_sites_sans_point.py](../../../scripts/ead/qgis_validation_sites_sans_point.py)
(Python de QGIS) part du projet du 12/11 (`validation_sites_20261112.qgz` :
fonds Plan IGN, photographies actuelles et 1950-1965, état-major, cadastre,
OSM ; couches du référentiel) et écrit dans `data/geo/` :

- `validation_sites_sans_point_<date>.gpkg` : `sites_a_valider` (306 points :
  `coord_manuel`, sinon la proposition ; les **145 sites sans piste** sont posés
  en grille sur une **aire d'attente** au sud-ouest de la ville, `a_placer =
  oui`, à glisser à leur place ; `coord_origine` = position d'origine) et
  `osm_candidats` (229 lieux OSM) ;
- `validation_sites_sans_point_<date>.qgz` (chemins relatifs) : couche
  « **Sites à valider** », couleur selon `piste` (turquoise solide, jaune
  approximative, rouge à placer, violet décidé), étiquette = forme ; formulaire
  du 12/11 : **`decision`** (liste : oui / non / autre point / à revoir),
  **`lieu_manuel`**, **`remarque`** modifiables, le reste en lecture seule ;
  trois exemples de notices avec lien **ARK** cliquable (209 sites en ont,
  liens tirés des IR actuels `bnr2mnesys`) ; couche « Lieux OSM candidats ».

Saisie : remplir `decision` (et `lieu_manuel`, `remarque`) ; **déplacer le
point** pour corriger la position (outil de déplacement, couche en édition).
`--reporter` relit le GeoPackage et écrit dans le CSV `decision`,
`lieu_manuel`, `remarque` et, si le point a bougé de plus d'environ 1 m par
rapport à `coord_origine`, `coord_manuel` (sauvegarde
`*.backup_<date>_<heure>_avant_report.csv`). Testé sur copies le 2026-11-14.
Ne pas relancer `qgis_validation_sites_sans_point.py` pendant la saisie : il
réécrit le GeoPackage (les saisies déjà reportées dans le CSV sont reprises).

## Bilan (2026-11-14)

306 sites, 281 groupes (après `MEMES_SITES`) :

| Proposition | Sites | Liens retenus |
|---|---|---|
| OSM | 20 | 156 |
| adresse des notes | 25 | 97 |
| aucune piste | 145 | 201 |
| milieu de la rue co-indexée | 106 | 447 |
| point référentiel | 1 | 2 |
| rue du référentiel | 9 | 37 |

Sur les 100 sites les plus cités, 67 ont une piste. Exemples : Église
Sainte-Élisabeth → OSM « Église Sainte-Elisabeth de Hongrie » ; usine
Motte-Bossut → 78 boulevard du Général Leclerc ; Hospice Barbieux → 35 rue de
Barbieux ; Médiathèque de Roubaix → OSM ; Château Vaissier → Rue de Mouvaux
(approximatif). La Médiathèque La Grand'Plage reprend le point OSM de la Médiathèque de
Roubaix, le Château du Congo celui du Château Vaissier. Sans piste : Samirami,
Stade Roubaisien…

**Château Vaissier (2026-11-14)** : point donné par l'utilisateur,
`POINT (3.144005 50.695785)`, à **Tourcoing**, saisi en `coord_manuel` et
`decision = oui` pour ses trois noms. **Exception décidée par l'utilisateur** :
bien que hors de Roubaix, il sera un point géolocalisé du référentiel (entré
comme lieu créé à géométrie POINT : `retirer_hors_roubaix`, qui ne traite
que les rues, ne le concerne pas ; pas de quartier de rattachement).

Les sites disparus (châteaux, usines, brasseries) n'ont souvent qu'une piste
approximative : à placer à la main, par exemple sur les plans anciens de la
BN-R ([cartes_anciennes](cartes_anciennes.md)) ou les fonds anciens de l'IGN.
