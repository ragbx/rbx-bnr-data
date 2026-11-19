# carte_leaflet.py

[scripts/ead/carte_leaflet.py](../../../scripts/ead/carte_leaflet.py)

**Prototype** (14/11/2026) de la carte « public » en page HTML Leaflet,
alternative à la carte uMap produite par [geogname_ir2kml.py](geogname_ir2kml.md).
Pour la comparaison uMap / Leaflet : une page Leaflet donne la maîtrise de la
présentation (charte BnR), des rues cliquables sans zones fantômes, une
recherche et des liens directs, et ne dépend pas d'une instance uMap ; en
contrepartie, il faut l'héberger et la maintenir.

```bash
python scripts/ead/geogname_ir2kml.py   # d'abord, si les IR ont changé
python scripts/ead/carte_leaflet.py
```

## Données

- `results/ead/indexation/bnr2mnesysgeo_lieux.umap` (carte uMap « public ») :
  quartiers et leur couleur, voies et sites cités dans les 37 IR de test,
  nombre d'occurrences, lien vers la recherche de la BN-R ;
- `data/geo/referentiel_lieux_roubaix.csv` : anciens noms (`nom_ancien`),
  catégorie des sites ;
- `rues_rattachement.csv` / `rues_quartiers_13_repartition.csv` : quartier(s)
  de chaque voie (fonction `quartiers_des_rues` de [quartiers2kml.py](quartiers2kml.md)).

Au 14/11/2026 : 31 quartiers, 516 voies (24 avec ancien nom, toutes avec un
quartier), 2 sites ; les 549 lieux se retrouvent tous dans le référentiel.

## Archives municipales (2026-11-19)

Les notices des inventaires Mnesys des Archives municipales sont ajoutées aux fiches. Elles viennent de
`results/ead/indexation/archives_carte_notices.csv`, produit par
[archives_carte_notices.py](../../../scripts/ead/archives_carte_notices.py) à lancer avant la carte.

```bash
python scripts/ead/archives_carte_notices.py
```

- **Formes retenues** (choix de l'utilisateur) : celles alignées sur le référentiel par le texte, les décisions
  validées (table `data/geo/concordance_mnesys_lieux.csv` et propositions automatiques non refusées) et la
  concordance BnR. Les formes « à traiter », les « non » de la table et les communes et pays sont écartés.
  Lecture seule des IR d'origine `data/ead/mnesys/`.
- **Rattachement** : par `lieu_id`. Une rue ou un site cité seulement par les archives est ajouté à la carte avec
  la géométrie du référentiel ; un lieu sans géométrie est signalé et reste absent.
- **Présentation** : groupes « Archives municipales · <fonds> · <titre de l'IR> » (unittitle de l'archdesc, sinon
  titleproper ; première phrase seulement). Chaque notice montre titre, cote et date, **sans lien** (pas d'ARK).
  Les inventaires des archives figurent dans le filtre par instrument de recherche.

Au 2026-11-19 : 7 108 notices de 23 IR, 12 028 lignes lieu × notice, 682 lieux dont 665 sur la carte
(17 rues sans tracé au référentiel : Rue d'Ath, Rue Centrale, Rue Fulton…). La carte passe à 637 voies
(+121, citées seulement par les archives) et 117 sites (+6). Vérifié dans le navigateur intégré : fiche du
quartier Épeule (100 documents, 16 inventaires), fiche d'une rue nouvelle (Allée des Lilas, 384W), filtre
« 3Fi » (340 rues, 17 sites, 17 quartiers).

### Sites validés : notices des Archives (2026-11-19)

`archives_carte_notices.py` reprend aussi le repérage des sites dans les IR Mnesys
(`data/geo/propositions_sites_mnesys_<date>_notices.csv`, produit par `sites_candidats.py`). Une notice dont un
**site validé de la carte** est le sujet (lien « sujet » ou « lieu », comme pour la BnR ; jamais une simple
mention) est rattachée à ce site. Le rattachement se fait par la clé du site, colonne `site_cles` de
`sites_carte.csv`, et par les noms de `sites_fusions.csv`. Ces lignes ont l'origine « site » ; celles tirées des
geogname ont l'origine « geogname ».

Premier passage : **344 notices pour 55 sites**, venues de 88 inventaires des Archives. Exemples : Hôpital de la
Fraternité 44 → 77 (série M), La Piscine 54 → 89, Condition Publique 184 → 219, Conservatoire 114 → 136,
Hôtel de ville 553 → 576.

## Fusion des sites (2026-11-19)

Les sites sont placés dans cet ordre : sites validés (`sites_carte.py`), sites de l'uMap, sites cités seulement
par les archives. Un lieu de **même nom, ou à 15 m au plus** d'un site déjà placé, le rejoint : ses notices
sont fusionnées, sans doublon (clé : titre, date, ARK, IR). Il ne donne plus de second point.

Avant, deux choses posaient problème :
- les notices d'un site uMap identique à un site validé étaient perdues (le site uMap était sauté) ;
- les archives créaient des doublons, quand l'identifiant du site validé (`sp:…`, `bdtopo:…`) n'était pas celui
  du référentiel.

Effet au 2026-11-19 : 117 → 112 sites. Cinq doublons ont été supprimés (Parc de la Potennerie, Cimetière de
Roubaix, Hospice Barbieux, Hospice de Blanchemaille, Église Sainte Elisabeth). Le Parc Barbieux gagne
172 notices BnR de son site uMap, l'Hôtel de ville 1.

Le même jour, `bnr2mnesysgeo` et l'uMap ont été régénérés en entier : *Parc de la Potennerie* passe des voies
aux sites (parcs = points depuis le 14/11 ; 6 geogname, MED_AFF et OBS_JOU). La carte compte 636 voies.

## Noms d'un même site (2026-11-19)

`data/geo/sites_fusions.csv` est tenu à la main et lu par `sites_carte.py`. Colonnes : `nom`, `site_cle_cible`,
`remarque`. Il sert à regrouper sur un site les notices qui le désignent sous un autre nom :
- une notice dont le titre est l'un de ces noms (casse, accents et ponctuation ignorés) rejoint le site cible,
  comme sujet ;
- les liens retenus (sujet, lieu) de la clé de ce nom, dans les propositions de sites, aussi.

Première entrée : *Hôtel-Dieu*, *Hôpital Napoléon* et *Hôpital Napoléon de Charles Iguel* sont un seul bâtiment
avec l'Hospice de Blanchemaille. Résultat : +12 notices « Hôtel-Dieu » (11 MED_CP, 1 MED_PAR). Le site passe à
16 documents. Les liens d'Hôpital Napoléon ne sont que des mentions et ne sont pas retenus.
Les deux noms sont aussi des **variantes** du lieu `bnr:hospice blanchemaille` dans le référentiel. Ils sont
saisis dans `lieux_ajoutes.csv` : `noms_anciens.csv` est appliqué avant les lieux ajoutés et ne peut pas les
porter. Un geogname « Hôtel-Dieu » ou « Hôpital Napoléon » s'aligne donc désormais sur l'hospice.

Le même jour, la forme BnR *l'ENAI/ENSAIT* (MED_MON) est alignée sur le site ENSAIT : ses geogname ont été
ajoutés à l'extraction figée `rbx-bnr_geogname_origine.csv`, et elle est validée dans
`concordance_geogname_lieux.csv`.

## Sortie

`results/ead/indexation/carte_bnr/index.html` : **un seul fichier**, données
incluses (0,55 Mo ; coordonnées arrondies à 5 décimales, environ 1 m).
Dépendances en ligne : Leaflet 1.9.4 (cdnjs), fonds de carte.

## Fonctions

| Fonction | Détail |
|---|---|
| Fonds | Géoplateforme de l'IGN (`data.geopf.fr/wmts`, sans clé) : Plan IGN (par défaut), photographies aériennes actuelles, **photographies aériennes 1950-1965** (`ORTHOIMAGERY.ORTHOPHOTOS.1950-1965`, png, jusqu'au niveau 18), **carte de l'état-major 1820-1866** (`GEOGRAPHICALGRIDSYSTEMS.ETATMAJOR40`, jpeg, jusqu'au niveau 15, agrandie au-delà) ; **photographies aériennes 1965-1980** (`ORTHOIMAGERY.ORTHOPHOTOS.1965-1980`, style `BDORTHOHISTORIQUE` obligatoire — `normal` renvoie une erreur 400 ; chargement lent) ; OpenStreetMap en secours. Fonds historiques ajoutés le 14/11/2026. Écartées : photographies 1980-1995 (pas de tuiles sur Roubaix, 404 aux niveaux 12 à 16). Ajoutées aussi le 14/11/2026 : **carte de 1950 au 1/50 000** (`GEOGRAPHICALGRIDSYSTEMS.MAPS.SCAN50.1950`, jpeg, jusqu'au niveau 15) et **carte de Cassini**, exemplaire du roi (`AN-IGNF_GEOGRAPHICALGRIDSYSTEMS.CASSINI`, jpeg, jusqu'au niveau 14 ; l'exemplaire de la BnF, `BNF-IGNF_…`, existe aussi). Ordre dans la boîte : Plan IGN, photographies actuelles, 1965-1980, 1950-1965, carte de 1950, état-major, Cassini, OpenStreetMap |
| Quartiers | couleurs de la carte uMap (4 couleurs de la palette, voisins différents), remplissage 40 % ; fiche : nombre de voies citées, mentions, lien BN-R |
| Rues | noir ; **cliquables à 8 px près** (rendu canvas, `tolerance: 8`), sans zones fantômes ; survol et rue ouverte en turquoise épais ; fiche : ancien(s) nom(s), quartier(s) cliquables, mentions, bouton « Voir les documents sur la BN-R » |
| Sites | ronds rouges `#C03730` |
| Recherche | nom actuel ou ancien, sans tenir compte des accents ; « rue de la Gare » propose « Avenue Jean Lebas » ; clavier (flèches, Entrée, Échap) |
| Quartier | liste ou clic sur un quartier d'une fiche : zoom, ses voies en avant, le reste estompé |
| Lien direct | `index.html#voie=grande_rue`, `#quartier=epeule` (mis à jour à l'ouverture d'une fiche) |
| Panneau | bandeau aux 4 couleurs de la palette ; chiffres (rues, quartiers, sites) ; recherche avec message « Aucun lieu trouvé » ; bouton « Toute la ville » quand un quartier est choisi ; pied « Sources : BN-R · fond IGN » ; repliable partout (▴) |
| Boîte des fonds et couches | en français (« Fond de carte et couches »), intitulés « Fond de carte » / « Couches », petite légende devant chaque couche, style du panneau |
| Iframe | lien « Ouvrir en plein écran ↗ » (adresse courante, avec le lien direct) affiché seulement quand la page est dans un iframe ; page d'essai `carte_bnr/test_iframe.html` (non publiée) |
| Téléphone | panneau replié après une recherche ou un choix ; boutons de zoom et boîte des couches en bas |

« Mentions dans les inventaires » = nombre d'occurrences dans les 37 IR de
test (la BN-R en indexe 52) : à revoir avant une mise en ligne.

## Vérifié (14/11/2026, navigateur intégré, serveur local)

Aucune erreur dans la console ; fonds IGN chargés ; recherche « rue de la
gare » → Avenue Jean Lebas ; lien `#voie=grande_rue` (Grande Rue en
surbrillance, fiche ouverte) ; clic sur « Centre-Ville » dans la fiche ;
clic à 6 px de la Grande Rue → sa fiche ; affichage téléphone (375 × 812) ;
panneau et boîte des couches revus, et intégration en iframe (même jour).
Non vérifié : les liens vers la BN-R depuis la page, l'accessibilité au
lecteur d'écran.

## Titre et liens (2026-11-14)

Titre « **Bn-r : accès par plan** » (onglet et panneau), choisi par
l'utilisateur et d'abord saisi directement dans `../carte-bnr/index.html`
(commit « MaJ titre ») : reporté dans le script le même jour, pour ne pas être
perdu à la regénération. Liens BN-R au format de [geogname_ir2kml](geogname_ir2kml.md)
(filtre `fq`, plein texte pour les sites et le repli) ; un lieu sans lien
affiche « Pas encore de document en ligne sur la BN-R. » (23 lieux). **Ne pas
modifier `index.html` à la main dans `carte-bnr` : passer par ce script.**

## Publication (GitHub Pages)

Décision du 14/11/2026 : dépôt dédié **public `medrbx/carte-bnr`** (compte
GitHub de la médiathèque), séparé de ce dépôt de travail. Dossier local
préparé à côté de `rbx-bnr-data` : `../carte-bnr/` (`index.html`,
`README.md`, `.nojekyll`). Adresse prévue : `https://medrbx.github.io/carte-bnr/`.

Premier envoi (après création du dépôt vide sur GitHub) :

```bash
cd ../carte-bnr
git init -b main && git add . && git commit -m "Carte des lieux de Roubaix (prototype)"
git remote add origin git@github.com:medrbx/carte-bnr.git && git push -u origin main
```

puis Settings → Pages → « Deploy from a branch », `main`, `/ (root)`.
Mise à jour : relancer le script, copier `results/ead/indexation/carte_bnr/index.html`
dans `../carte-bnr/`, commit et push. `test_iframe.html` n'est pas publié.

## Prévisualiser

La page peut s'ouvrir directement dans un navigateur (double-clic). Dans
Claude Code, `.claude/launch.json` (configuration `carte-bnr`) la sert sur
`http://localhost:8765` (`python -m http.server` sur `carte_bnr/`).

## Lieux disparus (2026-11-19)

Les lieux de `data/geo/lieux_disparus.csv` sont distingués sur la carte : **voies disparues en pointillés**,
**sites disparus en point évidé** (cercle blanc cerclé de rouge), avec la mention « Voie disparue » / « Site
disparu » et la période (`date_debut – date_fin`, « ? » pour une date inconnue) dans la fiche ; légende du
panneau et du choix des couches. Rattachement par `lieu_id` (voies et sites de l'uMap via le référentiel,
sites validés via `point_id`, un `sp:<clé>` valant `bnr:<clé>`). Les 41 sites « à classer » de
`lieux_ajoutes.csv` restent affichés comme les autres. Au 2026-11-19 : 10 voies, 6 sites.
Dans le choix des couches, les lieux disparus sont des **couches à part** (« Rues disparues », « Sites
disparus »), cochables indépendamment des rues et sites actuels : décocher Rues et Sites ne montre que les
disparus. Un lieu ouvert par la recherche ou un lien (#site=, #voie=) réaffiche sa couche si elle est masquée.
