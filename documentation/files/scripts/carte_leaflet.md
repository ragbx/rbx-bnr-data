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
