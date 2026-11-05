# Script : geogname_concordance_lieux.py

**Emplacement :** `scripts/ead/geogname_concordance_lieux.py`
(nommé `geogname_referentiel.py` jusqu'au 2026-10-27)

Propose la **concordance entre les formes des IR et les lieux** : une ligne
par forme BnR (libellé + `@source` d'origine), rattachée quand c'est possible
à un objet de référence actuel — voie, espace public, quartier ou commune —
avec un identifiant pérenne et la géométrie à utiliser.

**Vocabulaire** (depuis le 2026-10-27) : le **référentiel** est la **table
des lieux** (une ligne par lieu, futur concept des thésaurus géographiques ;
`data/geo/referentiel_geogname.csv`, construit par
[geogname_referentiel_lieux.py](geogname_referentiel_lieux.md)) ; la **concordance**
produite ici relie ce que l'on trouve effectivement dans les IR à ces lieux.
C'est un outil pour nourrir le référentiel, entre autres. Chaîne : formes des
IR → concordance → référentiel des lieux → thésaurus SKOS → réécriture des IR
(cf. [indexation géographique](indexation_geographique.md)).

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/indexation/rbx-bnr_geogname_origine.csv` | Formes BnR d'avant corrections (hors `<geogname>` vides et hors `source="chrono"`) |
| `data/geo/topo_roubaix.csv` | Voies TOPO (DGFiP) : identifiant `59512_<code voie>`, date de création — cf. [geogname_concordance](geogname_concordance.md) |
| `data/geo/concordance_renommages.csv` | Anciens noms et variantes → voie actuelle — cf. [geogname_concordance](geogname_concordance.md) |
| `CORRECTIONS_RUE` de [geogname2geo.py](geogname2geo.md) | Correspondances validées à la main |
| `data/geo/bdtopo_roubaix_voies.geojson` | BD TOPO `voie_nommee` de Roubaix (917 objets) |
| `data/geo/bdtopo_roubaix_zones.geojson` | BD TOPO espaces publics (`zone_d_activite_ou_d_interet`, nature « Espace public ») et `zone_d_habitation` de Roubaix |
| `data/geo/bdtopo_nord_communes.csv` | Communes du Nord (INSEE, nom) |
| `data/geo/filaire_voies_roubaix.geojson` | Filaire MEL, en secours (voie ou géométrie absente de TOPO/BD TOPO) |
| `data/geo/quartiers_historiques_mel.geojson` | **Quartiers historiques de Roubaix** (31), open data MEL : couche WFS `ville_roubaix:quartiers_historiques` de data.lillemetropole.fr, récupérée le 2026-10-27 (nom, identifiant, polygone) |
| `results/ead/ead_cor/bnr2mnesys/*.xml` | Années des documents (`unitdate`) |

Les trois extraits BD TOPO (IGN, édition 2026-06-15, reprojetés du Lambert 93
en WGS84 avec `pyproj`) sont produits une fois par
`--bdtopo <fichier .gpkg du Nord>` puis réutilisés : le GeoPackage (4,4 Go)
n'a pas à rester disponible.

## Rattachement

Pour chaque forme, dans l'ordre :

1. **décision manuelle** (`DECISIONS` dans le script : parc Barbieux → espace
   public, `source="adresse"`, point fourni à la main ; `Pas de mention
   d'adresse` → non toponyme) ;
2. forme indexée **comme quartier** (`source="quartier"`) → **quartier
   historique MEL** : par nom normalisé, sinon à l'unique quartier dont le
   nom contient tous les mots de la forme (`Centre` → CENTRE VILLE, `Hutin`
   → HUTIN - ORAN - CARTIGNY), avec une alerte ; forme indexée **comme
   adresse** → adresse (point), sans autre rattachement ;
3. libellé de type parc / square / jardin → **espace public** BD TOPO ;
4. **concordance** : ancien nom → voie actuelle, sauf si une voie homonyme
   existait déjà en 1987 (on la garde, avec une alerte) ; variante → voie
   actuelle, seulement sans voie homonyme ;
5. `CORRECTIONS_RUE` ;
6. **voie** par nom normalisé (sans accents, casse, ponctuation ni
   articles) dans TOPO et la BD TOPO — objets BD TOPO retrouvés aussi par
   l'identifiant BAN —, puis filaire MEL ;
7. **espace public** BD TOPO ;
8. **commune** de l'extrait BD TOPO — Nord et départements voisins : Aisne,
   Pas-de-Calais, Somme — (libellé seul ou qualifié « (Nord) ») ; **pays**
   (courte liste) ; **lieu hors Nord** (qualificatif entre parenthèses) ;
9. **quartier** historique MEL, par nom normalisé seulement (forme sans
   `@source`) ;
10. sinon : voie non rattachée, quartier absent des quartiers MEL, ou « à
    identifier ».

Une forme indexée comme quartier mais rattachée ailleurs (`Gare, place de
la` → voie) reçoit l'alerte « indexé comme quartier dans les IR ». Jusqu'au
2026-10-27, les quartiers étaient pris dans la BD TOPO (`zone_d_habitation`),
qui mêle quartiers et résidences : 7 formes n'y étaient pas trouvées et
Armentières, Chemin Neuf étaient pris pour une commune et une voie.

## Sortie

`data/geo/concordance_geogname_lieux.csv` (nommé `referentiel_geogname.csv` jusqu'au 2026-10-27) — une ligne par forme :

| Colonne | Contenu |
|---|---|
| `a_traiter` | **Ce qui reste à régler à la main** sur la ligne (motifs séparés par ` ; `) ; vide quand il n'y a rien à faire ou dès que `validation` est remplie — cf. « Relecture » ci-dessous |
| `forme_bnr`, `source_bnr` | Libellé et `@source` d'origine |
| `occurrences`, `ir` | Nombre de `<geogname>`, IR concernés |
| `annee_min`, `annee_max` | Années des documents indexés |
| `coord_origine` | `point` / `polygone` si un `coord` existait à l'origine |
| `categorie` | voie, espace public, quartier, commune, lieu hors Nord, pays, non toponyme, à identifier |
| `source_cible` | `@source` proposée (`rue`, `adresse`, `quartier`…) |
| `libelle_retenu` | Libellé à écrire, cf. « Libellé retenu » ci-dessous |
| `nom_ancien` | Pour un renommage : l'ancien nom (forme BnR remise à l'endroit) |
| `ref_nom`, `ref_id` | Objet de référence : nom et identifiant (`59512_<code>` pour une voie, `cleabs` BD TOPO pour un espace public ou un quartier, `INSEE <code>` pour une commune, id MEL en secours) |
| `ref_origine` | TOPO, BD TOPO (table), filaire MEL |
| `geometrie` | Provenance de la géométrie : objets BD TOPO, filaire MEL, point manuel, ou `point d'origine` / `polygone d'origine` à défaut |
| `coord` | **Coordonnées calculées** à partir de `geometrie`, au format des `@coord` des IR (cf. « Coordonnées » ci-dessous) |
| `coord_manuel` | **À remplir à la main**, conservée d'une exécution à l'autre ; **prioritaire** sur `coord` |
| `methode` | Comment le rattachement a été fait |
| `alerte` | Cas à trancher : renommage, voie actuelle plus récente que les documents, absente des sources actuelles, espace public… |
| `validation`, `remarque` | **À remplir à la main** — conservées d'une exécution à l'autre |

### Coordonnées (`coord`, depuis le 2026-10-27)

`coord` est calculée par `coordonnees()` à partir de `geometrie`, en
`[lat, lon]` (l'inverse du GeoJSON), 7 décimales, en JSON :

| Cas | Source | `coord` |
|---|---|---|
| voie BD TOPO ou filaire MEL | `bdtopo_roubaix_voies.geojson`, `filaire_voies_roubaix.geojson` | **tracé** : `[[lat, lon], …]` (un tronçon) ou `[[[lat, lon], …], …]` (plusieurs) ; objets en double dédoublonnés, tronçons contigus fusionnés (`linemerge`) |
| quartier MEL | `quartiers_historiques_mel.geojson` | **contour** du polygone `[[lat, lon], …]` |
| espace public BD TOPO | même fichier | **point central** `[lat, lon]` (centroïde, ou point intérieur s'il tombe hors du polygone) |
| point manuel (parc Barbieux) | `DECISIONS` | ce point |
| point ou polygone d'origine | `@coord` d'origine des IR | la valeur **la plus fréquente** pour la forme, remise en JSON compact |
| communes, pays, lieux hors Nord, à identifier, voies anciennes | — | vide (pas de géométrie dans les extraits) |

Au 2026-10-27 : 979 lignes (10 615 `<geogname>`) ont une `coord` ; 294 lignes
(1 596 `<geogname>`) n'en ont pas, dont 40 communes et 90 voies. Le plus long
tracé fait 15 065 caractères, sous la limite d'une cellule LibreOffice /
Excel (32 767).

Un **polygone source invalide** (contour qui se recoupe) est réparé au
calcul : on garde la partie surfacique de `make_valid()`, et la ligne reçoit
la remarque « géométrie source invalide, réparée (make_valid) ». Cas au
2026-10-27 : le quartier MEL **Hommelet**, dont le contour comporte une pointe
de largeur nulle (surface conservée à 100 %).

Pour donner une géométrie à une ligne qui n'en a pas, ou corriger celle
proposée, remplir `coord_manuel` au même format : le script d'application
devra la préférer à `coord`.

Décisions du 2026-10-27 sur ce qui est écrit dans `@coord` des IR : un
polygone y est accepté (quartiers) ; pour les voies, **le tracé** est retenu
(plutôt qu'un point ou un polygone autour du tracé), y compris en plusieurs
tronçons.

Le même jour, l'étiquetage point / polygone de `coord_origine` et `geometrie`
a été corrigé (`lstrip("[ ")` faisait passer les polygones pour des points :
32 lignes, dont 9 `geometrie` devenues `polygone d'origine`).

### Relecture : la colonne `a_traiter`

Pour trouver les cas à régler, un seul filtre suffit : **`a_traiter` non
vide** (207 lignes, 1 236 `<geogname>` au 2026-10-27, après le passage des
quartiers à la MEL). Motifs possibles :

| Motif | Lignes | Origine |
|---|---|---|
| `absente des sources actuelles (nom ancien ?)` | 83 | alerte |
| `lieu à identifier` | 46 | `categorie` = à identifier |
| `voie actuelle créée en …, documents dès …` | 15 | alerte |
| `CORRECTIONS_RUE et référence divergent (référence : …)` | 13 | libellé `CORRECTIONS_RUE` ≠ nom de référence (hors cas douteux) |
| `voie sans géométrie (référence TOPO seule)` | 13 | voie rattachée sans géométrie BD TOPO ni MEL |
| `type de voie différent de la référence (cas douteux…)` | 8 | alerte |
| `nom en partie ou jadis porté par …` | 6 | alerte |
| `espace public : source adresse proposée…` | 5 | alerte |
| `libellé de quartier construit d'après le nom MEL : à valider` | 5 | alerte |
| `renommage : nom actuel à confirmer` | 4 | `nom_ancien` rempli |
| `quartier rattaché par inclusion du nom (…) : à confirmer` | 4 | alerte (Centre, Anseele, Hutin, Oran Cartigny) |
| `homonyme actuel …` | 2 | alerte |
| `indexé comme quartier dans les IR, rattaché comme …` | 1 | alerte (`Gare, place de la`) |
| libellé imparfait dans toutes les sources (Vercoutère, Sevigné, Chenier, Saint Rédempteur) | 5 | `LIBELLES_A_VERIFIER` dans le script |
| `coordonnées d'origine illisibles (tronquées ?) : saisir coord_manuel` | 0 | `coord` qui n'est pas du JSON valide (le polygone tronqué d'Oran Cartigny ne sert plus depuis le rattachement MEL) |
| `quartier absent des quartiers historiques MEL` | 0 | alerte |

Une ligne réglée se marque en remplissant `validation` (toute valeur non
vide) : à la prochaine exécution, `a_traiter` se vide. La colonne `alerte`
reste, elle, inchangée (diagnostic du script).

### Libellé retenu (`libelle_final`, règles décidées le 2026-10-27)

`forme_bnr` garde la forme brute des IR (c'est la clé). `libelle_retenu` est
le libellé à écrire dans les IR :

| Cas | Libellé retenu |
|---|---|
| voie rattachée (nom normalisé, concordance) | **graphie de la référence** (accents, casse, articles, nom officiel complet) ; une référence TOPO en capitales (`PL D AUDENAERDE`) est développée et remise en casse ; `harmoniser()` répare la référence quand elle est moins bien écrite que la BnR (mot en minuscule : `Rue racine` → `Rue Racine` ; accents manquants : `Avenue Le Notre` → `Avenue Le Nôtre`) |
| voie rattachée par `CORRECTIONS_RUE` | **forme de `CORRECTIONS_RUE`** (fautes de la BnR : `Boulevard de Cambai` → `Boulevard de Cambrai`) |
| renommage (`concordance (ancien nom de …)`) | **nom actuel** ; l'ancien nom est gardé dans la colonne `nom_ancien` (`Boulevard de Paris`, `Rue Neuve`) |
| type de voie différent de la référence (`Rue d'Alsace` / `Avenue d'Alsace`, `Boulevard d'Halluin` / `Cour d'Halluin`, `Rue de Malplaquet`, `Rue Frasez`) | **libellé BnR conservé**, alerte « type de voie différent de la référence (cas douteux, libellé non corrigé) » |
| espace public | graphie de la référence seulement si elle ne diffère que par les accents, la casse ou la ponctuation |
| quartier | nom du quartier historique MEL, **sans article**, dans la graphie de la BD TOPO pour le même nom (`l'Épeule` → `Épeule`, `Fresnoy-Mackellerie`, `Hutin Oran Cartigny`) ; à défaut, nom MEL remis en casse et accentué d'après la forme BnR (`Crouy`, `Linne`, `Sainte Elisabeth`…, signalé « à valider ») |
| commune | nom officiel **suivi de son département**, tiré du code INSEE (`Roubaix (Nord)`, `Vermelles (Pas-de-Calais)`) ; jusqu'au 2026-10-27 au soir, « (Nord) » était ajouté à tort aux communes des départements voisins (Auchy-les-Mines, Vermelles) |
| non rattaché (voie ancienne, lieu hors Nord, pays, à identifier) | forme BnR remise à l'endroit (ci-dessous) |

Retouches communes : `St`/`Ste` développés (`Rue Saint-André`),
`Rond-Point`, `Allé` → `Allée`. Corrections à la main : `LIBELLES_FORCES`
(sur la forme BnR) et `LIBELLES_FINAUX` (sur le libellé final). Enfin, les
lignes rattachées à la même voie dont les libellés ne diffèrent que par les
accents ou la casse reçoivent tous la variante la plus accentuée.

Mise en forme de la forme BnR (`libelle_affiche`) :

- forme inversée remise à l'endroit, type de voie en tête avec majuscule :
  `Epeule, rue de l\'` → `Rue de l'Epeule`, `Barbieux, parc` → `Parc Barbieux` ;
- apostrophes déséchappées (`\'` → `'`), retours chariot et espaces
  multiples supprimés (`Président  Auriol, rue du` → `Rue du Président Auriol`) ;
- qualificatif entre parenthèses renvoyé en fin : `Royale, rue (Lille)` →
  `Rue Royale (Lille)` ;
- prénoms et titres restés dans le type capitalisés : `Bernard, rue claude`
  → `Rue Claude Bernard` ; articles, `St`, `point` laissés tels quels ;
- cas que la règle ne sait pas traiter : `LIBELLES_FORCES` dans le script
  (`Clément, Jean-Baptiste place` → `Place Jean-Baptiste Clément`).

Les formes sans virgule (communes, quartiers, `Grand place`…) ne changent
pas. Le CSV est écrit avec des fins de ligne `\n` (plus de `\r\n`).

Les années (`annee_min`, `annee_max`) sont lues dans les IR par `c_id` :
depuis l'alignement des id sur Mnesys (2026-10-27), les `c_id` de
`rbx-bnr_geogname_origine.csv` ont été traduits en id Mnesys (sauvegarde
`rbx-bnr_geogname_origine.backup_20261027_avant_id_mnesys.csv`), seule
modification apportée à cet état figé.

## Premier passage (2026-09-26)

1 273 formes, 12 211 `<geogname>` :

| Catégorie | Formes | geogname | Rattachées | Alertes |
|---|---|---|---|---|
| voie | 1 020 | 10 033 | 935 | 108 |
| espace public | 10 | 344 | 10 | 5 |
| quartier | 38 | 650 | 31 | 7 |
| commune | 41 | 717 | 41 | 0 |
| lieu hors Nord | 109 | 258 | – | – |
| pays | 8 | 74 | – | – |
| non toponyme | 1 | 1 | – | – |
| à identifier | 46 | 134 | – | – |

Géométrie des voies : BD TOPO pour 9 473 `<geogname>`, filaire MEL pour 92,
point d'origine pour 55, aucune pour 413 (voies anciennes absentes des
sources actuelles : rue de la Gare, rue du Moulin, rue Saint-Georges,
rue Pauvrée, rue des Longues Haies…).

---

## Utilisation

Depuis la racine du projet, après [geogname_concordance.py](geogname_concordance.md) :

    python scripts/ead/geogname_concordance_lieux.py                     # extraits BD TOPO déjà présents
    python scripts/ead/geogname_concordance_lieux.py --bdtopo <.gpkg>    # (ré)extrait la BD TOPO
