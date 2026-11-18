# Reprise — indexation géographique, 2026-11-18

Note de passation pour reprendre sur un autre poste (la mémoire de Claude Code est
locale à chaque machine : **lire ce fichier en début de session**). Tout ce qui est
cité est dans le dépôt (commit automatique « MaJ » : vérifier `git log`, pas `git status`).

Environnement : `conda run -n rbx-bnr-data python …`, depuis la racine du dépôt.

## Règles de travail (rappel des décisions utilisateur)

- **Référentiel des lieux** (`data/geo/referentiel_lieux_roubaix.csv` + `.gpkg`, régénérés
  ensemble par `scripts/ead/referentiel_lieux_roubaix.py`) : **ne jamais le modifier ni le
  régénérer sans prévenir l'utilisateur** ; sauvegarde `*.backup_AAAAMMJJ_avant_<objet>.*`
  avant chaque modification ; les saisies se font dans les CSV, jamais dans le gpkg.
- Alignement des IR sur le référentiel **par le texte du libellé seul** (ignorer les @coord
  des IR, ne pas passer par les ref_id de l'ancienne concordance).
- Mnesys ne gère pas la géo : pas de contrainte Mnesys sur @coord/libellés.
- Rues = lignes, jamais de point. **Square** : avec tracé de voie = rue ; square-jardin sans
  voie = point (comme les parcs, points depuis le 2026-11-14).
- Rester sur le périmètre demandé ; proposer l'élargissement, ne pas le construire sans accord.

## 0. Geogname de tous les IR Mnesys : état au 2026-11-18, fin de journée (lire en premier)

Plan, décisions de l'utilisateur et étapes : [plan_geogname_mnesys_20261118.md](plan_geogname_mnesys_20261118.md).
Fiches des scripts : `documentation/files/scripts/mnesys_geo_*.md`. Les §1 et §2 ci-dessous sont l'historique
(sites du référentiel ; diagnostic des Fi) : le §2 est dépassé par ce qui suit.

**À faire d'abord sur un autre poste.** Au soir du 2026-11-18, tout ce qui suit était **non suivi par git**
(scripts `scripts/ead/mnesys_geo_*.py`, fiches, plan, `results/ead/indexation/{fi,mnesys}_*`,
`results/ead/ead_cor/{fi,mnesys}_geo/`), y compris les CSV que l'utilisateur a validés à la main. Il faut les commiter et
les pousser (commit automatique « MaJ » ou à la main), puis `git pull` sur l'autre poste, avant toute reprise.
Python : sur le poste du 18/11, `/opt/anaconda3/envs/ds/bin/python` (l'env `bnr-data` n'existait pas) ; ailleurs,
`conda run -n rbx-bnr-data python`. Tout se lance depuis la racine du dépôt.

### Principe

Les `<geogname>` des IR de `data/ead/mnesys/` (jamais modifiés) sont réécrits d'après des fichiers de propositions CSV
où l'utilisateur écrit `decision` = `oui` / `non` (et peut corriger `lieu_id`). Les IR corrigés vont dans
`results/ead/ead_cor/<série>_geo/`, le rapport dans `results/ead/indexation/<série>_geo_rapport.csv`. Réécriture
(convention BnR) : texte = libellé du lieu, `@source` = thésaurus `bnr_geo_*` (remplace un `@source` Mnesys existant),
`@coord` = géométrie WKT. Séries : `--serie fi` (séries Fi) et `--serie mnesys` (tous les IR) ; les décisions sont
globales, appliquées à tous les IR où la forme apparaît ; une forme ne figure avec un `lieu_id` que dans un fichier.

| Script (`scripts/ead/`) | Rôle |
|---|---|
| `mnesys_geo_diagnostic.py` | état de l'alignement de tous les IR (lecture seule) |
| `mnesys_geo_quartiers.py`, `_reprise.py`, `_voies.py`, `_sites.py`, `_territoires.py` | propositions par famille, dans cet ordre ; chaque famille exclut les formes des précédentes et des séries précédentes |
| `mnesys_geo_applique.py` | écrit les IR corrigés et le rapport |
| `mnesys_geo_commun.py` | séries, chemins, chaîne des familles |

Règle d'application (`retenue`) : `non` jamais ; `oui` toujours (il faut un `lieu_id`) ; vide : seulement si `methode` =
`automatique`. Un `lieu_id` absent du référentiel (lieu écarté, lieu OSM à créer, concept des territoires à créer)
laisse la forme « en attente » : signalée à l'écran et dans le rapport.

### Où en est chaque fichier

| Fichier (`results/ead/indexation/`) | État |
|---|---|
| `fi_{quartiers,reprise,voies,sites}_propositions_20261118.csv` | **validés par l'utilisateur, appliqués** dans `results/ead/ead_cor/fi_geo/` (6 IR, 389 geogname réécrits). **Ne jamais les régénérer** (écraserait les saisies). Alignement des Fi : 86,1 % → 94,5 %. 8 occurrences (4 formes) en attente : 3 lieux OSM à créer (Monument Nadaud, Palais de justice, Monument aux martyrs de la Résistance.) et *Pont des Couteaux* à réintégrer |
| `mnesys_quartiers_propositions_20261118.csv` | généré, **à valider** (17 formes) |
| `mnesys_reprise_propositions_20261118.csv` | généré, **à valider** (57 formes) |
| `mnesys_voies_propositions_20261118.csv` | généré, **à valider** (274 formes : 82 confiance haute, 89 moyenne, 103 sans proposition) ; l'utilisateur a dit commencer par celui-ci |
| `mnesys_sites_propositions_20261118.csv` | généré, **à valider** (138 formes ; les communes en sont sorties) |
| `mnesys_territoires_propositions_20261118.csv` | communes et pays absents du thésaurus des territoires (voir plus bas) |
| `mnesys_geo_diagnostic_{formes,ir}_20261118.csv` | diagnostic des 29 IR : 80,2 % alignés par le référentiel, 7,8 % par des décisions, 10,4 % à traiter |

Aucune saisie n'est encore faite dans les fichiers `mnesys_*`. Essai d'application sur tous les IR
(`python scripts/ead/mnesys_geo_applique.py --serie mnesys`, dossier provisoire recréé à chaque exécution) : 1 367
geogname réécrits avec les décisions des Fi et les propositions automatiques.

### Famille « territoires » (communes et pays absents du thésaurus, piste E1)

`mnesys_geo_territoires.py` (fiche `scripts/mnesys_geo_territoires.md`) propose les concepts à créer dans
`data/geo/referentiel_communes_pays.csv` : communes françaises par `geo.api.gouv.fr` (code INSEE, centre), villes
étrangères par Wikidata (QID, coordonnées), rattachements de la concordance BnR (Lomme → Lille…). **Il interroge des services
sur Internet** (seuls des noms de communes et de pays sont envoyés) ; les réponses utiles sont gardées dans
`<série>_territoires_sources_<date>.json` et `--hors-ligne` rejoue sans réseau. Les 64 communes que `fi_sites`
listait sans proposition y sont reprises (lignes inertes ; `mnesys_geo_applique.py` accepte une forme dans deux fichiers si
l'une des lignes n'a pas de `lieu_id`). **État : le CSV n'existe pas encore.** Le 2026-11-18, `geo.api.gouv.fr` était injoignable (connexion refusée,
depuis 13 h 30) ; une première version du CSV (216 formes) a été jetée à cause de défauts de recherche depuis corrigés, et
la régénération a échoué. À relancer quand le service répond (`curl -s https://geo.api.gouv.fr/departements | head -c 100`) :
`python scripts/ead/mnesys_geo_territoires.py --serie mnesys --date AAAAMMJJ` (quelques minutes : Wikidata limite le débit).
Vérifier ensuite le CSV : lignes « manuelles » (villes étrangères, noms approchés, rattachements), « aucune » (typos, anciens
États, précisions inconnues), `parent_au_referentiel` = « non » (départements ou pays à créer aussi). Le cache Wikidata est gardé (`mnesys_territoires_sources_20261118.json`, 650 entités) : seul geo.api.gouv.fr sera
interrogé. Premier essai : 216 formes, dont environ 150 concepts à créer et 23 parents.

### Reste à faire, dans l'ordre

1. L'utilisateur valide les CSV `mnesys_*` (commencer par les voies). Ne pas les régénérer ensuite.
2. Étape 3 du plan : table de décisions partagée `data/geo/concordance_mnesys_lieux.csv` (amorcée avec le référentiel, les
   décisions des Fi et la concordance BnR), pour que les IR BnR en profitent.
3. **Lot référentiel unique**, avec l'accord de l'utilisateur et sauvegardes `*.backup_AAAAMMJJ_avant_<objet>.*` :
   création des concepts des territoires validés (et de leurs départements ou pays parents), des 3 lieux OSM et du lieu
   écarté des Fi, des sites validés (`referentiel_lieux_roubaix.py`, comparaison avant/après).
4. Étape 5 : appliquer à tous les IR, y compris les formes déjà alignées par le référentiel (80 %, ≈ 11 458 occurrences,
   hors de tout fichier de propositions : famille à créer), dans `results/ead/ead_cor/mnesys_geo/`.
5. Étape 6, avec l'utilisateur : SKOS (`geogname2skos.py`), cartes, chargement des thésaurus `bnr_geo_*` dans Mnesys et
   réimport des IR.
6. Restes des Fi : *Ecole de plein air* (18 occ.), *Monument aux morts*, *Marché*, *Halles centrales*, courée *Bayart*,
   *Jeanne d'Arc, rue* (tracé connu de l'utilisateur, à voir plus tard), *Saint George, rue*, *Vincent Van der Meersch,
   avenue* (« oui » sans `lieu_id` : candidate 59512_3680).

## 1. Sites validés ajoutés au référentiel (fait le 2026-11-18)

Source : `data/geo/noms_points_sites_20261117.xlsx`, onglet « à nommer » (111 lignes,
colonne « libellé retenu » remplie par l'utilisateur). Report par
**`scripts/ead/sites_vers_referentiel.py`** (relançable, idempotence vérifiée), puis
`referentiel_lieux_roubaix.py`. Doc : `documentation/files/scripts/referentiel_lieux_roubaix.md`
(section « Sites validés reportés »).

- 111 lignes → 107 lieux (4 fusions : même point = un lieu).
- 56 points BD TOPO « ne pas conserver » réintégrés (validation « validé » dans
  `referentiel_lieux_roubaix_ecartes.csv`, `libelle_manuel` si le libellé retenu diffère).
- 47 nouveaux points `bnr:<clé>` : Château Vaissier (disparu) dans `lieux_disparus.csv` ;
  5 actuels + **41 « à classer »** dans `lieux_ajoutes.csv`.
- **3e catégorie « à classer »** (décision utilisateur) : colonne `etat` de
  `lieux_ajoutes.csv` (« actuel » / « à classer ») → motif `a_traiter`
  « à classer : lieu actuel ou disparu ? ».
- **Sites hors Roubaix gardés** (exception Château Vaissier du 14/11 étendue aux 9 autres :
  églises de Hem, filatures Wattrelos/Tourcoing, Stade Amédée Prouvost, Usine Holden…).
- Vélodrome roubaisien renommé « Vélodrome du Parc Barbieux » ; Parc de la Potennerie
  déjà présent.
- **Square Destombes = rue** (décision utilisateur) : pas de point, « Square Destombes »
  en variante de la rue Square Pierre Destombes (dict `RATTACHEMENTS` du script).
- Résultat : **1 079 lieux** (31 quartiers, 937 rues, 111 points). Concordance
  `data/geo/concordance_geogname_lieux.csv` régénérée : inchangée (1 255 formes alignées,
  13 sans lieu).
- Sauvegardes : `data/geo/*.backup_20261118_avant_sites.*` (avant l'ajout) et
  `*.backup_20261118_avant_destombes.*`.

**Reste à faire (sites)** :
- trancher les 41 « à classer » (actuel ou disparu ; un disparu passe dans
  `lieux_disparus.csv` avec ses dates) ;
- 7 sites « à revoir » de la validation du 12/11 ;
- non régénérés : SKOS (`geogname2skos.py`), carte (`carte_leaflet.py` — l'`index.html`
  de la carte est retouché à la main : faire un diff avant de régénérer) ;
- étape 6 de la méthode sites (geogname @source bnr_geo_rbx_sites + @coord dans les IR) :
  les 4 notices du site « Square Destombes » iront vers la rue ; réaffectations par notice
  de la bibliothèque municipale (cf. `data/geo/bibliotheque_municipale_notices_20261117.xlsx`).

## 2. Geogname des inventaires Mnesys Fi (diagnostic du matin ; dépassé : voir le §0)

10 inventaires Mnesys avec « Fi » dans le nom (`data/ead/mnesys/`, aucun « FI ») :
1AEFi, 1NumFi, 3Fi (`FRAM59100_3Fi_20210427_import.xml`), 4Fi à 10Fi. Pas de 1Fi ni 2Fi.
4 644 geogname, 761 formes distinctes ; 3Fi = 74 % ; 1NumFi, 6Fi (plans), 7Fi (brevets) : aucun.

Script **`scripts/ead/fi_geogname_alignement.py`** (lecture seule) →
`results/ead/indexation/fi_geogname_alignement_20261118.csv` (une ligne par forme : statut,
lieu, candidats, @source Mnesys, piste quartier, décision déjà prise dans la concordance BnR).

**Couverture (alignement par le texte, référentiel à 1 079 lieux)** : 86,1 % des occurrences
(3 998 / 4 644 ; 482 formes, 399 lieux distincts). Avant l'ajout des sites : 84,0 %
(+98 occ : églises Saint-Martin / Saint-Joseph / Saint-Jean-Baptiste, Condition Publique,
Cimetière, Hôpital de la Fraternité, hospices Barbieux et Blanchemaille, Salle Watremez,
Colisée, Square Destombes). Par IR : 10Fi 98 %, 9Fi 93 %, 4Fi 88 %, 3Fi 84 %, 5Fi 83 %,
1AEFi 69 %.

**@source des geogname Fi** : 91 % sans @source (texte libre) ; seul le 3Fi cite des
thésaurus Mnesys (`thesaurus/index_Lieux.xml` 233 + variante mal encodée
`thesaurus--SLASH--index_Lieux.xml` 26, `thesaurus/FRAM59100_THES_000004.xml` 147),
absents du dépôt ; 37 `@id="candidat_…"` (termes candidats Mnesys). Aucun lien vers les
thésaurus bnr_geo_*, aucun @coord. Pas de @source quartier/rue/adresse : les homonymes
(quartier vs rue) ne se départagent pas par la source.

**Pistes pour dépasser 90 % (rien n'est fait)** :
1. quartiers « X, quartier de… » (classés hors référentiel) : 21 formes / 66 occ s'alignent
   en retirant « , quartier… » (colonne `piste_quartier`) ;
2. 24 formes / 116 occ déjà résolues à la main dans la concordance BnR (colonne
   `concordance_libelle`) ;
3. fautes de saisie (Baurepaire, Pallissy, Galillée, Courbertin, Parc des sport…) : le bon
   lieu est en tête des candidats ;
4. sites sans nom reconnu (« Gare » seul, Musée de la Piscine, Centre Hospitalier, Halles
   centrales, monuments, Roubaix 2000, ~15 courées) : variantes à ajouter ou lieux à créer ;
5. hors Roubaix (~160 occ : Bradford, Verviers, Croix…) → thésaurus communes/pays
   (`data/geo/referentiel_communes_pays.csv`) ; « Roubaix » seul 64 occ.

Question ouverte : intégrer ou non les Fi à la concordance (aujourd'hui elle ne lit que
`results/ead/indexation/rbx-bnr_geogname_origine.csv`, les IR BnR) — à décider avec
l'utilisateur.
