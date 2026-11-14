# sites_candidats.py

[scripts/ead/sites_candidats.py](../../../scripts/ead/sites_candidats.py)

Repère dans les IR les notices qui citent un **bâtiment ou un site de Roubaix**
(église, hôpital, gare, hôtel de ville, salle, musée, école, parc, usine…) et
les rapproche des points du référentiel des lieux. C'est l'étape 1 de la
méthode « sites », lancée le 2026-11-12. Le script lit seulement les IR :
il n'écrit ni dans les IR ni dans le référentiel.

```bash
python scripts/ead/sites_candidats.py [--date AAAAMMJJ]
```

## Entrées

- `results/ead/ead_cor/bnr2mnesysgeo/*.xml` : les IR de test (37) ;
- `data/geo/referentiel_lieux_roubaix.csv` et `_ecartes.csv` : les points en
  service et les points écartés (BD TOPO), qui ont déjà une géométrie ;
- `data/geo/referentiel_communes_pays.csv` : les noms des autres villes.

## Méthode

Trois sources dans chaque notice `<c>`, de la plus sûre à la moins sûre :

1. **corpname** : une collectivité indexée qui désigne un bâtiment (un mot de
   type : église, salle, théâtre…) ou qui est qualifiée « (Roubaix, Nord) » ;
2. **subject** : un sujet RAMEAU qui nomme un bâtiment (« Théâtre Louis
   Richard »), pas un thème (« Manufacture et industrie textile ») ;
3. **texte** : `unittitle` et `scopecontent`, un mot de type suivi d'un nom à
   majuscule (« église Saint-Martin », « hôpital de la Fraternité »). Quelques
   sites sont pris sans nom : hôtel de ville, mairie, conservatoire,
   Condition Publique, Colisée, Cave aux Poètes, ENSAIT. Un mot de type
   précédé d'un type de voie (« rue de l'Hospice », « place de la Gare ») est
   ignoré.

Chaque forme est réduite à une clé, sans accents, articles, « Roubaix » ni
« Nord ». Une clé correspond à un site candidat. Elle est comparée aux points
du référentiel :

| correspondance | règle |
|---|---|
| exact | même clé |
| approché | les mots distinctifs de l'un sont tous dans l'autre, même famille |
| variante connue | nom listé dans `VARIANTES` (mairie → hôtel de ville, conservatoire national → conservatoire, La Piscine…) |
| autre ville | toutes les formes nomment une autre ville (« gare de Lille ») : pas de point proposé |
| aucune | pas de point : site disparu, sans point BD TOPO ou nom à rapprocher à la main |

**Alertes** (à lire, rien n'est écarté automatiquement) : voie homonyme,
nom de quartier, nom de personne indexé dans la même notice, notices dans
une autre ville (d'après la forme, ou les `geogname` du thésaurus des
territoires de la notice et de ses ancêtres).

## Sorties

- `data/geo/propositions_sites_<date>.csv` : une ligne par site candidat,
  triée par nombre de notices. Les colonnes `decision`, `lieu_manuel` et
  `remarque` se remplissent à la main et sont conservées d'une exécution à
  l'autre (clé : `site_cle`).
- `data/geo/propositions_sites_<date>_notices.csv` : une ligne par notice et
  par site (IR, identifiant `c`, forme, champ, contexte, titre).

## Limites connues

- Le nom capté dans le texte s'arrête au premier mot sans majuscule. Les
  titres tout en capitales captent parfois un mot de trop.
- Un même site peut apparaître sous plusieurs clés (« ensait » et « ensait
  ecole nationale superieure… », « salle watremez » et « salle
  wattremez »). Le regroupement se fait à la validation (`lieu_manuel`) ou par
  `VARIANTES`.
- Les règles du type de lien sont des heuristiques : `mention` reste à
  trancher à la main, et `lieu ou organisateur` aussi.

## Étapes 2 et 3 : faux positifs et type de lien

Chaque lien notice × site reçoit un `lien` et un `motif` (fichier
`_notices`), par la fonction `type_de_lien`, règles appliquées dans l'ordre :

1. **exclu** : forme qui nomme une autre ville ; organisation qui n'est pas un
   bâtiment (compagnie, association, société, amis, orchestre…) ;
2. **organisateur** : `corpname` avec un `@role` (imprimeur, éditeur,
   organisateur…) ;
3. **la rue confirme** : une rue indexée dans la même notice passe à moins de
   100 m du point (ou de son emprise) → `sujet` dans un IR d'images, sinon
   `lieu`. C'est le signal le plus fort. Les affiches indexent en effet la
   salle en `corpname` et sa rue en `geogname` (salle Watremez et rue de
   l'Hospice, conservatoire et rue de Soubise) ;
4. `subject` RAMEAU → **sujet** ;
5. « proposé / organisé / édité… par » juste avant le site → **organisateur** ;
6. titre d'un IR d'images (`IR_IMAGES` : MED_CP, MED_PHO, MED_PHD, MED_PBI,
   MED_PAR, MED_IMA, ARA_CPS, MUS_VAI, MED_PLA) → **sujet** ;
7. « à / au / dans / devant… » juste avant le site → **lieu** dans un IR
   d'événements (`IR_EVENEMENTS` : MED_AFF, MED_MAR, MUS_EVE, MED_EPH), sinon
   **mention** ;
8. titre qui commence par le site → **sujet** ;
9. `corpname` sur une affiche, sans rue proche : institution (conservatoire,
   médiathèque, musée, archives, école…) → **lieu ou organisateur**, salle ou
   autre bâtiment → **lieu** ;
10. sinon → **mention**.

Seuls `sujet` et `lieu` sont retenus pour relier la notice au point.

**Exception `LIEU_SI_RUE`** : sur une affiche, le **conservatoire** n'est
retenu comme lieu que si la rue indexée dans la notice le confirme. Sinon, il
devient `lieu ou organisateur`, même quand le titre commence par lui
(« Le Conservatoire en balade » : il organise). Décision de l'utilisateur du
2026-11-12. Résultat sur MED_AFF : 109 lieux (rue de Soubise, rue de Lille) et
169 « lieu ou organisateur ».

Même règle, même jour, pour les musées et les archives. Sur les affiches :
- La Piscine : 21 lieux, 25 « lieu ou organisateur » ;
- La Manufacture : 13 lieux, 5 « lieu ou organisateur » ;
- Archives nationales du monde du travail : 20 lieux, 21 « lieu ou
  organisateur ».

La médiathèque n'a pas encore de point : son `lieu_id` sera à ajouter à
`LIEU_SI_RUE` quand elle en aura un.

Colonnes ajoutées au fichier des sites :
- `proposition` : `sûr` (point au même nom, variante connue ou `lieu_manuel`,
  et au moins un lien retenu), `probable` (nom approché), `exclu` (aucun lien
  retenu ou autre ville), `sans point` ;
- `nb_retenus` et `liens` : le décompte des types de lien ;
- `quartier_point` : le quartier MEL qui contient le point. Il est unique, ce
  qui donne le quartier sans ambiguïté, même quand la rue du site traverse
  plusieurs quartiers.

`lieu_manuel` (lieu_id ou libellé d'un point) remplace le point proposé :
correspondance `manuel`.

## Adresses et dates des titres et des notes

Ajout du 2026-11-12, à la demande de l'utilisateur (exemple de l'I.T.R. :
« cet établissement situé au 37 rue du Collège… créé en 1895 »).

- **Phrases retenues** : seulement les phrases du titre (`unittitle`) ou de la
  note (`scopecontent`) qui parlent du site (`phrase_du_site`). Une phrase est
  retenue si elle contient les mots distinctifs du site, son mot de type ou
  « établissement / bâtiment / édifice ». Elle est écartée si elle parle d'une
  société, d'une entreprise, d'une maison… : dans les lettres à en-tête
  (MED_LBI, MED_LET), l'adresse donnée est d'abord celle de l'expéditeur.
  Cette exclusion ne joue pas pour la famille industrie.
- **Adresses** (`ADRESSE`) : numéro, bis ou ter, puis type de voie et nom. Elles
  sont géocodées par la **BAN** (`data/geo/ban_roubaix.csv`, numérotation
  actuelle : un numéro ancien a pu changer). Le statut est « trouvée »,
  « numéro absent de la BAN » ou « voie inconnue de la BAN ».
- **Dates** (`DATE`) : « créé / fondé / construit / bâti / inauguré / détruit /
  démoli / ouvert / fermé en 1895 ».

Colonnes ajoutées :
- fichier des notices : `adresses`, `adresse`, `adresse_ban`,
  `distance_adresse_m` (distance au point proposé), `dates` ;
- fichier des sites : `adresse_note` (adresse géocodée la plus citée, avec son
  nombre), `distance_adresse_m`, `quartier_adresse`, `adresse_wkt` (WGS84),
  `dates_note`.

Alertes ajoutées : adresse à plus de 150 m du point (`DISTANCE_ADRESSE_M`) ;
site sans point mais avec une adresse, et point existant à moins de 30 m de
cette adresse.

Résultat : 115 liens ont une adresse et 67 sont trouvées dans la BAN. 52 liens
ont une date. 53 sites ont une adresse géocodée, dont 37 sans point. Exemples :
- Institut Turgot : 76 rue du Collège, à 1 m du lycée Turgot ;
- I.T.R. : 37 rue du Collège, à 695 m de l'école André Maginot ;
- usine Motte-Bossut : 78 boulevard du Général Leclerc, « construite 1903 » ;
- hospice Barbieux : 35 rue de Barbieux.

## État au 2026-11-12

950 sites candidats, 5 467 liens notice × site, 4 940 notices. Correspondances :
48 exact (2 282 notices), 8 variante connue (527), 88 approché (416), 101
autre ville, 705 aucune. 2 990 notices sont reliées à un point proposé
(144 sites : 9 points du référentiel, 135 points écartés).

Types de lien : sujet 2 051, lieu 1 163, mention 1 576, lieu ou organisateur
415, organisateur 38, exclu 224. **2 205 notices** ont un lien retenu (sujet
ou lieu) vers un point. Propositions : 53 sûr, 53 probable, 139 exclu, 705
sans point. Sondage de 40 liens retenus (graine 20261112) : 38 justes.

## Validation (étape 5)

Le 2026-11-12, 106 sites (53 sûrs, 53 probables) ont été validés dans QGIS
(`data/geo/validation_sites_20261112.qgz` + `.gpkg`, générés par des scripts
du scratchpad). Les décisions sont reportées dans
`propositions_sites_20261112.csv` (colonnes `decision`, `lieu_manuel`,
`remarque`), que le script relit :

- `decision = non` sans `lieu_manuel` : plus de point, correspondance « refusé
  à la validation », proposition `refusé` ;
- `decision = oui` ou `autre point` avec un point : proposition `validé` ;
- `decision = à revoir` : proposition `à revoir`.

Quand la remarque donnait le bon point (« c'est l'école primaire », libellé du
lycée Maxence Van der Meersch), le report l'a converti en `autre point` +
`lieu_manuel`. La conversion est signalée dans `remarque` ;
`validation_sites_20261112.csv` garde la saisie brute.

Correctif né de la validation : à libellé égal, le point de la même famille
passe d'abord (« Hôpital Victor Provo » : l'hôpital, pas la station de
tramway), pour les correspondances exactes comme approchées.

Résultat : 81 sites validés sur 59 points (56 points aujourd'hui écartés du
référentiel), **2 054 notices** (2 166 après le correctif de la gare) reliées par un lien sujet ou lieu, dans 25
quartiers. 17 refusés, 7 à revoir (76 notices), 1 sans décision (Institut
Turgot).

### Gare sans nom (correctif du 2026-11-12)

Les cartes postales titrent « La Gare », « Intérieur de la Gare + annexes »,
sans nom après le mot : la règle « mot de type suivi d'un nom à majuscule » les
manquait (22 notices reliées à la gare seulement). `GARE_SEULE` prend
désormais « gare » sans nom comme la gare de Roubaix. Sont exclus : « Alma-Gare »
(quartier), « chef de gare », la gare de marchandises ou de triage, la gare
routière, une gare suivie d'un nom propre (« Gare Lille-Flandres », « Gare
Saint Sauveur », « gare de Fives » : `TYPE_TEXTE` s'en charge) et les voies
(« place / rue / avenue de la Gare » : décision de l'utilisateur du 2026-11-12, les cartes « Place de la Gare » restent indexées sur la voie seulement). Résultat : 181 notices citent la gare,
dont 134 avec un lien retenu (130 sujet), au lieu de 22. Le total passe à
2 284 notices reliées à un point.
