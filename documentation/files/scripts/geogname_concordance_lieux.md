# Script : geogname_concordance_lieux.py

**Emplacement :** `scripts/ead/geogname_concordance_lieux.py`

Construit la **concordance des formes des IR avec les lieux**
(`data/geo/concordance_geogname_lieux.csv`) : chaque forme de `<geogname>`
trouvée dans les IR est alignée sur un lieu du
[référentiel des lieux de Roubaix](referentiel_lieux_roubaix.md), **par le
texte seul**. C'est l'étape 2 de l'[indexation géographique](indexation_geographique.md).

Réécrit le 2026-10-29. L'ancien script du même nom (rattachement aux sources
brutes, méthode d'avant le référentiel) est archivé dans
`archives/indexation_geo_avant_referentiel/`.

---

## Utilisation

```bash
# après referentiel_lieux_roubaix.py
conda run -n rbx-bnr-data python scripts/ead/geogname_concordance_lieux.py
```

Le script s'arrête si la concordance ou le référentiel est ouvert dans
LibreOffice (fichier de verrou `.~lock.…#`).

## Entrées

| Fichier | Rôle |
|---|---|
| `results/ead/indexation/rbx-bnr_geogname_origine.csv` | Les `<geogname>` des IR dans leur état d'avant corrections (texte, `@source`) ; vides et `@source="chrono"` ignorés ; `@coord` ignoré |
| `data/geo/referentiel_lieux_roubaix.csv` | Lieux retenus : seule source des lieux |
| `data/geo/referentiel_lieux_roubaix_ecartes.csv` | Lieux « ne pas conserver », pour signaler une forme qui y renvoie |

## Méthode

1. **Une ligne par forme** : texte exact du `<geogname>` et `@source`
   d'origine (le même texte peut désigner un quartier ou un parc selon
   l'`@source`).
2. **Clé de la forme** : remise à l'endroit (« Achille Screpel, rue » →
   « rue Achille Screpel »), puis sans accents, casse, ponctuation ni
   articles (même clé que le référentiel, `cle_voie`) ; colonne `forme_cle`.
3. **Comparaison aux noms de chaque lieu**, par rang de préférence ; on
   s'arrête au premier rang qui donne un lieu :
   1. libellé, noms BAN, BD TOPO, MEL, TOPO ;
   2. variantes ;
   3. anciens noms (OSM) — alignement à vérifier.
4. **Plusieurs lieux** : l'`@source` départage (quartier → thésaurus
   quartiers, rue → rues, adresse → points) ; sinon statut « plusieurs
   lieux ».
5. **Aucun lieu** : si la forme renvoie à un lieu écarté, statut « lieu
   écarté » ; si elle porte un type de voie ou une `@source` de Roubaix,
   statut « sans lieu », avec des candidats ; sinon (commune, pays…),
   « hors référentiel », sans traitement pour l'instant (futur thésaurus
   communes_pays).
6. **Candidats** (3 au plus) : lieux dont le nom ressemble à la forme, type
   de voie mis à part (tous les mots de l'un dans l'autre, sinon ratio de
   ressemblance ≥ 0,75), ceux du thésaurus attendu d'abord.

## Sortie : `data/geo/concordance_geogname_lieux.csv`

| Colonne | Contenu |
|---|---|
| `a_traiter` | Ce qui reste à régler ; vidé dès que `validation` est remplie |
| `forme`, `source` | Texte exact et `@source` d'origine (clé de la ligne) |
| `occurrences`, `ir` | Nombre de `<geogname>`, IR concernés |
| `forme_cle` | Chaîne réellement comparée |
| `statut` | aligné / plusieurs lieux / lieu écarté / sans lieu / hors référentiel / à retirer |
| `alignement` | Nom du lieu sur lequel le texte a été trouvé (libellé, nom BAN…, variante, ancien nom) ou « manuel » |
| `lieu_id`, `thesaurus` | Lieu retenu |
| `libelle_retenu` | **Forme finalement retenue** : libellé du lieu dans le référentiel, qui remplacera le texte du `<geogname>` |
| `candidats` | Lieux proposés quand il n'y a pas de lieu unique (« libellé [lieu_id] ») |
| `libelle_manuel` | Saisie : libellé exact d'un lieu du référentiel, pour choisir le lieu |
| `validation`, `remarque` | Saisie : « validé » par convention ; « hors référentiel » (commune, pays… pris pour une voie de Roubaix) et « à retirer » (`<geogname>` qui n'est pas un lieu) tranchent la forme sans lieu et deviennent son statut |

Les colonnes `libelle_manuel`, `validation` et `remarque` sont conservées
d'une exécution à l'autre (clé : `forme`, `source`). Une graphie à corriger
se corrige dans le référentiel (`libelle_manuel` du lieu), pas ici.

## Premier passage (2026-10-29)

| Statut | Formes | `<geogname>` |
|---|---:|---:|
| aligné | 968 | 10 347 |
| sans lieu | 100 | 677 |
| lieu écarté | 3 | 8 |
| hors référentiel | 202 | 1 179 |

Alignements : 947 sur le libellé, 13 sur le nom TOPO (codes rattachés par
les fusions du référentiel, ex. « Rue d'Alsace » → Avenue d'Alsace), 3 sur
le nom MEL, 3 sur une variante, 2 sur un ancien nom (« Boulevard de Paris »
→ Boulevard du Général de Gaulle). 541 lieux du référentiel sont cités sur
979. Restent à traiter 105 formes : noms anciens (Rue de la Gare, Rue du
Moulin, Rue Saint-Georges…), coquilles (« Cambai », « Mulbhouse »),
formes incomplètes (« Centre », « Anseele », « Faidherbe, place »).

## Propositions et décisions (2026-10-29)

Propositions pour les 105 formes à traiter :
`data/geo/propositions_concordance_20261029.csv` (action, libellé proposé,
confiance, justification, candidats ; colonnes `decision` et
`remarque_decision` pour la décision). Les 35 propositions « sûr » ont été
reportées (accord global) : 31 `libelle_manuel` + « validé », 3 « hors
référentiel », 1 « à retirer » ; sauvegarde
`concordance_geogname_lieux.backup_20261029_avant_surs.csv`.

Les 6 « probable » ont ensuite été validés (2026-10-29), « 17 Grand'Place »
étant aligné sur le point Hôtel de ville (ex-« Mairie de Roubaix », libellé
changé dans le référentiel) plutôt que sur la Grand-Place ; sauvegarde
`concordance_geogname_lieux.backup_20261029_avant_probables.csv`.

Les 2 squares écartés sont revenus dans le référentiel comme rues (règle
« les squares sont des rues ») : leurs formes s'alignent sur le libellé.

| Statut | Formes | `<geogname>` |
|---|---:|---:|
| aligné | 1 005 | 10 647 |
| sans lieu | 62 | 378 |
| hors référentiel | 205 | 1 185 |
| à retirer | 1 | 1 |

Les 26 « à vérifier » ont ensuite été tranchés : « Rue de la Gare » (81
`<geogname>`) alignée sur l'Avenue Jean Lebas, dont c'est l'ancien nom
(décision de l'utilisateur) ; 8 formes alignées malgré un type de voie
différent, le nom étant unique (Fosse-aux-Chênes, Jules Brame, Ampère,
Émile Zola, Frasez, Raoul Dufy) ; 16 laissées sans lieu, avec une remarque
« plus tard » (Boulevard d'Halluin, Boulevard Constantin Descat, Rue de
l'Union, places, quai…). Sauvegarde
`concordance_geogname_lieux.backup_20261029_avant_a_verifier.csv`.

| Statut | Formes | `<geogname>` |
|---|---:|---:|
| aligné | 1 015 | 10 766 |
| sans lieu | 52 | 259 |
| hors référentiel | 205 | 1 185 |
| à retirer | 1 | 1 |

Restent 52 formes sans lieu, toutes « plus tard » (noms anciens et voies à
identifier : couche historique).

Couche historique (2026-10-29) : reprise des décisions de l'ancien dépôt
rbx-geo2 (`data/geo/reprise_rbx_geo2_20261029.csv`) — 32 formes alignées sur
la voie actuelle dont elles sont l'ancien nom ou sur la Rue des Longues Haies
créée, 5 voies hors Roubaix (« hors référentiel »), « Rue de la Redoute »
alignée sur la Rue Émile Moreau (documents 1884-1929) ; Rue
Saint-Vincent-de-Paul et quais gardés comme voies disparues. Sauvegarde
`concordance_geogname_lieux.backup_20261029_avant_rbx_geo2.csv`.

| Statut | Formes | `<geogname>` |
|---|---:|---:|
| aligné | 1 047 | 10 985 |
| sans lieu | 16 | 21 |
| hors référentiel | 209 | 1 204 |
| à retirer | 1 | 1 |

Restent 16 formes sans lieu (21 `<geogname>`), aucune dans rbx-geo2 : Rue de
la Balance, Rue de l'Union, Rue Saint-André, Rue Chapelle Carette…

## Validation des alignements dans QGIS (2026-10-29)

- **Lots A + B** : les 23 formes alignées autrement que par le libellé exact
  (nom TOPO / MEL, variante, ancien nom) ou dont l'`@source` pointe un autre
  thésaurus (`data/geo/propositions_concordance_20261029_lots_AB.csv`,
  projet `validation_concordance_AB_20261029.qgz`) : toutes « validé » ;
  « Barbieux » [rue] rattaché au Parc Barbieux. Sauvegarde
  `concordance_geogname_lieux.backup_20261029_avant_lots_AB.csv`.
- **Lot E, sondage des alignements par libellé exact** : 64 formes, 3 292
  `<geogname>` (`data/geo/propositions_concordance_20261029_lot_E.csv`,
  projet `validation_concordance_E_20261029.qgz`) — 24 ciblées (ancienne
  position à plus de 150 m, ou document antérieur à la date de création
  TOPO), 20 les plus citées, 20 tirées au hasard. Les 64 sont « validé »,
  y compris la Place Notre-Dame et la Place de l'Abreuvoir, signalées « à
  vérifier ». Sauvegarde
  `concordance_geogname_lieux.backup_20261029_avant_lot_E.csv`.

Après ces validations, 175 formes portent « validé ». Parmi les formes
alignées, 872 ne sont pas validées individuellement (6 598 `<geogname>`) ; le
sondage n'ayant relevé aucune erreur, leur alignement par le libellé exact
est tenu pour fiable.

## Thésaurus communes_pays : lot D (2026-10-29)

Les formes « hors référentiel » (communes, départements, pays…) sont
rattachées au thésaurus **communes_pays**, tenu dans
`data/geo/referentiel_communes_pays.csv`. Une version GeoPackage
(`referentiel_communes_pays.gpkg`, couche `communes_pays`) est exportée du CSV
par `ogr2ogr`. Pour l'instant, ce thésaurus compte 183 concepts : Champagne (France)
a été retiré, Ardennes (France) ajouté, puis 4 concepts du reliquat (voir plus bas),
puis Mouvaux (Nord) le 2026-10-30 (forme « Duriez, rue des », validée dans
`validation_sans_lieu_20261030.qgz`).

Colonnes du CSV :

| Colonne | Contenu |
|---|---|
| `lieu_id` | code INSEE (commune), `dep` + code (département), sinon QID Wikidata, sinon `cp_` + libellé |
| `thesaurus` | `communes_pays` |
| `categorie` | commune, département, région, pays, ville étrangère, lieu-dit, territoire, continent |
| `libelle` | forme RAMEAU : « Lille (Nord) », « Nord (France) », « Paris (France) », « Bruxelles (Belgique) », « Belgique » |
| `parent` | libellé du niveau supérieur (département, pays), pour la future hiérarchie SKOS |
| `insee`, `wikidata`, `bnf` | identifiants |
| `coord_wkt`, `coord_manuel` | point (centre de commune geo.api.gouv.fr, sinon Wikidata) ; `coord_manuel` l'emporte |
| `formes_ir` | formes des IR rattachées au concept |
| `sources`, `remarque` | provenance |

L'alignement n'est pas automatique. Une forme est rattachée à un concept par
son `libelle_manuel`, et `appliquer_manuel` cherche ce libellé dans le
référentiel de Roubaix puis dans communes_pays.

Conventions décidées le 2026-10-29 :

- « Roubaix » est le concept « Roubaix (Nord) » ;
- les voies et quartiers hors Roubaix sont rattachés à leur commune :
  - bd Constantin Descat et rue Claude Bernard → Tourcoing ;
  - rue du Créchet et rue Verte → Croix ;
  - rue Royale → Lille ;
  - Flers-Breucq → Villeneuve-d'Ascq ;
  - Ménilmontant → Paris ;
- les communes disparues et les noms anciens sont rattachés à la commune
  actuelle :
  - Ascq et Annappes → Villeneuve-d'Ascq ;
  - Lomme → Lille ;
  - Malo-les-Bains → Dunkerque ;
  - Cravant → Deux Rivières ;
  - Saussay-la-Vache → Saussay-la-Campagne ;
  - Châlons → Châlons-en-Champagne…
- exceptions à RAMEAU, sans forme inversée : « Lac de Garde (Italie) »,
  « Fort de Bois-Bourrus (Meuse) ».

Les propositions sont dans `data/geo/propositions_concordance_20261029_lot_D.csv`
(209 formes). Elles ont été validées dans QGIS, projet
`validation_concordance_D_20261029.qgz` :

- 180 « sûr » validés en bloc ;
- 20 autres validés ;
- 2 libellés corrigés ;
- 7 formes laissées sans décision : Flandre, Ostrebosc (Eure), « Centre »
  (MED_PAR, sans doute le quartier), La Vaux-Marie (Meuse), Laussage,
  Ravin de la Hayette (Meuse), Tolbecq.

Deux `<geogname>` ont été corrigés dans les IR source `bnr2mnesys`, car la
concordance travaille par forme :

- MED_PAR, « Le quai d'Anvers » : « Anvers » → « Anvers, quai d' » ;
- MED_VDM : « France; Belgique » → « France » + « Belgique » ;
- MED_AFF, AFF_001_B_2_078 (« Le théâtre des opérations militaires dans l'Est
  et en Champagne », 1917) : « Champagne » → « Ardennes (France) » +
  « Marne (France) ». La décision a été prise le 2026-10-29. Le concept
  Champagne (France), qui n'avait pas de point, est retiré, et Ardennes (France)
  (dep08) est ajouté. La forme « Champagne » repasse « hors référentiel », puisqu'elle
  n'existe plus dans les IR. Les deux nouvelles formes sont rattachées à leur
  concept par le script d'application, qui reconnaît un texte égal au libellé
  exact d'un concept. Sauvegarde :
  `concordance_geogname_lieux.backup_20261029_avant_champagne.csv`.

Les 8 formes de voies hors Roubaix, auparavant « hors référentiel », passent
« validé ». Sauvegarde : `concordance_geogname_lieux.backup_20261029_avant_lot_D.csv`.

| Statut | Formes | `<geogname>` |
|---|---:|---:|
| aligné | 1 247 (dont 200 sur communes_pays) | 12 155 |
| sans lieu | 16 | 21 |
| hors référentiel | 9 (7 non tranchées + « France; Belgique » et « Champagne », qui n'existent plus dans les IR) | 34 |
| à retirer | 1 | 1 |

### Reliquat du lot D (2026-10-29)

Les 7 formes restantes ont été tranchées une à une, d'après les notices.
Sauvegarde : `concordance_geogname_lieux.backup_20261029_avant_reliquat_D.csv`.

| Forme | `<geogname>` | Décision |
|---|---:|---|
| Flandre | 15 | **Flandre (France)** (Wikidata Q36769). La Flandre française correspond à 14 notices sur 15. Point saisi à la main au mont Cassel (`coord_manuel`). |
| Ostrebosc (Eure) | 12 | **Ostrebosc (Eure)**, lieu-dit de Coudray (BAN). C'est la ferme familiale de la correspondance Jacques / Marie-Josèphe (permission « via Coudray »). |
| Centre (MED_PAR) | 1 | Quartier **Centre-Ville** : la notice LAB_PH20_226 porte sur l'association Action Roubaix Centre. |
| La Vaux-Marie (Meuse) | 1 | **La Vaux-Marie (Meuse)**, lieu-dit, champ de bataille de septembre 1914. Point BAN de la ferme de la Vaux-Marie (Courcelles-sur-Aire). |
| Ravin de la Hayette (Meuse) | 1 | **Ravin de la Hayette (Meuse)**, lieu-dit. Point **approché** (`coord_manuel`) entre le monument du Mort-Homme et la cote 304, à environ 1,5 km près. |
| Laussage | 1 | **À retirer** : non identifiable (BOU_JAC_1512, « par Laussage », sans doute une lecture fautive). |
| Tolbecq | 1 | **À retirer** : non identifié. La notice PHO_PDV_LED2_D15 dit « commune du département du Nord » ; série alphabétique Gruson, Lys-lez-Lannoy, Tolbecq, Toufflers, Tressin. |

| Statut | Formes | `<geogname>` |
|---|---:|---:|
| aligné | 1 252 (dont 204 sur communes_pays) | 12 185 |
| sans lieu | 16 | 21 |
| hors référentiel | 2 (« France; Belgique » et « Champagne », qui n'existent plus dans les IR) | 2 |
| à retirer | 3 | 3 |

## Lot C : formes sans lieu (2026-10-29)

Les 16 formes sans lieu ont été revues une à une, d'après les notices et la
BAN de la métropole. Sauvegarde :
`concordance_geogname_lieux.backup_20261029_avant_lot_C.csv`.

- **Alignées (2)** :
  - « Nabuchodonosor, place » → Rue Nabuchodonosor : la place est un
    élargissement de la rue ;
  - « Verte, rue de la place » → Place Verte.
- **Laissées sans lieu, avec une piste en remarque (6)** :
  - « Duriez, rue des » : rue des Duriez à Mouvaux ;
  - « Téléphone, rue du » : rue du Téléphone à Croix, en limite de Roubaix ;
  - « Dunkerque, rue de » : Tourcoing ou le Quai de Dunkerque ;
  - « Saint-André, rue » et « André, rue St » : une rue Saint-André
    disparue ? (Cour et Impasse Saint-André existent) ;
  - « Paraboles, avenue des » : voie récente (Phildar) inconnue de la BAN.
- **Laissées sans lieu pour la couche historique (6)** : rues de la
  Balance, de l'Union (2 formes), Chapelle Carette (2 formes), Latérale,
  Jeanne d'Arc, Pergolèse.

| Statut | Formes | `<geogname>` |
|---|---:|---:|
| aligné | 1 254 | 12 187 |
| sans lieu | 14 | 19 |
| hors référentiel | 2 (formes qui n'existent plus dans les IR) | 2 |
| à retirer | 3 | 3 |
