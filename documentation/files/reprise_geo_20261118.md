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

## 2. Geogname des inventaires Mnesys Fi (diagnostic, en cours)

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
