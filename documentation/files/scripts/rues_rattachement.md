# rues_rattachement.py

[scripts/ead/rues_rattachement.py](../../../scripts/ead/rues_rattachement.py)

Prépare la **table de rattachement des rues ambiguës** à un quartier, au
niveau des 31 et des 13 quartiers, pour validation. Analyse dérivée
(2026-11-13) : ni le référentiel ni les IR ne sont modifiés.

```bash
python scripts/ead/quartiers_13.py        # d'abord
python scripts/ead/rues_rattachement.py
```

## Ordre de priorité pour une notice (décision du 13/11/2026)

1. **Point** (site validé, ou adresse « n° + voie » géocodée) → le quartier
   du point, toujours, même contre une rue indexée ;
2. **rue sûre** (≥ 95 % de sa longueur dans un quartier) → son quartier ;
3. sinon, **la convention de cette table**.

La table ne sert donc qu'aux notices sans point. Une notice dont les rues
ambiguës n'ont qu'un quartier en commun est réglée par ce croisement.

## Règles de proposition (rue ambiguë)

Dans cet ordre :

1. **repère** : la rue mène à un site repère (`REPERES` : avenue Jean Lebas
   → quartier de la gare, exemple donné par l'utilisateur) ;
2. **tous traversés** : sinon, tous les quartiers où passe 5 % ou plus de la
   longueur.

La règle **éponyme** (une rue qui porte le nom d'un quartier qu'elle traverse
va dans ce quartier : Rue de l'Épeule → Épeule) a été **retirée le
2026-11-13**, jugée mauvaise par l'utilisateur. Les 11 rues concernées (Rue
de l'Épeule, Avenue des Nations Unies, Place de la Fosse-aux-Chênes, Rue de
l'Alma, Place du Trichon, Boulevard d'Armentières, Avenue Linné, Quai du
Sartel, Rue Édouard Vaillant, Rue du Chemin Neuf, Passerelle Hutin) relèvent
ensuite de « majoritaire » ou « tous traversés ».

La règle **majoritaire** (75 % ou plus de la longueur dans un quartier → ce
quartier) a été **retirée le même jour**, à la demande de l'utilisateur : les
27 rues concernées passent en « tous traversés ». Saisies déjà faites
conservées ; seule la Rue Jean Moulin (« oui ») change à 13 : proposition
« Moulin - Potennerie » → les deux quartiers à 13 de ses trois quartiers à 31,
cohérent avec le « oui » à 31.

`type_31` / `type_13` : « limite » si la moitié au moins du tracé longe
(≤ 15 m) une limite de quartier, sinon « traversante ».

## Points d'une notice

- site : lien sujet ou lieu vers un site dont la décision est « oui » ou
  « autre point » dans `propositions_sites_20261112.csv` ; quartier =
  `quartier_point` ;
- adresse : « n° + voie » du titre ou de la présentation (règle `ADRESSE`
  de sites_candidats.py), dont la voie est une rue indexée de la notice,
  géocodée par la BAN locale (numéro exact, sinon numéro voisin de même
  parité à ±10), point à 50 m au plus du tracé. Numérotation actuelle.

## Sorties (data/geo/)

- `rues_rattachement.csv` : une ligne par rue ambiguë à 31 (toute rue
  ambiguë à 13 l'est aussi à 31), triée par `notices_sans_point_31` ;
  `regle_13` = « sûre » quand la rue n'est ambiguë qu'à 31. Colonnes
  manuelles conservées (clé `lieu_id`) : `decision`, `quartier_31_manuel`,
  `quartier_13_manuel`, `remarque` ;
- `rues_rattachement.gpkg` : couche `rues_rattachement` (tracés, mêmes
  colonnes), chargée dans `quartiers_13.qgz` (couleur = règle à 31) ;
- `rues_rattachement_conflits_points.csv` : notices dont le point est hors
  du quartier d'une rue sûre indexée (le point l'emporte ; à contrôler).

## Corriger les propositions dans QGIS

1. Ouvrir `data/geo/quartiers_13.qgz`. La couche « Rues ambiguës (règle à
   31) » est en tête ; couleur = règle proposée (violet repère, rouge
   tous traversés), tirets blancs = rue déjà décidée.
2. Basculer la couche en édition (crayon), puis ouvrir le formulaire d'une
   rue (outil « Identifier » avec « Ouvrir automatiquement le formulaire »,
   ou table attributaire en vue formulaire). Seules quatre zones sont
   modifiables :
   - **Décision** : oui (la proposition est acceptée, aux deux niveaux) /
     non / à revoir ;
   - **Quartier(s) à 31 — correction** : cases à cocher des 31 quartiers,
     plusieurs possibles ; remplace la proposition à 31 ;
   - **Quartier(s) à 13 — correction** : à remplir seulement si le résultat
     diffère de celui déduit des 31 cochés ;
   - **Remarque**.
   Pour corriger, cocher les quartiers suffit : la décision « non » est
   facultative.
3. Enregistrer les modifications (disquette), puis relancer le script :
   il relit les saisies, recalcule `retenu_31` / `retenu_13` et signale en
   ALERTE une décision ou un nom de quartier inconnus.

Une saisie vidée dans QGIS n'efface pas une valeur déjà présente dans le
CSV : pour retirer une décision, la vider aussi dans le CSV.

## Bilan (13/11/2026)

8 658 notices indexent au moins une rue avec un tracé ; 3 541 ont un point.

| | 31 quartiers | 13 quartiers |
|---|---|---|
| réglées par un point | 3 541 | 3 541 |
| dont point contre une rue sûre | 66 | 54 |
| réglées par une rue sûre | 2 530 | 3 398 |
| réglées par croisement de rues | 69 | 44 |
| **laissées à la convention** | **2 518** | **1 675** |

Règles proposées (129 rues ambiguës à 31), sans les règles éponyme et
majoritaire (13/11) :

| règle | rues (31) | notices (31) | rues (13) | notices (13) |
|---|---|---|---|---|
| repère (gare) | 1 | 141 | 0 | 0 |
| tous traversés | 128 | 2 616 | 94 | 1 806 |
