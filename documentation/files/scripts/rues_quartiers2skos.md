# rues_quartiers2skos.py

[scripts/ead/rues_quartiers2skos.py](../../../scripts/ead/rues_quartiers2skos.py)

Produit un **thésaurus SKOS de test pour Mnesys** : les rues de Roubaix et
les 31 quartiers MEL dans un seul fichier, chaque rue reliée par
`skos:related` à son ou ses quartiers de rattachement. Test du 14/11/2026 :
les thésaurus `bnr_geo_*` de [geogname2skos.py](geogname2skos.md) ne sont
pas modifiés.

```bash
python scripts/ead/rues_quartiers2skos.py
```

## Entrées

- `results/ead/indexation/thesaurus/bnr_geo_rbx_voies.xml` et
  `bnr_geo_rbx_quartiers.xml` : libellés, notation, type, anciens noms
  (`altLabel`), formes des IR (`hiddenLabel`) ;
- `data/geo/rues_rattachement.csv` (colonne `retenu_31`) : rues ambiguës,
  voir [rues_rattachement.py](rues_rattachement.md) ;
- `data/geo/rues_quartiers_13_repartition.csv` (colonne `q31_principal`) :
  rues sûres, voir [quartiers_13.py](quartiers_13.md).

## Sortie

`results/ead/indexation/mnesys_test/bnr_geo_rbx_rues_quartiers.xml`
(RDF/XML), schéma `https://www.bn-r.fr/thesaurus/geo_rbx_rues_quartiers`,
URI `…#rue_<frag>` et `…#quartier_<frag>` (fragments de geogname2skos).

- rues et quartiers sont tous concepts de tête ;
- `skos:related` est écrit **du seul côté de la rue** (rue → quartier),
  décision du 14/11/2026 ;
- `skos:editorialNote` sur chaque rue : règle de rattachement (rue sûre et
  part du tracé, rue ambiguë et règle, décision manuelle, sans tracé) ;
- pas de géométrie (pas de GeoSPARQL dans ce test).

## Ce que fait l'import Mnesys (essai du 14/11/2026)

Export du thésaurus importé (`FRAM59100_THES_000010.xml`, format
`<thesaurus><terme id="geo_rbx_rues_quartiers#rue_…">`) :

- `skos:prefLabel` → `<title>` ; `skos:altLabel` → `<relation type="EP">`
  (employé pour) ; `skos:related` → `<relation type="TA" ref="<schéma>#<id>"/>`
  (terme associé) ;
- `skos:hiddenLabel` (632) et `skos:editorialNote` ne sont **pas repris** ;
- **les quartiers doivent être dans le même fichier** : un premier essai
  avec des liens vers `geo_rbx_quartiers#…` (thésaurus séparé) a donné
  1 070 `TA` importés mais aucun quartier visible sur les fiches, `ref` ne
  correspondant à aucun terme du thésaurus.

**Import réussi (14/11/2026, export `FRAM59100_THES_000011.xml`)** avec
les quartiers dans le même fichier : rues et quartiers deviennent des
`<terme>`, chaque `skos:related` devient
`<relation type="TA" ref="geo_rbx_rues_quartiers#quartier_…">Libellé</relation>`,
résolu vers le terme quartier. Le quartier Alma Gare manquait dans cet export
parce qu'il avait été supprimé à la main dans Mnesys : la suppression d'un
terme laisse en place les TA qui pointent vers lui (40 rues ici), sans
avertissement.

## Variante : quartiers hors des entrées

```bash
python scripts/ead/rues_quartiers2skos.py --quartiers-hors-tete
```

Écrit `bnr_geo_rbx_rues_quartiers_hors_tete.xml` : mêmes concepts et mêmes
liens, mais les 31 quartiers ne sont plus concepts de tête (ni
`skos:hasTopConcept` ni `skos:topConceptOf`, `skos:inScheme` conservé).
But : que les quartiers restent cibles des TA sans apparaître comme entrées
du thésaurus dans Mnesys. **Échec à l'import (14/11/2026)** : Mnesys liste
tous les concepts du schéma comme entrées, en tête ou non ; les quartiers
restent mélangés aux rues.

## Variante : deux branches « Rues » et « Quartiers »

```bash
python scripts/ead/rues_quartiers2skos.py --deux-branches
```

Écrit `bnr_geo_rbx_rues_quartiers_branches.xml` : deux concepts de tête,
`#branche_rues` (« Rues ») et `#branche_quartiers` (« Quartiers ») ; chaque
rue et chaque quartier a un `skos:broader` vers sa branche, et la branche
liste ses `skos:narrower`. Liens rue → quartier inchangés (1 070). Les
quartiers restent des entrées, mais dans leur branche. À tester à l'import
(14/11/2026).

**Import réussi mais écarté (14/11/2026)** : les deux branches apparaissent
dans Mnesys ; l'utilisateur préfère une seule branche.

## Variante : quartiers en note de la rue

```bash
python scripts/ead/rues_quartiers2skos.py --quartiers-en-note
```

Écrit `bnr_geo_rbx_rues_quartiers_note.xml` : les 938 rues seules, en tête,
sans concept quartier ni `skos:related`. Les quartiers de rattachement
(même règle que les liens) sont écrits en texte dans `skos:scopeNote` :
« Quartier(s) : Entrepont, Centre-Ville, Hommelet ». 909 rues ont la note
(29 sans tracé n'en ont pas).

Choix de la propriété (14/11/2026) : un premier fichier portait le même
texte dans `skos:scopeNote`, `skos:definition` et `skos:note`, préfixé du nom
de la propriété ; Mnesys n'a affiché que `scopeNote`, sous l'intitulé
« note ». Seule `scopeNote` est donc écrite, sans préfixe.

Un essai précédent avec les quartiers dans un thésaurus séparé
(`geo_rbx_quartiers`, importé dans Mnesys) a échoué : Mnesys ne résout pas
un `related` vers un autre thésaurus.

## Sortie au format Mnesys (abandonnée)

Mnesys n'importe que du SKOS : cette sortie ne sert pas.

`results/ead/indexation/mnesys_test/bnr_geo_rbx_rues_quartiers_mnesys.xml` :
les rues au format natif des thésaurus Mnesys, sans passer par SKOS. Format
relevé sur l'export `FRAM59100_THES_000010.xml` (UTF-8 sans BOM, LF,
tabulations, `<title/>` vide, termes triés par id, TA puis EP) :

```xml
<terme id="geo_rbx_rues_quartiers#rue_allee_des_clarisses">
	<title>Allée des Clarisses</title>
	<relation type="TA">Épeule</relation>
	<relation type="EP">Impasse du Couvent</relation>
</terme>
```

Les TA portent le **libellé du quartier** (demande du 14/11/2026), et non un
`ref`. Contrôle : en remplaçant dans l'export Mnesys chaque `ref` par le
libellé du quartier, on obtient exactement ce fichier. Seules les rues y
figurent, comme dans l'export.

## Bilan (14/11/2026)

969 concepts (938 rues, 31 quartiers), 1 070 `related` (rue → quartier),
tous vers un quartier du fichier ; aucun libellé en double. Rues à 1
quartier : 792 ; à 2 : 87 ; à 3 : 19 ; à 4 : 10 ; à 7 : 1 (Avenue Alfred
Motte) ; sans quartier : 29 (sans tracé).
