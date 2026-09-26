# Script : geogname_referentiel.py

**Emplacement :** `scripts/ead/geogname_referentiel.py`

Propose le **référentiel des `<geogname>`** : une ligne par forme BnR
(libellé + `@source` d'origine), rattachée quand c'est possible à un objet
de référence actuel — voie, espace public, quartier ou commune — avec un
identifiant pérenne et la géométrie à utiliser. Le référentiel est la
**référence en amont** : les corrections des IR doivent en découler.

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
2. libellé de type parc / square / jardin → **espace public** BD TOPO ;
3. **concordance** : ancien nom → voie actuelle, sauf si une voie homonyme
   existait déjà en 1987 (on la garde, avec une alerte) ; variante → voie
   actuelle, seulement sans voie homonyme ;
4. `CORRECTIONS_RUE` ;
5. **voie** par nom normalisé (sans accents, casse, ponctuation ni
   articles) dans TOPO et la BD TOPO — objets BD TOPO retrouvés aussi par
   l'identifiant BAN —, puis filaire MEL ;
6. **espace public** BD TOPO ;
7. **commune** du Nord (libellé seul ou qualifié « (Nord) ») ; **pays**
   (courte liste) ; **lieu hors Nord** (qualificatif entre parenthèses) ;
8. **quartier** BD TOPO (`zone_d_habitation`, nature « Quartier ») ;
9. sinon : voie non rattachée, quartier absent de la BD TOPO, ou « à
   identifier ».

## Sortie

`data/geo/referentiel_geogname.csv` — une ligne par forme :

| Colonne | Contenu |
|---|---|
| `forme_bnr`, `source_bnr` | Libellé et `@source` d'origine |
| `occurrences`, `ir` | Nombre de `<geogname>`, IR concernés |
| `annee_min`, `annee_max` | Années des documents indexés |
| `coord_origine` | `point` / `polygone` si un `coord` existait à l'origine |
| `categorie` | voie, espace public, quartier, commune, lieu hors Nord, pays, non toponyme, à identifier |
| `source_cible` | `@source` proposée (`rue`, `adresse`, `quartier`…) |
| `libelle_retenu` | Libellé à écrire (la forme BnR, sauf décision) |
| `ref_nom`, `ref_id` | Objet de référence : nom et identifiant (`59512_<code>` pour une voie, `cleabs` BD TOPO pour un espace public ou un quartier, `INSEE <code>` pour une commune, id MEL en secours) |
| `ref_origine` | TOPO, BD TOPO (table), filaire MEL |
| `geometrie` | Géométrie à utiliser : objets BD TOPO, filaire MEL, point manuel, ou `point d'origine` / `polygone d'origine` à défaut |
| `methode` | Comment le rattachement a été fait |
| `alerte` | Cas à trancher : renommage, voie actuelle plus récente que les documents, absente des référentiels, espace public… |
| `validation`, `remarque` | **À remplir à la main** — conservées d'une exécution à l'autre |

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
référentiels actuels : rue de la Gare, rue du Moulin, rue Saint-Georges,
rue Pauvrée, rue des Longues Haies…).

---

## Utilisation

Depuis la racine du projet, après [geogname_concordance.py](geogname_concordance.md) :

    python scripts/ead/geogname_referentiel.py                     # extraits BD TOPO déjà présents
    python scripts/ead/geogname_referentiel.py --bdtopo <.gpkg>    # (ré)extrait la BD TOPO
