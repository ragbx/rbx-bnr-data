# Script : geogname_applique_referentiel.py

**Emplacement :** `scripts/ead/geogname_applique_referentiel.py`

**Test** de l'application du [référentiel des lieux](geogname_referentiel_lieux.md)
aux `<geogname>` des IR : lit les IR de `results/ead/ead_cor/bnr2mnesys/`
(**non modifiés**) et écrit les IR corrigés dans
**`results/ead/ead_cor/bnr2mnesysgeo/`** (dossier recréé à chaque exécution).
Dernier étage de la chaîne de l'[indexation géographique](indexation_geographique.md).

---

## Traitement

Pour chaque `<geogname>` :

1. **Forme** : retrouvée dans la concordance formes ↔ lieux
   (`data/geo/concordance_geogname_lieux.csv`) par l'état d'origine
   (`results/ead/indexation/rbx-bnr_geogname.csv` : IR, composant, texte et
   `@source` actuels → forme et `@source` d'origine), sinon par son texte
   normalisé (apostrophes déséchappées, espaces).
2. **Lieu** : celui de la forme dans le référentiel des lieux
   (`data/geo/referentiel_geogname.csv`, même regroupement que
   [geogname_referentiel_lieux.py](geogname_referentiel_lieux.md)).
3. **Réécriture** :

   | Attribut | Valeur écrite |
   |---|---|
   | texte | libellé du lieu (Mnesys relie au concept par le libellé seul) |
   | `@source` | `thesaurus--SLASH--bnr_geo_rbx_quartiers.xml`, `…bnr_geo_rbx_rues.xml`, `…bnr_geo_rbx_points.xml` ou `…bnr_geo_communes_pays.xml` |
   | `@coord` | `coord_manuel` du lieu, sinon `coord` (tracé, polygone ou point, `[lat, lon]`) ; conservé tel quel si le lieu n'a pas de géométrie |

   Les autres attributs sont conservés. Les `<geogname>` vides,
   chronologiques ou sans lieu (non toponymes) ne sont pas modifiés.

Aucune validation n'est exigée : par défaut, les lieux encore « à traiter »
sont appliqués comme les autres et comptés à part.

**Statut** de chaque `<geogname>` (rapport) :

- `à traiter` : sa forme pose problème dans la concordance (colonne
  `a_traiter`), ou son lieu a un problème qui lui est propre dans le
  référentiel (pas de géométrie, libellés divergents, lieu à identifier…).
  Le renvoi « n forme(s) à traiter dans la concordance » d'un lieu ne compte
  pas : il vise les autres formes du lieu ;
- `sans réserve` sinon ;
- suffixe `rue et quartier éloignés` : **contrôle spatial** — dans un même
  `<controlaccess>`, une rue indexée à plus de 150 m d'un quartier indexé
  (les deux `<geogname>` sont marqués).

## Options

| Option | Effet |
|---|---|
| `--ir MED_PHO …` | ne traite que ces IR |
| `--sans-reserve` | ne réécrit que les `<geogname>` `sans réserve` et sans incohérence rue / quartier ; les autres restent tels qu'ils sont dans `bnr2mnesys/` (statut suffixé `(non appliqué)`) |
| `--sans-rues-ponctuelles` | **retire** les `<geogname>` de rue représentés par un simple point : rue ancienne laissée telle quelle (`source="rue"`, point d'origine) ou rue rattachée sans tracé ; un `<controlaccess>` vidé est retiré aussi (statut suffixé `retiré (rue ponctuelle)`) |
| `--sans-geogname-sans-source` | **retire** les `<geogname>` restés sans `@source` (cas non appliqués) ; un `<controlaccess>` vidé est retiré aussi (statut suffixé `retiré (sans source)`) |
| `--sans-non-appliques` | avec `--sans-reserve` : **retire** tous les `<geogname>` non appliqués, restés avec leur ancienne `@source` ; un `<controlaccess>` vidé est retiré aussi (statut suffixé `retiré (non appliqué)`) |
| `--sortie <dossier>` | écrit ailleurs que dans `bnr2mnesysgeo/` (dossier recréé ; refusé s'il s'agit de `bnr2mnesys/`) ; le rapport prend le nom du dossier |

## Sorties

- `results/ead/ead_cor/bnr2mnesysgeo/*.xml` : les IR corrigés ;
- `results/ead/indexation/<dossier de sortie>_rapport.csv` : une ligne par
  `<geogname>` — `eadid`, `c_id`, `texte_avant`, `source_avant`,
  `coord_avant`, `lieu_id`, `texte_apres`, `source_apres`, `statut`
  (`sans réserve`, `à traiter`, éventuellement suffixés, ou `chrono`, `vide`,
  `introuvable`).

## Premier test (2026-10-27)

- 37 IR, **12 212 `<geogname>`** : 12 210 réécrits (10 588 vers un lieu sans
  réserve, 1 622 vers un lieu encore à traiter), 1 chronologique
  (`1946-1959` de MED_CP) et 1 non toponyme (`Pas de mention d'adresse`)
  laissés tels quels.
- Par thésaurus : rues 10 031, communes_pays 1 182, quartiers 652, points 345 ;
  les libellés distincts dans les IR sont exactement ceux du référentiel
  (572, 197, 31, 7).
- **Rien d'autre ne change** dans les IR : en dehors des lignes `<geogname>`,
  0 ligne modifiée.
- Taille : 52 Mo → 61 Mo (tracés des voies dans `@coord`).
- **72 `<geogname>` en double** dans un même `<controlaccess>` : un
  composant indexé à la fois sous l'ancien et le nouveau nom d'une voie
  (`boulevard de Paris` + `Boulevard du Général de Gaulle` : 42 ; `rue Neuve`
  + `Rue du Maréchal Foch` : 15…) porte désormais deux fois le même concept.
  Non dédoublonnés : **décision du 2026-10-27, on les garde pour le
  moment** (question à reposer plus tard).

## Version sans réserve de MED_PHO (2026-10-27)

`results/ead/ead_cor/bnr2mnesysgeo_sans_reserve/FR595126101_MED_PHO.xml`
(rapport `bnr2mnesysgeo_sans_reserve_rapport.csv`) : **704 `<geogname>`
réécrits sur 1 017** (rues 517, quartiers 113, points 40, communes et pays
34) ; 313 laissés tels quels — 238 `à traiter` (dont `Centre` 169, `Sainte
Elisabeth` 11, `Gare, rue de la` 7, renommages, voies anciennes, communes à
identifier…), 75 pour une rue et un quartier éloignés (`Barbieux` /
`Alfred Motte, avenue`, `Centre` / `Gare, place de la`…). Aucune autre ligne
modifiée, aucun doublon.

Puis, à la demande (2026-10-27), avec `--sans-rues-ponctuelles` : **62
`<geogname>` de rue réduits à un point retirés** — 61 rues anciennes laissées
de côté (Avenue Alfred Motte, Place de la Gare, Rue de l'Alma…) et la Rue
Notre-Dame, rattachée mais sans tracé —, ainsi qu'un `<controlaccess>` devenu
vide. Le fichier compte alors **954 `<geogname>`** : rues 516, quartiers 113,
points 40 (Parc Barbieux), communes et pays 34, et 251 laissés à l'ancienne
(203 `quartier`, 47 sans source, 1 `rue` sans coordonnées). Hors
`<geogname>`, seules les deux lignes du `<controlaccess>` vidé disparaissent.

Enfin, avec `--sans-geogname-sans-source` : **47 `<geogname>` sans `@source`
retirés** (tous « à traiter » : Gare, rue de la ; Paris, boulevard de ;
squares Pierre Catteau et Pierre Destombes ; départements et communes à
identifier…), et 7 `<controlaccess>` devenus vides. Le fichier compte alors
**907 `<geogname>`, tous avec un `@source`** : 703 vers un thésaurus (rues
516, quartiers 113, points 40, communes et pays 34) et 204 à l'ancienne
(203 `quartier`, 1 `rue`). Hors `<geogname>`, seules disparaissent les
lignes des 8 `<controlaccess>` vidés. Le GeoJSON ne change pas (ces
`<geogname>` n'avaient pas de coordonnées).

Puis avec `--sans-non-appliques` : **204 `<geogname>` non appliqués
retirés** (203 `quartier`, dont `Centre` 169, et 1 `rue`) et 1
`<controlaccess>` de plus devenu vide. **Version finale du test : 703
`<geogname>`, tous rattachés à un thésaurus** (rues 516, quartiers 113,
points 40, communes et pays 34) ; sur 1 017 au départ, 314 retirés. Hors
`<geogname>`, seules disparaissent les lignes des 9 `<controlaccess>` vidés.
GeoJSON : 146 entités (17 polygones, 90 lignes, 38 multi-lignes, 1 point),
toutes rattachées.

## Utilisation

Depuis la racine du dépôt, après
[geogname_referentiel_lieux.py](geogname_referentiel_lieux.md) :

    python scripts/ead/geogname_applique_referentiel.py
    python scripts/ead/geogname_applique_referentiel.py --ir MED_PHO --sans-reserve \
        --sans-rues-ponctuelles --sans-geogname-sans-source --sans-non-appliques \
        --sortie results/ead/ead_cor/bnr2mnesysgeo_sans_reserve
