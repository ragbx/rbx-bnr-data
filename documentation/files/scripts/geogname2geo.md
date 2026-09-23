# Script : geogname2geo.py

**Emplacement :** `scripts/ead/geogname2geo.py`

Extrait les `<geogname>` géoréférencés des instruments de recherche et en
génère un GeoJSON — comme `controlaccess2skos.py`, dont il partage les
fichiers d'entrée.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/bnr2mnesys/*.xml` | IR transformés ; seuls les enfants `<geogname>` de `<controlaccess>` portant un attribut `coord` sont retenus |
| `data/geo/filaire_voies_roubaix.geojson` | Référentiel officiel des voies de Roubaix, export WFS de Lille Métropole (couche `ville_roubaix:filaire_des_voies_de_la_ville_de_roubaix`, récupéré le 2026-09-22) |

`coord` est soit un point `[lat, lon]` (source `rue`/`adresse`), soit un
polygone `[[lat, lon], ...]` (source `quartier`). GeoJSON attend l'ordre
`lon,lat` ; la conversion est faite par le script. Les `geogname` sans
`coord` (non géoréférencés) sont ignorés.

Un même toponyme (texte + source) revient dans de nombreux IR avec
exactement le même `coord` (vérifié : aucune incohérence de géocodage dans
le corpus) — les sorties ne contiennent donc qu'une entité par toponyme
unique, pas une par occurrence, avec deux compteurs en propriétés :
`documents` (nombre de balises `<geogname>` pour ce toponyme, tous IR
confondus — plusieurs par IR si le toponyme est indexé sur plusieurs `<c>`)
et `instruments_de_recherche` (nombre d'IR distincts où il apparaît).

Quelques `coord` de polygone sont tronqués dans les données sources (limite
de champ en amont) et donc illisibles ; ils sont ignorés avec un
avertissement affiché à l'exécution plutôt que de faire planter le script.

### Alignement des rues sur le filaire officiel

Pour la source `rue`, le point géocodé (souvent approximatif) est remplacé
par la vraie géométrie de la voie — `LineString`, ou `MultiLineString` si
plusieurs tronçons partagent le même nom officiel.

Les noms de rue de la BnR sont à l'envers (`"Achille Screpel, rue"` au lieu
de `"Rue Achille Screpel"`) ; `reconstruire()` les remet à l'endroit avant
comparaison, normalisée (accents, casse), avec `nom_de_la_rue` du filaire.

Sur les 405 toponymes `rue` : 352 correspondances exactes après
reconstruction. Les 53 restants ont été vérifiés à la main
(`CORRECTIONS_RUE` dans le script) : 36 corrigent une coquille ou une
variante mineure côté BnR (accent, article manquant, type de voie
différent — ex. l'appariement automatique proposerait à tort "Menin" →
"Denain", mais "Alsace" est bien "Avenue d'Alsace" et pas une "Rue
d'Alsace" distincte). Les 17 restants n'ont aucune correspondance fiable
dans le filaire actuel (rue disparue/renommée, ou toponyme trop ambigu
comme "Notre-Dame") et gardent leur point géocodé d'origine.

388/405 rues sont donc alignées sur le filaire ; la propriété
`geometrie_officielle` du GeoJSON distingue les deux cas.

## Résultats

**GeoJSON** — `results/ead/indexation/rbx-bnr.geojson`, en WGS84
(EPSG:4326, la seule référence prévue par le format), et
`results/ead/indexation/rbx-bnr_epsg27561.geojson`, la même
`FeatureCollection` reprojetée en Lambert Nord France (EPSG:27561) via
`ogr2ogr` — c'est ce second fichier qu'il faut ouvrir dans QGIS pour que
les coordonnées correspondent au fond de plan utilisé (la conversion
NTF/WGS84 passe par une grille officielle PROJ, pas une formule à la
main). Chaque `Feature` porte une géométrie `Point`/`Polygon` (toponymes
géocodés, anneau de polygone refermé si besoin) ou `LineString`/
`MultiLineString` (rues alignées sur le filaire officiel — voir plus haut),
et les propriétés `source`, `documents`, `instruments_de_recherche`,
`geometrie_officielle` (bool), ainsi qu'une propriété `url` : lien vers la
recherche « Plan de Roubaix » de bn-r.fr, filtrée sur le texte brut de
l'IR —

    resultat.php?type_rech=pl&index[]=<source>&value[]="<toponyme>"&decouvrir_par_terme=<toponyme>

`index[]` reprend directement la `source` du geogname (`rue`, `quartier` ou
`adresse`) : ce sont des index distincts côté site, vérifiés manuellement
un par un, qui correspondent exactement aux trois valeurs de `source`
utilisées ici (contrairement à l'index générique `sujets_tous` de la
recherche avancée classique, plus large et moins précis).
`decouvrir_par_terme` n'affecte que le libellé affiché sur la page de
résultats (fil d'ariane), pas le filtrage.

Pour une rue appariée, `name` porte le **nom officiel** du filaire
(cohérent avec la géométrie alignée), et non plus le texte brut de l'IR ;
ce dernier reste disponible dans `rue_autre_forme`, toujours renseignée
pour la source `rue`. Pour une rue non appariée, `name` retombe sur le
texte BnR reconstruit à l'endroit (pas de nom officiel disponible) et
`rue_autre_forme` reste le texte brut d'origine. Inchangé pour
`quartier`/`adresse` : `name` = texte brut, `rue_autre_forme` absente
(`null`).

**CSV** — un par source, `results/ead/indexation/rbx-bnr_rue.csv`,
`rbx-bnr_quartier.csv`, `rbx-bnr_adresse.csv` : une extraction de
`rbx-bnr.geojson`, régénérée automatiquement à chaque exécution du script
(pas un traitement à part). Mêmes propriétés que le GeoJSON (hors
`source`, constante dans chaque fichier), plus `coordinates` — le contenu
brut de `geometry.coordinates` (JSON), donc la géométrie complète (point,
polygone, ligne ou multi-ligne selon la source), pas un point simplifié.

---

## Utilisation

L'attribut `coord` est déjà présent dans les IR sources (géoréférencement
fait en amont, dans Mnesys) et simplement conservé par
[ead_bnr2mnesys.py](ead_bnr2mnesys.md) ; lancer ce dernier au préalable
n'est donc pas nécessaire pour ce script, juste avoir des IR transformés à
jour dans `bnr2mnesys/`.

Depuis la racine du projet :

    python scripts/ead/geogname2geo.py
