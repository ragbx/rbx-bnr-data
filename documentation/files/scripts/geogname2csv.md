# Script : geogname2csv.py

**Emplacement :** `scripts/ead/geogname2csv.py`

Extrait tous les `<geogname>` des instruments de recherche dans un CSV
unique, **une ligne par élément** `<geogname>` — sans dédoublonnage ni
filtrage, contrairement à [geogname2geo.py](geogname2geo.md) qui ne garde
que les toponymes géoréférencés, regroupés par toponyme unique.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/bnr2mnesys/*.xml` | IR transformés ; tous les `<geogname>` sont retenus, avec ou sans `source`/`coord` |
| `results/ead/indexation/rbx-bnr_geogname_origine.csv` | État de référence **figé** : même extraction (colonnes `eadid` à `coord`), faite sur les IR du commit `d6ea6ad`, avant les corrections automatiques du 2026-09-26. À ne pas régénérer |
| `data/geo/filaire_voies_roubaix.geojson` | Filaire officiel des voies de Roubaix (cf. [geogname2geo.py](geogname2geo.md)), pour retrouver l'`id` des tronçons |

Dans le corpus actuel, tous les `<geogname>` sont enfants d'un
`<controlaccess>`.

## Résultat

`results/ead/indexation/rbx-bnr_geogname.csv` (UTF-8) :

| Colonne | Contenu |
|---|---|
| `eadid` | Identifiant de l'IR (`eadheader/eadid`) |
| `c_id` | `@id` du `<c>` ancêtre le plus proche ; vide si le `<geogname>` est indexé au niveau de l'`archdesc` |
| `geogname` | Texte de l'élément |
| `source` | Attribut `@source` (`rue`, `quartier`, `adresse`…) |
| `coord` | Attribut `@coord` brut : point `[lat, lon]`, polygone de quartier `[[lat, lon], ...]`, ou tracé de rue `[[lat, lon], ...]` (un tronçon) / `[[[lat, lon], ...], ...]` (plusieurs) |
| `geogname_origine` | Texte dans l'état de référence (avant corrections) |
| `source_origine` | `@source` dans l'état de référence (avant corrections automatiques) |
| `coord_origine` | `@coord` dans l'état de référence |
| `nom_officiel` | Pour une rue dont `coord` est un tracé du filaire : `nom_de_la_rue` de la voie dans le filaire (ex. `Bordeaux, quai de` → `Quai de Bordeaux`). Vide sinon. N'est jamais écrit dans les IR |
| `filaire_id` | Pour une rue dont `coord` est un tracé du filaire : `id` des tronçons de la voie dans le filaire (`filaire_des_voies_de_la_ville_de_roubaix.N`, séparés par ` \| `). Vide sinon |

`nom_officiel` et `filaire_id` sont déduits en comparant les coordonnées du
`coord` au filaire, pas le libellé : ils rendent visible l'appariement fait
lors de l'alignement (cf. historique), qui n'est pas stocké ailleurs.

Un attribut absent donne une cellule vide. Une ligne a été corrigée quand
`geogname`, `source` ou `coord` diffère de sa valeur `_origine`.

Chaque `<geogname>` est rapproché de sa version d'origine par `eadid`,
`c_id`, texte et rang de ce texte dans le composant. Si le texte a été
corrigé, repli : dans un composant où il reste exactement un `<geogname>`
sans correspondance de chaque côté, les deux sont appariés. Les
suppressions de `<geogname>` (vides de MED_LET, chrono de MED_CP) ne sont
pas tracées ainsi : voir l'historique ci-dessous.

## Points d'attention

Relevés au 2026-09-26 (37 IR) :

- **7 `<geogname>` sans `c_id`** : indexés au niveau de l'`archdesc`.
- **`source`** : vide 1 389, `quartier` 636, `adresse` 330 ; le reste en
  `rue` (9 856).
- **Rues** : 9 365 rattachées au filaire (tracé dans `coord`, `filaire_id`
  renseigné, 478 voies distinctes) ; 491 non appariées (41 valeurs, ex.
  `Gare, rue de la`, `Paris, boulevard de`) gardent leur point d'origine
  (150) ou n'ont pas de `coord` (341). Parmi elles, de
  grandes voies roubaisiennes absentes du filaire (boulevard de Paris, rue
  Pauvrée, rue des Longues Haies, quais d'Anvers et de Calais…) et des
  parcs/squares, qui ne sont pas des voies.

Les `<geogname>` vides éventuels sont **conservés** dans le CSV pour
signalement : filtrer sur `geogname` vide pour les retrouver.

### Historique : `<geogname>` vides de MED_LET

La première extraction (2026-09-26) comptait 55 `<geogname source="rue"/>`
vides, sans texte ni `coord`, tous dans `FR595126101_MED_LET`. Ils étaient
déjà vides dans l'IR BnR d'origine (`data/ead/bnr/FR595129901_MED_03.xml`) :
des index « rue » créés mais jamais renseignés. La source BnR en comptait
56 ; le 56e (`<geogname/>` sans `@source`, composant `Let_0567`) avait déjà
disparu à la transformation [ead_bnr2mnesys](ead_bnr2mnesys.md).

Ils ont été corrigés à la main le même jour, directement dans
`results/ead/ead_cor/bnr2mnesys/FR595126101_MED_LET.xml`. La source BnR
n'a pas été modifiée : relancer `ead_bnr2mnesys.py` sur `MED_03` les
réintroduirait.

### Historique : période indexée en geogname dans MED_CP

Dans `FR595126101_MED_CP`, composant `CP_A06_L2_S4_013` (« Le Tramway »),
la période `1946-1959` était balisée `<geogname source="chrono">` au lieu
de `<subject source="chrono">` — erreur déjà présente dans l'IR BnR
d'origine (`data/ead/bnr/FR595129901_MED_04.xml`). Corrigée à la main le
2026-09-26 dans `results/ead/ead_cor/bnr2mnesys/FR595126101_MED_CP.xml`
(passée en `<subject source="thesaurus--SLASH--bnr_chrono.xml">`) ; même
remarque que ci-dessus pour une relance d'`ead_bnr2mnesys.py`.

### Historique : voies sans `source` passées en `rue`

Au 2026-09-26, 4 717 `<geogname>` n'avaient pas de `@source`, dont environ
3 500 libellés de voie (repérés par un mot de voie : rue, boulevard, place,
parc…). Chaque libellé a été comparé, après remise à l'endroit et
normalisation (accents, casse, ponctuation — mêmes règles que
[geogname2geo.py](geogname2geo.md)), aux `<geogname source="rue">` du
corpus et au filaire officiel des voies de Roubaix :

| Statut | Valeurs | Lignes | Traitement |
|---|---|---|---|
| 1 — forme identique en `rue` dans le corpus | 346 | 2 904 | corrigé |
| 2 — forme voisine en `rue` (accent, casse, tiret) | 21 | 228 | corrigé |
| 3 — filaire officiel seulement | 101 | 196 | corrigé (cf. ci-dessous) |
| 4 — aucune correspondance | 70 | 185 | à traiter |

Pour les statuts 1 et 2, les 3 132 `<geogname>` concernés (12 IR, dont
2 747 dans `MED_PAR`) ont reçu `source="rue"`, et le `coord` de la forme
`rue` du corpus quand elle en avait un (2 817 cas ; aucun conflit de
`coord` entre formes). Le libellé n'a pas été modifié. 27 valeurs restent
sans `coord`, faute d'en avoir un côté corpus (ex. `Gare, rue de la`,
`Paris, boulevard de`, `Moulin, rue du`). Les valeurs d'avant correction
sont dans `source_origine`/`coord_origine`.

Correction automatique faite le 2026-09-26 directement dans
`results/ead/ead_cor/bnr2mnesys/*.xml` ; même remarque que ci-dessus pour
une relance d'`ead_bnr2mnesys.py`.

### Historique : `coord` des rues remplacé par le tracé du filaire

Le même jour, pour **toute** `<geogname source="rue">` appariée au filaire
officiel — `coord` venus de Mnesys, statuts 1-2 ci-dessus, et les 196 du
statut 3, qui ont reçu `source="rue"` à cette occasion — le `coord` a été
remplacé par le tracé complet de la voie dans le filaire, au lieu d'un
point. L'appariement suit exactement la règle de
[geogname2geo.py](geogname2geo.md) (nom remis à l'endroit et normalisé,
plus `CORRECTIONS_RUE`) ; aucun nom officiel ambigu n'était en jeu.

Format, dans l'ordre `[lat, lon]` des IR (inverse du GeoJSON) et avec les
coordonnées du filaire telles quelles : `[ [ lat, lon ], ... ]` pour une
voie d'un seul tronçon, `[ [ [ lat, lon ], ... ], [ ... ] ]` pour plusieurs
tronçons (dans l'ordre du filaire). La plus longue valeur produite fait
~1 200 caractères, loin des ~15 000 où des polygones de quartier ont été
tronqués en amont.

9 317 `<geogname>` concernés dans 22 IR. Puis 48 de plus, après ajout à
`CORRECTIONS_RUE` de 10 graphies validées à la main : 6 variantes de
corrections déjà validées (`Inkermann, rue d'`, `Alsace, rue d'`,
`Barbe d'or, rue`, `Saint-Hubert, rue`, `Sept-Ponts, rue des`,
`Sept-Ponts, Rue des` — 34 geogname) et 4 qui ne diffèrent du filaire que
par l'article (`Vivier, rue de` → Rue du Vivier, `Audenaerde, place d'` →
Place Audenaerde, `Lannes, rue de` → Rue Lannes, `Lafontaine, rue` → Rue La
Fontaine — 14 geogname) ; soit 9 365 au total. Les rues non appariées sont
inchangées. Anciennes valeurs dans `source_origine`/`coord_origine`.

Écartés (non validés) : les rapprochements où seul le type de voie diffère
(`Fosse aux Chênes, rue de la` → Avenue…, `Union, rue de l'` → Avenue…,
`Villas, avenue des` → Allée…, `Potennerie, parc de la` → Rue…, etc.).

### Historique : parc Barbieux passé en `adresse`

Le parc Barbieux n'est pas une voie et n'est donc pas dans le filaire. Le
2026-09-26, les 328 `<geogname>` `Barbieux, parc` (260) et `Parc Barbieux`
(68), tous en `source="rue"` (dont 70 passés en `rue` le même jour, cf.
ci-dessus ; 258 l'étaient déjà), ont été basculés en `source="adresse"`
avec un `coord` unique fourni à la main :
`[50.67722273760695, 3.1625329346302813]` (ordre `[lat, lon]` des IR ; la
valeur fournie était au format GeoJSON `[lon, lat]`). Libellés inchangés.

Même traitement pour un `<geogname source="rue">Barbieux</geogname>` isolé
de `FR595126101_MED_CP` (composant `CP_A16_L2_S3_019`, carte postale
« Café - Laiterie - Parc Barbieux - Roubaix ») : passé en `adresse` avec le
même `coord`, et libellé corrigé en `Parc Barbieux` — seule correction de
texte à ce jour (`geogname_origine` = `Barbieux`). Soit 329 `<geogname>` au
total.

Non concernés : `Barbieux, rue de` (voie, alignée sur le filaire) et
`Barbieux` en `quartier`.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/geogname2csv.py
