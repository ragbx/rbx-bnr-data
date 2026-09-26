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
| `coord` | Attribut `@coord` brut : point `[lat, lon]` ou polygone `[[lat, lon], ...]` |

Un attribut absent donne une cellule vide.

## Points d'attention

Relevés au 2026-09-26 (37 IR) :

- **7 `<geogname>` sans `c_id`** : indexés au niveau de l'`archdesc`.
- **`source`** : vide 4 717, `quartier` 636, `adresse` 1, `chrono` 1
  (valeur isolée, probable erreur de saisie) ; le reste en `rue`.
- **6 909 lignes avec `coord`**.

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

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/geogname2csv.py
