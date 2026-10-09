# Script : ead_nettoie_texte.py

**Emplacement :** `scripts/ead/ead_nettoie_texte.py`

Nettoie le texte des IR : HTML stocké comme du texte, apostrophes échappées,
défauts d'encodage, retours chariot et espaces superflus.

C'est un **correctif appliqué directement** aux IR de
`results/ead/ead_cor/bnr2mnesys/` (base de travail), comme
[ead_tri_unitid.py](ead_tri_unitid.md). L'état des IR avant correctifs est
conservé dans `results/ead/ead_cor/archives/bnr2mnesys_20261009/`.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/ead_nettoie_texte.py              # à blanc : décompte seulement
    python scripts/ead/ead_nettoie_texte.py --appliquer  # réécrit les IR

Le script est rejouable : sur des IR déjà nettoyés, il ne modifie rien.

## Périmètre

Tous les éléments portant du texte sont traités, sauf :

- les `<odd>` (résumé des liens, donnée maître) et les `<dao>`/`<daoloc>`,
  jamais modifiés ; les `<daodesc>` sont traités ;
- les contenus déjà balisés en ligne (`<emph>`, `<lb/>`, et tout élément
  placé au milieu d'un texte), produits par un passage précédent.

## Traitements

Appliqués dans cet ordre au texte de chaque élément.

### 1. Encodage

- Séquences UTF-8 lues comme du Windows-1252 : `FraternitÃ©` → `Fraternité`.
  Seules sont reconnues les séquences commençant par `Ã` ou `Â`, par `â€`
  ou par `Å` suivi d'un guillemet ; une séquence qui ne se décode pas est
  laissée telle quelle (`Âge` reste `Âge`).
- Caractères de contrôle U+0080 à U+009F, qui sont des octets Windows-1252
  mal décodés : U+009C → `œ`, U+0096 → `–`, U+0085 → `…`, etc.
- U+0092 (et U+0091) est rendu par une **apostrophe droite** `'`, et non
  par l'apostrophe typographique d'origine : c'est la forme employée dans
  tous les titres et termes d'index du corpus.

### 2. Apostrophes échappées

`\'` → `'` (`Epeule, rue de l\'` → `Epeule, rue de l'`).

### 3. HTML → EAD

Les balises HTML présentes en clair dans le texte (`&lt;p&gt;…` dans le
fichier) sont converties :

| HTML | EAD |
|---|---|
| chaque paragraphe (`p`, `div`, `h5`, rangée de tableau) | un `<p>` ; dans `<physdesc>`, `<descrules>` et `<creation>`, une ligne séparée de la précédente par `<lb/>` |
| `em`, `strong`, `sup` | `<emph render="italic">`, `"bold"`, `"super"` |
| `a href` | `<extref href>` (texte seul dans `<physdesc>`, où la DTD l'interdit) |
| `br` | `<lb/>` |
| `ul` / `li` | `<list>` / `<item>` |
| cellules d'une rangée de tableau | séparées par ` ; ` |
| `span`, `img`, attributs (`style`, `align`, `class`…) | abandonnés |

Les paragraphes vides sont abandonnés. Un élément vide une fois nettoyé est
supprimé, ainsi que son parent s'il ne contient plus rien.

### 4. Espaces

- Retours chariot et sauts de ligne remplacés par une espace. Dans un `<p>`
  sans HTML, une ligne vide sépare deux paragraphes.
- Espaces multiples réduites à une seule (insécable si la suite en
  contenait une) ; espaces de début et de fin supprimées.
- Exception : les `<persname>`/`<corpname>` contenant une suite de trois
  espaces ou plus sont seulement rognés. Ce sont plusieurs noms saisis dans
  un même élément (`Decroix, Jules     Vernier     Verley & Cie`), à
  traiter avec l'indexation.

Les valeurs d'attributs reçoivent les traitements 1 et 2 et sont rognées.

## Application du 2026-10-09

| Correction | Nombre |
|---|---|
| Éléments contenant du HTML | 45 258 |
| Retours chariot | 4 663 |
| Textes aux espaces corrigées | 3 741 |
| Paragraphes ajoutés (un `<p>` scindé en plusieurs) | 946 |
| Apostrophes échappées | 746 |
| Caractères de contrôle Windows-1252 | 529 |
| Séquences mal décodées | 47 |
| Attributs rognés (`@normal`, `extref/@href`) | 46 |
| Éléments vides supprimés | 9 |

Balises EAD créées : 1 513 `<emph>` (1 106 italique, 357 gras, 50
exposant), 190 `<lb/>`, 59 `<extref>` en ligne, 2 `<list>`.

Non corrigés : 10 `<persname>`/`<corpname>` à noms multiples, dans `MED_LET`.

Vérifications faites contre l'archive : texte de chaque élément identique
aux balises et espaces près (seul écart : le ` ; ` ajouté entre deux
cellules d'un tableau de `MED_PAR`), composants, liens et `<odd>` inchangés,
aucune nouvelle erreur de validation DTD, ordre des cotes toujours
respecté.

## Effets de bord à connaître

- L'`<eadid>` de `ARA_CPS` a perdu son espace finale
  (`FR595126101_ARA_CPS`). La colonne `ir` de
  `results/ead/ead_cor/concordance_id.csv` la contient toujours.
- Les formes de `<geogname>` écrites avec `\'` ont changé. Le référentiel
  `data/geo/referentiel_geogname.csv` et les extractions de
  `results/ead/indexation/` (cf.
  [Indexation géographique](indexation_geographique.md)), établis sur les
  anciennes formes, sont à régénérer avant d'être réutilisés.
