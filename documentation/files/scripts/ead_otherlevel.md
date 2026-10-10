# Script : ead_otherlevel.py

**Emplacement :** `scripts/ead/ead_otherlevel.py`

Rend conformes à la DTD EAD 2002 les niveaux de description hors liste.

L'attribut `level` d'un `<c>` n'admet que onze valeurs : `class`,
`collection`, `file`, `fonds`, `item`, `otherlevel`, `recordgrp`, `series`,
`subfonds`, `subgrp`, `subseries`. Toute autre valeur — dans les IR,
uniquement `subfile` — est reportée dans l'attribut `otherlevel`, placé
juste après `level` :

```xml
<c level="subfile" id="…">
<c level="otherlevel" otherlevel="subfile" id="…">
```

C'est un **correctif appliqué directement** aux IR de
`results/ead/ead_cor/bnr2mnesys/` (base de travail), à la suite de
[ead_tri_unitid.py](ead_tri_unitid.md),
[ead_nettoie_texte.py](ead_nettoie_texte.md) et
[ead_remonte_controlaccess.py](ead_remonte_controlaccess.md).

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/ead_otherlevel.py              # à blanc : décompte par IR
    python scripts/ead/ead_otherlevel.py --appliquer  # réécrit les IR

Le script est rejouable : sur des IR déjà corrigés, il ne modifie rien.

## Application du 2026-10-09

1 403 `<c>` corrigés, dans 5 IR :

| IR | `subfile` |
|---|---|
| `MED_DIL` | 1 053 |
| `MED_PRO` | 246 |
| `MED_PHO` | 57 |
| `MED_IMA` | 33 |
| `MED_PAR` | 14 |

Tous étaient placés sous un `file` ; 211 contiennent des `item`, les 1 192
autres sont des composants terminaux.

Vérification : seules les 1 403 lignes d'ouverture de ces `<c>` ont changé,
et l'erreur de validation correspondante a disparu.

## Écarts à la DTD restants

12 IR sur 37 sont valides. Les 25 autres ne le sont pas pour deux raisons :

| Écart | Occurrences | IR | Statut |
|---|---|---|---|
| Attribut `coord` sur `<geogname>` | 6 909 | 21 | **considéré comme correct**, à ne pas modifier (décision du 2026-10-09) |
| Élément `<resource>` | 806 | 11 | **question ouverte**, voir ci-dessous |

### Question ouverte : `<resource>` → `<relatedmaterial>` ?

Notée le 2026-10-09, non tranchée et non appliquée.

`<resource>` n'est pas un élément EAD. Les 806 occurrences ont toutes la
même forme, un seul `<extref>`, placé juste après le `<did>` :

```xml
<resource>
  <extref href="http://www.bn-r.fr/fr/notice.php?id=CP_A01_L1_S1_053%2F070">Autres vues de la gare</extref>
</resource>
```

Elles renvoient vers d'autres contenus de la bn-r : 680 vers une autre
notice (`notice.php?id=…`, le plus souvent « Autres vues de… »), 39 vers un
espace thématique (« Une forme enrichie ici », `MED_PUB`), 17 vers un ARK,
9 vers d'autres pages bn-r, 1 vers un site externe ; 52 n'ont pas de `href`
et 8 ont le `href` et le libellé inversés. Les `<otherfindaid>` des mêmes
fichiers (336) pointent, eux, presque tous vers des sites externes.

Proposition : remplacer `<resource>` par `<relatedmaterial>`, sans toucher
au contenu. C'est l'élément EAD des documents liés, il accepte directement
un `<extref>` à cet emplacement (vérifié contre la DTD) et il préserve la
distinction liens internes / liens externes.

Alternatives écartées : `<otherfindaid>` (valide, mais mêlerait liens
internes et externes), `<separatedmaterial>` (documents retirés du fonds),
`<altformavail>` (adapté aux 39 « forme enrichie », mais exige un `<p>`
autour du lien).

À vérifier avant de décider : comment Mnesys importe et affiche
`<relatedmaterial>`. Le changement ne
règle pas le contenu des liens (adresses de l'ancien site, liens sans
`href`).
