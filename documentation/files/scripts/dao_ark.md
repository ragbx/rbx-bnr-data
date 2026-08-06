# Module : dao_ark.py

**Emplacement :** `scripts/ead/dao_ark.py`

Module utilitaire (pas de point d'entrée en ligne de commande) portant la
mécanique d'insertion des liens DAO/ARK dans un EAD. Partagé entre
[ead_bnr2mnesys.py](ead_bnr2mnesys.md) (pipeline complet bn-r → Mnesys) et
l'application de bureau **`app/ead_dao_converter`** (pré-traitement avant
publication), qui l'importe via un ajout de `scripts/ead/` à `sys.path`
(déclaré aussi dans son spec PyInstaller, pour que l'exécutable autonome
l'embarque).

---

## Interface

| Fonction | Rôle |
|---|---|
| `merge_daogrp(element)` | Fusionne dans le premier `<daogrp>` le contenu des `<daogrp>` suivants, quand un même `<archdesc>`/`<c>` en contient plusieurs en enfants directs. Ignore les doublons : `<daodesc>` de même texte, `<dao>`/`<daoloc>` de même couple `(href, role)` |
| `add_ark_links(element, link_builder, tags=("archdesc", "c"))` | Pour chaque élément de `tags`, ajoute les liens renvoyés par `link_builder(el)` (liste de `(href, role)`) sous forme de `<dao>`/`<daoloc>` — voir cas ci-dessous. Retourne le nombre de liens effectivement ajoutés |

`link_builder` est fourni par l'appelant : `ead_bnr2mnesys.py` y construit les
ARK bn-r (`publication:current` / `publication:previous`, voir son étape 6) ;
`app/ead_dao_converter` y construit d'autres liens selon son propre besoin de
pré-traitement.

## Cas traités par `add_ark_links`

- `<daogrp>` déjà présent → ajout d'un `<daoloc>` par lien dans le groupe
  (sans doublon de `role`) ;
- `<dao>` présent (sans `<daogrp>`) → transformation en `<daogrp>` contenant
  un `<daoloc>` reprenant les attributs de l'ancienne `<dao>`, puis un
  `<daoloc>` par lien ;
- ni `<dao>` ni `<daogrp>` → création d'un `<dao>` (lien unique) ou d'un
  `<daogrp>` (plusieurs liens).

Dans les deux derniers cas, la balise est insérée avant le premier enfant
`<c>` ou `<dsc>` s'il existe.

---

## Utilisation dans ead_bnr2mnesys.py

- Étape 5 (fusion des `<daogrp>` multiples) → `merge_daogrp` ;
- Étape 6 (ajout des liens ARK bn-r) → `add_ark_links`, via `_add_dao_ark`.

Voir [Transformations appliquées](ead_bnr2mnesys.md#transformations-appliquées)
pour le détail métier de ces deux étapes.
