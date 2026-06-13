# Les liens DAO : `<dao>` et `<daogrp>`

Dans les instruments de recherche EAD transformés
(`results/ead/ead_cor/bnr2mnesys/*.xml`), les **objets numériques associés**
(Digital Archival Objects) relient un composant `<c>` à ses fichiers : images
de diffusion et de conservation, audio, vidéo, PDF, et liens de publication en
ligne. Cette page recense les structures et les cas de figure réellement
présents (37 IR), pour comprendre ce que les scripts d'appariement consomment.

---

## Deux conteneurs

| Conteneur | Nb | Description |
|---|---|---|
| `<daogrp>` | 34 083 | Groupe de plusieurs `<daoloc>` décrivant **un même document** (ses versions accès / conservation / publication) |
| `<dao>` isolé | 1 709 | Un lien unique, **hors** `<daogrp>` |

Les `<dao>` isolés sont presque toujours un simple lien de publication
(`publication:current`, 1 614), parfois un accès audio (90) ; ils n'ont pas de
fichier de conservation à apparier.

    <dao href="https://www.bn-r.fr/ark:/20179/BNRm01781244634uwtxd1" role="publication:current"/>

Un `<daogrp>` contient des `<daoloc>` et, dans 6 664 cas, un `<daodesc>`
optionnel portant une légende :

    <daogrp>
      <daodesc><p>1858 - Distribution solennelle des prix…</p></daodesc>
      <daoloc href="CSV/RBX_CSV_PAL_1858_01.tif" role="preservation:image:first"/>
      <daoloc href="CSV/RBX_CSV_PAL_1858_08.tif" role="preservation:image:last"/>
      <daoloc href="CSV/RBX_CSV_PAL_1858_01.jpg" role="access:image:first"/>
      <daoloc href="CSV/RBX_CSV_PAL_1858_08.jpg" role="access:image:last"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244634UCd4Y1" role="publication:current"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNR52641" role="publication:previous"/>
    </daogrp>

---

## L'élément de lien

Chaque lien (`<daoloc>` dans un groupe, `<dao>` isolé) porte :

| Attribut | Obligatoire | Valeurs |
|---|---|---|
| `href` | oui | Chemin du fichier (relatif au support) ou URL de publication |
| `role` | oui | Usage du lien (voir grammaire ci-dessous) |
| `audience` | non | `internal` (2 930 cas, sur des `access:image` à diffusion restreinte) |

---

## Grammaire des `role`

Le `role` suit le motif **`usage:média[:position]`** :

- **usage** : `access` (diffusion), `preservation` (conservation), `publication`
  (mise en ligne) ;
- **média** : `image`, `audio`, `video`, `pdf` ; pour `publication`, ce champ
  vaut `current` (lien courant) ou `previous` (ancien lien) ;
- **position** (images uniquement) : `first` / `last`, bornes d'une **plage**
  (cf. ci-dessous).

Fréquence des `role` rencontrés :

| role | Occurrences |
|---|--:|
| `publication:current` | 35 697 |
| `publication:previous` | 30 003 |
| `access:image` | 15 190 |
| `preservation:image` | 6 697 |
| `access:image:first` / `:last` | 6 666 / 6 666 |
| `preservation:image:first` / `:last` | 3 290 / 3 234 |
| `preservation:audio` | 2 764 |
| `access:audio` | 1 111 |
| `access:pdf` / `preservation:pdf` | 31 / 31 |
| `access:video` / `preservation:video` | 22 / 19 |

---

## Liens unitaires vs plages `first`/`last`

Deux façons de décrire les fichiers d'un document :

- **unitaire** : un `<daoloc>` par fichier (`access:image`, `preservation:image`) ;
- **plage** : une suite de fichiers décrite par ses seules bornes
  (`access:image:first` + `access:image:last`). Les fichiers intermédiaires
  sont **implicites** et reconstitués par
  [dao_first_last_developpe.py](../scripts/dao_first_last_developpe.md).

---

## Avec ou sans conservation

Dans un groupe, un lien d'accès est censé avoir un lien de conservation
(`preservation:*`) de même média. Quand il manque, l'accès est **orphelin** :
c'est ce que détecte [dao_sans_conservation.py](../scripts/dao_sans_conservation.md).
La présence (ou non) du `preservation` est l'axe structurant des cas ci-dessous.

---

## Les cas de figure (signatures de `<daogrp>`)

En condensant les bornes `first`/`last`, 17 combinaisons de rôles existent. Les
principales :

| Nb | Composition | Lecture |
|--:|---|---|
| 12 202 | `publication:current` + `previous` | **Publication seule** : aucun fichier référencé dans l'IR, juste les liens en ligne |
| 6 684 | `access:image` + `preservation:image` + publications | Image **unitaire avec conservation** (cas nominal) |
| 5 670 | `access:image` + publications | Image **unitaire sans conservation** → orphelin |
| 3 107 | `access:image:[first/last]` + `preservation:image:[first/last]` + publications | **Plage** d'images avec conservation |
| 2 803 | `access:image` + `publication:current` | Image unitaire sans conservation, sans lien `previous` |
| 2 099 | `access:image:[first/last]` + publications | Plage d'images **sans conservation** |
| 1 270 | `access:image:[first/last]` + `publication:current` | Plage d'images sans conservation, sans `previous` |
| 185 | `access:audio` + `access:image:[first/last]` + `preservation:audio` + `preservation:image:[first/last]` + publications | **Fonds sonore** : audio + images associées (pochette, livret) |
| 31 | `access:pdf` + `preservation:pdf` + publications | Document **PDF** |
| 16 / 2 | `access:video` (+ `preservation:video`) + publications | **Vidéo** |

(Le reste : quelques combinaisons mixtes rares — audio sans conservation, image
+ vidéo, audio + image unitaire.)

Deux constats transversaux :

- la grande majorité des groupes portent un couple `publication:current` +
  `previous` ; un sous-ensemble n'a que `current` ;
- environ un tiers des groupes images n'ont **pas** de `preservation` (orphelins
  unitaires ou plages), ce que la chaîne d'appariement cherche à résoudre.

---

## Voir aussi

- [Chaîne d'appariement des DAO](../scripts/dao_appariement.md) — les scripts qui
  exploitent ces structures
- [Script ead_bnr2mnesys](../scripts/ead_bnr2mnesys.md) — produit ces fichiers
