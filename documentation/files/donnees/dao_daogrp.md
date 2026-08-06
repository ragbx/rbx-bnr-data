# Les liens DAO : `<dao>` et `<daogrp>`

Dans les instruments de recherche EAD transformés
(`results/ead/ead_cor/bnr2mnesys/*.xml`), les **objets numériques associés**
(Digital Archival Objects) relient un composant `<c>` à ses fichiers : images
de diffusion et de conservation, audio, vidéo, PDF, et liens de publication en
ligne. Cette page recense les structures et les cas de figure réellement
présents (37 IR), pour comprendre ce que les scripts d'appariement utilisent.

---

## Deux éléments

| Elément | Nb | Description |
|---|---|---|
| `<daogrp>` | 34 083 | Groupe de plusieurs `<daoloc>` décrivant **un même document** (ses versions accès / conservation / publication) |
| `<dao>` isolé | 1 709 | Un lien unique, **hors** `<daogrp>` |

Les `<dao>` isolés sont presque toujours un simple lien de publication
(`publication:current`, 1 614), parfois un accès audio (90) ; ils n'ont pas de
fichier de conservation à apparier.

Ce lien n'existe pas dans la source : c'est le cas d'un `<archdesc>`/`<c>` qui
n'avait **aucun** `<dao>`/`<daogrp>` dans l'IR bn-r d'origine (ici la racine de
la collection ARA_CPS) — [ead_bnr2mnesys.py](../scripts/ead_bnr2mnesys.md) (étape
6, cas 3) lui crée un lien de publication à partir de son `id`.

- Départ (`data/ead/bnr/FR595129901_ARA_01.xml`) : *(aucun `<dao>`/`<daogrp>`)*
- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_ARA_CPS .xml`) :

      <dao href="https://www.bn-r.fr/ark:/20179/BNRm01781244634uwtxd1" role="publication:current"/>

Un `<daogrp>` contient des `<daoloc>` et, dans 6 664 cas, un `<daodesc>`
optionnel portant une légende. Départ / arrivée pour un exemple avec plage
d'images et conservation :

- Départ (`data/ead/bnr/FR595129901_CSV_01.xml`) : rôle simple `image:first`/
  `:last`, `href` en simple nom de fichier, pas de conservation ni de
  publication :

      <daogrp>
        <daodesc><p>1858 - Distribution solennelle des prix aux élèves des écoles académiques</p></daodesc>
        <daoloc href="RBX_CSV_PAL_1858_01.jpg" role="image:first"/>
        <daoloc href="RBX_CSV_PAL_1858_08.jpg" role="image:last"/>
      </daogrp>

- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_CSV.xml`) : rôles
  préfixés `access:`, conservation ajoutée par appariement au référentiel,
  liens de publication ajoutés :

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

## Exemples par cas de figure

Un `<daogrp>` réel pour chacun des principaux cas du tableau ci-dessus, avec son
point de départ dans l'IR source `data/ead/bnr` (vocabulaire `image`/`mp3`/
`mp4`/`pdf`, `href` en simple nom de fichier, pas de conservation ni de
publication) et son arrivée dans l'IR transformé
[ead_bnr2mnesys.py](../scripts/ead_bnr2mnesys.md).

**Publication seule** (12 202) — aucun fichier référencé, juste les liens en ligne :

- Départ (`data/ead/bnr/FR595129901_LAI_01.xml`, série `LAI_D01`) : *(aucun
  `<dao>`/`<daogrp>` — une série n'a pas de fichier propre, seuls ses
  composants `file` en ont)*
- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_LAI.xml`) :

      <daogrp>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244634ZB0IOD" role="publication:current"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNR59358" role="publication:previous"/>
      </daogrp>

**Image unitaire avec conservation** (6 684) — cas nominal :

- Départ (`data/ead/bnr/FR595129901_MED_16.xml`) :

      <dao href="RBX_MED_CHA_0091.jpg" role="image"/>

- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_MED_CHA.xml`) : le tif de
  conservation est retrouvé par appariement au référentiel, le `href` de
  diffusion complété du dossier qu'il indique :

      <daogrp>
        <daoloc href="MED/MED_CHA/RBX_MED_CHA_0091.tif" role="preservation:image"/>
        <daoloc href="MED/MED_CHA/RBX_MED_CHA_0091.jpg" role="access:image"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244641TPOobF" role="publication:current"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNR22820" role="publication:previous"/>
      </daogrp>

**Image unitaire sans conservation** (5 670) — orphelin, à résoudre par la chaîne
d'appariement :

- Départ (`data/ead/bnr/FR595129901_MED_07.xml`) :

      <dao href="RBX_MED_AFF_001_C_2_011.jpg" role="image"/>

- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_MED_AFF.xml`) : aucun
  fichier de conservation trouvé dans le référentiel, le `href` reste donc en
  simple nom de fichier (pas de dossier ajouté) :

      <daogrp>
        <daoloc href="RBX_MED_AFF_001_C_2_011.jpg" role="access:image"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm017812446364GCJyr" role="publication:current"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNR57391" role="publication:previous"/>
      </daogrp>

**Plage d'images sans conservation** (2 099) :

- Départ (`data/ead/bnr/FR595129901_MED_04.xml`) :

      <daogrp>
        <daodesc><p>La Gare</p></daodesc>
        <daoloc href="RBX_MED_CP_A01_L1_S1_078.jpg" role="image:first"/>
        <daoloc href="RBX_MED_CP_A01_L1_S1_081.jpg" role="image:last"/>
      </daogrp>

- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_MED_CP.xml`) :

      <daogrp>
        <daodesc><p>La Gare</p></daodesc>
        <daoloc href="RBX_MED_CP_A01_L1_S1_078.jpg" role="access:image:first"/>
        <daoloc href="RBX_MED_CP_A01_L1_S1_081.jpg" role="access:image:last"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244637UUXitF" role="publication:current"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNR48281" role="publication:previous"/>
      </daogrp>

**Fonds sonore** (185) — audio + images associées (pochette, livret), avec
conservation des deux médias :

- Départ (`data/ead/bnr/FR595129901_MED_15.xml`) : deux `<dao>` isolés pour
  l'audio (anciennes URL absolues du site `bn-r.fr/musique/`) à côté d'un
  `<daogrp>` pour la plage d'images :

      <dao href="http://www.bn-r.fr/musique/RBX_MED_FLRS_FLR_78_0101_A_01.mp3" role="mp3"/>
      <dao href="http://www.bn-r.fr/musique/RBX_MED_FLRS_FLR_78_0101_B_02.mp3" role="mp3"/>
      <daogrp>
        <daodesc><p>Valse favorite/Caprice d'oiseau</p></daodesc>
        <daoloc href="RBX_MED_FLRS_FLR_78_0101_3.jpg" role="image:first"/>
        <daoloc href="RBX_MED_FLRS_FLR_78_0101_4.jpg" role="image:last"/>
      </daogrp>

- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_MED_FLRS.xml`) : tout
  est fusionné dans un seul `<daogrp>` ; les wav de conservation (variantes
  `96kHz24B`/`44kHz24B`) et les tif de la plage sont ajoutés par appariement,
  les URL absolues des mp3 réécrites en chemin relatif :

      <daogrp>
        <daodesc><p>Valse favorite/Caprice d'oiseau</p></daodesc>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLR_78_0101_A_01_96kHz24B.wav" role="preservation:audio"/>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLR_78_0101_A_01_44kHz24B.wav" role="preservation:audio"/>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLR_78_0101_B_02_96kHz24B.wav" role="preservation:audio"/>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLR_78_0101_B_02_44kHz24B.wav" role="preservation:audio"/>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLRS_FLR_78_0101_3.tif" role="preservation:image:first"/>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLRS_FLR_78_0101_4.tif" role="preservation:image:last"/>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLRS_FLR_78_0101_A_01.mp3" role="access:audio"/>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLRS_FLR_78_0101_B_02.mp3" role="access:audio"/>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLRS_FLR_78_0101_3.jpg" role="access:image:first"/>
        <daoloc href="MED/MED_FLRS/FLR_78_0101/RBX_MED_FLRS_FLR_78_0101_4.jpg" role="access:image:last"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm017812446381dZtjQ" role="publication:current"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNR23141" role="publication:previous"/>
      </daogrp>

**PDF** (31) :

- Départ (`data/ead/bnr/FR595129901_OBS_01.xml`) :

      <dao href="RBX_OBS_JOU_QLI_001.pdf" role="pdf"/>

- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_OBS_JOU.xml`) : ici le
  même fichier sert à la fois de master de conservation et de diffusion :

      <daogrp>
        <daoloc href="OBS/OBS_JOU/RBX_OBS_JOU_QLI_001.pdf" role="preservation:pdf"/>
        <daoloc href="OBS/OBS_JOU/RBX_OBS_JOU_QLI_001.pdf" role="access:pdf"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244641l7J5O7" role="publication:current"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNR25856" role="publication:previous"/>
      </daogrp>

**Vidéo** (16/2) — accès et conservation dans des formats différents (`.mov` /
`.mp4`), avec une image d'illustration :

- Départ (`data/ead/bnr/FR595129901_MDF_01.xml`) : deux `<dao>` isolés, dont
  une ancienne URL absolue du site `bn-r.fr/video/` :

      <dao href="RBX_MDF_MTX_1409_D01.jpg" role="image"/>
      <dao href="http://www.bn-r.fr/video/MTX/RBX_MDF_MTX_2014_D01.mp4" role="mp4"/>

- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_MDF_MTX.xml`) : fusionnés
  en un `<daogrp>`, le mov de conservation ajouté par appariement, l'URL
  réécrite en chemin relatif :

      <daogrp>
        <daoloc href="MDF/MDF_MTX/RBX_MDF_MTX_2014_D01.mov" role="preservation:video"/>
        <daoloc href="MDF/MDF_MTX/RBX_MDF_MTX_2014_D01.mp4" role="access:video"/>
        <daoloc href="RBX_MDF_MTX_1409_D01.jpg" role="access:image"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244634p2FaYj" role="publication:current"/>
        <daoloc href="https://www.bn-r.fr/ark:/20179/BNR3" role="publication:previous"/>
      </daogrp>

---

## Voir aussi

- [Chaîne d'appariement des DAO](../scripts/dao_appariement.md) — les scripts qui
  exploitent ces structures
- [Script ead_bnr2mnesys](../scripts/ead_bnr2mnesys.md) — produit ces fichiers
