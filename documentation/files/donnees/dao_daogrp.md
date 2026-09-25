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
| `<daogrp>` | 34 234 | Groupe de plusieurs `<daoloc>` décrivant **un même document** (ses versions accès / conservation / publication) |
| `<dao>` isolé | 1 668 | Un lien unique, **hors** `<daogrp>` |

Les `<dao>` isolés sont presque toujours un simple lien de publication
(`publication:current`, 1 657) ; ils n'ont pas de fichier de conservation à
apparier. Une poignée reste un lien d'accès isolé sans conservation ni
publication (`access:audio` 8, `access:image` 2, `access:video` 1).

Ce lien n'existe pas dans la source : c'est le cas d'un `<archdesc>`/`<c>` qui
n'avait **aucun** `<dao>`/`<daogrp>` dans l'IR bn-r d'origine (ici la racine de
la collection ARA_CPS) — [ead_bnr2mnesys.py](../scripts/ead_bnr2mnesys.md) (étape
6, cas 3) lui crée un lien de publication à partir de son `id`.

- Départ (`data/ead/bnr/FR595129901_ARA_01.xml`) : *(aucun `<dao>`/`<daogrp>`)*
- Arrivée (`results/ead/ead_cor/bnr2mnesys/FR595126101_ARA_CPS .xml`) :

      <dao href="https://www.bn-r.fr/ark:/20179/BNRm01781244634uwtxd1" role="publication:current"/>

Un `<daogrp>` contient des `<daoloc>` et, dans 6 762 cas, un `<daodesc>`
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
- **média** : `image`, `audio`, `video`, `pdf`, `ocr_alto` (fichier ALTO
  d'océrisation) ; pour `publication`, ce champ vaut `current` (lien courant)
  ou `previous` (ancien lien) ;
- **position** (images uniquement) : `first` / `last`, bornes d'une **plage**
  (cf. ci-dessous).

Fréquence des `role` rencontrés :

| role | Occurrences |
|---|--:|
| `publication:current` | 35 891 |
| `publication:previous` | 30 003 |
| `access:image` | 15 241 |
| `preservation:image` | 13 218 |
| `access:image:first` / `:last` | 6 764 / 6 764 |
| `preservation:image:first` / `:last` | 4 886 / 4 827 |
| `preservation:audio` | 2 847 |
| `access:audio` | 1 111 |
| `access:pdf` / `preservation:pdf` | 31 / 31 |
| `access:video` / `preservation:video` | 22 / 19 |

`publication:previous`, `access:audio`, `access:pdf`/`preservation:pdf` et
`access:video`/`preservation:video` sont identiques à la précédente mise à
jour de cette page : ce sont `preservation:image` et
`preservation:image:first`/`:last` qui ont le plus progressé (référentiel de
conservation enrichi entre-temps, cf. section suivante).

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

21 combinaisons distinctes de `role` existent dans les `<daogrp>` du corpus.
Classées par famille de média, présence de conservation et présence du lien
`publication:previous`, elles se répartissent **intégralement** dans les 12
catégories suivantes (la somme des effectifs vaut 34 234, soit tous les
`<daogrp>` du corpus) :

| Nb | Composition | Lecture |
|--:|---|---|
| 12 202 | `publication:current` + `previous` | **Publication seule** : aucun fichier référencé dans l'IR, juste les liens en ligne |
| 11 767 | `access:image` + `preservation:image` + `publication:current` + `previous` | Image **unitaire avec conservation** (cas nominal) |
| 4 275 | `access:image:[first/last]` + `preservation:image:[first/last]` + `publication:current` + `previous` | **Plage** d'images avec conservation |
| 1 490 | `access:image` + `publication:current` | Image unitaire **sans conservation**, sans lien `previous` → orphelin |
| 1 373 | `access:image` + `preservation:image` + `publication:current` | Image unitaire avec conservation, sans lien `previous` |
| 946 | `access:image:[first/last]` + `publication:current` | Plage d'images sans conservation, sans lien `previous` |
| 923 | `access:image:[first/last]` + `publication:current` + `previous` | Plage d'images **sans conservation** |
| 515 | `access:image` + `publication:current` + `previous` | Image unitaire sans conservation → orphelin |
| 422 | `access:image:[first/last]` + `preservation:image:[first/last]` + `publication:current` | Plage d'images avec conservation, sans lien `previous` |
| 270 | `access:audio` (+ `access:image:[first/last]`) + `preservation:audio` (+ `preservation:image:[first/last]`) + `publication:current` + `previous` | **Fonds sonore** : audio, seul ou avec images associées (pochette, livret) |
| 31 | `access:pdf` + `preservation:pdf` + `publication:current` + `previous` | Document **PDF** |
| 20 | `access:video` (+ `access:image`) + `preservation:video` (+ `preservation:image`) + `publication:current` + `previous` | **Vidéo** |

Deux anomalies ponctuelles, noyées dans les lignes ci-dessus mais à connaître :

- **71 plages d'images à conservation asymétrique**, comptées dans les deux
  lignes « Plage … avec conservation » (4 275 + 422) : une seule des deux
  bornes `first`/`last` a son fichier de conservation apparié (64 cas où seul
  `first` l'a, 6 où seul `last` l'a, 1 cumulant cette asymétrie à l'absence de
  `publication:previous`) — signe que le référentiel de conservation ne
  couvre pas encore les deux extrémités de la plage ;
- **2 vidéos atypiques**, comptées dans la ligne « Vidéo » (20) : une
  orpheline (`access:video` + publications, aucune conservation, comme un
  `access:image` isolé) et une à conservation complète (image ET vidéo toutes
  deux appariées à leur fichier de conservation).

Deux constats transversaux :

- 30 003 `<daogrp>` sur 34 234 (88 %) portent le couple `publication:current` +
  `previous` ; les 4 231 restants (12 %) n'ont que `current` — un lien
  `previous` suppose que le composant existait déjà dans l'ancien bn-r, ce qui
  n'est pas le cas d'un contenu nouvellement décrit ;
- sur les 21 711 groupes portant une image (unitaire ou en plage), 17 837
  (82 %) ont désormais leur fichier de conservation apparié ; les 3 874
  restants (18 %) sont orphelins, ce que détecte
  [dao_sans_conservation.py](../scripts/dao_sans_conservation.md).

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

**Image unitaire avec conservation** (11 767, ou 1 373 sans lien `previous`) — cas nominal :

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

**Image unitaire sans conservation** (515, ou 1 490 sans lien `previous`) — orphelin, à résoudre par la chaîne
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

**Plage d'images sans conservation** (923, ou 946 sans lien `previous`) :

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

**Fonds sonore** (270) — audio + images associées (pochette, livret), avec
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

**Vidéo** (20 au total : 16 avec une image d'illustration sans conservation +
2 sans image + 2 cas rares — une vidéo orpheline sans conservation, une avec
conservation complète image et vidéo) — accès et conservation dans des formats
différents (`.mov` / `.mp4`), avec une image d'illustration :

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

## Le résumé `<odd>` (donnée maître)

> Pour la marche à suivre pas à pas (corriger un lien, lancer l'app), voir le
> [Guide pratique : corriger un lien via `<odd>`](guide_odd_publication.md).

Depuis [ead_bnr2mnesys.py](../scripts/ead_bnr2mnesys.md) (étape 10), chaque `<c>`
possédant un `<dao>` isolé ou un `<daogrp>` reçoit, juste après, un `<odd>` qui
résume ses liens en clair, un `<p>` par lien :

    <daogrp>
      <daoloc href="MED/MED_CHA/RBX_MED_CHA_0091.tif" role="preservation:image"/>
      <daoloc href="MED/MED_CHA/RBX_MED_CHA_0091.jpg" role="access:image"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244641TPOobF" role="publication:current"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNR22820" role="publication:previous"/>
    </daogrp>
    <odd>
      <p>preservation:image MED/MED_CHA/RBX_MED_CHA_0091.tif</p>
      <p>access:image MED/MED_CHA/RBX_MED_CHA_0091.jpg</p>
      <p>publication:current https://www.bn-r.fr/ark:/20179/BNRm01781244641TPOobF</p>
      <p>publication:previous https://www.bn-r.fr/ark:/20179/BNR22820</p>
    </odd>

Chaque `<p>` suit le format **`role href [audience]`** (le segment `audience` est
omis quand l'attribut est absent ; dans tout le corpus, `audience` ne prend
d'ailleurs que la valeur `internal`).

**Ce `<odd>` est considéré comme la donnée maître** : une correction faite à la
main sur ses `<p>` (ajout, modification, suppression d'un lien) doit être
répercutée sur `<dao>`/`<daogrp>` avec `sync_dao_from_odd()`
(`app/ead_prepublication/ead_preprocess.py`, cf.
[EAD Pré-publication](../../../app/ead_prepublication/README.md)). Deux cas se
présentent selon le moment du cycle de vie de l'IR : sur le fichier de
`results/ead/ead_cor/bnr2mnesys/` (sortie initiale d'`ead_bnr2mnesys.py`), avant
le tout premier import dans Mnesys ; ou, une fois l'IR déjà dans Mnesys, sur un
export XML fraîchement récupéré depuis Mnesys après correction du `<odd>`
**dans Mnesys même** (cycle détaillé dans le
[Guide pratique](guide_odd_publication.md#2-le-cycle-en-3-étapes)) :

L'appariement se fait sur le triplet complet `(href, role, audience)`, pas sur le
`href` seul, qui peut légitimement se répéter dans un même `<daogrp>` sous des
`role` différents (ex. un même pdf en `preservation:pdf`/`access:pdf`, cf.
« PDF » ci-dessus) :

- un triplet du `<odd>` déjà présent parmi les `<dao>`/`<daoloc>` → rien à faire ;
- un triplet du `<odd>` absent → un nouveau `<daoloc>`/`<dao>` est créé (mécanique de
  `dao_ark.add_ark_links`, cf. [dao_ark.py](../scripts/dao_ark.md)) ;
- un `<dao>`/`<daoloc>` existant dont le triplet n'apparaît plus dans le `<odd>` →
  supprimé. Un simple changement de `role`/`audience` sur un `href` se traduit
  donc par une suppression de l'ancien triplet et un ajout du nouveau (pas de
  modification en place).

Le `<odd>` lui-même n'est jamais modifié ni supprimé par cette synchronisation :
il reste la référence pour les exécutions suivantes. Les `<p>` qui ne commencent
pas par un `role` reconnu (cf. grammaire ci-dessus) sont ignorés — ce sont
d'éventuelles notes éditoriales, sans rapport avec les dao, qui peuvent cohabiter
dans le même `<odd>`.

---

## Voir aussi

- [Chaîne d'appariement des DAO](../scripts/dao_appariement.md) — les scripts qui
  exploitent ces structures
- [Script ead_bnr2mnesys](../scripts/ead_bnr2mnesys.md) — produit ces fichiers
