# La Grand-Plage -- Médiathèque et Archives de Roubaix : grille de lecture des éléments `<dao>` / `<daogrp>` des instruments de recherche

date : 25/09/2026

Ce document, rédigé à destination de la société Boscop, explique comment lire le contenu des éléments `<dao>` / `<daogrp>`  présents dans les composants `c` des instruments de recherche encodé en EAD par la Grand-PLage à l'aide du logiciel Mnesys.

À la date de rédaction de ce document, les règles qui suivent ne sont pas encore en production au sein de la Grand-Plage. En cas d'incompatibilité majeure avec le fonctionnement de Ligeo diffusion, elles peuvent encore être amendées.

---

## 1. Fonctions des éléments `<dao>` / `<daogrp>`

Les éléments `<dao>` / `<daogrp>` remplissent deux fonctions à la fois :
- pointer vers les fichiers numériques liés au document :
  - version de diffusion, destinée à être publiée sur Ligeo,
  - version de conservation,
  - éventuellement fichier(s) issu(s) de l'océreisation
- porter le ou les identifiants pérennes (lien ark) de la notice.
 
Un même `<daogrp>` mélange donc typiquement les deux types de liens, par exemple pour une image avec sa conservation et sa publication :
```xml
    <daogrp>
      <daoloc href="MED/MED_CHA/RBX_MED_CHA_0091.tif" role="preservation:image"/>
      <daoloc href="MED/MED_CHA/RBX_MED_CHA_0091.jpg" role="access:image"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244641TPOobF" role="publication:current"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNR22820" role="publication:previous"/>
    </daogrp>
```

---

## 2. L'attribut `role` : à quoi sert chaque lien

Chaque lien (`<dao>` ou `<daoloc>`) porte un attribut `role` qui indique son
usage, en respectant la syntaxe `usage:média[:position]` :

| role | Signification | À faire |
|---|---|---|
| `publication:current` | Lien pérenne de la notice, généré dans Mnesys | à utiliser pour renvoyer vers la notice du document |
| `publication:previous` | "Ancien" lien pérenne de la notice, généré par l'outil bn-r Decalog | rediriger vers le lien `publication:current` |
| `access:image` / `access:audio` / `access:video` / `access:pdf` / `access:ocr_alto` | Fichier de diffusion | fichier à publier, sauf s'il porte `audience="internal"` (voir §4) |
| `preservation:image` / `preservation:audio` / `preservation:video` / `preservation:pdf` / `preservation:ocr_alto` | Fichier de conservation  | ne pas prendre en considération, à usage interne de Roubaix |
| `…:first` / `…:last` (sur `access:` ou `preservation:`) | Bornes d'une plage de plusieurs fichiers | Voir §3 |


---

## 3. Plage de fichiers : `:first` / `:last`

Quand le document décrit par un composant comprend plusieurs fichiers (les pages d'un document, les deux faces d'un disque...), ces derniers ne sont pas tous listés : seuls le premier et le dernier sont donnés, avec le rôle suffixé `:first` / `:last`.
Les fichiers intermédiaires sont implicites.

Exemple : une série de photos d'une distribution de prix, décrite par ses
seules bornes :
```xml
    <daogrp>
      <daodesc><p>1858 - Distribution solennelle des prix aux élèves des écoles académiques</p></daodesc>
      <daoloc href="CSV/RBX_CSV_PAL_1858_01.tif" role="preservation:image:first"/>
      <daoloc href="CSV/RBX_CSV_PAL_1858_08.tif" role="preservation:image:last"/>
      <daoloc href="CSV/RBX_CSV_PAL_1858_01.jpg" role="access:image:first"/>
      <daoloc href="CSV/RBX_CSV_PAL_1858_08.jpg" role="access:image:last"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244634UCd4Y1" role="publication:current"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNR52641" role="publication:previous"/>
    </daogrp>
```

`RBX_CSV_PAL_1858_01.jpg` (première) et `RBX_CSV_PAL_1858_08.jpg` (dernière)
encadrent en réalité huit fichiers, `_01.jpg` à `_08.jpg`. Le nom de
fichier ne diffère entre les deux bornes que par un segment numérique (ici
`01` → `08`) ; c'est sur ce segment qu'il faut énumérer, en conservant à
l'identique le préfixe, l'extension et le nombre de chiffres (zéros initiaux
compris). Une plage `preservation:` et la plage `access:` correspondante,
dans le même groupe, décrivent toujours les mêmes fichiers.


---

## 4. Restriction d'accès : `audience="internal"`

Un lien `access:*` peut porter `audience="internal"` : le fichier existe,
mais n'est pas destiné à la diffusion publique (droits non réglés,
personnes identifiables...). Les modalités de diffusion seront définies ensemble en cours de projet.

```xml
    <daogrp>
      <daodesc><p>Lecture en section jeunesse</p></daodesc>
      <daoloc href="MED/MED_PHD/RBX_MED_PHD_ETA03_S01_A_D02_001.tif" role="preservation:image:first"/>
      <daoloc href="MED/MED_PHD/RBX_MED_PHD_ETA03_S01_A_D02_002.tif" role="preservation:image:last"/>
      <daoloc href="MED/MED_PHD/RBX_MED_PHD_ETA03_S01_A_D02_001.jpg" audience="internal" role="access:image:first"/>
      <daoloc href="MED/MED_PHD/RBX_MED_PHD_ETA03_S01_A_D02_002.jpg" audience="internal" role="access:image:last"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244639E22yyw" role="publication:current"/>
    </daogrp>
```

Quand il est présent, `audience` ne prend dans ce corpus que la valeur
`internal` ; son absence signifie un accès public normal.

---

## 5. Focus sur les liens pérennes ark

Comme mentionné plus haut, on indique dans les éléments `<dao>` / `<daogrp>` les liens pérennes ark.

On distingue :
- les "anciens" liens : anciens entre guillemets car, de fait, ce sont les liens actuellement générés et par l'outil bn-r Decalog. Ces liens ont été reportés dans les instruments de recherche et devront permettre une redirection vers les nouveaux liens, générés par Mnesys. Ils ont pour `role` la valeur `publication:previous`.
- les "nouveaux" liens, générés par Mnesys. Les liens reportés en `<dao>` ou `<daoloc>` sont construits de la manière suivante : "https://www.bn-r.fr/ark:/20179/BNR" + valeur de l'attribut `id` de l'élément `<c>`, pour lequel on est certain que Mnesys génère une valeur unique. Ils ont pour `role` la valeur `publication:current`.

---

## 6. Les cas courants, en un coup d'œil

### Pas de fichier à afficher, juste une notice en ligne
Un élément `<dao>` / `<daogrp>` ne comprend parfois que des liens de `role` 'publication'. Dans la plupart des cas, il s'agit de composants de niveaux supérieurs (série, sous-série, dossier, ...).

Par exemple :

```xml
    <daogrp>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244634ZB0IOD" role="publication:current"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNR59358" role="publication:previous"/>
    </daogrp>
```

### Une image, une plage d'images
Voir §2 et §3 ci-dessus (avec ou sans `preservation:image`).

### Un journal océrisé (fichiers ALTO)
Un numéro de journal océrisé (`results/ead/corpus_ocr/RBX_PRA_*.xml`) porte, en
plus de l'image et de sa conservation, un fichier ALTO d'OCR par page, sous le
rôle `ocr_alto` (`access:ocr_alto` / `preservation:ocr_alto`). Numéro de plusieurs pages : la plage OCR suit la même logique que la plage d'images (§3), avec un fichier `.xml` (ALTO) par page :

```xml
    <daogrp>
      <daoloc href="MED/MED_PRA/PRA_AVE/1888/PRA_AVE_18881202_001.tif" role="preservation:image:first"/>
      <daoloc href="MED/MED_PRA/PRA_AVE/1888/PRA_AVE_18881202_004.tif" role="preservation:image:last"/>
      <daoloc href="MED/MED_PRA/PRA_AVE/1888/PRA_AVE_18881202_001.jpg" role="access:image:first"/>
      <daoloc href="MED/MED_PRA/PRA_AVE/1888/PRA_AVE_18881202_004.jpg" role="access:image:last"/>
      <daoloc href="MED/MED_PRA/PRA_AVE/1888/PRA_AVE_18881202_001.xml" role="preservation:ocr_alto:first"/>
      <daoloc href="MED/MED_PRA/PRA_AVE/1888/PRA_AVE_18881202_004.xml" role="preservation:ocr_alto:last"/>
      <daoloc href="MED/MED_PRA/PRA_AVE/1888/PRA_AVE_18881202_001.xml" role="access:ocr_alto:first"/>
      <daoloc href="MED/MED_PRA/PRA_AVE/1888/PRA_AVE_18881202_004.xml" role="access:ocr_alto:last"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01786604597vmXjzx" role="publication:current"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNR74857" role="publication:previous"/>
    </daogrp>
```
Ici `access:ocr_alto` et `preservation:ocr_alto` pointent vers le même fichier `.xml` : contrairement à l'image, il n'y a qu'une seule version du fichier ALTO (pas de distinction diffusion/master).

Numéro de journal d'une seule page, sans plage :

```xml
    <daogrp>
      <daoloc href="MED/MED_PRA/PRA_ERT/1901/PRA_ERT_19010614_001.tif" role="preservation:image"/>
      <daoloc href="MED/MED_PRA/PRA_ERT/1901/PRA_ERT_19010614_001.jpg" role="access:image"/>
      <daoloc href="MED/MED_PRA/PRA_ERT/1901/PRA_ERT_19010614_001.xml" role="preservation:ocr_alto"/>
      <daoloc href="MED/MED_PRA/PRA_ERT/1901/PRA_ERT_19010614_001.xml" role="access:ocr_alto"/>
    </daogrp>
```

### Un enregistrement sonore
Un document sonore associe souvent plusieurs fichiers audio et une plage d'images (pochette, livret) :

```xml
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
```

### Un PDF
Le même fichier PDF peut servir à la fois de master de conservation et de fichier
de diffusion :

```xml
    <daogrp>
      <daoloc href="OBS/OBS_JOU/RBX_OBS_JOU_QLI_001.pdf" role="preservation:pdf"/>
      <daoloc href="OBS/OBS_JOU/RBX_OBS_JOU_QLI_001.pdf" role="access:pdf"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244641l7J5O7" role="publication:current"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNR25856" role="publication:previous"/>
    </daogrp>
```

### Une vidéo
Un document vidéo avec une image qui peut illustrer la vidéo sans faire partie d'une plage :

```xml
    <daogrp>
      <daoloc href="MDF/MDF_MTX/RBX_MDF_MTX_2014_D01.mov" role="preservation:video"/>
      <daoloc href="MDF/MDF_MTX/RBX_MDF_MTX_2014_D01.mp4" role="access:video"/>
      <daoloc href="RBX_MDF_MTX_1409_D01.jpg" role="access:image"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNRm01781244634p2FaYj" role="publication:current"/>
      <daoloc href="https://www.bn-r.fr/ark:/20179/BNR3" role="publication:previous"/>
    </daogrp>
```

---
