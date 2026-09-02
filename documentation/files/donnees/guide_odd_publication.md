# Corriger un lien d'un IR via l'élément `<odd>`

Ce document explique comment **corriger un lien de fichier (dao) sur un
instrument de recherche déjà importé dans Mnesys**, sans toucher au XML
`<dao>`/`<daogrp>` à la main. Pour la référence technique complète des
structures `<dao>` / `<daogrp>` / `<odd>`, voir
[Les liens DAO : structures et cas de figure](dao_daogrp.md).

---

## 1. Le principe :  l'élément `<odd>` est la donnée maître

Sur un élément `<c>` qui a des fichiers associés, deux éléments coexistent :

- un élément `<dao>` (lien unique) ou un élément `<daogrp>` (plusieurs éléments `<daoloc>`) : la
  structure XML réellement utilisée par l'EAD pour stocker les liens ;
- juste après, un élément `<odd>` qui **reprend ces mêmes liens en texte lisible**, chaque lien se trouvant dans un élément `<p>`.

**On ne corrige jamais les éléments `<dao>`/`<daogrp>` directement : on corrige l'élément
`<odd>`**, puis on répercute la correction avec l'application
**[EAD Pré-publication](../../../app/ead_prepublication/README.md)**.

---

## 2. Le cycle en 3 étapes

Une correction de lien se fait toujours dans cet ordre :

1. **Corriger l'élément `<odd>` dans Mnesys** — dans l'interface d'édition de la
   notice, sur le composant concerné.
2. **Exporter puis synchroniser** — exporter l'IR complet depuis Mnesys, puis
   lancer l'application **EAD Pré-publication** sur ce fichier exporté : elle
   répercute la correction de l'élément `<odd>` sur l'élément `<dao>`/`<daogrp>`.
3. **Réimporter dans Mnesys** le fichier produit par l'application — **sous le
   même nom** que le fichier exporté à l'étape 2.

Tant que les étapes 2 et 3 n'ont pas été faites, l'élément `<dao>`/`<daogrp>` affiché
dans Mnesys ne reflète **pas encore** la correction de l'élément `<odd>` : la synchronisation ne se fait pas en direct.

---

## 3. Étape 1 — corriger l'élément `<odd>` dans Mnesys

Chaque lien tient sur un élément `<p>`, au format :

    role href [audience]

- **role** : un des rôles reconnus (voir liste ci-dessous). Il indique l'usage
  du lien : diffusion, conservation, publication en ligne...
- **href** : le chemin du fichier (dans le bucket S3) ou l'URL de publication.
- **audience** *(optionnel)* : uniquement `internal` dans ce corpus — un accès
  restreint. Absent dans la grande majorité des cas.

Exemple, pour une image avec conservation et publication :

```xml
<odd>
  <p>preservation:image MED/MED_CHA/RBX_MED_CHA_0091.tif</p>
  <p>access:image MED/MED_CHA/RBX_MED_CHA_0091.jpg</p>
  <p>publication:current https://www.bn-r.fr/ark:/20179/BNRm01781244641TPOobF</p>
  <p>publication:previous https://www.bn-r.fr/ark:/20179/BNR22820</p>
</odd>
```

### Rôles reconnus

| Rôle | Usage |
|---|---|
| `access:image`, `access:image:first`, `access:image:last` | Image de diffusion (unitaire, ou borne d'une plage) |
| `preservation:image`, `preservation:image:first`, `preservation:image:last` | Image de conservation |
| `access:audio` / `preservation:audio` | Audio de diffusion / conservation |
| `access:video` / `preservation:video` | Vidéo de diffusion / conservation |
| `access:pdf` / `preservation:pdf` | PDF de diffusion / conservation |
| `access:ocr_alto` / `preservation:ocr_alto` | Fichier ALTO d'océrisation de diffusion / conservation |
| `publication:current` | ARK de publication actuel |
| `publication:previous` | Ancien ARK de publication (bn-r) |

Détail complet de cette syntaxe (`usage:média[:position]`) dans
[Grammaire des role](dao_daogrp.md#grammaire-des-role).

Une ligne qui ne commence pas par un de ces rôles est **ignorée** par l'application :
elle peut donc aussi servir de note éditoriale ordinaire, sans risque qu'elle
soit prise pour un lien.

### Cas d'ajout, modification, suppression

- **Ajouter un lien** : ajouter une ligne `role href` (ou
  `role href audience`).
- **Supprimer un lien** : supprimer la ligne correspondante.
- **Changer le fichier d'un lien** (même rôle, autre `href`) : remplacer le
  `href` dans la ligne existante.
- **Changer le rôle ou l'audience d'un lien** : remplacer la ligne. Techniquement,
  cela équivaut à supprimer l'ancien lien et en ajouter un nouveau (le triplet
  `href` + `role` + `audience` identifie un lien dans son ensemble — il n'y a
  pas de « modification en place »).

**Attention** :

- Le `href` doit être **exact** (aucune correction automatique de faute de
  frappe). Un `href` mal recopié crée un nouveau lien au lieu de corriger l'ancien,
  et laisse l'ancien lien orphelin dans le `<daogrp>` (l'application le supprimera au
  passage suivant, puisqu'il n'est plus dans l'élément `<odd>`).
- Un même `href` peut légitimement apparaître **deux fois avec des rôles
  différents** (ex. un pdf qui sert à la fois de master de conservation et de
  fichier de diffusion : `preservation:pdf` et `access:pdf` sur le même
  fichier). C'est normal, ce n'est pas un doublon à corriger.
- N'éditez **que** l'élément `<odd>`. L'élément `<dao>`/`<daogrp>` est régénéré par l'application à
  partir de l'élément `<odd>` : le modifier directement n'aurait pas d'effet durable (et
  serait écrasé au prochain passage de l'app).

---

## 4. Étape 2 — exporter et lancer la synchronisation

1. Dans Mnesys, **exporter l'IR complet** et récupérer le fichier téléchargé.
2. Ouvrir **EAD Pré-publication** (`app/ead_prepublication/`, voir son
   [README](../../../app/ead_prepublication/README.md) pour le lancement ou
   l'exécutable Windows).
3. **Fichier source** : sélectionner le fichier XML tout juste exporté.
4. **Fichier de sortie** — **point d'attention** : Mnesys exige au réimport
   que le fichier porte **exactement le même nom** que celui qui a été
   exporté. L'application suggère par défaut `<nom>_sync.xml` : soit remplacer ce nom
   suggéré par le nom exact du fichier exporté avant de lancer, soit laisser
   le nom suggéré puis **renommer** le fichier produit pour qu'il soit
   identique au fichier exporté, avant l'étape 3.
5. Cliquer **Lancer la synchronisation**.
6. Lire le journal : il indique combien d'éléments `<c>` ont un élément `<odd>`
   (`N/M <c> possèdent un <odd>`), puis le résultat
   (`X ajoutés, Y supprimés`).

Le fichier source **n'est jamais modifié** : la synchronisation écrit un
nouveau fichier.

> Cette opération porte sur l'IR complet exporté : éviter de faire ce
> cycle export → synchro → réimport en même temps qu'une autre personne édite
> le même IR dans Mnesys, sous peine d'écraser ses modifications au réimport.

---

## 5. Étape 3 — réimporter dans Mnesys

Réimporter dans Mnesys le fichier produit par l'application (celui dont le nom a été
aligné sur le fichier exporté à l'étape 2).

### Vérifier le résultat

- Les compteurs affichés dans le journal de l'application doivent correspondre à ce
  que vous attendiez (un ajout et une suppression pour un rôle changé, par
  exemple — voir « changer le rôle » ci-dessus).
- Avant réimport, on peut ouvrir le fichier de sortie et vérifier l'élément
  `<daogrp>` de l'élément `<c>` corrigé : le lien doit refléter exactement l'élément `<odd>`.
- Relancer la synchronisation sur ce même fichier de sortie ne doit plus rien
  changer (`0 ajoutés, 0 supprimés`) : c'est le signe que l'élément `<dao>`/`<daogrp>`
  est bien aligné sur l'élément `<odd>`.
- Après réimport, vérifier dans Mnesys que le lien du composant corrigé
  s'affiche comme attendu.

---

## Aller plus loin

- [Les liens DAO : structures et cas de figure](dao_daogrp.md) — détail
  technique complet des structures `<dao>`/`<daogrp>`/`<odd>`, cas de figure
  réels du corpus.
- [README de EAD Pré-publication](../../../app/ead_prepublication/README.md) —
  détail de l'implémentation (`sync_dao_from_odd()`).
