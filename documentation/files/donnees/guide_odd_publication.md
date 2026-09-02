# Corriger un lien d'un IR via `<odd>` : guide pratique

Ce guide s'adresse à qui doit **corriger un lien de fichier (dao) sur un
instrument de recherche déjà importé dans Mnesys**, sans toucher au XML
`<dao>`/`<daogrp>` à la main. Pour la référence technique complète des
structures `<dao>` / `<daogrp>` / `<odd>`, voir
[Les liens DAO : structures et cas de figure](dao_daogrp.md).

---

## 1. Le principe : `<odd>` est la donnée maître

Sur un `<c>` qui a des fichiers associés, deux éléments coexistent :

- un `<dao>` (lien unique) ou un `<daogrp>` (plusieurs `<daoloc>`) : la
  structure XML **réellement utilisée** par Mnesys pour afficher les liens ;
- juste après, un `<odd>` qui **résume ces mêmes liens en texte lisible**, un
  `<p>` par lien.

**On ne corrige jamais le `<dao>`/`<daogrp>` directement : on corrige le
`<odd>`**, puis on répercute la correction avec l'application
**[EAD Pré-publication](../../../app/ead_prepublication/README.md)**.

Pourquoi passer par le `<odd>` plutôt que d'éditer le `<daogrp>` directement ?
Parce que c'est du texte simple, sans risque de casser une balise XML, et parce
que ça garde une trace lisible de l'état voulu — utile pour relire ou faire
relire une notice sans connaître le XML.

---

## 2. Le cycle en 3 étapes

Une correction de lien se fait toujours dans cet ordre :

1. **Corriger le `<odd>` dans Mnesys** — dans l'interface d'édition de la
   notice, sur le composant concerné.
2. **Exporter puis synchroniser** — exporter l'IR complet depuis Mnesys, puis
   lancer l'application **EAD Pré-publication** sur ce fichier exporté : elle
   répercute la correction du `<odd>` sur le `<dao>`/`<daogrp>`.
3. **Réimporter dans Mnesys** le fichier produit par l'application — **sous le
   même nom** que le fichier exporté à l'étape 2.

Tant que les étapes 2 et 3 n'ont pas été faites, le `<dao>`/`<daogrp>` affiché
dans Mnesys ne reflète **pas encore** la correction du `<odd>` : c'est normal,
la synchronisation ne se fait pas en direct.

---

## 3. Étape 1 — corriger le `<odd>` dans Mnesys

Chaque lien tient sur un `<p>`, au format :

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
| `publication:current` | ARK de publication actuel |
| `publication:previous` | Ancien ARK de publication (bn-r) |

Détail complet de cette grammaire (`usage:média[:position]`) dans
[Grammaire des role](dao_daogrp.md#grammaire-des-role).

Une ligne qui ne commence pas par un de ces rôles est **ignorée** par l'app :
elle peut donc aussi servir de note éditoriale ordinaire, sans risque qu'elle
soit prise pour un lien.

### Cas d'ajout, modification, suppression

- **Ajouter un lien** : ajouter une ligne `<p>role href</p>` (ou
  `<p>role href audience</p>`).
- **Supprimer un lien** : supprimer la ligne `<p>` correspondante.
- **Changer le fichier d'un lien** (même rôle, autre `href`) : remplacer le
  `href` dans la ligne existante.
- **Changer le rôle ou l'audience d'un lien** : remplacer la ligne. Techniquement,
  cela équivaut à supprimer l'ancien lien et en ajouter un nouveau (le triplet
  `href` + `role` + `audience` identifie un lien dans son ensemble — il n'y a
  pas de « modification en place »).

**Attention** :

- Le `href` doit être **exact** (aucune correction automatique de faute de
  frappe). Un `href` mal recopié crée un nouveau lien au lieu de corriger l'ancien,
  et laisse l'ancien lien orphelin dans le `<daogrp>` (l'app le supprimera au
  passage suivant, puisqu'il n'est plus dans le `<odd>`).
- Un même `href` peut légitimement apparaître **deux fois avec des rôles
  différents** (ex. un pdf qui sert à la fois de master de conservation et de
  fichier de diffusion : `preservation:pdf` et `access:pdf` sur le même
  fichier). C'est normal, ce n'est pas un doublon à corriger.
- N'éditez **que** le `<odd>`. Le `<dao>`/`<daogrp>` est régénéré par l'app à
  partir du `<odd>` : le modifier directement n'aurait pas d'effet durable (et
  serait écrasé au prochain passage de l'app).

---

## 4. Étape 2 — exporter et lancer la synchronisation

1. Dans Mnesys, **exporter l'IR complet** (pas seulement le `<c>` corrigé) au
   format XML EAD standard, et récupérer le fichier téléchargé.
2. Ouvrir **EAD Pré-publication** (`app/ead_prepublication/`, voir son
   [README](../../../app/ead_prepublication/README.md) pour le lancement ou
   l'exécutable Windows).
3. **Fichier source** : sélectionner le fichier XML tout juste exporté.
4. **Fichier de sortie** — **point d'attention** : Mnesys exige au réimport
   que le fichier porte **exactement le même nom** que celui qui a été
   exporté. L'app suggère par défaut `<nom>_sync.xml` : soit remplacer ce nom
   suggéré par le nom exact du fichier exporté avant de lancer, soit laisser
   le nom suggéré puis **renommer** le fichier produit pour qu'il soit
   identique au fichier exporté, avant l'étape 3.
5. Cliquer **Lancer la synchronisation**.
6. Lire le journal : il indique combien de `<c>` ont un `<odd>`
   (`N/M <c> possèdent un <odd>`), puis le résultat
   (`X ajoutés, Y supprimés`).

Le fichier source **n'est jamais modifié** : la synchronisation écrit un
nouveau fichier.

> Cette opération porte sur l'IR complet exporté : éviter de faire ce
> cycle export → synchro → réimport en même temps qu'une autre personne édite
> le même IR dans Mnesys, sous peine d'écraser ses modifications au réimport.

---

## 5. Étape 3 — réimporter dans Mnesys

Réimporter dans Mnesys le fichier produit par l'app (celui dont le nom a été
aligné sur le fichier exporté à l'étape 2).

### Vérifier le résultat

- Les compteurs affichés dans le journal de l'app doivent correspondre à ce
  que vous attendiez (un ajout et une suppression pour un rôle changé, par
  exemple — voir « changer le rôle » ci-dessus).
- Avant réimport, on peut ouvrir le fichier de sortie et vérifier le
  `<daogrp>` du `<c>` corrigé : le lien doit refléter exactement le `<odd>`.
- Relancer la synchronisation sur ce même fichier de sortie ne doit plus rien
  changer (`0 ajoutés, 0 supprimés`) : c'est le signe que le `<dao>`/`<daogrp>`
  est bien aligné sur le `<odd>`.
- Après réimport, vérifier dans Mnesys que le lien du composant corrigé
  s'affiche comme attendu.

---

## Aller plus loin

- [Les liens DAO : structures et cas de figure](dao_daogrp.md) — détail
  technique complet des structures `<dao>`/`<daogrp>`/`<odd>`, cas de figure
  réels du corpus.
- [README de EAD Pré-publication](../../../app/ead_prepublication/README.md) —
  détail de l'implémentation (`sync_dao_from_odd()`).
- [ead_bnr2mnesys.py](../scripts/ead_bnr2mnesys.md) — étape 10, génération
  initiale du `<odd>` à partir du `<dao>`/`<daogrp>`, lors de la première
  transformation d'un IR (avant son premier import dans Mnesys).
