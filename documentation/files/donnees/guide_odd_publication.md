# Corriger les liens d'un IR via `<odd>` : guide pratique

Ce guide s'adresse à qui doit **corriger un lien de fichier (dao) dans un
instrument de recherche déjà transformé**, sans toucher au XML à la main plus
que nécessaire. Pour la référence technique complète des structures `<dao>` /
`<daogrp>` / `<odd>`, voir
[Les liens DAO : structures et cas de figure](dao_daogrp.md).

---

## 1. Contexte : où et pourquoi

Les fichiers concernés sont ceux de `results/ead/ead_cor/bnr2mnesys/` — la
sortie de [ead_bnr2mnesys.py](../scripts/ead_bnr2mnesys.md). Dans ces fichiers,
chaque composant `<c>` qui a des fichiers associés porte deux choses :

- un `<dao>` (lien unique) ou un `<daogrp>` (plusieurs `<daoloc>`) : la
  structure XML **réellement utilisée** par Mnesys pour afficher les liens ;
- juste après, un `<odd>` qui **résume ces mêmes liens en texte lisible**, un
  `<p>` par lien.

**Le `<odd>` est la donnée maître.** Concrètement : si un lien est faux (mauvais
fichier, rôle incorrect...), **on corrige le `<odd>`**, jamais directement le
`<dao>`/`<daogrp>` — puis on lance l'application
**[EAD Pré-publication](../../../app/ead_prepublication/README.md)** pour
répercuter la correction sur le XML réellement utilisé.

Pourquoi passer par le `<odd>` plutôt que d'éditer le `<daogrp>` directement ?
Parce que c'est du texte simple, sans risque de casser une balise XML, et parce
que ça garde une trace lisible de l'état voulu — utile pour relire ou faire
relire une notice sans connaître le XML.

---

## 2. Le format d'une ligne `<odd>`

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

---

## 3. Corriger un lien

Toujours dans le `<odd>` du `<c>` concerné :

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

## 4. Lancer l'application EAD Pré-publication

1. Ouvrir **EAD Pré-publication** (`app/ead_prepublication/`, voir son
   [README](../../../app/ead_prepublication/README.md) pour le lancement ou
   l'exécutable Windows).
2. **Fichier source** : sélectionner le fichier EAD édité (celui de
   `results/ead/ead_cor/bnr2mnesys/`).
3. **Fichier de sortie** : suggéré automatiquement (`<nom>_sync.xml`, dans le
   même dossier) — modifiable si besoin.
4. Cliquer **Lancer la synchronisation**.
5. Lire le journal : il indique combien de `<c>` ont un `<odd>`
   (`N/M <c> possèdent un <odd>`), puis le résultat
   (`X ajoutés, Y supprimés`).

Le fichier source **n'est jamais modifié** : la synchronisation écrit un
nouveau fichier.

### Vérifier le résultat

- Les compteurs affichés dans le journal doivent correspondre à ce que vous
  attendiez (un ajout et une suppression pour un rôle changé, par exemple —
  voir « changer le rôle » ci-dessus).
- Ouvrir le fichier de sortie et vérifier le `<daogrp>` du `<c>` corrigé : le
  lien doit refléter exactement le `<odd>`.
- Relancer la synchronisation sur le fichier de sortie ne doit plus rien
  changer (`0 ajoutés, 0 supprimés`) : c'est le signe que le `<dao>`/`<daogrp>`
  est bien aligné sur le `<odd>`.

---

## Aller plus loin

- [Les liens DAO : structures et cas de figure](dao_daogrp.md) — détail
  technique complet des structures `<dao>`/`<daogrp>`/`<odd>`, cas de figure
  réels du corpus.
- [README de EAD Pré-publication](../../../app/ead_prepublication/README.md) —
  détail de l'implémentation (`sync_dao_from_odd()`).
- [ead_bnr2mnesys.py](../scripts/ead_bnr2mnesys.md) — étape 10, génération
  initiale du `<odd>` à partir du `<dao>`/`<daogrp>`.
