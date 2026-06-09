# Script : ead_bnr2mnesys.py

**Emplacement :** `scripts/ead/ead_bnr2mnesys.py`

Transforme les fichiers EAD des instruments de recherche de la bn-r
en vue de leur import dans Mnesys.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `data/ead/bnr/` | Fichiers EAD source |
| `data/oai/oai_records_{date}.csv.gz` | Correspondances cote (unitid) / notices bn-r (pour liens ark)|
| `results/ead/indexation/controlaccess_extraction_name2.csv` | Noms typés persname / corpname |
| `results/ref/_ref_files_{date}.csv.gz` | Fichiers de conservation (chemins S3) |
| `results/ir/liste_instruments_recherche_{date}_transfert_mnesys.xlsx` | Liste des IR à traiter |

Seuls les instruments de recherche dont le statut est `TRANSFERER` dans le fichier
liste_instruments_recherche_{date}_transfert_mnesys.xlsx sont traités.

## Résultats

Les fichiers EAD transformés sont stockés dans `results/ead_cor/bnr2mnesys/`.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/ead_bnr2mnesys.py

---

## Transformations appliquées

Les transformations sont appliquées dans l'ordre suivant sur chaque fichier EAD :

### 1. Mise à jour des métadonnées de l'instrument de recherche
- `<eadid>` : remplacé par la nouvelle valeur issue du fichier Excel.
- `<archdesc/did/unitid>` : remplacé par le nouveau identifiant de l'IR.
- `<archdesc/did/repository>` : remplacé par le nouveau nom du service versant.

### 2. Nettoyage du contenu textuel
- Décodage des entités HTML (`&amp;`, `&lt;`, etc.) dans tout l'arbre XML.
- Suppression des espaces en début/fin de texte dans les enfants de `<controlaccess>`.

### 3. Réorganisation des accès (indexation)
- `<origination>` : les éléments `<name>` et `<persname>` qu'il contient sont déplacés dans
  `<controlaccess>` (créé si absent). `<origination>` est supprimé s'il devient vide.
  Lors du déplacement, les `<name>` sont reclassés en `<persname>` ou `<corpname>` selon la
  liste CSV si une correspondance est trouvée.

### 4. Ajout des anciens ARK BnR
- Pour chaque élément `<c>` dont le `<unitid>` figure dans la table de correspondance OAI,
  une balise pointant vers l'ancien ARK BnR (`https://www.bn-r.fr/ark:/20179/<osiros_id>`)
  est insérée selon trois cas :
  - `<daogrp>` déjà présent → ajout d'un `<daoloc role="publication:previous">` dans le groupe.
  - `<dao>` présent (sans `<daogrp>`) → transformation en `<daogrp>` contenant un `<daoloc>`
    reprenant les attributs de l'ancienne `<dao>` et un `<daoloc role="publication:previous">`.
  - Ni `<dao>` ni `<daogrp>` → création d'un `<dao role="publication:previous">` pointant vers l'ARK.
- Dans les cas 2 et 3, la balise est insérée avant le premier enfant `<c>` s'il existe.

### 5. Mise à jour des rôles des `<dao>`
- Pour tous les éléments `<dao>` et `<daoloc>` dont l'attribut `role` commence par `image`,
  le préfixe `access:` est ajouté devant la valeur existante.

### 6. Ajout des chemins de conservation
- Pour chaque `<dao>`/`<daoloc>` dont le `href` correspond (par basename sans extension) à un
  fichier du CSV de référence (s3_key non null), un nouveau `<daoloc role="preservation:...">`
  est ajouté avec le chemin S3 (`s3_key`). Le `role` est obtenu en remplaçant `access:` par
  `preservation:` dans le `role` de l'élément source.
- Si le `<dao>` est isolé (hors `<daogrp>`), il est converti en `<daogrp>` + `<daoloc>` au préalable.

### 7. Tri des `<daoloc>` dans les `<daogrp>`
- Dans chaque `<daogrp>`, les `<daoloc>` sont réordonnés : `preservation:` en premier,
  `access:` ensuite, `publication:` en dernier.

### 8. Reclassement des balises `<name>`
- Dans `<controlaccess>`, les balises `<name>` sont remplacées par `<persname>` ou `<corpname>`
  selon la liste CSV. Les `<name>` sans correspondance sont laissés tels quels.

### 9. Suppression des `<repository>` hors contexte
- Toutes les balises `<repository>` situées en dehors de `<archdesc/did>` sont supprimées.

### 10. Nettoyage final
- Suppression des attributs dont la valeur est une chaîne vide.
- Suppression récursive des éléments XML vides (sans texte, sans attribut, sans enfant non vide).
