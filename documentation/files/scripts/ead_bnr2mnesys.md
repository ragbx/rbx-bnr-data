# Script : ead_bnr2mnesys.py

**Emplacement :** `scripts/ead/ead_bnr2mnesys.py`

Transforme les fichiers EAD des instruments de recherche de la bn-r
en vue de leur import dans Mnesys.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `data/ead/bnr/` | Fichiers EAD source |
| `data/oai/oai_records_20260430.csv.gz` | Correspondances cote → osiros_id (anciens ARK) |
| `results/ead/indexation/controlaccess_extraction_name2.csv` | Noms typés persname / corpname |
| `results/ref/_ref_files_20260502.csv.gz` | Fichiers de conservation (chemins S3) |
| `results/ir/liste_instruments_recherche_20260521_transfert_mnesys.xlsx` | Liste des IR à traiter |

Seuls les instruments de recherche dont le statut est `TRANSFERER` dans le fichier
Excel sont traités.

## Résultats

Les fichiers EAD transformés sont écrits dans `results/ead_cor/bnr2mnesys/`.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/ead_bnr2mnesys.py

---

## Transformations appliquées

Les transformations sont appliquées dans l'ordre suivant sur chaque fichier EAD :

1. **Mise à jour des métadonnées** — `<eadid>`, `<unitid>` et `<repository>` sont
   remplacés par les valeurs cibles du fichier Excel.
2. **Nettoyage du contenu textuel** — décodage des entités HTML, suppression des
   espaces parasites dans `<controlaccess>`.
3. **Réorganisation des accès** — les éléments `<name>` et `<persname>` de
   `<origination>` sont déplacés dans `<controlaccess>`.
4. **Ajout des anciens ARK BnR** — insertion d'un lien `<dao role="publication:previous">`
   ou `<daoloc role="publication:previous">` vers l'ancien ARK pour chaque composant
   dont la cote figure dans la table OAI.
5. **Mise à jour des rôles** — les `role` commençant par `image` sont préfixés par
   `access:`.
6. **Ajout des chemins de conservation** — pour chaque `<dao>`/`<daoloc>` correspondant
   à un fichier référencé (s3_key non null), ajout d'un `<daoloc role="preservation:...">`.
7. **Tri des `<daoloc>`** — dans chaque `<daogrp>` : `preservation:` en premier,
   `access:` ensuite, `publication:` en dernier.
8. **Reclassement des `<name>`** — remplacement par `<persname>` ou `<corpname>`
   selon la liste de référence.
9. **Suppression des `<repository>` hors contexte.**
10. **Nettoyage final** — suppression des attributs vides et des éléments XML vides.
