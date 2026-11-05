# Script : ead_ids_mnesys.py

**Emplacement :** `scripts/ead/ead_ids_mnesys.py`

Aligne les `@id` des IR de `results/ead/ead_cor/bnr2mnesys/` sur ceux que
Mnesys leur a attribués, d'après un **export des IR depuis Mnesys**.

## Pourquoi

Mnesys **ignore les `@id` qu'on lui fournit** : à chaque import, il attribue
à chaque `<archdesc>` et `<c>` un id neuf, de la forme `a01` + horodatage de
l'import (secondes depuis 1970) + suffixe. Constaté le 2026-10-27 sur
l'export du jour : aucun des 35 854 `<c>` n'avait gardé son id `m0…` ; les
dates contenues dans les id montrent un import le 2026-07-02 pour 32 IR,
puis des réimports (MED_MS le 2026-08-08, MED_AFF / MED_CAT / MED_PAR le
2026-10-09, ARA_CPS le 2026-10-17), chacun avec des id neufs.

Or les IR de travail, `concordance_id.csv` et les liens ARK
(`https://www.bn-r.fr/ark:/20179/BNR<id>`) doivent porter **les id de
Mnesys**. Ce script les réaligne ; il est à **relancer après tout import
ou réimport d'IR dans Mnesys**, sur un export frais.

## Traitement

1. **Appariement**, IR par IR (mêmes noms de fichiers dans les deux
   dossiers), des composants de `bnr2mnesys` et de l'export : les
   `<archdesc>` entre eux, puis les `<c>` par (`unitid`, `unittitle`), puis
   par `unitid` seul (Mnesys retire certaines apostrophes des titres), puis,
   pour une même clé en double, dans l'ordre du document. L'ordre des `<c>`
   n'intervient pas (Mnesys les retrie). **Un IR dont un composant reste
   sans correspondant n'est pas modifié** (signalé).
2. **Remplacement**, par substitution de texte (le reste du fichier est
   inchangé), des `@id` et de l'id contenu dans les liens ARK BnR
   (`<dao>`/`<daoloc>` et `<odd>`).
3. **Report** des nouveaux id dans `results/ead/ead_cor/concordance_id.csv`
   (relue par [ead_bnr2mnesys.py](ead_bnr2mnesys.md) : une régénération
   reprend ainsi les id Mnesys) et dans la colonne `c_id` de
   `results/ead/indexation/rbx-bnr_geogname_origine.csv` (dates des
   documents pour l'[indexation géographique](indexation_geographique.md)).
4. **Table de correspondance** :
   `results/ead/ead_cor/correspondance_id_mnesys_<date>.csv`.

Avant d'écrire : copie des IR dans
`results/ead/ead_cor/archives/bnr2mnesys_<date>_avant_id_mnesys/` et des
deux CSV en `<nom>.backup_<date>_avant_id_mnesys.csv`.

## Sortie

`correspondance_id_mnesys_<date>.csv`, une ligne par composant :

| Colonne | Contenu |
|---|---|
| `fichier` | IR |
| `unitid`, `unittitle` | Cote et titre (titre normalisé : minuscules, sans apostrophes) |
| `id_avant` | `@id` avant alignement |
| `id_mnesys` | `@id` attribué par Mnesys |
| `appariement` | `archdesc`, `unitid+titre`, `unitid seul` ou `clé en double, rang` |

## Utilisation

Depuis la racine du dépôt :

    python scripts/ead/ead_ids_mnesys.py <dossier de l'export Mnesys>              # à blanc
    python scripts/ead/ead_ids_mnesys.py <dossier de l'export Mnesys> --appliquer

Puis régénérer `results/ead/indexation/rbx-bnr_geogname.csv` :

    python scripts/ead/geogname2csv.py

Rejouable : sur des IR déjà alignés, le script ne change rien.

## Historique

**2026-10-27** — premier alignement, sur l'export
`Instruments_de_recherche1793082337` : 35 891 id remplacés (37 `<archdesc>`,
35 854 `<c>` : 35 691 appariés par unitid + titre, 163 par unitid seul),
71 745 liens ARK réécrits, `concordance_id.csv` et `rbx-bnr_geogname_origine.csv`
mis à jour. L'opération a d'abord été faite par des scripts ponctuels, puis
versionnée ici : rejoué depuis les sauvegardes du matin, le script reproduit
les fichiers à l'octet près. Les anciens ARK `BNRm0…` ne sont donc plus
dans les IR ; ceux déjà affichés dans Mnesys ne changeront qu'au réimport.
