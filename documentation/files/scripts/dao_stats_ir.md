# Script : dao_stats_ir.py

**Emplacement :** `scripts/ead/dao_stats_ir.py`

Mesure la couverture en objets numérisés (DAO) de chaque instrument de
recherche : combien de composants de dernier niveau sont numérisés ou non, et
comment cela se répartit par fonds.

Remplace l'ancien `dao_extraction.sh` (`dao_liste.py` / `dao_liste_flat.py`),
qui opérait sur les anciens dossiers `bnr`/`mnesys` ; ce script est branché sur
le corpus à jour `bnr2mnesys`.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/bnr2mnesys/*.xml` | IR transformés ; chaque composant de dernier niveau est classé selon ses liens DAO |

Un composant de dernier niveau est un `<c>` sans `<c>` enfant. Il est compté
**« avec dao »** s'il porte au moins un lien de fichier (role commençant par
`access:` ou `preservation:`). Les liens `publication:*` (ARK), ajoutés à
presque tous les composants par [ead_bnr2mnesys.py](ead_bnr2mnesys.md), **ne
comptent pas** : ils ne signalent pas une numérisation.

Le fonds (`dao_racine`) est déduit du nom du fichier numérisé (préfixe
`RBX_<fonds>_…`), avec quelques regroupements hérités de l'ancien script.

## Résultat

`results/dao/dao_stats_ir_{date}.xlsx`, deux feuilles :

| Feuille | Contenu |
|---|---|
| `par IR` | Par inventaire : nb de composants `avec dao` / `sans dao`, et total (ligne `Total` en marge) |
| `par fonds` | Par (inventaire, `dao_racine`) : nb de composants avec dao |

Colonnes d'inventaire : `inventaire_fichier`, `inventaire_identifiant`,
`inventaire_titre`, `inventaire_soustitre`, `archdesc_unitid`.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/dao_stats_ir.py
