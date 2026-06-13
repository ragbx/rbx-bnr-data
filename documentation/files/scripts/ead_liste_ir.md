# Script : ead_liste_ir.py

**Emplacement :** `scripts/ead/ead_liste_ir.py`

Recense les instruments de recherche EAD source de la bn-r et leurs métadonnées
principales dans un tableur. Cette liste sert ensuite, une fois complétée d'un
statut, de pilotage à [ead_bnr2mnesys.py](ead_bnr2mnesys.md) (qui ne traite que
les IR au statut `TRANSFERER`).

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `data/ead/bnr/*.xml` | IR source ; les fichiers `_prettified` et `_corr` sont ignorés |

## Résultat

`results/ir/liste_instruments_recherche_{date}.xlsx` — une ligne par IR, triée
par nom de fichier.

| Colonne | Source dans l'EAD |
|---|---|
| `source` | Dossier d'origine (`bnr`) |
| `file` | Nom du fichier |
| `eadid` | `//eadid` |
| `titleproper` | `//titleproper` |
| `archdesc_unitid` | `archdesc//did/unitid` |
| `repository` | `archdesc//did/repository` |

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/ead_liste_ir.py
