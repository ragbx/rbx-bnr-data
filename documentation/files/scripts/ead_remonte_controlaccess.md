# Script : ead_remonte_controlaccess.py

**Emplacement :** `scripts/ead/ead_remonte_controlaccess.py`

Remonte avant les `<c>` enfants les `<controlaccess>` placés après eux.

La DTD EAD 2002 impose que les blocs descriptifs d'un `<c>` précèdent ses
sous-composants. Le `<controlaccess>` fautif est déplacé juste avant le
premier `<c>` enfant ; son contenu n'est pas modifié.

C'est un **correctif appliqué directement** aux IR de
`results/ead/ead_cor/bnr2mnesys/` (base de travail), à la suite de
[ead_tri_unitid.py](ead_tri_unitid.md) et
[ead_nettoie_texte.py](ead_nettoie_texte.md).

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/ead_remonte_controlaccess.py              # à blanc : liste les cotes
    python scripts/ead/ead_remonte_controlaccess.py --appliquer  # réécrit les IR

Le script est rejouable : sur des IR déjà corrigés, il ne modifie rien.

## Application du 2026-10-09

12 `<controlaccess>` déplacés, dans 6 IR :

| IR | Cotes |
|---|---|
| `LAR_PUB` | `PUB_CDR` |
| `MED_DIL` | `DIL4_PDC3` |
| `MED_PAR` | `BOH`, `PRE` |
| `MED_PHO` | `PHO_DIA_DEL`, `PHO_PDV_LED`, `PHO_PNU_DUP01`, `PHO_TIR_DOR`, `PHO_TIR_PAS`, `PHO_TIR_PET` |
| `MED_PIA` | `RBX_MED_PIA1` |
| `VAH_PUB` | `PUB_MEM` |

Vérification : les fichiers contiennent exactement les mêmes lignes
qu'avant, dans un autre ordre, et l'erreur de validation correspondante a
disparu.

## Écarts à la DTD restants

Après ce correctif, 12 IR sur 37 sont valides. Les 25 autres ne le sont pas
pour trois raisons :

| Écart | Occurrences | IR | Statut |
|---|---|---|---|
| Attribut `coord` sur `<geogname>` | 6 909 | 21 | **considéré comme correct**, à ne pas modifier (décision du 2026-10-09) |
| `level="subfile"` sur `<c>` | 1 403 | 5 | corrigé ensuite par [ead_otherlevel.py](ead_otherlevel.md) |
| Élément `<resource>` | 806 | 11 | en attente |
