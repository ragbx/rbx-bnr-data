# Script : ead_tri_unitid.py

**Emplacement :** `scripts/ead/ead_tri_unitid.py`

Trie, à chaque niveau des IR, les `<c>` frères dans l'ordre alphanumérique
de leur `<did>/<unitid>`.

C'est un **correctif appliqué directement** aux IR de
`results/ead/ead_cor/bnr2mnesys/`, considérés comme base de travail : il ne
repart pas des sources bn-r et ne passe pas par
[ead_bnr2mnesys.py](ead_bnr2mnesys.md). L'état des IR avant correctifs est
conservé dans `results/ead/ead_cor/archives/bnr2mnesys_20261009/`.

---

## Utilisation

Depuis la racine du projet :

    python scripts/ead/ead_tri_unitid.py              # à blanc : compte seulement
    python scripts/ead/ead_tri_unitid.py --appliquer  # réécrit les IR

Le script est rejouable : sur des IR déjà triés, il ne modifie rien.

## Règle de tri

Le tri porte sur les `<c>` enfants directs de chaque `<dsc>` et de chaque
`<c>`. Seul leur ordre change ; leur contenu, leur parent et leur `id` sont
intacts.

- Tri naturel : les suites de chiffres sont comparées comme des nombres
  (`D9` avant `D10`, `S2` équivaut à `S02`).
- Insensible à la casse.
- Un nombre passe avant une lettre à la même position
  (`CP_A01_L1_S3_001` avant `CP_A01_L1_S3_A_001`).
- Une espace vaut un tiret bas (`RAD S02` est classé après `RAD_S01`).
- À cote égale, l'ordre d'origine est conservé.

Le tri s'applique **à tous les niveaux**, y compris ceux dont les cotes sont
des codes tirés du titre et non des numéros (racine de `MED_PHD` :
`PHD_ETA`, `PHD_URB`, `PHD_SOC`… → `PHD_ETA`, `PHD_EVE`, `PHD_GUE`…).
L'ordre antérieur de ces niveaux n'est donc pas conservé (décision du
2026-10-09).

## Application du 2026-10-09

Avant le tri, 336 groupes de `<c>` frères sur 2 118 n'étaient pas dans
l'ordre de leur cote, dans 33 des 37 IR ; ce désordre venait des sources
bn-r (`data/ead/bnr/`). Le tri a déplacé 2 208 `<c>`.

Cotes irrégulières laissées telles quelles, que le tri place selon la règle
ci-dessus :

| IR | Cote | Particularité | Effet |
|---|---|---|---|
| `MED_AFF` | `AFF_O024` | lettre O au lieu de zéro | classée en fin d'IR |
| `MED_MAR` | `MAR_AFF_AM01_C085` | tiret bas manquant | classée avant `MAR_AFF_AM01_C_001` |
| `MED_RAD` | `RAD S02` | espace | classée comme `RAD_S02` |
| `MED_AFF` | `AFF_033_ LOC` | espace | – |
| `MED_AFF` | `AFF_004_TER_C_`, `AFF_037_LOC_C_` | tiret bas final | – |
