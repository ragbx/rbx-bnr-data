# Script : mnesys_id.py

**Emplacement :** `scripts/ead/mnesys_id.py`

Module utilitaire de génération d'identifiants au format Mnesys pour les
éléments `<c>` (et `<archdesc>`) des EAD. Importé par
[ead_bnr2mnesys.py](ead_bnr2mnesys.md) et
[corpusocr2ead.py](corpusocr2ead.md) ; peut aussi se lancer seul pour un test.

---

## Format des identifiants

Le format a été établi par rétro-ingénierie des 76 742 id commençant par `a0`
des IR de `data/ead/mnesys` (vérification du 2026-06-10 : 100 % conformes et
uniques). Un id fait 19 caractères, en trois parties :

    a0  1461682068  RUeGyW
    │   │           └─ 6 caractères aléatoires de l'ALPHABET
    │   └─ timestamp Unix en secondes
    └─ préfixe constant

`ALPHABET` reprend l'alphabet observé : alphanumérique **sans `K`, `N`, `k`**
(absents des id Mnesys), soit 60 caractères.

## Interface

| Fonction | Rôle |
|---|---|
| `ids_existants(dossier="data/ead/mnesys")` | Ensemble des `id` des `<c>` des EAD d'un dossier (parse tolérant aux `&` non échappés) |
| `nouvel_id(deja_pris, prefixe="a0")` | Nouvel id absent de `deja_pris` (qui est mis à jour) ; `prefixe` permet des séries distinguables des id natifs (les scripts BnR utilisent `m0`) |

L'unicité est garantie par le timestamp (une seconde par génération), le suffixe
aléatoire (60⁶ ≈ 4,7 × 10¹⁰ combinaisons) et le contrôle contre `deja_pris`.

---

## Utilisation

Comme module :

    from mnesys_id import ids_existants, nouvel_id
    deja_pris = ids_existants()
    i = nouvel_id(deja_pris, prefixe="m0")

En direct (génère 5 id de test) :

    python scripts/ead/mnesys_id.py
