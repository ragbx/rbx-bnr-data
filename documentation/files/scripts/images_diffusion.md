# Scripts : images de diffusion

**Emplacement :** `scripts/img/`

Chaîne de préparation des **images de diffusion** à partir des fichiers de
conservation : on constitue des corpus d'images représentatifs, on en copie les
fichiers maîtres (TIFF) sur disque dur, puis on les convertit dans un format
allégé destiné à la consultation en ligne.

Pour la distinction conservation / diffusion, voir
[Fichiers de conservation](../donnees/fichiers.md).

---

## Flux

```
extraction_corpus_tif.py   → results/corpus/corpus_<nom>_<date>.csv.gz   (manifestes des 3 corpus)
│
telechargement_corpus.py   → copie des TIFF maîtres sur disque dur       (+ journal)
│
├── resolution_corpus.ipynb   → calibrage de la réduction de résolution
└── tif_convert.py            → conversion TIFF → JP2 pour la diffusion
```

Les trois corpus retenus pour la mise au point sont :

| Slug | Contenu | Définition (corpus_code) |
|---|---|---|
| `presse` | Presse ancienne | tous les `PRA_*` |
| `iconographie` | Cartes postales, images, affiches | `MED_CP`, `MED_IMA`, `MED_AFF`, `AMR_AFF`, `AMR_DEL` |
| `manuscrits_plans` | Manuscrits, plans, monnaies | `MED_MS`, `MED_PLA`, `MED_MON` |

---

## Étapes

| Script | Entrée | Sortie | Rôle |
|---|---|---|---|
| [extraction_corpus_tif.py](extraction_corpus_tif.md) | fichier de référence | `corpus_<nom>_<date>.csv.gz` | Sélectionne les TIFF de chaque corpus (échantillon équilibré, plafonds en nombre et en volume) |
| [telechargement_corpus.py](telechargement_corpus.md) | manifestes | TIFF copiés sur disque + journal | Copie les fichiers maîtres depuis le stockage d'origine |
| [tif_convert.py](tif_convert.md) | dossier de TIFF | JP2 (ou TIFF pyramidal) | Convertit les TIFF en images allégées pour la diffusion web |

`resolution_corpus.ipynb` complète la chaîne : il analyse les distributions de
résolution par corpus et calibre la réduction (niveaux par corpus, plancher en
pixels). Voir le notebook pour le détail.

---

## Environnements conda

Attention, deux environnements distincts :

- `extraction_corpus_tif.py` et `telechargement_corpus.py` se lancent avec
  **`conda run -n ds`** (comme la plupart des scripts du dépôt) ;
- `tif_convert.py` se lance avec **`conda run -n vips`** : c'est le seul
  environnement disposant d'une `libvips` compilée avec le support JPEG2000
  (`.jp2`). Voir [sa fiche](tif_convert.md).
