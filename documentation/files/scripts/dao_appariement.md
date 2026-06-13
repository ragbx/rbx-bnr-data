# Chaîne d'appariement des DAO

Vue d'ensemble de la chaîne qui, à partir des instruments de recherche EAD
transformés pour Mnesys, apparie les liens de diffusion (DAO) à leurs fichiers
de conservation et recense ceux qui n'ont pas de correspondance.

Chaque étape a sa documentation détaillée ; cette page en donne l'enchaînement,
les entrées et les sorties.

---

## Flux

```
ead_bnr2mnesys.py                         → ead_cor/bnr2mnesys/*.xml
│
├── A. Liens isolés (un fichier par lien)
│   dao_sans_conservation.py              → dao_sans_conservation.csv
│   └── dao_appariement_conservation.py   → dao_appariement_conservation.csv
│
└── B. Plages décrites par leurs bornes first/last
    dao_first_last_developpe.py           → dao_first_last_developpe.csv
    │                                       (+ dao_first_last_ambigus.csv)
    └── dao_first_last_verif_ref.py       → dao_first_last_verif_ref.csv
        ├── dao_first_last_plages_lacunaires.py
        │                                 → dao_first_last_plages_lacunaires.csv
        └── dao_first_last_access_sans_conservation.py
                                          → dao_first_last_access_sans_conservation.csv
```

Toutes les sorties sont écrites dans `results/ead/ead_cor/`. Le référentiel de
fichiers `results/ref/_ref_files_{date}.csv.gz` (le plus récent) sert de
source de vérité pour la conservation.

---

## Les deux familles de liens DAO

Dans les `<daogrp>` des IR, un lien de diffusion peut être exprimé de deux
manières, traitées par deux sous-chaînes distinctes :

- **A. Liens isolés** — chaque fichier a son propre lien `access:*` /
  `preservation:*`. L'appariement se fait lien à lien.
- **B. Plages `first`/`last`** — une suite de fichiers n'est décrite que par
  ses deux bornes (`access:image:first` / `access:image:last`). Les fichiers
  intermédiaires sont implicites et doivent être reconstitués.

---

## A. Liens isolés

| Script | Entrée | Sortie | Rôle |
|---|---|---|---|
| [ead_bnr2mnesys.py](ead_bnr2mnesys.md) | EAD bn-r | `bnr2mnesys/*.xml` | Transforme les IR pour Mnesys et apparie, dans chaque `<daogrp>`, les liens d'accès à un fichier de conservation |
| [dao_sans_conservation.py](dao_sans_conservation.md) | `bnr2mnesys/*.xml` | `dao_sans_conservation.csv` | Liste les liens d'accès **orphelins** (sans conservation appariée) |
| [dao_appariement_conservation.py](dao_appariement_conservation.md) | `dao_sans_conservation.csv` + référentiel | `dao_appariement_conservation.csv` | Tente d'apparier les orphelins au référentiel par critères élargis (exacte / normalisée / padding), y compris fichiers non versés sur S3 |

But : distinguer les orphelins dont le fichier de conservation existe (sous un
nom légèrement différent, ou pas encore versé sur S3) de ceux qui n'ont
vraiment aucune correspondance.

---

## B. Plages first/last

| Script | Entrée | Sortie | Rôle |
|---|---|---|---|
| [dao_first_last_developpe.py](dao_first_last_developpe.md) | `bnr2mnesys/*.xml` | `dao_first_last_developpe.csv`, `dao_first_last_ambigus.csv` | Développe les plages : reconstitue tous les fichiers entre `first` et `last`, intermédiaires compris. L'`access` n'est développé que si le `preservation` de même famille est absent du groupe |
| [dao_first_last_verif_ref.py](dao_first_last_verif_ref.md) | `dao_first_last_developpe.csv` + référentiel | `dao_first_last_verif_ref.csv` | Vérifie la présence de chaque fichier développé dans le référentiel et sur S3 (uuid, statut, `s3_key`) |
| [dao_first_last_plages_lacunaires.py](dao_first_last_plages_lacunaires.md) | `dao_first_last_verif_ref.csv` | `dao_first_last_plages_lacunaires.csv` | Isole les plages **non contiguës** : distingue le sur-développement d'une numérotation à trous des fichiers réellement manquants |
| [dao_first_last_access_sans_conservation.py](dao_first_last_access_sans_conservation.md) | `dao_first_last_verif_ref.csv` + référentiel | `dao_first_last_access_sans_conservation.csv` | Catégorise les liens d'accès sans conservation déclarée : `conservation_existe`, `diffusion_seule` ou `absent` |

But : pour chaque fichier d'une plage (y compris les implicites), savoir s'il
existe en conservation et s'il est versé sur S3, et isoler les vrais manques
des artefacts de développement.

---

## Exécution

Le lanceur `scripts/ead/dao_appariement.sh` enchaîne toute la partie
appariement (hors `ead_bnr2mnesys.py`), en supposant les IR déjà transformés
dans `bnr2mnesys/*.xml`. Depuis la racine du dépôt :

    bash scripts/ead/dao_appariement.sh

Il appelle les scripts via `conda run -n ds`, dans l'ordre du flux. Les
sous-chaînes A et B partent toutes deux de `bnr2mnesys/*.xml` et sont
indépendantes. Pour rejouer une étape seule, les scripts se lancent aussi un à
un :

    # prérequis (lourd, rarement rejoué)
    conda run -n ds python scripts/ead/ead_bnr2mnesys.py

    # A. liens isolés
    conda run -n ds python scripts/ead/dao_sans_conservation.py
    conda run -n ds python scripts/ead/dao_appariement_conservation.py

    # B. plages first/last
    conda run -n ds python scripts/ead/dao_first_last_developpe.py
    conda run -n ds python scripts/ead/dao_first_last_verif_ref.py
    conda run -n ds python scripts/ead/dao_first_last_plages_lacunaires.py
    conda run -n ds python scripts/ead/dao_first_last_access_sans_conservation.py
