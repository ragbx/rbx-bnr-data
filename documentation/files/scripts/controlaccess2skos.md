# Script : controlaccess2skos.py

**Emplacement :** `scripts/ead/controlaccess2skos.py`

Extrait les accès indexés des instruments de recherche (contenu des
`<controlaccess>`) et en génère les thésaurus SKOS de la BnR, en un seul
passage. Fusionne les anciens `controlaccess_extraction.py` et `csv2skos.py` :
tout part des mêmes fichiers `bnr2mnesys/*.xml`, ce qui évite que le CSV
intermédiaire ne se désynchronise du corpus.

---

## Données d'entrée

| Fichier | Rôle |
|---|---|
| `results/ead/ead_cor/bnr2mnesys/*.xml` | IR transformés ; les enfants de `<controlaccess>` y portent l'attribut `source` normalisé par [ead_bnr2mnesys.py](ead_bnr2mnesys.md) |

Le thésaurus cible est porté par la donnée : l'attribut `source`, de la forme
`thesaurus--SLASH--bnr_<nom>.xml`, désigne directement le fichier à produire.
Aucun filtrage en dur par balise ni par valeur.

## Résultats

**Inventaire de l'indexation** —
`results/ead/indexation/controlaccess_extraction.csv`, une ligne par enfant de
`<controlaccess>`, conservé pour l'analyse.

| Colonne | Description |
|---|---|
| `eadid`, `titleproper` | Identifiant et titre de l'IR |
| `balise` | `subject`, `geogname`, `persname`, `corpname`, `genreform` |
| `concept` | Texte de l'accès |
| `source`, `role`, `normal` | Attributs de l'élément |

**Thésaurus SKOS** — `results/ead/indexation/thesaurus/bnr_<nom>.xml`, un par
valeur de `source` de thésaurus. Chaque fichier contient un `skos:ConceptScheme`
et, par valeur distincte, un `skos:Concept` (`skos:prefLabel` @fr,
`skos:inScheme`). Namespace : `https://www.bn-r.fr/thesaurus/<nom>#`.

Thésaurus produits : `bnr_genreform`, `bnr_persname`, `bnr_corpname`,
`bnr_chrono`, `bnr_theme`, `bnr_rameau`. Les accès sans source de thésaurus
(geogname `rue`/`quartier` désactivés, valeurs sans source ou hors vocabulaire)
figurent dans le CSV mais sont ignorés pour le SKOS.

---

## Utilisation

Depuis la racine du projet, après [ead_bnr2mnesys.py](ead_bnr2mnesys.md)
(qui pose les attributs `source`) :

    python scripts/ead/controlaccess2skos.py
