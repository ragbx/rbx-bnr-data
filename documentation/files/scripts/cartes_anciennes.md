# cartes_anciennes.py

[scripts/ead/cartes_anciennes.py](../../../scripts/ead/cartes_anciennes.py)

Liste les **cartes et plans de la BN-R** (Médiathèque et Archives
municipales) d'après les IR d'origine (`data/ead/bnr`), notamment pour
choisir des plans anciens à géoréférencer comme fonds de la carte Leaflet
([carte_leaflet.py](carte_leaflet.md)). 14/11/2026.

```bash
python scripts/ead/cartes_anciennes.py
```

## IR lus

| IR | Contenu | Composants |
|---|---|---|
| `FR595129901_MED_23` | Collection des cartes et plans de la médiathèque (PLA_A grand, PLA_B moyen, PLA_C petit format) | 255 |
| `FR595129901_AMR_008` | Collection des cartes et plans des Archives (PLA_1, cotes 6 Fi) | 63 |
| `FR595129901_AMR_006` | Plans cadastraux : 1804, 1819, 1826, 1847, 1884 (cotes 1 G) | 21 |
| `FR595129901_AMR_001` | Pièces remarquables : genre « Cartes et plans » ou titre plan/carte — surtout des plans de bâtiments (Goutte de lait, Parc des sports…) | 50 (onglet à part) |

## Sortie

`results/ead/indexation/cartes_anciennes_bnr.xlsx` :

- **Cartes et plans** (339 lignes, triées par année) : fonds, série, cote
  BN-R, cote d'origine (« Cote : Ca001 », « Cote Archives : 6 Fi 1 »),
  titre, date, année, auteur(s) et rôle, échelle, description matérielle,
  présentation, numérisé (oui/non), nombre d'images, première image,
  Roubaix (mention dans le titre ou la présentation), **plan de ville**
  (série cadastre, ou titre plan/carte + Roubaix : candidat au
  géoréférencement ; inclut aussi quelques plans de bâtiments, à trier) ;
- **Plans AMR_001** (50 lignes), mêmes colonnes ;
- **Synthèse** : par fonds et par siècle, documents, numérisés, plans de ville.

## Bilan (14/11/2026)

339 cartes et plans, dont 275 numérisés et 134 « plans de ville ».

Limites : la date est celle de l'IR, souvent la date d'édition ; pour les
reproductions (« Il s'agit d'une reproduction d'une carte datant de… »), la
date de l'original n'est que dans la présentation. 124 documents de MED_23
sont sans date. Les fichiers d'image sont les noms de l'IR
(`RBX_AMR_PLA_6Fi001.jpg`…), pas des adresses web.
