# rameau_lieux.py

Repère, parmi les vedettes RAMEAU (`subject source=bnr_rameau`) des IR actuels
(`results/ead/ead_cor/bnr2mnesys/`), celles qui désignent un lieu. Lecture seule.

```bash
python scripts/ead/rameau_lieux.py
```

Classement **fait à la main** (liste des 928 termes relue le 14/11/2026, dictionnaires
`LIEUX` et `DOUTEUX` du script) ; un nouveau terme RAMEAU n'y entre pas tout seul.

Sortie `results/ead/indexation/rameau_lieux.xlsx` :
- `lieux` : vedettes de lieu (édifice / site nommé, commune, département / région, pays,
  site naturel, lieu + subdivision, à trancher), occurrences, notices, IR, correspondance
  dans `referentiel_communes_pays.csv` ou `referentiel_lieux_roubaix.csv` ;
- `sujets_localises` : sujet qualifié par un lieu, « Grèves (Roubaix, Nord) » → sujet + lieu.

Non retenus : types de lieux sans nom (Châteaux, Montagnes, Ponts…), événements
(Paris-Roubaix, traités, batailles), périodiques, partis.
