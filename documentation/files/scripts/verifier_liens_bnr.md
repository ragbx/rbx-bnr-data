# verifier_liens_bnr.py

[scripts/ead/verifier_liens_bnr.py](../../../scripts/ead/verifier_liens_bnr.py)

Vérifie sur la BN-R les liens « Voir sur la BN-R » de la carte publique
(`results/ead/indexation/bnr2mnesysgeo_lieux.umap`, produite par
[geogname_ir2kml.py](geogname_ir2kml.md)) : nombre de résultats de chaque lien.
14/11/2026.

```bash
python scripts/ead/verifier_liens_bnr.py            # rapport seul
python scripts/ead/verifier_liens_bnr.py --repli    # + table de repli
```

- une requête par lien, `--pause` secondes entre deux (défaut 1 : environ
  10 minutes pour 549 liens), sans session (cookies), trois essais ;
- nombre lu dans « N résultats » de la page ; 0 si « Aucun résultat » ;
- sortie `results/ead/indexation/liens_bnr_verification.csv` : thésaurus,
  libellé, occurrences (IR de test), résultats, type de lien (filtre fq /
  plein texte), lien ;
- `--repli` : les voies et quartiers dont le filtre `fq` ne donne aucun
  résultat sont ajoutés à `data/geo/liens_bnr_repli.csv` (thésaurus,
  libellé, motif ; lignes existantes gardées). Relancer ensuite
  `geogname_ir2kml.py` (lien plein texte pour ces lieux), `carte_leaflet.py`,
  puis ce script pour contrôler.

Le nombre de résultats BN-R (documents des 52 IR en ligne) n'est pas
comparable au nombre d'occurrences de la carte (37 IR de test).
