# Données : instruments de recherche (EAD)

Les instruments de recherche (IR) de la bn-r sont encodés en EAD (Encoded Archival
Description), un format XML standard pour la description archivistique. Dans le cadre
de la refonte, ils sont transformés pour être importés dans Mnesys.

---

## Fichiers de travail

| Fichier | Origine | Rôle |
|---|---|---|
| `data/ead/bnr/*.xml` | Export bn-r | Fichiers EAD source |
| `data/oai/oai_records_*.csv.gz` | Script `oai/bnr_moissonnage.py` | Correspondances cote → osiros_id (anciens ARK) |
| `results/ir/liste_instruments_recherche_*.xlsx` | Script `ead/ead_liste_ir.py` | Liste de pilotage des IR à traiter |
| `results/ead_cor/bnr2mnesys/*.xml` | Script `ead/ead_bnr2mnesys.py` | Fichiers EAD transformés, prêts pour Mnesys |

---

## Workflow

1. **Exporter** les fichiers EAD depuis la bn-r vers `data/ead/bnr/`.
2. **Moissonner** le serveur OAI pour mettre à jour les correspondances cote → osiros_id.
   → `scripts/oai/bnr_moissonnage.py`
3. **Générer la liste de pilotage** des IR, puis renseigner le statut `TRANSFERER`
   pour les IR à traiter.
   → `scripts/ead/ead_liste_ir.py`
4. **Transformer** les fichiers EAD.
   → `scripts/ead/ead_bnr2mnesys.py`

---

## Pour aller plus loin

- [Script ead_bnr2mnesys](../scripts/ead_bnr2mnesys.md) — détail des transformations appliquées
