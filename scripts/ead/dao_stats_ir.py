"""
Statistiques de couverture DAO par instrument de recherche.

Pour chaque IR de results/ead/ead_cor/bnr2mnesys, compte les composants de
dernier niveau (les <c> sans <c> enfant) selon qu'ils portent ou non un objet
numérisé, et ventile par fonds.

Un composant est compté « avec dao » s'il porte au moins un lien de fichier
(role commençant par access: ou preservation:). Les liens publication:* (ARK),
ajoutés à presque tous les composants par ead_bnr2mnesys.py, ne comptent pas :
ils ne signalent pas une numérisation. Le fonds (dao_racine) est déduit du nom
du fichier numérisé (préfixe RBX_<fonds>_…).

À lancer depuis la racine du dépôt. Produit
results/dao/dao_stats_ir_{date}.xlsx avec deux feuilles :
  - « par IR »    : nb de composants avec/sans dao par inventaire ;
  - « par fonds » : nb de composants avec dao par (inventaire, fonds).
"""
import re
from datetime import datetime
from glob import glob
from os.path import basename, join, splitext

import pandas as pd
from lxml import etree

DOSSIER = join("results", "ead", "ead_cor", "bnr2mnesys")
ROLES_FICHIER = ("access:", "preservation:")

# corrections de regroupement de fonds reprises de l'ancien dao_liste.py
CORRECTIONS_FONDS = {
    "MUS_VAI": "MUS_VAI",
    "LAI_": "LAI",
    "AMR_OBJ": "AMR_OBj",
    "AMR_PUV": "AMR_PUV",
    "MeD_PAR": "MED_PAR",
}


def racine_fonds(href):
    """Fonds (dao_racine) déduit du nom de fichier d'un lien numérisé."""
    base = splitext(basename(href))[0]
    m = re.match(r"^RBX_([a-zA-Z0-9]+_[a-zA-Z0-9]+)_[a-zA-Z0-9]+", base) or re.match(
        r"^([a-zA-Z0-9]+_[a-zA-Z0-9]+)_[a-zA-Z0-9]+", base
    )
    racine = m.group(1) if m else "INCONNU"
    for motif, valeur in CORRECTIONS_FONDS.items():
        if motif in racine:
            return valeur
    return racine


def premier_texte(element, chemin):
    """Texte du premier élément trouvé par xpath, ou None."""
    trouve = element.xpath(chemin)
    return trouve[0].text if trouve and trouve[0].text else None


lignes = []
for path in sorted(glob(join(DOSSIER, "*.xml"))):
    root = etree.parse(path).getroot()
    contexte = {
        "inventaire_fichier": basename(path),
        "inventaire_identifiant": premier_texte(root, "//eadheader//eadid"),
        "inventaire_titre": premier_texte(root, "//eadheader//titleproper"),
        "inventaire_soustitre": premier_texte(root, "//eadheader//subtitle"),
        "archdesc_unitid": premier_texte(root, "/ead/archdesc/did/unitid"),
    }

    for c in root.xpath("/ead/archdesc/dsc//c[not(c)]"):
        href_fichier = next(
            (
                d.get("href")
                for d in c.xpath("./dao | ./daogrp/daoloc")
                if (d.get("role") or "").startswith(ROLES_FICHIER)
            ),
            None,
        )
        lignes.append(
            {
                **contexte,
                "unitid": premier_texte(c, "did/unitid"),
                "dao": "avec dao" if href_fichier else "sans dao",
                "dao_racine": racine_fonds(href_fichier) if href_fichier else None,
                "n": 1,
            }
        )

composants = pd.DataFrame(lignes)
index_ir = [
    "inventaire_fichier",
    "inventaire_identifiant",
    "inventaire_titre",
    "inventaire_soustitre",
    "archdesc_unitid",
]
# pivot_table écarte les lignes dont une clé d'index est NaN : on comble les
# champs d'inventaire absents (sous-titre, etc.) pour ne perdre aucun IR.
composants[index_ir] = composants[index_ir].fillna("N/A")

par_ir = composants.pivot_table(
    index=index_ir,
    columns="dao",
    values="n",
    aggfunc="sum",
    fill_value=0,
    margins=True,
    margins_name="Total",
).reset_index()

par_fonds = (
    composants[composants["dao"] == "avec dao"]
    .pivot_table(
        index=index_ir + ["dao_racine"],
        values="n",
        aggfunc="sum",
        fill_value=0,
        margins=True,
        margins_name="Total",
    )
    .reset_index()
    .rename(columns={"n": "nb_dao"})
)

date = datetime.now().strftime("%Y%m%d")
sortie = join("results", "dao", f"dao_stats_ir_{date}.xlsx")
with pd.ExcelWriter(sortie) as writer:
    par_ir.to_excel(writer, sheet_name="par IR", index=False)
    par_fonds.to_excel(writer, sheet_name="par fonds", index=False)

total = len(composants)
avec = (composants["dao"] == "avec dao").sum()
print(f"{total} composants de dernier niveau dans {composants['inventaire_fichier'].nunique()} IR")
print(f"  avec dao : {avec}  |  sans dao : {total - avec}")
print(f"détail : {sortie}")
