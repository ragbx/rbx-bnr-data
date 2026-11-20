"""Notices des Archives municipales (inventaires Mnesys) par rue, quartier et site de Roubaix, pour les fiches de la
carte « Bn-r : accès par plan » (carte_leaflet.py ; 2026-11-19, demande utilisateur).

Lecture seule des IR d'origine (data/ead/mnesys/FRAM59100_*.xml). Chaque <geogname> non vide est rattaché à un lieu
par sa forme, comme dans mnesys_geo_diagnostic.py (classer) :
  - alignée par le texte sur le référentiel des lieux, ou décision validée (table de décisions
    data/geo/concordance_mnesys_lieux.csv « oui », propositions automatiques non refusées), ou concordance BnR ;
  - une forme refusée dans la table (« non ») et les formes « à traiter » sont écartées ;
  - seuls les lieux de Roubaix sont gardés : thésaurus quartiers, rues, points (les communes et pays non).
Choix de l'utilisateur (2026-11-19) : alignés + validés ; les notices des archives n'ont pas de lien (pas d'ARK) :
la fiche montre la cote et le titre.

Sites (2026-11-19) : s'y ajoutent les notices dont un site validé de la carte est le sujet (titre), d'après le
repérage des sites dans les IR Mnesys (sites_candidats.py --prefixe propositions_sites_mnesys : liens « sujet » ou
« lieu », comme pour la BnR) ; le site est rattaché par sa clé (colonne site_cles de data/geo/sites_carte.csv, et
noms de data/geo/sites_fusions.csv) ; lieu_id = point_id du site validé, origine « site ».

Sortie : results/ead/indexation/archives_carte_notices.csv — thesaurus, lieu_id, libelle, ir, c_id, cote, date,
normal, titre, forme ; une ligne par lieu × notice. Quartier déduit (2026-11-20) : une notice citant un site ou une rue
d'un seul quartier est aussi rattachée à ce quartier (origine « quartier déduit »). Les <geogname> hors de tout <c> (archdesc) sont comptés.

    python scripts/ead/archives_carte_notices.py      (env conda rbx-bnr-data)
    python scripts/ead/carte_leaflet.py
"""
import csv
import re
import sys
import unicodedata
from collections import Counter
from glob import glob
from os.path import basename, dirname, exists, join, realpath

from lxml import etree

sys.path.insert(0, dirname(realpath(__file__)))
from mnesys_geo_commun import SERIES, lire_formes, lire_table  # noqa: E402
from mnesys_geo_diagnostic import classer  # noqa: E402
from geogname_concordance_lieux import REFERENTIEL, lire  # noqa: E402

RACINE = dirname(dirname(dirname(realpath(__file__))))
SORTIE = join(RACINE, "results/ead/indexation/archives_carte_notices.csv")
ROUBAIX = ("quartiers", "rues", "points")
COLONNES = ["thesaurus", "lieu_id", "libelle", "ir", "c_id", "cote", "date", "normal", "titre", "forme", "origine"]
GEO = join(RACINE, "data", "geo")
SITES_CARTE = join(GEO, "sites_carte.csv")
FUSIONS = join(GEO, "sites_fusions.csv")
LIENS_RETENUS = {"sujet", "lieu"}


def normal(texte):
    """« Hôtel-Dieu » → « hotel dieu » (casse, accents, ponctuation), comme sites_carte.py."""
    t = unicodedata.normalize("NFD", texte.lower()).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", " ", t).strip()


def liens_sites():
    """[(point_id, nom affiché, ir, c_id, forme)] : notices Mnesys dont un site validé de la carte est le sujet."""
    props = sorted(glob(join(GEO, "propositions_sites_mnesys_*_notices.csv")))
    if not props or not exists(SITES_CARTE):
        print("  ! pas de repérage des sites Mnesys (sites_candidats.py --prefixe propositions_sites_mnesys)")
        return []
    point_de = {}
    with open(SITES_CARTE, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["afficher"].strip().lower() == "non":
                continue
            for k in r["site_cles"].split(" | "):
                if k.strip():
                    point_de[k.strip()] = (r["point_id"], r["nom"].strip() or r["nom_propose"])
    notices = list(csv.DictReader(open(props[-1], encoding="utf-8")))
    if exists(FUSIONS):   # noms d'un même site : clé du nom → point du site cible
        cle_de_forme = {normal(n["forme"]): n["site_cle"] for n in notices}
        for fu in csv.DictReader(open(FUSIONS, encoding="utf-8")):
            cle, cible = cle_de_forme.get(normal(fu["nom"])), fu["site_cle_cible"].strip()
            if cle and cible in point_de:
                point_de.setdefault(cle, point_de[cible])
    return [(*point_de[n["site_cle"]], n["ir"], n["c_id"], n["forme"]) for n in notices
            if n["lien"] in LIENS_RETENUS and n["site_cle"] in point_de]


def texte(e):
    return " ".join("".join(e.itertext()).split()) if e is not None else ""


def main():
    refuses = {f for f, t in lire_table().items() if t["decision"] == "non"}
    libelle = {l["lieu_id"]: l.get("libelle_manuel", "").strip() or l["libelle"] for l in lire(REFERENTIEL)}
    lieu_de = {r["forme"]: r for r in classer(lire_formes("mnesys"))
               if r["categorie"] != "à traiter" and r["thesaurus"] in ROUBAIX and r["forme"] not in refuses}

    lignes, vues, hors_c, ecartees, infos = [], set(), Counter(), Counter(), {}
    for f in sorted(glob(SERIES["mnesys"]["ir"])):
        ir = basename(f)[:-4].removeprefix("FRAM59100_")
        try:
            arbre = etree.parse(f)
        except etree.XMLSyntaxError:   # esperluettes non échappées : aucun <geogname>
            continue
        for c in arbre.iter("c"):   # cote, date et titre de chaque notice (liens des sites)
            did = c.find("did")
            if did is not None:
                ud = did.find("unitdate")
                infos[(ir, c.get("id", ""))] = (texte(did.find("unitid")), texte(ud),
                                               (ud.get("normal") or "") if ud is not None else "",
                                               texte(did.find("unittitle")))
        for g in arbre.iter("geogname"):
            forme = texte(g)
            if not forme:
                continue
            r = lieu_de.get(forme)
            if r is None:
                ecartees[forme] += 1
                continue
            c = next(g.iterancestors("c"), None)
            if c is None:
                hors_c[ir] += 1
                continue
            cle = (r["lieu_id"], ir, c.get("id"))
            if cle in vues:
                continue
            vues.add(cle)
            did = c.find("did")
            ud = did.find("unitdate") if did is not None else None
            lignes.append({"thesaurus": r["thesaurus"], "lieu_id": r["lieu_id"],
                           "libelle": libelle.get(r["lieu_id"], r["lieu"]), "ir": ir, "c_id": c.get("id", ""),
                           "cote": texte(did.find("unitid")) if did is not None else "",
                           "date": texte(ud), "normal": (ud.get("normal") or "") if ud is not None else "",
                           "titre": texte(did.find("unittitle")) if did is not None else "", "forme": forme,
                           "origine": "geogname"})

    sites = 0
    for point_id, nom, ir, c_id, forme in liens_sites():
        if (point_id, ir, c_id) in vues or (ir, c_id) not in infos:
            continue
        vues.add((point_id, ir, c_id))
        cote, date, norm, titre = infos[(ir, c_id)]
        lignes.append({"thesaurus": "points", "lieu_id": point_id, "libelle": nom, "ir": ir, "c_id": c_id, "cote": cote,
                       "date": date, "normal": norm, "titre": titre, "forme": forme, "origine": "site"})
        sites += 1

    # Quartier déduit (2026-11-20, même règle que geogname_ajouts.py) : une notice citant un site ou une rue situés
    # dans un seul quartier est aussi rattachée à ce quartier (rue ambiguë : rien) ; sans doublon (clé vues).
    ref = {l["lieu_id"]: l for l in lire(REFERENTIEL)}
    quartier_id = {l["libelle"]: l["lieu_id"] for l in ref.values() if l["thesaurus"] == "quartiers"}
    deduits = 0
    for l in list(lignes):
        lieu = ref.get(l["lieu_id"])
        if l["thesaurus"] not in ("rues", "points") or lieu is None:
            continue
        qs = [q for q in lieu["quartiers"].split(" | ") if q]
        if len(qs) != 1 or qs[0] not in quartier_id:
            continue
        cle = (quartier_id[qs[0]], l["ir"], l["c_id"])
        if cle in vues:
            continue
        vues.add(cle)
        lignes.append({**l, "thesaurus": "quartiers", "lieu_id": quartier_id[qs[0]],
                       "libelle": libelle.get(quartier_id[qs[0]], qs[0]), "forme": l["libelle"], "origine": "quartier déduit"})
        deduits += 1

    lignes.sort(key=lambda l: (l["thesaurus"], l["libelle"], l["ir"], l["normal"] or l["date"] or "9999", l["cote"]))
    with open(SORTIE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, COLONNES, lineterminator="\n")
        w.writeheader()
        w.writerows(lignes)

    print(f"→ {SORTIE}")
    print(f"  {len(lignes)} lignes lieu × notice ; lieux : "
          f"{dict(Counter(t for t, _ in {(l['thesaurus'], l['lieu_id']) for l in lignes}))} ; "
          f"notices : {len({(l['ir'], l['c_id']) for l in lignes})} dans {len({l['ir'] for l in lignes})} IR")
    print(f"  <geogname> écartés (à traiter, refusés, hors Roubaix) : {sum(ecartees.values())} ; "
          f"hors de tout <c> : {sum(hors_c.values())} {dict(hors_c) if hors_c else ''}")
    print(f"  quartiers déduits (site ou rue d'un seul quartier) : {deduits}")
    print(f"  notices rattachées à un site validé (sujet) : {sites} "
          f"({len({l['lieu_id'] for l in lignes if l['origine'] == 'site'})} sites)")
    print(f"  sans titre : {sum(1 for l in lignes if not l['titre'])} ; sans cote : {sum(1 for l in lignes if not l['cote'])}")


if __name__ == "__main__":
    main()
