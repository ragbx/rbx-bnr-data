"""Vérifie sur la BN-R les liens de la carte publique : nombre de résultats de chaque lien.

Lit results/ead/indexation/bnr2mnesysgeo_lieux.umap (produit par geogname_ir2kml.py), interroge
chaque lien (une requête toutes les --pause secondes, sans session : un lien fq ne dépend pas
des filtres d'une visite précédente) et lit le nombre de résultats (« N résultats » de la page).

Sortie : results/ead/indexation/liens_bnr_verification.csv (thésaurus, libellé, occurrences dans
les IR de test, résultats BN-R, type de lien, lien). Avec --repli, les voies et quartiers dont le
filtre fq ne donne aucun résultat sont ajoutés à data/geo/liens_bnr_repli.csv : geogname_ir2kml.py
leur fera un lien plein texte (relancer ensuite geogname_ir2kml.py, carte_leaflet.py, et ce script).

    python scripts/ead/verifier_liens_bnr.py [--pause 1] [--repli]
"""
import argparse
import csv
import json
import re
import time
import urllib.request
from os.path import dirname, exists, join, realpath

RACINE = dirname(dirname(dirname(realpath(__file__))))
UMAP = join(RACINE, "results/ead/indexation/bnr2mnesysgeo_lieux.umap")
SORTIE = join(RACINE, "results/ead/indexation/liens_bnr_verification.csv")
REPLI = join(RACINE, "data/geo/liens_bnr_repli.csv")
CALQUES = {"Quartiers": "quartiers", "Voies": "rues", "Sites": "points"}
LIEN = re.compile(r"\[\[(.+?)\|")
NB = re.compile(r'class="nb-result"[^>]*>\s*(\d+)')


def resultats(url, essais=3):
    """Nombre de résultats de la page BN-R ; 0 si « Aucun résultat » ; None si illisible."""
    for essai in range(essais):
        try:
            requete = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (verification liens carte BnR)"})
            page = urllib.request.urlopen(requete, timeout=60).read().decode("utf-8", "replace")
            m = NB.search(page)
            if m:
                return int(m.group(1))
            if "Aucun résultat" in page:
                return 0
        except OSError:
            pass
        time.sleep(5 * (essai + 1))
    return None


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--pause", type=float, default=1.0, help="secondes entre deux requêtes (défaut 1)")
    p.add_argument("--repli", action="store_true", help="ajouter les fq sans résultat à liens_bnr_repli.csv")
    a = p.parse_args()

    with open(UMAP, encoding="utf-8") as f:
        umap = json.load(f)
    lignes = []
    for calque in umap["layers"]:
        thesaurus = CALQUES.get(calque["_umap_options"]["name"])
        if not thesaurus:
            continue  # zones de clic
        for feat in calque["features"]:
            desc = feat["properties"].get("description", "")
            lien = LIEN.search(desc)
            occ = re.match(r"(\d+) occurrence", desc)
            lignes.append({"thesaurus": thesaurus, "libelle": feat["properties"]["name"],
                           "occurrences": int(occ.group(1)) if occ else "",
                           "lien": lien.group(1).replace("%5B", "[").replace("%5D", "]") if lien else ""})

    for i, l in enumerate(lignes, 1):
        l["type"] = "plein texte" if "type_rech=rs" in l["lien"] else ("filtre fq" if l["lien"] else "")
        l["resultats"] = resultats(l["lien"]) if l["lien"] else ""
        if i % 25 == 0:
            print(f"  {i}/{len(lignes)}", flush=True)
        time.sleep(a.pause)

    with open(SORTIE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["thesaurus", "libelle", "occurrences", "resultats", "type", "lien"])
        w.writeheader()
        w.writerows(lignes)

    zero = [l for l in lignes if l["resultats"] == 0]
    illisibles = [l for l in lignes if l["lien"] and l["resultats"] is None]
    print(f"{len(lignes)} liens → {SORTIE}")
    print(f"  sans résultat : {len(zero)}", *[f"{l['thesaurus']} : {l['libelle']} ({l['type']})" for l in zero], sep="\n    ")
    print(f"  illisibles (à relancer) : {len(illisibles)}")

    if a.repli:
        deja = {}
        if exists(REPLI):
            with open(REPLI, encoding="utf-8") as f:
                deja = {(r["thesaurus"], r["libelle"]): r for r in csv.DictReader(f)}
        for l in zero:
            if l["type"] == "filtre fq":
                deja.setdefault((l["thesaurus"], l["libelle"]), {
                    "thesaurus": l["thesaurus"], "libelle": l["libelle"], "mode": "plein texte",
                    "motif": f"filtre fq sans résultat (vérification du {time.strftime('%Y-%m-%d')})"})
        with open(REPLI, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["thesaurus", "libelle", "mode", "motif"])
            w.writeheader()
            w.writerows(sorted(deja.values(), key=lambda r: (r["thesaurus"], r["libelle"])))
        print(f"  repli : {len(deja)} lieux → {REPLI}")


if __name__ == "__main__":
    main()
