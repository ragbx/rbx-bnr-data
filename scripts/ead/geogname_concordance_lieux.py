"""
Concordance des formes des IR avec les lieux : aligne chaque forme de
<geogname> trouvée dans les IR sur un lieu du référentiel des lieux de
Roubaix, par le texte seul.

Sources :
  - results/ead/indexation/rbx-bnr_geogname_origine.csv : les <geogname> des
    IR dans leur état d'avant corrections (hors vides et @source="chrono") ;
  - data/geo/referentiel_lieux_roubaix.csv (et _ecartes.csv) : le référentiel,
    seule source des lieux.

Une ligne par forme : texte exact du <geogname> et @source d'origine. La
forme est remise à l'endroit (« Achille Screpel, rue » → « rue Achille
Screpel ») puis réduite à une clé (sans accents, casse, ponctuation ni
articles, cf. cle_voie de referentiel_lieux_roubaix.py), comparée aux clés des
noms de chaque lieu, par ordre de préférence : libellé et noms des sources
(BAN, BD TOPO, MEL, TOPO), puis variantes, puis anciens noms. Les @coord des
IR sont ignorés.

Quand le texte désigne plusieurs lieux, l'@source départage (quartier →
thésaurus quartiers, rue → rues, adresse → points). Sans lieu unique, la
colonne candidats propose les lieux au nom le plus ressemblant. Une forme
sans lieu ni type de voie et sans @source de Roubaix (commune, pays…) est
« hors référentiel » : elle relève du thésaurus communes_pays
(data/geo/referentiel_communes_pays.csv), où elle n'est alignée que par
libelle_manuel (pas d'alignement automatique).

libelle_retenu est le libellé du lieu retenu : c'est lui qui remplacera le
texte du <geogname>. Une graphie à corriger se corrige dans le référentiel
(libelle_manuel), pas ici. Dans la concordance, libelle_manuel sert à
choisir le lieu quand l'alignement n'en trouve aucun ou plusieurs : il doit
être le libellé exact d'un lieu du référentiel ou d'un concept de
communes_pays.

La colonne a_traiter liste ce qui reste à régler ; elle se vide dès que
validation est remplie (« validé » par convention). Deux valeurs de validation
tranchent une forme sans lui donner de lieu : « hors référentiel » (commune,
pays… prise à tort pour une voie de Roubaix) et « à retirer » (<geogname> qui
n'est pas un lieu, à retirer des IR) ; elles deviennent son statut. Les colonnes libelle_manuel, validation et remarque,
remplies à la main, sont conservées d'une exécution à l'autre (clé : forme,
source).

Sortie : data/geo/concordance_geogname_lieux.csv.

À lancer depuis la racine du dépôt, après referentiel_lieux_roubaix.py :
    python scripts/ead/geogname_concordance_lieux.py
"""
import csv
import re
import sys
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from os.path import basename, dirname, exists, join

sys.path.insert(0, dirname(__file__))
from referentiel_lieux_roubaix import GEO, SORTIE as REFERENTIEL, SORTIE_ECARTES, cle_voie  # noqa: E402

csv.field_size_limit(sys.maxsize)

ORIGINE_CSV = join("results", "ead", "indexation", "rbx-bnr_geogname_origine.csv")
SORTIE = join(GEO, "concordance_geogname_lieux.csv")
# Thésaurus des lieux hors de Roubaix (communes, départements, pays…), tenu à la main.
COMMUNES_PAYS = join(GEO, "referentiel_communes_pays.csv")

COLONNES = [
    "a_traiter", "forme", "source", "occurrences", "ir", "forme_cle", "statut", "alignement",
    "lieu_id", "thesaurus", "libelle_retenu", "candidats", "libelle_manuel", "validation", "remarque",
]
MANUELLES = ["libelle_manuel", "validation", "remarque"]

# Noms d'un lieu comparés à la forme, par rang de préférence : au premier rang
# qui donne au moins un lieu, on s'arrête.
CHAMPS = [
    [("libelle", "libellé"), ("nom_ban", "nom BAN"), ("nom_bdtopo", "nom BD TOPO"), ("nom_mel", "nom MEL"),
     ("nom_topo", "nom TOPO")],
    [("variantes", "variante")],
    [("nom_ancien", "ancien nom")],
]
THESAURUS_SOURCE = {"quartier": "quartiers", "rue": "rues", "adresse": "points"}
# Valeurs de validation qui tranchent une forme sans lui donner de lieu : hors du
# référentiel de Roubaix (commune, pays…), ou <geogname> qui n'est pas un lieu, à
# retirer des IR.
HORS_REFERENTIEL = "hors référentiel"
A_RETIRER = "à retirer"
STATUTS = ["sans lieu", "plusieurs lieux", "lieu écarté", "aligné", HORS_REFERENTIEL, A_RETIRER]
TYPE_VOIE = re.compile(
    r"\b(rue|boulevard|avenue|place|quai|impasse|chemin|square|parc|all[ée]e?|cour|passage|route|pont|sentier|"
    r"contour|cit[ée]|carrefour|rond[- ]point|parvis|mail|esplanade|promenade|ruelle|grand[' ]place|grande rue)\b",
    re.I,
)
NB_CANDIDATS = 3
RESSEMBLANCE_MIN = 0.75


def reconstruire(forme):
    """'Achille Screpel, rue' → 'rue Achille Screpel' (forme BnR à l'envers)."""
    forme = forme.replace("\\'", "'")
    if "," not in forme:
        return forme.strip()
    nom, type_voie = (p.strip() for p in forme.split(",", 1))
    if not type_voie:
        return nom
    return f"{type_voie}{nom}" if type_voie.endswith("'") else f"{type_voie} {nom}"


def lire(fichier):
    with open(fichier, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def noms(lieu, champ):
    return [n.strip() for n in lieu[champ].split(" | ") if n.strip()]


def indexer(lieux):
    """Par rang de CHAMPS : clé → {lieu_id: champ par lequel la clé a été trouvée}."""
    index = [defaultdict(dict) for _ in CHAMPS]
    for lieu in lieux:
        for rang, champs in enumerate(CHAMPS):
            for champ, etiquette in champs:
                for nom in noms(lieu, champ):
                    index[rang][cle_voie(nom)].setdefault(lieu["lieu_id"], etiquette)
    return index


def chercher(cles, index):
    """(rang, {lieu_id: champ}) au premier rang où une des clés est trouvée, sinon (None, {})."""
    for rang, par_cle in enumerate(index):
        trouves = {}
        for k in cles:
            for lieu_id, champ in par_cle.get(k, {}).items():
                trouves.setdefault(lieu_id, champ)
        if trouves:
            return rang, trouves
    return None, {}


def sans_type(k):
    """Clé sans son type de voie initial (« rue gare » → « gare »)."""
    type_voie, _, reste = k.partition(" ")
    return reste if reste and TYPE_VOIE.fullmatch(type_voie) else k


def ressemblance(k, cles_lieu):
    """Ressemblance entre la clé de la forme et la plus proche des clés d'un lieu, type de
    voie mis à part : ratio de difflib, augmenté de 1 si les mots de l'une sont tous dans
    l'autre (« place spriet » / « place charles louis spriet »)."""
    nom = sans_type(k)
    mots = set(nom.split())
    meilleure = 0
    for c in cles_lieu:
        autre = sans_type(c)
        autres = set(autre.split())
        inclus = bool(mots and autres and (mots <= autres or autres <= mots))
        meilleure = max(meilleure, inclus + SequenceMatcher(None, nom, autre).ratio())
    return meilleure


def candidats(k, thesaurus, lieux, cles_par_lieu):
    """Lieux au nom le plus ressemblant, ceux du thésaurus attendu d'abord."""
    scores = sorted(
        ((ressemblance(k, cles_par_lieu[l["lieu_id"]]), l["thesaurus"] != thesaurus, l["libelle"], l) for l in lieux),
        key=lambda s: (s[1], -s[0], s[2]),
    )
    return [s[3] for s in scores if s[0] >= RESSEMBLANCE_MIN][:NB_CANDIDATS]


def decrire(lieu, ecarte=False):
    return f"{lieu['libelle']} [{lieu['lieu_id']}]" + (" (écarté)" if ecarte else "")


def aligner(forme, source, lieux, ecartes, index, index_ecartes, cles_par_lieu):
    """Statut, alignement, lieu retenu (ou None), candidats et motifs d'une forme."""
    texte = reconstruire(forme)
    k = cle_voie(texte)
    cles = [k] + [c for c in [cle_voie(forme)] if c != k]
    par_id = {l["lieu_id"]: l for l in lieux}
    r = {"forme_cle": k, "statut": "", "alignement": "", "lieu": None, "candidats": [], "motifs": []}

    _, trouves = chercher(cles, index)
    if len(trouves) > 1 and source in THESAURUS_SOURCE:
        du_thesaurus = {i: c for i, c in trouves.items() if par_id[i]["thesaurus"] == THESAURUS_SOURCE[source]}
        if len(du_thesaurus) == 1:
            trouves = du_thesaurus
    if len(trouves) == 1:
        (lieu_id, champ), = trouves.items()
        r.update(statut="aligné", alignement=champ, lieu=par_id[lieu_id])
        if champ == "ancien nom":
            r["motifs"].append("alignée sur un ancien nom du lieu (à vérifier)")
        return r
    if trouves:
        r.update(statut="plusieurs lieux", alignement=", ".join(sorted(set(trouves.values()))),
                 candidats=[decrire(par_id[i]) for i in sorted(trouves, key=lambda i: par_id[i]["libelle"])])
        r["motifs"].append(f"le texte désigne {len(trouves)} lieux : choisir (libelle_manuel)")
        return r

    _, trouves_ecartes = chercher(cles, index_ecartes)
    # Thésaurus attendu : celui de l'@source, sinon rues pour une forme qui porte un type de voie.
    thesaurus = THESAURUS_SOURCE.get(source) or ("rues" if TYPE_VOIE.search(texte) else None)
    proches = candidats(k, thesaurus, lieux, cles_par_lieu) if thesaurus else []
    if trouves_ecartes:
        par_id_ecartes = {l["lieu_id"]: l for l in ecartes}
        r.update(statut="lieu écarté",
                 candidats=[decrire(par_id_ecartes[i], ecarte=True) for i in sorted(trouves_ecartes)]
                 + [decrire(l) for l in proches])
        r["motifs"].append("lieu « ne pas conserver » dans le référentiel : choisir un autre lieu ou le réintégrer")
        return r
    if thesaurus:
        r.update(statut="sans lieu", candidats=[decrire(l) for l in proches])
        r["motifs"].append("aucun lieu du référentiel" + (" : voir candidats" if proches else ""))
        return r
    r["statut"] = "hors référentiel"
    return r


def appliquer_manuel(r, libelle_manuel, source, lieux):
    """Lieu choisi à la main par son libellé exact dans le référentiel."""
    choix = [l for l in lieux if l["libelle"] == libelle_manuel]
    if len(choix) > 1 and source in THESAURUS_SOURCE:
        choix = [l for l in choix if l["thesaurus"] == THESAURUS_SOURCE[source]] or choix
    if len(choix) == 1:
        r.update(statut="aligné", alignement="manuel", lieu=choix[0], motifs=[])
    elif choix:
        r["motifs"].append(f"libellé manuel présent dans {len(choix)} thésaurus")
    else:
        r["motifs"].append("libellé manuel absent du référentiel")


def main():
    for fichier in (SORTIE, REFERENTIEL):
        verrou = join(dirname(fichier), f".~lock.{basename(fichier)}#")
        if exists(verrou):
            sys.exit(f"{fichier} est ouvert (verrou {verrou}) : le fermer avant de relancer.")

    formes = defaultdict(lambda: {"occurrences": 0, "ir": set()})
    for g in lire(ORIGINE_CSV):
        if not g["geogname"].strip() or g["source"] == "chrono":
            continue
        f = formes[(g["geogname"], g["source"])]
        f["occurrences"] += 1
        f["ir"].add(g["eadid"].replace("FR595126101_", ""))

    lieux, ecartes = lire(REFERENTIEL), lire(SORTIE_ECARTES)
    communes_pays = lire(COMMUNES_PAYS) if exists(COMMUNES_PAYS) else []
    index, index_ecartes = indexer(lieux), indexer(ecartes)
    cles_par_lieu = {
        l["lieu_id"]: {cle_voie(n) for champs in CHAMPS for champ, _ in champs for n in noms(l, champ)} for l in lieux
    }
    manuelles = {(m["forme"], m["source"]): m for m in lire(SORTIE)} if exists(SORTIE) else {}

    lignes = []
    for (forme, source), f in formes.items():
        r = aligner(forme, source, lieux, ecartes, index, index_ecartes, cles_par_lieu)
        garde = {c: manuelles.get((forme, source), {}).get(c, "") for c in MANUELLES}
        if garde["libelle_manuel"].strip():
            appliquer_manuel(r, garde["libelle_manuel"].strip(), source, lieux + communes_pays)
        if garde["validation"].strip().lower() in (HORS_REFERENTIEL, A_RETIRER):
            r.update(statut=garde["validation"].strip().lower(), alignement="manuel", lieu=None, candidats=[], motifs=[])
        lieu = r["lieu"] or {}
        lignes.append({
            "a_traiter": "" if garde["validation"] else " ; ".join(r["motifs"]),
            "forme": forme, "source": source, "occurrences": f["occurrences"], "ir": ", ".join(sorted(f["ir"])),
            "forme_cle": r["forme_cle"], "statut": r["statut"], "alignement": r["alignement"],
            "lieu_id": lieu.get("lieu_id", ""), "thesaurus": lieu.get("thesaurus", ""),
            "libelle_retenu": lieu.get("libelle", ""), "candidats": " | ".join(r["candidats"]), **garde,
        })
    lignes.sort(key=lambda l: (STATUTS.index(l["statut"]), -l["occurrences"], l["forme"]))
    with open(SORTIE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(lignes)

    print(f"{len(lignes)} formes, {sum(l['occurrences'] for l in lignes)} geogname → {SORTIE}")
    print(f"{'statut':18s} {'formes':>7s} {'geogname':>9s}")
    for statut in STATUTS:
        du_statut = [l for l in lignes if l["statut"] == statut]
        print(f"{statut:18s} {len(du_statut):7d} {sum(l['occurrences'] for l in du_statut):9d}")
    print("  alignement :", dict(Counter(l["alignement"] for l in lignes if l["statut"] == "aligné").most_common()))
    print(f"  à traiter : {sum(1 for l in lignes if l['a_traiter'])} formes")
    cites = {(l["thesaurus"] == "communes_pays", l["lieu_id"]) for l in lignes if l["lieu_id"]}
    print(f"  lieux cités : {sum(1 for cp, _ in cites if not cp)} / {len(lieux)} ; "
          f"communes_pays : {sum(1 for cp, _ in cites if cp)} / {len(communes_pays)}")


if __name__ == "__main__":
    main()
