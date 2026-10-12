"""Règles de nommage des s3_key du ref, corpus par corpus, en regex et en
gabarit lisible, en deux temps :
  1_nommage_actuel            : les noms tels qu'ils sont ;
  2_nommage_avec_indicateur   : les mêmes règles avec l'indicateur de rôle
                                _ZA_/_ZD_/_ZO_ inséré juste avant le numéro de
                                vue (conservation / diffusion / OCR).
Les METS, déjà suffixés .mets.xml, gardent leur nom.

La partie « doc » (tout ce qui précède la vue) est découpée en segments nommés
(corpus_code, date, annee, cote, serie, niv_X, num…) déduits des valeurs, puis
confrontée à l'identifiant du ref (`unitid`) pour situer où il s'arrête.
La cote est propre aux archives (AMR) : chiffres, lettres, chiffres éventuels.

Les regex sont déduites des noms existants puis vérifiées sur ces mêmes noms.
Lecture seule sur le ref.

Usage : python scripts/s3/indicateur_z_regex.py [ref.csv.gz] [dossier_sortie]
"""
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

REF = sys.argv[1] if len(sys.argv) > 1 else "results/ref/_ref_files_20260630.csv.gz"
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else "results/ref/indicateur_z")

ROLE = {
    "tif": "ZA", "tiff": "ZA", "wav": "ZA",
    "jpg": "ZD", "jpeg": "ZD", "pdf": "ZD", "png": "ZD",
    "mp3": "ZD", "mp4": "ZD", "mov": "ZD", "wmv": "ZD",
    "txt": "ZO", "xml": "ZO",
}
IMG = {"tif", "tiff", "jpg", "jpeg", "png"}
MAX_ALT = 12       # au-delà, une liste de sigles devient une classe de lettres
MAX_MD = 10        # structures détaillées par corpus dans le markdown
ECH_COTE = 300     # noms échantillonnés par structure pour situer la cote
RUN = re.compile(r"\d+|[A-Za-z]+|[^A-Za-z\d]")
RUN_AN = re.compile(r"\d+|[A-Za-z]+")
COTE = re.compile(r"\d+[A-Za-z]+\d*")   # cote d'archives


def esp(n):
    """Nombre avec espace comme séparateur de milliers."""
    return f"{n:,}".replace(",", " ")


def runs(tok):
    """Découpe un segment en suites de chiffres, de lettres, ou caractère autre."""
    return RUN.findall(tok)


def run_type(r):
    return "D" if r[0].isdigit() else "A" if r[0].isalpha() else r


def structure(stem):
    """Clé de structure : par segment, la suite des types de suites."""
    return "_".join("".join(run_type(r) for r in runs(t)) for t in stem.split("_"))


def affiner(d):
    """Scinde une structure quand un segment mêlant chiffres et lettres ne prend
    que 2 à 4 valeurs de lettres (ex. 1D008 / 499W001 : deux séries distinctes)."""
    vals = defaultdict(set)
    decoupes = {}
    for st, stem in zip(d.struct, d.stem):
        toks = [runs(t) for t in stem.split("_")]
        decoupes[stem] = toks
        for i, t in enumerate(toks):
            if len(t) > 1 and any(r[0].isdigit() for r in t):
                for j, r in enumerate(t):
                    if r[0].isalpha():
                        vals[(st, i, j)].add(r)
    cles = {c for c, v in vals.items() if 2 <= len(v) <= 4}
    if not cles:
        return d.struct
    par_struct = defaultdict(list)
    for st, i, j in sorted(cles):
        par_struct[st].append((i, j))
    return pd.Series(
        [st + "".join(f"|{decoupes[stem][i][j]}" for i, j in par_struct.get(st, ()))
         for st, stem in zip(d.struct, d.stem)], index=d.index)


def classe_lettres(vals):
    if all(len(v) == 1 for v in vals):
        if len(vals) <= 8:
            return "[" + "".join(sorted(vals)) + "]"
        return "[A-Z]" if all(v.isupper() for v in vals) else "[A-Za-z]"
    return "[A-Z]+" if all(v.isupper() for v in vals) else "[A-Za-z]+"


def rx_alpha(vals):
    if len(vals) == 1:
        return re.escape(next(iter(vals)))
    if len(vals) <= MAX_ALT and not all(len(v) == 1 for v in vals):
        return "(?:" + "|".join(sorted(vals)) + ")"
    return classe_lettres(vals)


def gab_alpha(vals):
    if len(vals) == 1:
        return next(iter(vals))
    if len(vals) <= MAX_ALT:
        return "{" + "|".join(sorted(vals)) + "}"
    return "{lettre}" if all(len(v) == 1 for v in vals) else "{sigle}"


def rx_digits(lens):
    a, b = min(lens), max(lens)
    return rf"\d{{{a}}}" if a == b else rf"\d{{{a},{b}}}"


def gab_digits(lens):
    a, b = min(lens), max(lens)
    return f"{{N{a}}}" if a == b else f"{{N{a}-{b}}}"


def est_date(vals):
    """Au moins 98 % des valeurs sont des dates AAAAMMJJ plausibles."""
    ok = sum(1500 <= int(v[:4]) <= 2030 and 1 <= int(v[4:6]) <= 12 and 1 <= int(v[6:]) <= 31 for v in vals)
    return ok / len(vals) >= 0.98


def est_annee(vals):
    return sum(1500 <= int(v) <= 2030 for v in vals) / len(vals) >= 0.98


def est_mois(vals):
    return all(1 <= int(v) <= 12 for v in vals) and len(vals) >= 8


def nommer(tok, i, cols, valeurs, precedent, archives):
    """Nom du segment d'après sa forme et ses valeurs ; None = littéral."""
    types = [run_type(r) for r in tok]
    if types == ["D"]:
        v = valeurs[i]
        lens = {len(x) for x in v}
        if lens == {8} and est_date(v):
            return "date"
        if lens == {4} and est_annee(v):
            return "annee"
        if lens == {2} and precedent == "annee" and est_mois(v):
            return "mois"
        return "num"
    if types == ["A"]:
        v = cols[(i, 0)]
        if len(v) == 1:
            return None
        return "lettre" if all(len(x) == 1 for x in v) else "serie"
    if types == ["D", "-", "D"] and cols[(i, 0)] == {4} and cols[(i, 2)] == {4}:
        return "periode"
    if archives and types in (["D", "A"], ["D", "A", "D"]) and not (
            all(x[0].islower() for x in cols[(i, 1)])):
        return "cote"   # cote d'archives : chiffres, lettres, chiffres éventuels (pas « 005a », « 10bis »)
    if all(t in "DA" for t in types) and types[0] == "A":
        v = cols[(i, 0)]
        if len(v) == 1 and re.fullmatch(r"[A-Za-z]+", next(iter(v))):
            return "niv_" + next(iter(v))
        return "sous_serie"
    return "seg"


def construire(stems, exts, sans_vue=False, corpus=""):
    """Regex et gabarit d'une structure, avant et après ajout de l'indicateur,
    avec la partie doc découpée en segments nommés."""
    cols, valeurs = defaultdict(set), defaultdict(set)
    for s in stems:
        for i, t in enumerate(s.split("_")):
            if t.isdigit():
                valeurs[i].add(t)
            for j, r in enumerate(runs(t)):
                cols[(i, j)].add(len(r) if r[0].isdigit() else r)
    modele = [runs(t) for t in stems[0].split("_")]
    nt = len(modele)
    vue = not sans_vue and nt > 1 and len(modele[-1]) == 1 and modele[-1][0].isdigit()

    # seules les archives (AMR) ont des cotes ; une cote n'est pas un code de corpus :
    # dans AMR_4FI, « 4FI » est la cote, et le nom ne porte alors pas de groupe corpus_code
    archives = corpus.startswith("AMR_")
    code_est_cote = archives and any(COTE.fullmatch(t) for t in corpus.split("_")[1:])
    # segments du nom égaux au code du corpus (casse ignorée)
    nc = len(corpus.split("_"))
    span = None
    code_present = False
    for o in (0, 1):
        if nt < o + nc + (1 if vue else 0):
            continue
        formes = {"_".join(st.split("_")[o:o + nc]) for st in stems}
        if all(f.upper() == corpus.upper() for f in formes) and (
                o == 0 or all(st.startswith("RBX_") for st in stems)):
            code_present = True
            if not code_est_cote:
                span = (o, o + nc, formes)
            break

    # entrées : (index du dernier segment couvert, nom, regex, gabarit)
    entrees, compte, precedent = [], Counter(), None
    i = 0
    while i < nt:
        if span and i == span[0]:
            formes = span[2]
            entrees.append((span[1] - 1, "corpus_code",
                            "(?P<corpus_code>" + "|".join(sorted(re.escape(f) for f in formes)) + ")",
                            "<corpus_code:" + "|".join(sorted(formes)) + ">"))
            i = span[1]
            precedent = "corpus_code"
            continue
        tok = modele[i]
        prx, pgab = "", ""
        for j, r in enumerate(tok):
            v = cols[(i, j)]
            if r[0].isdigit():
                prx += rx_digits(v)
                pgab += gab_digits(v)
            elif r[0].isalpha():
                prx += rx_alpha(v)
                pgab += gab_alpha(v)
            else:
                prx += re.escape(r)
                pgab += r
        base = "vue" if (vue and i == nt - 1) else nommer(tok, i, cols, valeurs, precedent, archives)
        if base is None:
            entrees.append((i, None, prx, pgab))
        else:
            compte[base] += 1
            nom = base if compte[base] == 1 else f"{base}{compte[base]}"
            if len(tok) == 1 and tok[0][0].isdigit():
                pgab = pgab.strip("{}")
            entrees.append((i, nom, f"(?P<{nom}>{prx})", f"<{nom}:{pgab}>"))
        precedent = base
        i += 1

    ext_rx = "(?P<ext>" + "|".join(sorted(exts)) + ")"
    corps = entrees[:-1] if vue else entrees
    doc_rx = "_".join(e[2] for e in corps)
    doc_gab = "_".join(e[3] for e in corps)
    if vue:
        avant = rf"^(?P<doc>{doc_rx})_{entrees[-1][2]}\.{ext_rx}$"
        apres = rf"^(?P<doc>{doc_rx})_(?P<role>Z[ADO])_{entrees[-1][2]}\.{ext_rx}$"
        g_avant = f"{doc_gab}_{entrees[-1][3]}"
        g_apres = f"{doc_gab}_<role:ZA|ZD|ZO>_{entrees[-1][3]}"
    else:
        avant = rf"^(?P<doc>{doc_rx})\.{ext_rx}$"
        apres, g_avant, g_apres = "", doc_gab, ""
    return dict(avant=avant, apres=apres, g_avant=g_avant, g_apres=g_apres, vue=vue,
                a_code="oui" if span else "fonds + cote" if code_present else "non", entrees=entrees)


def norm(r):
    return (r.lstrip("0") or "0") if r[0].isdigit() else r.upper()


def fin_cote(stem, unitid):
    """Nombre de segments du nom couverts par la cote du ref, et si la cote est
    retrouvée en entier. Compare les suites chiffres/lettres (zéros de tête et
    casse ignorés) et prend la plus longue suite commune."""
    s = [(i, norm(r)) for i, t in enumerate(stem.split("_")) for r in RUN_AN.findall(t)]
    u = [norm(r) for r in RUN_AN.findall(unitid)]
    best = (0, -1)
    prev = [0] * (len(u) + 1)
    for a in range(1, len(s) + 1):
        cur = [0] * (len(u) + 1)
        for b in range(1, len(u) + 1):
            if s[a - 1][1] == u[b - 1]:
                cur[b] = prev[b - 1] + 1
                if cur[b] > best[0]:
                    best = (cur[b], a - 1)
        prev = cur
    n, fin = best
    if n < 2 or not any(x[1].isdigit() for x in s[fin - n + 1:fin + 1]):
        return None
    return s[fin][0] + 1, n == len(u)


def situer_cote(g, entrees):
    """Où s'arrête la cote du ref dans les segments de la structure."""
    vide = dict(exemple_unitid="", unitid_jusqua="", apres_unitid="", part_unitid_reconnu=0.0,
                dernier_nombre_dans_unitid="")
    e = g[g.unitid.notna()]
    if e.empty:
        return vide
    e = e.head(ECH_COTE)
    ok = [r for r in (fin_cote(s, u) for s, u in zip(e.stem, e.unitid)) if r]
    if not ok:
        return dict(vide, exemple_unitid=e.unitid.iloc[0])
    kmod, nb = Counter(r[0] for r in ok).most_common(1)[0]
    noms = [(fin, nom or gab) for fin, nom, _, gab in entrees]
    dedans = [n for fin, n in noms if fin < kmod]
    dehors = [n for fin, n in noms if fin >= kmod]
    return dict(
        exemple_unitid=e.unitid.iloc[0],
        unitid_jusqua=dedans[-1] if dedans else "",
        apres_unitid=" + ".join(dehors),
        part_unitid_reconnu=round(nb / len(e) * 100, 1),
        dernier_nombre_dans_unitid="oui" if not dehors else "non",
    )


PSEUDO = """\
```
ROLE = { tif, tiff, wav            -> "ZA"   # conservation
         jpg, jpeg, pdf, png,
         mp3, mp4, mov, wmv        -> "ZD"   # diffusion
         txt, xml                  -> "ZO" } # OCR

fonction ajouter_indicateur(nom):
    stem, ext = couper_au_dernier_point(nom)
    si stem finit par ".mets":                 # METS : déjà identifié
        retourner nom
    role = ROLE[minuscules(ext)]               # l'extension d'origine est conservée
    si stem correspond à ^(.*)_(\\d+)$ :       # le dernier segment est le numéro de vue
        retourner \\1 + "_" + role + "_" + \\2 + "." + ext
    sinon:
        signaler(nom)                          # pas de numéro de vue : cas à trancher

fonction lire_indicateur(nom):
    si nom correspond à ^(.*)_(Z[ADO])_(\\d+)\\.([^.]+)$ :
        retourner (doc=\\1, role=\\2, vue=\\3, ext=\\4)
    retourner aucun                            # ancien nom, METS ou cas particulier
```
"""

LEXIQUE = [
    "## Lecture des fiches",
    "",
    "- Une **structure** regroupe les noms de même forme (mêmes segments entre underscores, mêmes alternances chiffres/lettres).",
    "- **gabarit** : chaque segment nommé s'écrit `<nom:contenu>` ; `N3` = nombre à 3 chiffres, `N1-3` = 1 à 3 chiffres, "
    "`{A|B}` = valeurs observées, `{sigle}` / `{lettre}` = trop de valeurs pour les lister. Le texte hors chevrons est fixe.",
    "- **regex** : `doc` englobe tout ce qui précède la vue ; ses segments sont des groupes nommés imbriqués%s.",
    "- **Segments de `doc`**, nommés d'après leur forme et leurs valeurs (ce sont des déductions, pas une norme) :",
    "  - `corpus_code` : le code du corpus (`PRA_JRX`, `MED_CP`), quand il figure tel quel dans le nom ;",
    "  - `date` : 8 chiffres formant une date AAAAMMJJ ; `annee` : 4 chiffres entre 1500 et 2030 ; "
    "`mois` : 2 chiffres de 01 à 12 après une année ; `periode` : deux années reliées par un tiret ;",
    "  - `cote` : cote d'archives, réservée aux corpus AMR : des chiffres, des lettres, puis éventuellement des chiffres "
    "(`4FI`, `1D008`, `499W001`, `6Fi022`). Une cote n'est pas un code de corpus : `AMR_4FI` est le corpus, `4FI` la cote ;",
    "  - `niv_X` : une lettre ou un sigle fixe suivi d'un numéro (`D01`, `L1`, `PH02`) ; "
    "`sous_serie` : sigle variable suivi d'un numéro (`LFP1`, `CAR1`) ;",
    "  - `serie` : sigle en lettres prenant plusieurs valeurs ; `lettre` : lettre seule ;",
    "  - `num`, `num2`… : nombre sans signification déduite ; `seg` : segment d'une autre forme.",
    "- **Identifiant du ref** : comparaison du nom avec la colonne `unitid` du ref (zéros de tête et casse ignorés), "
    "sur un échantillon de la structure. Elle indique jusqu'à quel segment va cet identifiant et ce qui vient après.",
    "- Les regex décrivent les noms **observés** : largeurs de nombres et listes de sigles sont celles du ref.",
    "",
]


def bloc_cote(r):
    if not r["exemple_unitid"]:
        return "identifiant du ref (unitid) : non renseigné pour ces fichiers"
    if not r["part_unitid_reconnu"]:
        return f"identifiant du ref (unitid) : `{r['exemple_unitid']}` — non retrouvé dans le nom"
    t = (f"identifiant du ref (unitid) : `{r['exemple_unitid']}` — va jusqu'à `{r['unitid_jusqua']}` "
         f"({r['part_unitid_reconnu']} % de l'échantillon)")
    if r["dernier_nombre_dans_unitid"] == "oui":
        return t + " ; **le dernier segment fait partie de l'identifiant**"
    return t + f" ; ensuite : `{r['apres_unitid']}`"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(REF, dtype=str, low_memory=False, usecols=["corpus_code", "s3_key", "unitid"])
    k = df[df.s3_key.notna()].copy()
    k["corpus_code"] = k.corpus_code.fillna("_SANS_CORPUS")
    k["dir"] = k.s3_key.str.rsplit("/", n=1).str[0]
    k["base"] = k.s3_key.str.rsplit("/", n=1).str[-1]
    k["stem"] = k.base.str.replace(r"\.[^.]+$", "", regex=True)
    k["extraw"] = k.base.str.extract(r"\.([^.]+)$")[0]
    k["ext"] = k.extraw.str.lower()
    k["mets"] = k.stem.str.endswith(".mets")
    k["role"] = k.ext.map(ROLE)
    k["struct"] = k.stem.map(structure)
    pre = k.stem.str.replace(r"_(\d+)$", "", regex=True)
    num = k.stem.str.extract(r"_(\d+)$")[0]
    k["ok"] = num.notna() & ~k.mets
    k["new_base"] = (pre + "_" + k.role + "_" + num + "." + k.extraw).where(k.ok)
    # fichiers de document (d'autres fichiers = ce nom + _<n°>) et sous-vues avérées
    k["dl"] = k.dir.str.lower() + "/" + k.stem.str.lower()
    k["parent"] = k.dl.str.replace(r"_[^_/]+$", "", regex=True)
    k["doc"] = k.ok & k.dl.isin(set(k.loc[k.ok, "parent"]))
    k["sous_vue"] = k.ok & k.ext.isin(IMG) & k.parent.isin(set(k.loc[k.ext.isin(IMG), "dl"]))

    rows, md_av, md_ap = [], [], []
    for cc, d in k.groupby("corpus_code"):
        n = len(d)
        nimg = d[d.ext.isin(IMG)].struct.value_counts()
        regs = []
        # une structure ne mêle pas noms avec et sans le code du corpus
        up = d.stem.str.upper()
        code = up.str.startswith(f"RBX_{cc.upper()}_") | up.str.startswith(f"{cc.upper()}_")
        for (st, _, _), g in d.groupby(["struct", affiner(d), code]):
            # fichiers de document (pdf/txt d'une séance…) : le dernier nombre est une date ou une cote
            de_doc = g.doc.mean() > 0.5
            c = construire(g.stem.tolist(), set(g.extraw), sans_vue=de_doc, corpus=cc)
            avant, apres, vue = c["avant"], c["apres"], c["vue"]
            mets = bool(g.mets.all())
            if mets:
                statut = "METS, nom inchangé"
            elif de_doc:
                statut = "fichier de document, sans numéro de vue : à trancher"
            elif not vue:
                statut = "sans numéro de vue : à trancher"
            elif re.search(r"_D_D$", st) and nimg.get(st[:-2], 0) > nimg.get(st, 0):
                statut = "règle directe, sous-vue probable à vérifier"
            else:
                statut = "règle directe"
            r = dict(
                corpus_code=cc, structure=st, nb_cles=len(g), part=round(len(g) / n * 100, 2),
                statut=statut, corpus_code_dans_nom=c["a_code"],
                segments_doc=" + ".join(nom for _, nom, _, _ in (c["entrees"][:-1] if vue else c["entrees"]) if nom),
                gabarit_avant=c["g_avant"], gabarit_apres=c["g_apres"],
                regex_avant=avant, regex_apres=apres,
                exemple_avant=g.base.iloc[0], exemple_apres=g.new_base.iloc[0] if vue and not mets else "",
                nb_doc_sans_vue=int(g.doc.sum()), nb_sous_vues_averees=int(g.sous_vue.sum()),
                # vérifications sur les noms réels
                verif_avant=int(g.base.str.fullmatch(avant).sum()),
                verif_apres=int(g.new_base.dropna().str.fullmatch(apres).sum()) if apres else 0,
                nb_transformables=int(g.ok.sum()) if vue else 0,
            )
            r.update(situer_cote(g, c["entrees"]))
            regs.append(r)
        regs.sort(key=lambda r: -r["nb_cles"])
        # ambiguïté entre anciens et nouveaux noms au sein du corpus
        rx_av = [re.compile(r["regex_avant"]) for r in regs]
        rx_ap = [re.compile(r["regex_apres"]) for r in regs if r["regex_apres"]]
        news = d.new_base.dropna().unique()
        olds = d.base.unique()
        amb_new = sum(any(r.fullmatch(x) for r in rx_av) for x in news)
        amb_old = sum(any(r.fullmatch(x) for r in rx_ap) for x in olds)
        for r in regs:
            r["nouveaux_noms_acceptes_par_ancienne_regle"] = amb_new
            r["anciens_noms_acceptes_par_nouvelle_regle"] = amb_old
        rows += regs

        direct = sum(r["nb_cles"] for r in regs if r["statut"].startswith("règle directe"))
        titre = f"## {cc} — {esp(n)} clés, {len(regs)} structure(s)"
        sans_code = sum(r["nb_cles"] for r in regs if r["corpus_code_dans_nom"] == "non")
        via_cote = sum(r["nb_cles"] for r in regs if r["corpus_code_dans_nom"] == "fonds + cote")
        ligne_code = ("Le code du corpus figure dans tous les noms." if sans_code + via_cote == 0 else
                      " ".join(x for x in (
                          f"Le code du corpus se lit par le fonds et la cote dans {esp(via_cote)} nom(s)." if via_cote else "",
                          f"Le code du corpus est absent de {esp(sans_code)} nom(s) sur {esp(n)}." if sans_code else "") if x))

        # --- temps 1 : nommage actuel
        md_av += [titre, "", ligne_code, ""]
        for r in regs[:MAX_MD]:
            md_av += [f"**{esp(r['nb_cles'])} clés ({r['part']} %)**", "", "```",
                      f"gabarit : {r['gabarit_avant']}",
                      f"regex   : {r['regex_avant']}",
                      f"exemple : {r['exemple_avant']}", "```",
                      "- " + bloc_cote(r), ""]
        if len(regs) > MAX_MD:
            reste = regs[MAX_MD:]
            md_av += [f"… {len(reste)} autre(s) structure(s), {esp(sum(r['nb_cles'] for r in reste))} clés "
                      f"(détail dans le CSV).", ""]

        # --- temps 2 : nommage avec indicateur
        md_ap += [titre, "",
                  f"{esp(direct)} clés ({direct / n:.1%}) reçoivent l'indicateur par la règle directe."
                  + (f" {amb_new} nouveau(x) nom(s) resteraient accepté(s) par une règle actuelle du corpus."
                     if amb_new > 0 else "") + " " + ligne_code, ""]
        for r in regs[:MAX_MD]:
            md_ap += [f"**{esp(r['nb_cles'])} clés ({r['part']} %) — {r['statut']}**", "", "```"]
            if r["statut"].startswith("règle directe"):
                md_ap += [f"gabarit : {r['gabarit_apres']}",
                          f"regex   : {r['regex_apres']}",
                          f"avant   : {r['exemple_avant']}",
                          f"après   : {r['exemple_apres']}"]
            else:
                md_ap += [f"gabarit : {r['gabarit_avant']}   (inchangé)",
                          f"regex   : {r['regex_avant']}",
                          f"exemple : {r['exemple_avant']}"]
            md_ap += ["```"]
            extra = []
            if r["statut"].startswith("règle directe") and r["dernier_nombre_dans_unitid"] == "oui":
                extra.append(f"l'indicateur s'insère à l'intérieur de l'identifiant du ref (`{r['exemple_unitid']}`)")
            if r["nb_doc_sans_vue"]:
                extra.append(f"{r['nb_doc_sans_vue']} fichier(s) de document dont le dernier nombre n'est pas une vue")
            if r["nb_sous_vues_averees"]:
                extra.append(f"{r['nb_sous_vues_averees']} sous-vue(s) avérée(s)")
            md_ap += (["À noter : " + " ; ".join(extra) + "."] if extra else []) + [""]
        if len(regs) > MAX_MD:
            reste = regs[MAX_MD:]
            md_ap += [f"… {len(reste)} autre(s) structure(s), {esp(sum(r['nb_cles'] for r in reste))} clés "
                      f"(détail dans le CSV).", ""]

    res = pd.DataFrame(rows)
    cote = ["exemple_unitid", "unitid_jusqua", "apres_unitid", "part_unitid_reconnu", "dernier_nombre_dans_unitid"]
    res[["corpus_code", "structure", "nb_cles", "part", "corpus_code_dans_nom", "segments_doc", "gabarit_avant",
         "regex_avant", "exemple_avant", *cote, "verif_avant"]].to_csv(OUT / "1_nommage_actuel.csv", index=False)
    res[["corpus_code", "structure", "nb_cles", "part", "statut", "corpus_code_dans_nom", "segments_doc",
         "gabarit_apres", "regex_apres", "exemple_avant", "exemple_apres", *cote, "nb_transformables",
         "verif_apres", "nb_doc_sans_vue", "nb_sous_vues_averees", "nouveaux_noms_acceptes_par_ancienne_regle",
         "anciens_noms_acceptes_par_nouvelle_regle"]].to_csv(OUT / "2_nommage_avec_indicateur.csv", index=False)
    source = f"Source : `{REF}` — {esp(len(k))} s3_key, {k.corpus_code.nunique()} corpus. Lecture seule."
    av = ["# 1. Règles de nommage actuelles, par corpus", "", source, "",
          "État des noms tels qu'ils sont dans le ref, sans indicateur de rôle.", ""]
    av += [l % ", suivis de `vue` et `ext`" if "%s" in l else l for l in LEXIQUE]
    (OUT / "1_nommage_actuel.md").write_text("\n".join(av + md_av), encoding="utf-8")
    ap = ["# 2. Règles de nommage avec l'indicateur de rôle, par corpus", "", source, "",
          "Indicateur inséré juste avant le numéro de vue : `_ZA_` conservation, `_ZD_` diffusion, `_ZO_` OCR. "
          "Les fichiers METS gardent leur nom (suffixe `.mets.xml`). "
          "Les règles actuelles sont dans `1_nommage_actuel.md`.", "",
          "## Règle générale (pseudo-code)", "", PSEUDO]
    ap += [l % ", suivis de `role` (l'indicateur), `vue` et `ext`" if "%s" in l else l for l in LEXIQUE]
    ap += ["- **Statuts** : *règle directe* = l'indicateur s'insère avant le dernier nombre ; "
           "*sous-vue probable* = même règle, mais le dernier nombre pourrait être une sous-vue ; "
           "*sans numéro de vue* et *fichier de document* = nom laissé tel quel en attendant une décision ; "
           "*METS* = nom inchangé.", ""]
    (OUT / "2_nommage_avec_indicateur.md").write_text("\n".join(ap + md_ap), encoding="utf-8")
    for f in ("regles_regex_par_corpus.md", "regles_regex_par_corpus.csv"):  # ancienne sortie fusionnée
        (OUT / f).unlink(missing_ok=True)

    print(res.groupby("corpus_code_dans_nom").agg(corpus=("corpus_code", "nunique"), cles=("nb_cles", "sum")).to_string())
    avec_cote = res[res.segments_doc.str.contains("cote")]
    print("segment cote :", int(avec_cote.nb_cles.sum()), "clés ; corpus :", sorted(avec_cote.corpus_code.unique()))
    print("structures", len(res), "| corpus", res.corpus_code.nunique())
    print("verif regex avant :", int(res.verif_avant.sum()), "/", int(res.nb_cles.sum()))
    print("verif regex après :", int(res.verif_apres.sum()), "/", int(res.nb_transformables.sum()))
    print(res.groupby("statut").agg(structures=("structure", "size"), cles=("nb_cles", "sum")).to_string())
    a = res.drop_duplicates("corpus_code")
    print("ambiguïté :", a[(a.nouveaux_noms_acceptes_par_ancienne_regle > 0) | (a.anciens_noms_acceptes_par_nouvelle_regle > 0)]
          [["corpus_code", "nouveaux_noms_acceptes_par_ancienne_regle", "anciens_noms_acceptes_par_nouvelle_regle"]].to_string(index=False))
    noms = Counter()
    for s, nb in zip(res.segments_doc, res.nb_cles):
        for x in s.split(" + "):
            noms["niv_X" if x.startswith("niv_") else re.sub(r"\d+$", "", x)] += nb
    print("segments nommés (clés) :", dict(noms.most_common()))
    d = res[res.statut.str.startswith("règle directe")]
    print("unitid : non renseigné", int(d[d.exemple_unitid == ""].nb_cles.sum()),
          "| non retrouvée", int(d[(d.exemple_unitid != "") & (d.part_unitid_reconnu == 0)].nb_cles.sum()),
          "| dernier segment DANS unitid", int(d[d.dernier_nombre_dans_unitid == "oui"].nb_cles.sum()),
          "| vue après unitid", int(d[d.dernier_nombre_dans_unitid == "non"].nb_cles.sum()))
    print(d[d.dernier_nombre_dans_unitid == "oui"].groupby("corpus_code").nb_cles.sum().to_string())


if __name__ == "__main__":
    main()
