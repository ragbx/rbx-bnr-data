"""Analyse, corpus par corpus, des règles de nommage des s3_key du ref et de
l'impact de l'indicateur de rôle _ZA_/_ZD_/_ZO_ inséré juste avant le
numéro de vue (conservation / diffusion / OCR). Les METS, déjà suffixés
.mets.xml, gardent leur nom.

Lecture seule sur le ref : ne produit que des fichiers d'analyse.

Usage : python scripts/s3/indicateur_z_analyse.py [ref.csv.gz] [dossier_sortie]
"""
import re
import sys
from collections import Counter
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


def tok_class(tok):
    """Classe d'un segment (entre underscores) hors préfixe."""
    if re.fullmatch(r"\d+", tok):
        return f"N{len(tok)}"
    if re.fullmatch(r"[A-Za-z]", tok):
        return "<L>"
    if re.fullmatch(r"[A-Za-z]+", tok):
        return "<SERIE>"
    return re.sub(r"\d+", "#", tok)


def common_prefix(stems, seuil=0.9):
    """Plus long préfixe de segments partagé par au moins `seuil` des noms."""
    toks = [s.split("_") for s in stems]
    pref = []
    cur = toks
    n = len(toks)
    i = 0
    while True:
        c = Counter(t[i] for t in cur if len(t) > i + 1)  # garde au moins 1 segment après
        if not c:
            break
        val, nb = c.most_common(1)[0]
        if nb / n < seuil or re.fullmatch(r"\d+", val):
            break
        pref.append(val)
        cur = [t for t in cur if len(t) > i + 1 and t[i] == val]
        i += 1
    return pref


def motif(stem, pref):
    toks = stem.split("_")
    if toks[: len(pref)] == pref:
        return "_".join(pref + [tok_class(t) for t in toks[len(pref):]])
    return "_".join(tok_class(t) if i else t for i, t in enumerate(toks))


def categorie_sans_num(stem):
    if stem.endswith(".mets"):
        return "mets"
    if re.search(r"kHz\d+B$", stem):
        return "audio_suffixe_technique"
    if re.search(r"\(\d+\)$", stem):
        return "copie_(n)"
    if re.search(r"_\d+-\d+$", stem):
        return "intervalle"
    if re.search(r"_[A-Za-z]$", stem):
        return "lettre_finale"
    if re.search(r"\d(bis|ter|[a-z])$", stem):
        return "bis_ou_lettre_collee"
    if re.search(r"_\d*[A-Z]{1,3}\d+$", stem):
        return "code_sans_separateur"
    return "autre"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(REF, dtype=str, low_memory=False,
                     usecols=["corpus_code", "s3_key", "s3_uploaded", "publication_statut"])
    k = df[df.s3_key.notna()].copy()
    k["corpus_code"] = k.corpus_code.fillna("_SANS_CORPUS")
    k["dir"] = k.s3_key.str.rsplit("/", n=1).str[0]
    k["base"] = k.s3_key.str.rsplit("/", n=1).str[-1]
    k["stem"] = k.base.str.replace(r"\.[^.]+$", "", regex=True)
    k["extraw"] = k.base.str.extract(r"\.([^.]+)$")[0]
    k["ext"] = k.extraw.str.lower()
    k["mets"] = k.stem.str.endswith(".mets") | k.stem.str.lower().str.endswith("_mets")
    k["role"] = k.ext.map(ROLE)
    k.loc[k.mets, "role"] = "METS"  # déjà identifiés par .mets.xml : nom inchangé
    k["ok"] = k.stem.str.contains(r"_\d+$") & ~k.mets
    pre = k.stem.str.replace(r"_(\d+)$", "", regex=True)
    num = k.stem.str.extract(r"_(\d+)$")[0]
    k["new_base"] = (pre + "_" + k.role + "_" + num + "." + k.extraw).where(k.ok)
    k["new_key"] = (k.dir + "/" + k.new_base).where(k.ok)

    # --- repérages transverses
    k["dl"] = k.dir.str.lower() + "/" + k.stem.str.lower()
    k["parent"] = k.dl.str.replace(r"_[^_/]+$", "", regex=True)
    # fichier de niveau document : d'autres fichiers = ce nom + _<n°>
    k["doc"] = k.ok & k.dl.isin(set(k.loc[k.ok, "parent"]))
    # sous-vue : le nom sans son dernier n° existe aussi comme IMAGE
    img_stems = set(k.loc[k.ext.isin(IMG), "dl"])
    k["sous_vue"] = k.ok & k.parent.isin(img_stems) & k.ext.isin(IMG)
    k["lettre_avant_vue"] = k.stem.str.extract(r"_([A-Za-z])_\d+$")[0]
    k["echo_z"] = k.stem.str.contains(r"(?:^|_)Z[ADOM]?(?:_|$)", case=False)
    old = set(k.s3_key.str.lower())
    k["collision_ancienne"] = k.new_key.str.lower().isin(old)
    k["ext_maj"] = k.extraw != k.ext
    k["car_speciaux"] = k.stem.str.contains(r"[ ()]|\.(?!mets$)", regex=True)
    k["cat_sans_num"] = [categorie_sans_num(s) if not ok else "" for s, ok in zip(k.stem, k.ok)]

    # stems partagés par plusieurs rôles (appariement par stem aujourd'hui)
    nrole = k.groupby("dl").role.nunique()
    k["stem_multi_role"] = k.dl.map(nrole) > 1
    # diffusion image sans master de même stem
    a_stems = set(k.loc[k.role == "ZA", "dl"])
    k["zd_sans_master"] = (k.role == "ZD") & k.ext.isin(IMG) & ~k.dl.isin(a_stems)

    k["prof"] = False
    rows, md = [], []
    for cc, d in k.groupby("corpus_code"):
        n = len(d)
        pref = common_prefix(d.stem.tolist())
        mot = d.stem.map(lambda s: motif(s, pref))
        vc = mot.value_counts()
        top = vc.head(3)
        # profondeur variable : parmi les images, motif minoritaire = un motif plus
        # fréquent + un numéro supplémentaire (sous-vue probable, à vérifier)
        vci = mot[d.ext.isin(IMG)].value_counts()
        suspects = {m for m, c in vci.items()
                    if re.search(r"_N\d+_N\d+$", m) and vci.get(re.sub(r"_N\d+$", "", m), 0) > c}
        prof = mot.isin(suspects) & d.ext.isin(IMG) & ~d.sous_vue & ~d.doc
        k.loc[d.index, "prof"] = prof
        vue_l = num[d.index][d.ok].str.len().value_counts()
        largeur = "/".join(f"N{int(i)}" for i in vue_l.index[:3]) if len(vue_l) else "-"
        roles = d.role.value_counts().to_dict()
        exts = d.ext.value_counts().to_dict()
        sans = d[~d.ok & ~d.mets]
        cat = sans.cat_sans_num.value_counts().to_dict()
        lettres = d.lettre_avant_vue.dropna().str.upper().value_counts()
        n_ok = int(d.ok.sum())
        n_doc = int(d.doc.sum())
        n_sv = int(d.sous_vue.sum())
        n_prof = int((prof & ~d.sous_vue & ~d.doc).sum())
        n_let = int(d.lettre_avant_vue.notna().sum())
        n_mets = int(d.mets.sum())
        n_sans = len(sans)
        direct = n_ok - n_doc - n_sv - n_prof + n_mets
        # verdict
        exc = n_doc + n_sv + n_prof + n_sans
        if exc == 0:
            verdict = "1 - règle directe"
        elif exc / n <= 0.05:
            verdict = "2 - règle directe + exceptions à lister"
        elif exc / n <= 0.25:
            verdict = "3 - règle directe + part notable de cas à trancher"
        else:
            verdict = "4 - règle propre au corpus nécessaire"
        ex_av = d[d.ok].base.iloc[0] if n_ok else d.base.iloc[0]
        ex_ap = d[d.ok].new_base.iloc[0] if n_ok else ""
        rows.append(dict(
            corpus_code=cc, nb_cles=n, extensions=" ".join(f"{e}:{c}" for e, c in exts.items()),
            prefixe_commun="_".join(pref), nb_motifs=len(vc),
            motif_1=top.index[0], part_motif_1=round(top.iloc[0] / n * 100, 1),
            motif_2=top.index[1] if len(top) > 1 else "", motif_3=top.index[2] if len(top) > 2 else "",
            largeur_vue=largeur,
            nb_ZA=roles.get("ZA", 0), nb_ZD=roles.get("ZD", 0), nb_ZO=roles.get("ZO", 0),
            nb_regle_directe=direct, part_regle_directe=round(direct / n * 100, 1),
            nb_doc_sans_vue=n_doc, nb_sous_vues=n_sv, nb_profondeur_variable=n_prof,
            nb_sans_numero_final=n_sans, detail_sans_numero=" ".join(f"{c}:{v}" for c, v in cat.items()),
            nb_mets=n_mets,
            nb_lettre_avant_vue=n_let, lettres_avant_vue=" ".join(f"{l}:{c}" for l, c in lettres.head(8).items()),
            nb_echo_Z=int(d.echo_z.sum()), nb_collision_ancienne_cle=int(d.collision_ancienne.sum()),
            nb_stems_multi_roles=int(d.stem_multi_role.sum()), nb_ZD_image_sans_master=int(d.zd_sans_master.sum()),
            nb_ext_majuscule=int(d.ext_maj.sum()), nb_car_speciaux=int(d.car_speciaux.sum()),
            nb_deja_sur_s3=int((d.s3_uploaded == "True").sum()), nb_publies=int((d.publication_statut == "oui").sum()),
            long_max_nom=int(d.new_base.str.len().max()) if n_ok else int(d.base.str.len().max()),
            exemple_avant=ex_av, exemple_apres=ex_ap, verdict=verdict,
        ))

        # --- fiche markdown
        md.append(f"## {cc} — {n:,} clés — {verdict}\n".replace(",", " "))
        md.append(f"- **Extensions** : {', '.join(f'{e} {c}' for e, c in exts.items())}")
        md.append(f"- **Rôles** : " + ", ".join(f"{r} {c}" for r, c in roles.items()))
        md.append(f"- **Préfixe commun** : `{'_'.join(pref) or '(aucun)'}` ; **numéro de vue** : {largeur} ; **{len(vc)} motif(s)**")
        md.append("- **Règle de nommage observée** :")
        for m, c in top.items():
            ex = d.base[mot == m].iloc[0]
            md.append(f"  - `{m}` — {c:,} ({c / n:.0%}) — ex. `{ex}`".replace(",", " "))
        if len(vc) > 3:
            md.append(f"  - … {len(vc) - 3} autre(s) motif(s), {int(vc.iloc[3:].sum()):,} clés".replace(",", " "))
        md.append(f"- **Impact de l'indicateur** : {direct:,} clés ({direct / n:.1%}) suivent la règle directe".replace(",", " ")
                  + (f" — ex. `{ex_av}` → `{ex_ap}`" if n_ok else ""))
        pts = []
        if n_doc:
            e = d[d.doc].base.iloc[0]
            pts.append(f"{n_doc} fichier(s) de document sans numéro de vue (ex. `{e}`)")
        if n_sv:
            e = d[d.sous_vue].base.iloc[0]
            pts.append(f"{n_sv} sous-vue(s) (ex. `{e}`)")
        if n_prof:
            e = d[prof & ~d.sous_vue & ~d.doc].base.iloc[0]
            pts.append(f"{n_prof} nom(s) avec un numéro de plus que le motif courant, sous-vue probable à vérifier (ex. `{e}`)")
        if n_sans:
            e = sans.base.iloc[0]
            pts.append(f"{n_sans} nom(s) sans numéro final ({', '.join(f'{c} {v}' for c, v in cat.items())} ; ex. `{e}`)")
        if n_mets:
            pts.append(f"{n_mets} fichier(s) METS, nom inchangé car déjà suffixé `.mets.xml` (ex. `{d[d.mets].base.iloc[0]}`)")
        if n_let:
            e = d[d.lettre_avant_vue.notna()]
            pts.append(f"{n_let} nom(s) avec une lettre avant la vue ({', '.join(f'{l} {c}' for l, c in lettres.head(6).items())} ; ex. `{e.base.iloc[0]}` → `{e.new_base.iloc[0]}`)")
        if d.echo_z.sum():
            pts.append(f"{int(d.echo_z.sum())} nom(s) avec un segment Z existant (ex. `{d[d.echo_z].base.iloc[0]}`)")
        if d.zd_sans_master.sum():
            pts.append(f"{int(d.zd_sans_master.sum())} image(s) de diffusion sans master de même nom")
        if d.stem_multi_role.sum():
            pts.append(f"{int(d.stem_multi_role.sum())} clé(s) appariées par nom entre rôles (l'appariement devra ignorer l'indicateur)")
        if d.ext_maj.sum():
            pts.append(f"{int(d.ext_maj.sum())} extension(s) en majuscules")
        if d.car_speciaux.sum():
            pts.append(f"{int(d.car_speciaux.sum())} nom(s) avec espace, parenthèse ou point")
        pts.append(f"{int((d.s3_uploaded == 'True').sum())} objet(s) déjà sur S3 à renommer ; {int((d.publication_statut == 'oui').sum())} publié(s)")
        md += [f"  - {p}" for p in pts]
        md.append("")

    res = pd.DataFrame(rows).sort_values(["verdict", "nb_cles"], ascending=[True, False])
    res.to_csv(OUT / "regles_nommage_par_corpus.csv", index=False)
    entete = [
        "# Règles de nommage par corpus et impact de l'indicateur Z",
        "",
        f"Source : `{REF}` — {len(k):,} s3_key, {k.corpus_code.nunique()} corpus. Analyse en lecture seule.".replace(",", " "),
        "",
        "Indicateur inséré juste avant le numéro de vue : `_ZA_` conservation, `_ZD_` diffusion, `_ZO_` OCR. Les fichiers METS gardent leur nom (suffixe `.mets.xml`).",
        "",
        "Notation des motifs : `N3` = nombre à 3 chiffres, `<SERIE>` = sigle en lettres, `<L>` = lettre seule, `#` = chiffres collés à des lettres.",
        "",
    ]
    (OUT / "regles_nommage_par_corpus.md").write_text("\n".join(entete + md), encoding="utf-8")

    # exceptions ligne à ligne
    exc = k[~k.ok | k.doc | k.sous_vue | k.prof | k.lettre_avant_vue.notna() | k.echo_z].copy()
    exc["type"] = ""
    for col, lab in [("doc", "doc_sans_vue"), ("sous_vue", "sous_vue"), ("prof", "sous_vue_probable"), ("echo_z", "echo_Z"), ("mets", "mets")]:
        exc.loc[exc[col], "type"] += lab + ";"
    exc.loc[exc.lettre_avant_vue.notna(), "type"] += "lettre_avant_vue;"
    exc.loc[~exc.ok & ~exc.mets, "type"] += "sans_numero:" + exc.cat_sans_num + ";"
    exc[["corpus_code", "s3_key", "role", "type", "new_key"]].to_csv(OUT / "cas_particuliers.csv.gz", index=False)

    print(res.groupby("verdict").agg(corpus=("corpus_code", "size"), cles=("nb_cles", "sum")).to_string())
    print("total clés", len(k), "| règle directe", int(res.nb_regle_directe.sum()),
          "| doc", int(res.nb_doc_sans_vue.sum()), "| sous-vues", int(res.nb_sous_vues.sum()),
          "| profondeur variable", int(res.nb_profondeur_variable.sum()),
          "| sans n°", int(res.nb_sans_numero_final.sum()), "| mets", int(res.nb_mets.sum()),
          "| collisions", int(res.nb_collision_ancienne_cle.sum()), "| cas particuliers", len(exc))


if __name__ == "__main__":
    main()
