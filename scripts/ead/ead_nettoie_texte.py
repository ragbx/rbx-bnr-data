"""
Nettoie le texte des IR : HTML stocké comme du texte, apostrophes échappées,
défauts d'encodage, retours chariot et espaces superflus.

Correctif appliqué directement aux IR de results/ead/ead_cor/bnr2mnesys/
(base de travail). Les <odd> (résumé des liens, donnée maître) et les
<dao>/<daoloc> ne sont jamais modifiés ; les <daodesc> le sont.

Traitements, dans l'ordre, sur le texte de chaque élément :

1. Encodage : séquences UTF-8 lues comme du Windows-1252 ("FraternitÃ©" →
   "Fraternité"), puis caractères de contrôle U+0080-U+009F, qui sont des
   octets Windows-1252 mal décodés (U+0092 → apostrophe, U+009C → œ…).
   L'apostrophe est rendue droite, comme dans le reste des titres et index.
2. Apostrophes échappées : "\\'" → "'".
3. HTML : les balises présentes en clair dans le texte sont converties en
   EAD. Chaque paragraphe HTML donne un <p> (ou, dans <physdesc>,
   <descrules> et <creation>, une ligne séparée de la précédente par <lb/>) ;
   em/strong/sup → <emph render="italic|bold|super"> ; a → <extref> ;
   br → <lb/> ; ul/li → <list>/<item> ; un tableau donne une ligne par
   rangée. Les autres balises (span, div…) et tous les attributs de mise en
   forme sont abandonnés, ainsi que les paragraphes vides.
4. Espaces : retours chariot et sauts de ligne remplacés par une espace
   (dans un <p> sans HTML, une ligne vide sépare deux paragraphes), espaces
   multiples réduites à une seule, espaces de début et de fin supprimées.
   Les <persname>/<corpname> contenant une suite de trois espaces ou plus
   (plusieurs noms saisis dans un même élément) ne sont que rognés.

Les valeurs d'attributs reçoivent les traitements 1 et 2 et sont rognées.
Un élément vide une fois nettoyé est supprimé.

Sans option, le script ne modifie rien et affiche le décompte par IR. Avec
--appliquer, les fichiers sont réécrits. Rejouable : un second passage ne
change rien.

Utilisation, depuis la racine du dépôt :

    python scripts/ead/ead_nettoie_texte.py              # à blanc
    python scripts/ead/ead_nettoie_texte.py --appliquer
"""
import argparse
import re
from collections import Counter
from glob import glob
from os.path import basename, join

from lxml import etree, html

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")

# Éléments jamais modifiés (cf. docstring) ; <odd> avec sa descendance
INTOUCHABLES = ("odd", "dao", "daoloc")
# Balises en ligne produites par le traitement 3 : leur texte fait partie
# d'un contenu mixte déjà nettoyé
EN_LIGNE = ("emph", "lb")
# Éléments dont le contenu HTML donne des lignes (<lb/>) et non des <p>
EN_LIGNES = ("physdesc", "descrules", "creation")
# <extref> n'y est pas autorisé par la DTD EAD 2002
SANS_EXTREF = ("physdesc",)

HTML_BLOCS = {
    "p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "table", "thead", "tbody",
    "tr", "ul", "ol", "li", "blockquote",
}
HTML_EMPH = {
    "em": "italic", "i": "italic", "strong": "bold", "b": "bold",
    "sup": "super", "sub": "sub", "u": "underline",
}
HTML_CONNUES = HTML_BLOCS | set(HTML_EMPH) | {
    "span", "a", "br", "img", "td", "th", "col", "colgroup", "font",
}
MOTIF_HTML = re.compile(
    r"</?(?:%s)(?:\s[^<>]*)?/?>" % "|".join(sorted(HTML_CONNUES)), re.IGNORECASE
)

ANTISLASH_APOSTROPHE = chr(92) + "'"
# Octets Windows-1252 décodés à tort comme des caractères de contrôle
C1 = {
    code: bytes([code]).decode("cp1252")
    for code in range(0x80, 0xA0)
    if code not in (0x81, 0x8D, 0x8F, 0x90, 0x9D)
}
C1[0x91] = C1[0x92] = "'"
# Séquences UTF-8 lues comme du Windows-1252. Seules sont reconnues celles
# qui commencent par Ã ou Â (lettres accentuées, espace insécable), â€
# (ponctuation typographique) ou Å (œ, Œ) : un É suivi d'un guillemet
# fermant, par exemple, est du texte légitime.
MOTIF_MOJIBAKE = re.compile(r"[ÃÂ].|â[€\u0080].|Å[“’\u0093\u0092]", re.DOTALL)

ESPACES = " \t\r\n\f\v"
MOTIF_ESPACES = re.compile(r"[ \t\r\n\f\v ]{2,}|[\t\r\n\f\v]")


def _octet(caractere):
    try:
        return caractere.encode("cp1252")
    except UnicodeEncodeError:
        return bytes([ord(caractere)]) if ord(caractere) < 256 else b""


def corrige_encodage(texte, stats):
    """Traitements 1 et 2 d'une chaîne (texte ou valeur d'attribut)."""

    def demojibake(m):
        try:
            bon = b"".join(_octet(c) for c in m.group(0)).decode("utf-8")
        except UnicodeDecodeError:
            return m.group(0)
        stats["séquences mal décodées (mojibake)"] += 1
        return bon

    texte = MOTIF_MOJIBAKE.sub(demojibake, texte)
    for code, bon in C1.items():
        if chr(code) in texte:
            stats["caractères de contrôle Windows-1252"] += texte.count(chr(code))
            texte = texte.replace(chr(code), bon)
    if ANTISLASH_APOSTROPHE in texte:
        stats["apostrophes échappées"] += texte.count(ANTISLASH_APOSTROPHE)
        texte = texte.replace(ANTISLASH_APOSTROPHE, "'")
    return texte


def reduit_espaces(texte):
    """Réduit sauts de ligne et espaces multiples à une seule espace (insécable
    si la suite en contenait une)."""
    return MOTIF_ESPACES.sub(lambda m: " " if " " in m.group(0) else " ", texte)


# --- HTML → contenu EAD ---------------------------------------------------
# Un bloc est une liste de morceaux : str, ("lb",), ("emph", rendu, morceaux),
# ("extref", href, morceaux). Une liste à puces est ("list", [morceaux, …]).


def _balise(noeud):
    """Morceaux correspondant à un nœud HTML en ligne (sans son tail)."""
    if not isinstance(noeud.tag, str):
        return []
    tag = noeud.tag.lower()
    if tag == "br":
        return [("lb",)]
    if tag in HTML_EMPH:
        return [("emph", HTML_EMPH[tag], _en_ligne(noeud))]
    if tag == "a" and (noeud.get("href") or "").strip():
        return [("extref", noeud.get("href").strip(), _en_ligne(noeud))]
    if tag in HTML_BLOCS or tag in ("td", "th"):
        return [" "] + _en_ligne(noeud) + [" "]
    return [] if tag == "img" else _en_ligne(noeud)


def _en_ligne(noeud):
    """Morceaux correspondant au contenu d'un nœud HTML, sans structure de bloc."""
    morceaux = [noeud.text or ""]
    for enfant in noeud:
        morceaux += _balise(enfant) + [enfant.tail or ""]
    return morceaux


def _en_blocs(noeud, blocs):
    """Découpe le contenu d'un nœud HTML en blocs, ajoutés à `blocs`."""
    courant = [noeud.text or ""]
    for enfant in noeud:
        tag = enfant.tag.lower() if isinstance(enfant.tag, str) else None
        if tag in ("ul", "ol"):
            blocs.append(courant)
            blocs.append(("list", [_en_ligne(li) for li in enfant.iter("li")]))
            courant = []
        elif tag == "tr":
            blocs.append(courant)
            cellules = [_en_ligne(c) for c in enfant if c.tag in ("td", "th")]
            rangee = []
            for i, cellule in enumerate(cellules):
                rangee += ([" ; "] if i else []) + cellule
            blocs.append(rangee)
            courant = []
        elif tag in HTML_BLOCS:
            blocs.append(courant)
            _en_blocs(enfant, blocs)
            courant = []
        else:
            courant += _balise(enfant)
        courant.append(enfant.tail or "")
    blocs.append(courant)


def _normalise(morceaux, extref=True):
    """Fusionne les textes voisins, réduit les espaces, retire les <emph> vides
    et, si `extref` est faux, ne garde des liens que leur texte."""
    resultat = []
    for m in morceaux:
        if not isinstance(m, str):
            if m[0] == "emph":
                contenu = _normalise(m[2], extref)
                if not _texte(contenu).strip(ESPACES + " "):
                    m = _texte(contenu)
                else:
                    m = ("emph", m[1], contenu)
            elif m[0] == "extref":
                contenu = _normalise(m[2], extref)
                if not extref:
                    resultat += contenu
                    continue
                m = ("extref", m[1], contenu)
        if isinstance(m, str) and resultat and isinstance(resultat[-1], str):
            resultat[-1] += m
        else:
            resultat.append(m)
    return [reduit_espaces(m) if isinstance(m, str) else m for m in resultat if m != ""]


def _texte(morceaux):
    return "".join(
        m if isinstance(m, str) else " " if m[0] == "lb" else _texte(m[2]) for m in morceaux
    )


def _rogne(morceaux):
    """Retire espaces et <lb/> en début et en fin de bloc."""
    bords = ESPACES + " "
    while morceaux:
        if morceaux[0] == ("lb",) or morceaux[0] == "":
            morceaux = morceaux[1:]
        elif isinstance(morceaux[0], str) and morceaux[0] != morceaux[0].lstrip(bords):
            morceaux = [morceaux[0].lstrip(bords)] + morceaux[1:]
        elif morceaux[-1] == ("lb",) or morceaux[-1] == "":
            morceaux = morceaux[:-1]
        elif isinstance(morceaux[-1], str) and morceaux[-1] != morceaux[-1].rstrip(bords):
            morceaux = morceaux[:-1] + [morceaux[-1].rstrip(bords)]
        else:
            break
    return morceaux


def html_en_blocs(texte, extref=True):
    """Blocs EAD (cf. plus haut) correspondant à un texte contenant du HTML."""
    racine = html.fragment_fromstring(texte, create_parent="div")
    bruts = []
    _en_blocs(racine, bruts)
    blocs = []
    for bloc in bruts:
        if isinstance(bloc, tuple):
            items = [_rogne(_normalise(item, extref)) for item in bloc[1]]
            items = [item for item in items if item]
            if items:
                blocs.append(("list", items))
        else:
            bloc = _rogne(_normalise(bloc, extref))
            if bloc:
                blocs.append(bloc)
    return blocs


def _remplit(element, morceaux):
    """Écrit les morceaux d'un bloc comme contenu de `element`."""
    element.text = None
    dernier = None
    for m in morceaux:
        if isinstance(m, str):
            if dernier is None:
                element.text = (element.text or "") + m
            else:
                dernier.tail = (dernier.tail or "") + m
            continue
        if m[0] == "lb":
            dernier = etree.SubElement(element, "lb")
        elif m[0] == "emph":
            dernier = etree.SubElement(element, "emph", render=m[1])
            _remplit(dernier, m[2])
        else:
            dernier = etree.SubElement(element, "extref", href=m[1])
            _remplit(dernier, m[2])


def _retire(element):
    """Retire un élément en conservant l'indentation, puis son parent s'il ne
    contient plus rien."""
    parent = element.getparent()
    precedent = element.getprevious()
    if precedent is not None:
        precedent.tail = element.tail
    else:
        parent.text = element.tail
    parent.remove(element)
    if len(parent) == 0 and not (parent.text or "").strip() and not parent.attrib:
        _retire(parent)


def _ecrit_blocs(element, blocs):
    """Remplace le contenu de `element` par les blocs."""
    if not blocs:
        _retire(element)
        return
    if element.tag != "p":
        # Lignes séparées par <lb/> ; une liste donne une ligne par item
        lignes = []
        for bloc in blocs:
            lignes += bloc[1] if isinstance(bloc, tuple) else [bloc]
        morceaux = []
        for i, ligne in enumerate(lignes):
            morceaux += ([("lb",)] if i else []) + ligne
        _remplit(element, morceaux)
        return
    # Un <p> ou une <list> par bloc, à la suite de l'élément d'origine
    parent = element.getparent()
    precedent = element.getprevious()
    indentation = precedent.tail if precedent is not None else parent.text
    fin = element.tail
    position = parent.index(element)
    parent.remove(element)
    for i, bloc in enumerate(blocs):
        if isinstance(bloc, tuple):
            nouveau = etree.Element("list")
            for item in bloc[1]:
                _remplit(etree.SubElement(nouveau, "item"), item)
        else:
            nouveau = etree.Element("p")
            _remplit(nouveau, bloc)
        nouveau.tail = indentation if i < len(blocs) - 1 else fin
        parent.insert(position + i, nouveau)


def nettoie(root):
    """Applique les traitements à l'arbre d'un IR.

    Retourne le décompte des corrections et le nombre de noms multiples
    laissés tels quels.
    """
    stats = Counter()
    noms_multiples = 0
    for element in list(root.iter()):
        if not isinstance(element.tag, str):
            continue
        if element.tag in INTOUCHABLES or element.getparent() is not None and (
            element.getparent().tag == "odd"
        ):
            continue
        for attribut, valeur in element.attrib.items():
            nouvelle = corrige_encodage(valeur, stats).strip(ESPACES)
            if nouvelle != valeur:
                stats["attributs rognés ou corrigés"] += 1
                element.set(attribut, nouvelle)
        parent = element.getparent()
        if (
            len(element)
            or not (element.text or "").strip(ESPACES)
            or element.tag in EN_LIGNE
            or (element.tail or "").strip(ESPACES)
            or parent is not None and (parent.text or "").strip(ESPACES)
        ):
            continue  # élément sans texte, ou contenu mixte déjà balisé

        texte = corrige_encodage(element.text, stats)
        if MOTIF_HTML.search(texte):
            stats["éléments contenant du HTML"] += 1
            blocs = html_en_blocs(texte, extref=element.tag not in SANS_EXTREF)
            if not blocs:
                stats["éléments vides supprimés"] += 1
            elif element.tag == "p" and len(blocs) > 1:
                stats["paragraphes ajoutés"] += len(blocs) - 1
            _ecrit_blocs(element, blocs)
            continue

        if "\r" in texte:
            stats["retours chariot"] += texte.count("\r")
        if element.tag == "p" and re.search(r"\n\s*\n", texte.strip(ESPACES)):
            paragraphes = [
                _rogne([reduit_espaces(p)])
                for p in re.split(r"\n\s*\n", texte.replace("\r", ""))
            ]
            paragraphes = [p for p in paragraphes if p]
            stats["paragraphes ajoutés"] += len(paragraphes) - 1
            _ecrit_blocs(element, paragraphes)
            continue
        if element.tag in ("persname", "corpname") and re.search(r" {3,}\S", texte.strip()):
            noms_multiples += 1
            nouveau = texte.strip(ESPACES)
        else:
            nouveau = reduit_espaces(texte).strip(ESPACES)
        if nouveau != texte:
            stats["textes aux espaces corrigées"] += 1
        element.text = nouveau
    return stats, noms_multiples


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    parser.add_argument(
        "--appliquer", action="store_true", help="réécrit les IR (sinon : à blanc)"
    )
    args = parser.parse_args()

    total = Counter()
    total_noms = 0
    for chemin in sorted(glob(join(EAD_FOLDER, "*.xml"))):
        tree = etree.parse(chemin)
        stats, noms_multiples = nettoie(tree.getroot())
        total += stats
        total_noms += noms_multiples
        if stats:
            print(f"{basename(chemin)} : {sum(stats.values())} corrections")
            if args.appliquer:
                tree.write(
                    chemin,
                    encoding="UTF-8",
                    xml_declaration=True,
                    pretty_print=True,
                    with_comments=True,
                )
    print("Total" + ("" if args.appliquer else " (à blanc)") + " :")
    for libelle, n in total.most_common():
        print(f"  {n:>7}  {libelle}")
    if total_noms:
        print(f"Non corrigés : {total_noms} <persname>/<corpname> à noms multiples")
