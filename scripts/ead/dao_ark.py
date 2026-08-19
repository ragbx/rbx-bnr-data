"""
Fusion des <daogrp> et insertion de liens ARK (<dao>/<daoloc>) dans un EAD.

Logique partagée entre ead_bnr2mnesys.py (pipeline complet BnR -> Mnesys) et
app/ead_prepublication (préparation des EAD Mnesys en vue de leur publication).
"""
from lxml import etree


def merge_daogrp(element):
    """
    Fusionne dans le premier <daogrp> le contenu des <daogrp> suivants quand un
    même <archdesc>/<c> en contient plusieurs en enfants directs.

    Les doublons ne sont pas repris : <daodesc> de même texte, <dao>/<daoloc>
    de même couple (href, role).
    """
    for el in element.iter("archdesc", "c"):
        daogrps = [child for child in el if child.tag == "daogrp"]
        if len(daogrps) < 2:
            continue

        cible = daogrps[0]
        descs = {"".join(d.itertext()).strip() for d in cible.findall("daodesc")}
        liens = {
            (d.get("href"), d.get("role")) for d in cible if d.tag != "daodesc"
        }

        for daogrp in daogrps[1:]:
            for enfant in list(daogrp):
                if enfant.tag == "daodesc":
                    texte = "".join(enfant.itertext()).strip()
                    if texte in descs:
                        continue
                    descs.add(texte)
                else:
                    lien = (enfant.get("href"), enfant.get("role"))
                    if lien in liens:
                        continue
                    liens.add(lien)
                cible.append(enfant)
            el.remove(daogrp)


def add_ark_links(element, link_builder, tags=("archdesc", "c")):
    """
    Pour chaque élément de `tags` (par défaut <archdesc> et <c>), ajoute les liens
    renvoyés par `link_builder(el)` (liste de tuples (href, role)) sous forme de
    <dao>/<daoloc>, selon trois cas :

    - <daogrp> déjà présent : ajout d'un <daoloc> par lien dans le groupe
      (sans doublon de role).
    - <dao> présent (sans <daogrp>) : transformation en <daogrp> contenant un
      <daoloc> reprenant les attributs de l'ancienne <dao> et un <daoloc> par lien.
    - Ni <dao> ni <daogrp> : création d'un <dao> (lien unique) ou d'un <daogrp>
      (plusieurs liens).

    Dans les cas 2 et 3, la balise est insérée avant le premier enfant <c> ou
    <dsc> s'il existe.

    Retourne le nombre de liens effectivement ajoutés (les rôles déjà présents
    dans un <daogrp> existant ne sont pas comptés).
    """
    count = 0
    for el in element.iter(*tags):
        liens = link_builder(el)
        if not liens:
            continue

        insert_before = next(
            (child for child in el if child.tag in ("c", "dsc")), None
        )

        # Cas 1 : <daogrp> existe déjà
        daogrp = el.find("daogrp")
        if daogrp is not None:
            roles_presents = {d.get("role") for d in daogrp.findall("daoloc")}
            for url, role in liens:
                if role not in roles_presents:
                    new_daoloc = etree.SubElement(daogrp, "daoloc")
                    new_daoloc.set("href", url)
                    new_daoloc.set("role", role)
                    count += 1
            continue

        # Cas 2 : <dao> existe mais pas <daogrp> → conversion en <daogrp>
        if (old_dao := el.find("dao")) is not None:
            nouveau = etree.Element("daogrp")

            daoloc_from_dao = etree.SubElement(nouveau, "daoloc")
            for attr, value in old_dao.attrib.items():
                daoloc_from_dao.set(attr, value)

            for url, role in liens:
                new_daoloc = etree.SubElement(nouveau, "daoloc")
                new_daoloc.set("href", url)
                new_daoloc.set("role", role)
                count += 1

            el.remove(old_dao)

        # Cas 3 : ni <dao> ni <daogrp>
        elif len(liens) == 1:
            url, role = liens[0]
            nouveau = etree.Element("dao")
            nouveau.set("href", url)
            nouveau.set("role", role)
            count += 1
        else:
            nouveau = etree.Element("daogrp")
            for url, role in liens:
                new_daoloc = etree.SubElement(nouveau, "daoloc")
                new_daoloc.set("href", url)
                new_daoloc.set("role", role)
                count += 1

        # Insérer avant le premier enfant <c>/<dsc> ou à la fin
        if insert_before is not None:
            el.insert(list(el).index(insert_before), nouveau)
        else:
            el.append(nouveau)

    return count
