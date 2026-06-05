from lxml import etree


def _add_dao_from_osiros_id(element):
    """
    Gère les balises <dao> et <daogrp> pour chaque élément <c> avec un attribut osiros_id.
    Trois cas principaux :
    1. Si <daogrp> existe : ajoute un <daoloc> dedans.
    2. Si <dao> existe (sans <daogrp>) : transforme en <daogrp><daoloc> et ajoute le nouveau <daoloc>.
    3. Sinon : crée un <dao> simple.
    Les éléments sont insérés avant le premier enfant <c> s'il existe.
    """
    for c in element.findall(".//c"):
        if "osiros_id" in c.attrib:
            osiros_id = c.attrib["osiros_id"]
            print(osiros_id)

            # Trouver la position d'insertion : avant le premier enfant <c> s'il existe
            first_child_c = None
            for child in c:
                if child.tag == "c":
                    first_child_c = child
                    break

            # Cas 1 : <daogrp> existe déjà
            daogrp = c.find("daogrp")
            if daogrp is not None:
                # Vérifier si un <daoloc> avec role="old_ark" existe déjà
                old_ark_daoloc_exists = any(
                    daoloc.get("role") == "old_ark"
                    for daoloc in daogrp.findall("daoloc")
                )
                if not old_ark_daoloc_exists:
                    new_daoloc = etree.SubElement(daogrp, "daoloc")
                    new_daoloc.set("href", osiros_id)
                    new_daoloc.set("role", "old_ark")

            # Cas 2 : <dao> existe mais pas <daogrp>
            elif c.find("dao") is not None:
                # Créer <daogrp> et <daoloc>
                daogrp = etree.Element("daogrp")
                daoloc = etree.SubElement(daogrp, "daoloc")

                # Déplacer l'ancienne <dao> dans <daoloc>
                old_dao = c.find("dao")
                daoloc.append(old_dao)

                # Ajouter la nouvelle <daoloc> pour osiros_id
                new_daoloc = etree.SubElement(daogrp, "daoloc")
                new_daoloc.set("href", osiros_id)
                new_daoloc.set("role", "old_ark")

                # Insérer <daogrp> avant le premier enfant <c> ou à la fin
                if first_child_c is not None:
                    index = list(c).index(first_child_c)
                    c.insert(index, daogrp)
                else:
                    c.append(daogrp)

                # Supprimer l'ancienne <dao> (déjà déplacée)
                c.remove(old_dao)

            # Cas 3 : Ni <dao> ni <daogrp> n'existe
            else:
                new_dao = etree.Element("dao")
                new_dao.set("href", osiros_id)
                new_dao.set("role", "old_ark")

                # Insérer <dao> avant le premier enfant <c> ou à la fin
                if first_child_c is not None:
                    index = list(c).index(first_child_c)
                    c.insert(index, new_dao)
                else:
                    c.append(new_dao)
    return element


cas1 = """
<ead>
<c level="item" osiros_id="BNR156543">
  <did>
    <unitid>CP_A01_L1_S1_053</unitid>
    <unittitle type="">La Gare</unittitle>
    <unitdate normal="1950">1950</unitdate>
    <physdesc>
      <p>11 cartes postales noir et blanc 7 cartes postales couleur</p>
    </physdesc>
  </did>
  <scopecontent>
    <p>
      <p>La gare et le tramway.</p>
    </p>
  </scopecontent>
  <controlaccess>
    <subject source="chrono">1946-1959</subject>
  </controlaccess>
  <daogrp>
    <daodesc>
      <p>La Gare</p>
    </daodesc>
    <daoloc href="RBX_MED_CP_A01_L1_S1_053.jpg" audience="internal" role="image:first"/>
    <daoloc href="RBX_MED_CP_A01_L1_S1_070.jpg" audience="internal" role="image:last"/>
  </daogrp>
</c>
</ead>
"""

root = etree.fromstring(cas1)
newroot = _add_dao_from_osiros_id(root)
# print(etree.tostring(newroot, pretty_print=True, encoding="unicode"))
# cas 1 ok

cas2 = """
<ead>
<c level="item" osiros_id="BNR156543">
  <did>
    <unitid>CP_A01_L1_S1_053</unitid>
    <unittitle type="">La Gare</unittitle>
    <unitdate normal="1950">1950</unitdate>
    <physdesc>
      <p>11 cartes postales noir et blanc 7 cartes postales couleur</p>
    </physdesc>
  </did>
  <scopecontent>
    <p>
      <p>La gare et le tramway.</p>
    </p>
  </scopecontent>
  <controlaccess>
    <subject source="chrono">1946-1959</subject>
  </controlaccess>
  <daoloc href="RBX_MED_CP_A01_L1_S1_053.jpg" audience="internal"/>
</c>
</ead>
"""

root = etree.fromstring(cas2)
newroot = _add_dao_from_osiros_id(root)
print(etree.tostring(newroot, pretty_print=True, encoding="unicode"))
