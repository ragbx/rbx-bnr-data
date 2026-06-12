"""
Transformation de fichiers EAD BnR pour import dans Mnesys.

Ce script applique une série de transformations sur des fichiers XML EAD (Encoded Archival
Description) produits par la Bibliothèque numérique de Roubaix, en vue de leur import dans
le logiciel Mnesys.

Données d'entrée
----------------
- Un CSV de noms typés (persname / corpname) pour reclasser les balises <name>.
- Un CSV de correspondances cote → osiros_id issu de l'OAI, pour construire les anciens ARK BnR.
- Un fichier Excel listant les instruments de recherche à traiter (filtrés sur statut = "TRANSFERER").
- Un CSV de référence des fichiers de conservation (lignes avec s3_key non null),
  pour ajouter les chemins S3 aux balises <dao>/<daoloc>.
- Les fichiers EAD source dans data/ead/bnr/.

Résultats
---------
Les fichiers EAD transformés sont écrits dans results/ead/ead_cor/bnr2mnesys/.
La concordance ir / unitid / id (cf. étape 4) est tenue dans
results/ead/ead_cor/concordance_id.csv : créée à la première exécution, elle
est rechargée aux exécutions suivantes pour réattribuer les mêmes id.

Pipeline de transformations
---------------------------
Les transformations sont appliquées dans l'ordre suivant sur chaque fichier EAD :

1. Mise à jour des métadonnées de l'instrument de recherche
   - <eadid> : remplacé par la nouvelle valeur issue du fichier Excel.
   - <archdesc/did/unitid> : remplacé par le nouvel identifiant de l'IR.
   - <archdesc/did/repository> : le code présent dans bn-r est remplacé par son libellé.

2. Nettoyage du contenu textuel
   - Décodage des entités HTML (&amp;, &lt;, etc.) dans tout l'arbre XML.
   - Suppression des espaces en début/fin de texte dans les enfants de <controlaccess>.

3. Réorganisation des <origination>
   - <origination> : les éléments <name> et <persname> qu'il contient sont déplacés dans
     <controlaccess> (créé si absent). <origination> est supprimé s'il devient vide.
     Lors du déplacement, les <name> sont reclassés en <persname> ou <corpname> selon la
     liste CSV si une correspondance est trouvée.

4. Ajout des attributs id
   - Chaque <archdesc> et <c> sans attribut id en reçoit un, généré au format
     Mnesys avec le préfixe "m0" (cf. scripts/ead/mnesys_id.py).
   - Les id générés sont consignés dans la concordance ir / unitid / id
     (results/ead/ead_cor/concordance_id.csv). Aux exécutions suivantes, si une
     entrée existe pour (ir, unitid), l'id qu'elle contient est repris : les id
     restent stables d'une itération à l'autre. Les éléments sans <did>/<unitid>
     reçoivent un id nouveau à chaque exécution (pas de clé de concordance).

5. Fusion des <daogrp> multiples
   - Quand un même <archdesc>/<c> contient plusieurs <daogrp> directs, leurs contenus
     sont fusionnés dans le premier. Les <daodesc> de même texte et les <daoloc> de
     même couple (href, role) ne sont pas dupliqués.
   - La fusion est réappliquée après l'étape 8 : la conversion d'un <dao> isolé en
     <daogrp> peut recréer un doublon dans un <c> qui possédait déjà un <daogrp>.

6. Ajout des liens ARK BnR
   - Pour chaque <archdesc> et <c> portant un attribut id, l'ARK actuel construit à
     partir de cet id (https://www.bn-r.fr/ark:/20179/BNR<id>) est ajouté avec
     role="publication:current".
   - Pour chaque <c> dont le <unitid> figure dans la table de correspondance OAI,
     l'ancien ARK BnR (https://www.bn-r.fr/ark:/20179/<osiros_id>) est ajouté avec
     role="publication:previous".
   - Les balises sont insérées selon trois cas :
       * <daogrp> déjà présent → ajout d'un <daoloc> par lien dans le groupe (sans
         doublon de role).
       * <dao> présent (sans <daogrp>) → transformation en <daogrp> contenant un <daoloc>
         reprenant les attributs de l'ancienne <dao> et un <daoloc> par lien.
       * Ni <dao> ni <daogrp> → création d'un <dao> (lien unique) ou d'un <daogrp>
         (plusieurs liens).
     Dans les cas 2 et 3, la balise est insérée avant le premier enfant <c> ou <dsc>
     s'il existe.

7. Mise à jour des rôles des <dao>
   - Pour tous les éléments <dao> et <daoloc> dont l'attribut role commence par "image"
     ou vaut "mp3", "mp4" ou "pdf", le préfixe "access:" est ajouté devant la valeur.
     "mp3" est au passage renommé en "audio" et "mp4" en "video" (→ access:audio,
     access:video, access:pdf).

8. Ajout des chemins de conservation
   - Pour chaque <dao>/<daoloc> dont le href correspond (par basename sans extension) à un
     fichier du CSV de référence (s3_key non null), un nouveau <daoloc role="preservation:...">
     est ajouté avec le chemin S3 (s3_key). Le role est obtenu en remplaçant "access:" par
     "preservation:" dans le role de l'élément source.
   - L'appariement est contraint par famille de média (cf. FAMILLES_MEDIA) : un jpg de
     diffusion ne peut être apparié qu'à un fichier image en conservation (tif de
     préférence), un mp3 qu'à un fichier audio (wav de préférence), etc. Les fichiers
     de conservation hors familles connues (xml, txt…) sont ignorés.
   - Les fichiers OCR (file_type "ocr xml" du CSV) sont appariés de la même façon, par
     nom de base, aux liens des familles image et pdf : un <daoloc role="access:ocr">
     pointant vers le s3_key de l'OCR est ajouté dans le même <daogrp>.
   - Cas particulier de l'audio : les fichiers de conservation portent un suffixe de
     variante de numérisation (_96kHz24B, _44kHz24B, _TI), retiré pour l'appariement ;
     toutes les variantes trouvées sont ajoutées en preservation:audio (96 puis 44
     puis TI). Pour le fonds sonore FLRS, le nom EAD "RBX_MED_FLRS_*" correspond en
     conservation à "RBX_MED_*" (et "+" y devient "_").
   - Le href du lien de diffusion est complété avec le dossier du chemin de conservation :
     "RBX_MED_CP_001.jpg" + s3_key "MED/MED_CP/RBX_MED_CP_001.tiff"
     → "MED/MED_CP/RBX_MED_CP_001.jpg". Les URL absolues de l'ancien site
     (http://www.bn-r.fr/musique/…, http://www.bn-r.fr/video/…), qui ne font plus
     sens, sont réécrites de la même façon (seul le nom de fichier est conservé).
     Exception pour l'audio : quand un mp3 de la variante TI existe en conservation,
     la diffusion pointe directement vers lui (chemin S3 complet).
   - Si le <dao> est isolé (hors <daogrp>), il est converti en <daogrp> + <daoloc> au préalable.

9. Tri des enfants des <daogrp>
   - Dans chaque <daogrp>, <daodesc> est placé en premier, puis les <daoloc> sont
     réordonnés : preservation: d'abord, access: ensuite, publication: en dernier.

10. Reclassement des balises <name>
   - Dans <controlaccess>, les balises <name> sont remplacées par <persname> ou <corpname>
     selon la liste CSV. Les <name> sans correspondance sont laissés tels quels.

11. Suppression des <repository> hors contexte
   - Toutes les balises <repository> situées en dehors de <archdesc/did> sont supprimées.

12. Nettoyage final
   - Suppression des attributs dont la valeur est une chaîne vide.
   - Suppression récursive des éléments XML vides (sans texte, sans attribut, sans enfant
     non vide).

Utilisation
-----------
    python ead_bnr2mnesys.py
"""
import re
from html import unescape
from os.path import basename, dirname, exists, join, splitext

import pandas as pd
from lxml import etree

from mnesys_id import nouvel_id

# Familles de médias pour l'appariement access/preservation : un lien de diffusion
# ne peut être apparié qu'à un fichier de conservation de la même famille (un jpg
# ne peut pas pointer vers un wav). L'ordre des extensions donne la priorité du
# format retenu en conservation (master d'abord). Les extensions hors familles
# (xml, txt…) sont exclues de l'appariement.
FAMILLES_MEDIA = {
    "image": [".tif", ".tiff", ".jp2", ".png", ".jpg", ".jpeg"],
    "audio": [".wav", ".flac", ".mp3"],
    "video": [".mov", ".mp4", ".wmv"],
    "pdf": [".pdf"],
}

# Variantes de numérisation des fichiers audio de conservation : le suffixe est
# retiré du nom pour l'appariement, et toutes les variantes trouvées sont ajoutées
# en preservation:audio, dans l'ordre ci-dessous (master le plus riche d'abord).
MOTIF_VARIANTE_AUDIO = re.compile(r"_(\d+kHz\d+B|TI)$", re.IGNORECASE)
ORDRE_VARIANTES_AUDIO = {"96kHz24B": 0, "44kHz24B": 1, "TI": 2}


def famille_media(ext):
    """Famille de média (image, audio, video, pdf) d'une extension, ou None."""
    ext = ext.lower()
    for famille, extensions in FAMILLES_MEDIA.items():
        if ext in extensions:
            return famille
    return None


class EADbnr2mnesys:
    """
    Classe pour transformer des fichiers EAD (Encoded Archival Description) des IR BnR
    en vue de leur import dans Mnesys.
    """

    def __init__(self, names_csv_path, oai_csv_path, irs_excel_path, files_csv_path):
        """
        Initialise le transformateur avec les chemins vers les fichiers de données.

        Args:
            names_csv_path (str): Chemin vers le CSV contenant les noms (persname/corpname).
            oai_csv_path (str): Chemin vers le CSV contenant les correspondances OAI (cote -> osiros_id).
            irs_excel_path (str): Chemin vers le fichier Excel listant les instruments de recherche.
            files_csv_path (str): Chemin vers le CSV de référence des fichiers de conservation.
        """
        self.names_type = self._load_names(names_csv_path)
        self.oai_dict = self._load_oai(oai_csv_path)
        self.irs = self._load_irs(irs_excel_path)
        self.files_dict, self.ocr_dict, self.audio_diffusion = self._load_files(
            files_csv_path
        )

        # Dossiers de travail
        self.input_dir = join("data", "ead", "bnr")
        self.output_dir = join("results", "ead", "ead_cor", "bnr2mnesys")

        # Concordance ir / unitid / id : créée à la première exécution, elle est
        # rechargée ensuite pour réattribuer les mêmes id (cf. _add_ids).
        self.concordance_path = join("results", "ead", "ead_cor", "concordance_id.csv")
        self.concordance = self._load_concordance(self.concordance_path)
        self.ids_attribues = {ligne["id"] for ligne in self.concordance}

    def _load_names(self, csv_path):
        """Charge les noms (persname/corpname) depuis un CSV."""
        names = pd.read_csv(csv_path)
        return {
            "p": names[names["type"] == "persname"]["contenu"].tolist(),
            "c": names[names["type"] == "corpname"]["contenu"].tolist(),
        }

    def _load_oai(self, csv_path):
        """Charge les correspondances OAI (cote -> osiros_id) depuis un CSV."""
        oai = pd.read_csv(csv_path)
        oai = oai[~oai["cote"].isna()]
        return dict(zip(oai["cote"], oai["osiros_id"]))

    def _load_irs(self, excel_path):
        """Charge la liste des instruments de recherche depuis un Excel."""
        irs = pd.read_excel(excel_path)
        return irs[irs["statut"] == "TRANSFERER"]

    def _load_concordance(self, csv_path):
        """Charge la concordance ir / unitid / id des exécutions précédentes."""
        if not exists(csv_path):
            return []
        return pd.read_csv(csv_path, dtype=str).to_dict(orient="records")

    def _save_concordance(self):
        """Écrit la concordance ir / unitid / id, rechargée aux exécutions suivantes."""
        pd.DataFrame(self.concordance, columns=["ir", "unitid", "id"]).to_csv(
            self.concordance_path, index=False
        )

    def _load_files(self, csv_path):
        """Charge les chemins de conservation depuis un CSV, indexés par nom de fichier
        sans extension, puis par famille de média (cf. FAMILLES_MEDIA), puis par
        variante ("" hors audio). Charge aussi les fichiers OCR (file_type "ocr xml"),
        indexés par nom sans extension.

        Pour l'audio, le suffixe de variante (cf. MOTIF_VARIANTE_AUDIO) est retiré du
        nom et sert de clé de variante : toutes les numérisations d'une même piste
        sont conservées. Pour un même nom, une même famille et une même variante, le
        format le plus prioritaire est retenu (ex. tif avant jpg pour les images,
        wav avant mp3 pour l'audio). Les fichiers hors familles connues (txt…) sont
        ignorés.

        Charge enfin les mp3 de la variante TI, indexés par nom sans suffixe : ce
        sont les fichiers de diffusion audio (cf. _add_conservation_daoloc).
        """
        df = pd.read_csv(csv_path, low_memory=False)
        df = df.dropna(subset=["name", "s3_key"]).drop_duplicates(subset=["name"])

        files = {}
        rangs = {}
        audio_diffusion = {}
        for name, s3_key in zip(df["name"], df["s3_key"]):
            stem = splitext(name)[0]
            ext = splitext(s3_key)[1].lower()
            famille = famille_media(ext)
            if famille is None:
                continue
            variante = ""
            if famille == "audio":
                m = MOTIF_VARIANTE_AUDIO.search(stem)
                if m:
                    variante = m.group(1)
                    stem = stem[: m.start()]
                if variante.upper() == "TI" and ext == ".mp3":
                    audio_diffusion[stem] = s3_key
            rang = FAMILLES_MEDIA[famille].index(ext)
            if rang < rangs.get((stem, famille, variante), len(FAMILLES_MEDIA[famille])):
                rangs[(stem, famille, variante)] = rang
                files.setdefault(stem, {}).setdefault(famille, {})[variante] = s3_key

        ocr = df[df["file_type"] == "ocr xml"]
        ocr_dict = dict(zip(ocr["name"].apply(lambda x: splitext(x)[0]), ocr["s3_key"]))
        return files, ocr_dict, audio_diffusion

    def _find_ancestor(self, context, tag):
        """Trouve l'ancêtre d'un élément avec une balise donnée."""
        ancestors = context.xpath(f"ancestor::{tag}")
        return ancestors[0] if ancestors else None

    def _is_element_empty(self, element):
        """Vérifie si un élément XML est vide."""
        if element.text and element.text.strip() != "":
            return False
        if len(element.attrib) > 0:
            return False
        for child in element:
            if not self._is_element_empty(child):
                return False
        return True

    def _merge_daogrp(self, element):
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

    def _add_dao_ark(self, element):
        """
        Ajoute pour chaque <archdesc> et <c> les liens ARK BnR sous forme de
        <dao>/<daoloc> :

        - l'ARK actuel (role="publication:current"), construit à partir de
          l'attribut id de l'élément : https://www.bn-r.fr/ark:/20179/BNR<id> ;
        - l'ancien ARK (role="publication:previous") pour les <c> dont le
          <unitid> figure dans la table de correspondance OAI :
          https://www.bn-r.fr/ark:/20179/<osiros_id>.

        Trois cas selon la structure existante :

        - <daogrp> présent : ajout d'un <daoloc> par lien dans le groupe existant
          (sans doublon de role).
        - <dao> présent (sans <daogrp>) : transformation en <daogrp> avec un <daoloc>
          reprenant les attributs de l'ancienne <dao> et un <daoloc> par lien.
        - Ni <dao> ni <daogrp> : création d'un <dao> (lien unique) ou d'un <daogrp>
          (plusieurs liens).

        Dans les cas 2 et 3, la balise est insérée avant le premier enfant <c> ou
        <dsc> s'il existe.
        """
        for el in element.iter("archdesc", "c"):
            liens = []
            if el.get("id"):
                liens.append(
                    (
                        f"https://www.bn-r.fr/ark:/20179/BNR{el.get('id')}",
                        "publication:current",
                    )
                )
            unitid = el.find("./did/unitid")
            if el.tag == "c" and unitid is not None and unitid.text in self.oai_dict:
                liens.append(
                    (
                        f"https://www.bn-r.fr/ark:/20179/{self.oai_dict[unitid.text]}",
                        "publication:previous",
                    )
                )
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
                continue

            # Cas 2 : <dao> existe mais pas <daogrp> → conversion en <daogrp>
            if (old_dao := el.find("dao")) is not None:
                nouveau = etree.Element("daogrp")

                # Reprendre les attributs de l'ancienne <dao> dans un <daoloc>
                daoloc_from_dao = etree.SubElement(nouveau, "daoloc")
                for attr, value in old_dao.attrib.items():
                    daoloc_from_dao.set(attr, value)

                for url, role in liens:
                    new_daoloc = etree.SubElement(nouveau, "daoloc")
                    new_daoloc.set("href", url)
                    new_daoloc.set("role", role)

                el.remove(old_dao)

            # Cas 3 : ni <dao> ni <daogrp>
            elif len(liens) == 1:
                url, role = liens[0]
                nouveau = etree.Element("dao")
                nouveau.set("href", url)
                nouveau.set("role", role)
            else:
                nouveau = etree.Element("daogrp")
                for url, role in liens:
                    new_daoloc = etree.SubElement(nouveau, "daoloc")
                    new_daoloc.set("href", url)
                    new_daoloc.set("role", role)

            # Insérer avant le premier enfant <c>/<dsc> ou à la fin
            if insert_before is not None:
                el.insert(list(el).index(insert_before), nouveau)
            else:
                el.append(nouveau)


    def _update_dao_roles(self, element):
        """Préfixe 'access:' au role des <dao> et <daoloc> dont le role commence par
        'image' ou vaut 'mp3', 'mp4' ou 'pdf'. 'mp3' est renommé en 'audio' et 'mp4'
        en 'video' (→ access:audio, access:video, access:pdf)."""
        renommages = {"mp3": "audio", "mp4": "video"}
        for dao in element.xpath(".//*[self::dao or self::daoloc]"):
            role = dao.get("role", "")
            if role.startswith("image") or role in ("mp3", "mp4", "pdf"):
                dao.set("role", f"access:{renommages.get(role, role)}")

    def _add_conservation_daoloc(self, element):
        """
        Pour chaque <dao>/<daoloc> dont le href correspond (par basename sans extension,
        au sein de la même famille de média) à un fichier de conservation, ajoute un
        <daoloc role="preservation:..."> avec le s3_key.
        Le role de la nouvelle <daoloc> est obtenu en remplaçant 'access:' par 'preservation:'
        dans le role de l'élément source.
        Le href de l'élément source (lien de diffusion) est complété avec le dossier du
        chemin de conservation : dirname(s3_key)/basename(href). Pour l'audio, si un
        mp3 de la variante TI existe en conservation, le href de diffusion est remplacé
        par son chemin S3 complet.
        Pour les liens des familles image et pdf, le fichier OCR de même nom de base est
        ajouté de la même façon en <daoloc role="access:ocr">.
        Si le <dao> est isolé (hors <daogrp>), il est converti en <daogrp> + <daoloc> au préalable.
        """
        for dao_elem in list(element.xpath(".//dao | .//daoloc")):
            href = dao_elem.get("href", "")
            name_key, ext = splitext(basename(href))
            famille = famille_media(ext)
            if not name_key or famille is None:
                continue

            cle_conservation = name_key
            variantes = self.files_dict.get(name_key, {}).get(famille)
            if variantes is None and famille == "audio":
                # Fonds sonore FLRS : le nom EAD "RBX_MED_FLRS_*" correspond en
                # conservation à "RBX_MED_*", et "+" y devient "_".
                cle_conservation = name_key.replace("RBX_MED_FLRS_", "RBX_MED_").replace(
                    "+", "_"
                )
                variantes = self.files_dict.get(cle_conservation, {}).get(famille)
            ocr_key = (
                self.ocr_dict.get(name_key) if famille in ("image", "pdf") else None
            )
            if variantes is None and ocr_key is None:
                continue

            ajouts = []
            if variantes is not None:
                preservation_role = dao_elem.get("role", "").replace(
                    "access:", "preservation:", 1
                )
                s3_keys = [
                    variantes[v]
                    for v in sorted(
                        variantes, key=lambda v: (ORDRE_VARIANTES_AUDIO.get(v, 3), v)
                    )
                ]
                ajouts += [(s3_key, preservation_role) for s3_key in s3_keys]
                if famille == "audio" and cle_conservation in self.audio_diffusion:
                    # La diffusion audio pointe vers le mp3 de la variante TI
                    dao_elem.set("href", self.audio_diffusion[cle_conservation])
                else:
                    dao_elem.set("href", join(dirname(s3_keys[0]), basename(href)))
            if ocr_key is not None:
                ajouts.append((ocr_key, "access:ocr"))

            parent = dao_elem.getparent()

            if dao_elem.tag == "dao" and (parent is None or parent.tag != "daogrp"):
                # Convertir <dao> isolé en <daogrp>
                daogrp = etree.Element("daogrp")
                daoloc_original = etree.SubElement(daogrp, "daoloc")
                for attr, value in dao_elem.attrib.items():
                    daoloc_original.set(attr, value)
                for ajout_href, ajout_role in ajouts:
                    new_daoloc = etree.SubElement(daogrp, "daoloc")
                    new_daoloc.set("href", ajout_href)
                    new_daoloc.set("role", ajout_role)
                if parent is not None:
                    idx = list(parent).index(dao_elem)
                    parent.remove(dao_elem)
                    parent.insert(idx, daogrp)

            elif dao_elem.tag == "daoloc" and parent is not None and parent.tag == "daogrp":
                for ajout_href, ajout_role in ajouts:
                    new_daoloc = etree.SubElement(parent, "daoloc")
                    new_daoloc.set("href", ajout_href)
                    new_daoloc.set("role", ajout_role)

    def _sort_daogrp(self, element):
        """Trie les enfants de chaque <daogrp> : <daodesc> toujours en premier, puis les
        <daoloc> par role (preservation: d'abord, access: ensuite, publication: en dernier)."""
        role_order = {"preservation": 0, "access": 1, "publication": 2}

        def cle(e):
            if e.tag == "daodesc":
                return -1
            return next(
                (order for prefix, order in role_order.items() if e.get("role", "").startswith(prefix)),
                len(role_order),
            )

        for daogrp in element.xpath(".//daogrp"):
            enfants = sorted(daogrp, key=cle)
            for enfant in enfants:
                daogrp.remove(enfant)
            for enfant in enfants:
                daogrp.append(enfant)

    def _strip_whitespace(self, element):
        """Supprime les espaces en début/fin de texte dans les éléments `<controlaccess>`."""
        for controlaccess in element.xpath("//controlaccess//*"):
            if controlaccess.text is not None:
                controlaccess.text = controlaccess.text.strip()
            if controlaccess.tail is not None:
                controlaccess.tail = controlaccess.tail.strip()

    def _remove_empty_attributes(self, element):
        """Supprime les attributs vides dans tout l'arbre XML."""
        for elem in element.iter():
            empty_attrs = [attr for attr, value in elem.attrib.items() if value == ""]
            for attr in empty_attrs:
                del elem.attrib[attr]

    def _remove_empty_elements(self, element):
        """Supprime les éléments vides (récursivement)."""
        for elem in list(element.iter()):
            if self._is_element_empty(elem):
                parent = elem.getparent()
                if parent is not None:
                    parent.remove(elem)

    def _remove_html_entities(self, element):
        """Décode les entités HTML dans le texte et les queues des éléments."""
        for e in element.iter():
            if e.text:
                e.text = unescape(e.text)
            if e.tail:
                e.tail = unescape(e.tail)

    def _remove_name(self, element):
        """Remplace les balises `<name>` par `<persname>` ou `<corpname>` selon une liste
        préparée en amont."""
        for controlaccess in element.xpath("//controlaccess"):
            for name in controlaccess.xpath(".//name"):
                text = name.text.strip() if name.text else ""
                if text in self.names_type["c"]:
                    name.tag = "corpname"
                elif text in self.names_type["p"]:
                    name.tag = "persname"

    def _move_origination(self, element):
        """
        Déplace les éléments `<name>`/`<persname>` de `<origination>` vers `<controlaccess>`.
        Supprime `<origination>` s'il devient vide.
        """
        for origination in element.xpath("//origination"):
            name_elements = origination.xpath(".//name | .//persname")
            parent = origination.getparent()
            archdesc = (
                self._find_ancestor(origination, "archdesc") or parent.getparent()
            )

            controlaccess = parent.getparent().find("controlaccess")
            if controlaccess is None:
                controlaccess = etree.SubElement(parent.getparent(), "controlaccess")

            for e in name_elements:
                if e.tag == "name":
                    text = e.text.strip() if e.text else ""
                    if text in self.names_type["p"]:
                        new_tag = "persname"
                    elif text in self.names_type["c"]:
                        new_tag = "corpname"
                    else:
                        new_tag = e.tag
                    new_elem = etree.Element(new_tag)
                    new_elem.text = e.text
                    new_elem.attrib.update(e.attrib)
                    controlaccess.append(new_elem)
                else:
                    controlaccess.append(e)
                e.getparent().remove(e)

            if self._is_element_empty(origination):
                origination.getparent().remove(origination)

    def _update_eadid(self, element, new_eadid):
        """Met à jour la balise `<eadid>`."""
        eadid = element.find(".//eadid")
        if eadid is not None:
            eadid.text = new_eadid

    def _update_archdesc_unitid(self, element, new_unitid):
        """Met à jour la balise `<unitid>` dans `<archdesc/did>`."""
        unitid = element.find("./archdesc/did/unitid")
        if unitid is not None:
            unitid.text = new_unitid

    def _update_repository(self, element, new_repository):
        """Met à jour la balise `<repository>` dans `<archdesc/did>`."""
        repository = element.find("./archdesc/did/repository")
        if repository is not None:
            repository.text = new_repository

    def _remove_repositories(self, element):
        """Supprime les balises `<repository>` en dehors de `<archdesc/did>`."""
        for repo in element.xpath("//repository"):
            parent = repo.getparent()
            if (
                parent is None
                or parent.tag != "did"
                or (
                    parent.getparent() is not None
                    and parent.getparent().tag != "archdesc"
                )
            ):
                repo.getparent().remove(repo)

    def _update_controlaccess_source(self):
        pass

    def _add_ids(self, element, ir_id):
        """
        Ajoute un attribut id (format Mnesys, préfixe "m0") aux <archdesc> et <c>
        qui n'en ont pas.

        Si la concordance contient une entrée pour (ir, unitid), l'id qu'elle
        contient est repris ; sinon un nouvel id est généré et consigné dans la
        concordance. Les unitid en doublon dans un même IR sont appariés dans
        l'ordre du document. Les éléments sans <did>/<unitid> reçoivent un id
        nouveau à chaque exécution (pas de clé de concordance).
        """
        disponibles = {}
        for ligne in self.concordance:
            if ligne["ir"] == ir_id:
                disponibles.setdefault(ligne["unitid"], []).append(ligne["id"])

        for el in element.iter("archdesc", "c"):
            if el.get("id"):
                self.ids_attribues.add(el.get("id"))
                continue

            unitid_el = el.find("./did/unitid")
            unitid = (
                unitid_el.text.strip()
                if unitid_el is not None and unitid_el.text and unitid_el.text.strip()
                else None
            )

            if unitid and disponibles.get(unitid):
                el.set("id", disponibles[unitid].pop(0))
            else:
                nouveau = nouvel_id(self.ids_attribues, prefixe="m0")
                el.set("id", nouveau)
                if unitid:
                    self.concordance.append(
                        {"ir": ir_id, "unitid": unitid, "id": nouveau}
                    )

    def transform_ead(self, ir):
        """
        Applique toutes les transformations à un fichier EAD.

        Args:
            ir (dict): Dictionnaire contenant les métadonnées de l'instrument de recherche.
        """
        input_path = join(self.input_dir, ir["nom_fichier"])
        output_path = join(self.output_dir, ir["nouveau_nom_fichier"])

        if not exists(input_path):
            print(f"Fichier introuvable : {input_path}")
            return False

        try:
            tree = etree.parse(input_path)
            root = tree.getroot()
        except etree.XMLSyntaxError as e:
            print(f"XML invalide dans {input_path} : {e}")
            return False

        # Appliquer les transformations
        self._update_eadid(root, ir["nouveau_ead_id"])
        self._update_archdesc_unitid(root, ir["nouveau_archdesc_unitid"])
        self._update_repository(root, ir["nouveau_repository"])
        self._remove_html_entities(root)
        self._strip_whitespace(root)
        self._move_origination(root)
        self._add_ids(root, str(ir["nouveau_ead_id"]))
        self._merge_daogrp(root)
        self._add_dao_ark(root)
        self._update_dao_roles(root)
        self._add_conservation_daoloc(root)
        self._merge_daogrp(root)
        self._sort_daogrp(root)
        self._remove_name(root)
        self._remove_repositories(root)
        # self._update_controlaccess_source(root)
        self._remove_empty_attributes(root)
        self._remove_empty_elements(root)

        # Sauvegarder le fichier transformé
        tree.write(
            output_path,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=True,
            with_comments=True,
        )
        return True

    def run(self):
        """Exécute la transformation pour tous les instruments de recherche."""
        for ir in self.irs.to_dict(orient="records"):
            self.transform_ead(ir)
        self._save_concordance()


# --- Utilisation ---
if __name__ == "__main__":
    transformer = EADbnr2mnesys(
        names_csv_path=join(
            "results", "ead", "indexation", "controlaccess_extraction_name2.csv"
        ),
        oai_csv_path=join("data", "oai", "oai_records_20260430.csv.gz"),
        irs_excel_path=join(
            "results",
            "ir",
            "liste_instruments_recherche_20260521_transfert_mnesys.xlsx",
        ),
        files_csv_path=join("results", "ref", "_ref_files_20260502.csv.gz"),
    )
    transformer.run()
