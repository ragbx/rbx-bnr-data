"""Encodage JPEG trace de la chaine img3 (presse_mets.py) : lecture stricte du
master, preparation, ecriture controlee, trace XMP relue par jpeg_to_mix.py.

Repris de img2/common.py le 2026-10-03 pour rendre img3 autonome. img2 garde sa
propre version (meme trace XMP), sans la lecture stricte (load_master) ni
l'ecriture controlee (jpegsave_checked).

pyvips n'est importe que dans les fonctions : l'appelant doit pouvoir poser
VIPS_CONCURRENCY avant le premier import.
"""


def _libjpeg_version() -> str:
    """Version de l'encodeur JPEG (libjpeg-turbo) de l'environnement conda courant,
    lue dans conda-meta ; 'libjpeg' seul si introuvable (libvips ne l'expose pas)."""
    import glob
    import json
    import os
    import sys

    for prefix in (os.environ.get("CONDA_PREFIX"), sys.prefix):
        if not prefix:
            continue
        for meta in glob.glob(os.path.join(prefix, "conda-meta", "libjpeg-turbo-*.json")):
            try:
                with open(meta, encoding="utf-8") as f:
                    return "libjpeg-turbo " + json.load(f)["version"]
            except (OSError, ValueError, KeyError):
                pass
    return "libjpeg"


def tool_versions() -> str:
    """Ex. : libvips 8.18.0 / libjpeg-turbo 3.1.4.1"""
    import pyvips

    return f"libvips {pyvips.version(0)}.{pyvips.version(1)}.{pyvips.version(2)} / {_libjpeg_version()}"


def load_master(path: str):
    """Ouvre un master en flux, en refusant les fichiers abimes.

    Par defaut libvips tolere les erreurs de decodage : un TIFF LZW corrompu
    donne une image aux lignes fausses, sans erreur. Avec fail_on="error",
    l'erreur est levee a la lecture des pixels, donc pendant l'encodage (isolee
    par fascicule dans presse_mets.py).
    """
    import pyvips

    return pyvips.Image.new_from_file(path, access="sequential", fail_on="error")


def prepare_for_jpeg(image):
    """Transformations prealables a l'encodage JPEG. Retourne (image, etapes),
    etapes = operations reellement appliquees, reprises dans la trace XMP.

    - ICC -> sRGB si profil embarque et >= 3 bandes : les TIFF portent souvent un
      profil propre au scanner (couleurs trop saturees sans conversion) ; une image
      en niveaux de gris n'a pas de teinte a corriger.
    - cast uchar shift=True si l'image est en 16 bits (icc_transform conserve
      les 16 bits) : explicite plutot que la conversion automatique de jpegsave,
      qui depend de l'interpretation declaree. Tout autre format que 8/16 bits
      non signes leve une erreur (isolee par fascicule dans presse_mets.py).
    """
    steps = []
    if image.bands >= 3 and image.get_typeof("icc-profile-data") != 0:
        image = image.icc_transform("srgb")
        steps.append("icc_transform sRGB")
    if image.format == "ushort":
        image = image.cast("uchar", shift=True)
        steps.append("cast uchar shift=True")
    elif image.format != "uchar":
        # flottant, signe, 32 bits : le decalage de 8 bits n'aurait pas de sens
        raise ValueError(f"format de pixel non prevu pour le JPEG : {image.format}")
    return image, steps


def processing_actions(image, quality: int, steps: list) -> str:
    """Chaine decrivant la generation, ecrite dans le XMP (stEvt:parameters) et
    reprise telle quelle en MIX processingActions par jpeg_to_mix.py. Ex. :
    libvips 8.18.0 / libjpeg-turbo 3.1.4.1 ; icc_transform sRGB ; jpegsave Q=80,
    subsampling 4:2:0 (auto), optimize_coding, progressive
    """
    save = [f"Q={quality}"]
    if image.bands >= 3:
        # regle libvips en mode auto : 4:2:0 sous Q=90, 4:4:4 au-dela (verifie 8.18)
        save.append(f"subsampling {'4:2:0' if quality < 90 else '4:4:4'} (auto)")
    save += ["optimize_coding", "progressive"]
    return " ; ".join([tool_versions(), *steps, "jpegsave " + ", ".join(save)])


def _xmp_packet(creator_tool: str, actions: str, when: str, uuid: str) -> bytes:
    from xml.sax.saxutils import quoteattr

    return (
        '<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>'
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        '<rdf:Description rdf:about=""'
        ' xmlns:dc="http://purl.org/dc/elements/1.1/"'
        ' xmlns:xmp="http://ns.adobe.com/xap/1.0/"'
        ' xmlns:xmpMM="http://ns.adobe.com/xap/1.0/mm/"'
        ' xmlns:stEvt="http://ns.adobe.com/xap/1.0/sType/ResourceEvent#"'
        f' dc:identifier={quoteattr(uuid)}'
        f' xmp:CreatorTool={quoteattr(creator_tool)} xmp:CreateDate="{when}">'
        '<xmpMM:History><rdf:Seq><rdf:li rdf:parseType="Resource">'
        '<stEvt:action>converted</stEvt:action>'
        f'<stEvt:when>{when}</stEvt:when>'
        f'<stEvt:softwareAgent>{creator_tool}</stEvt:softwareAgent>'
        f'<stEvt:parameters>{actions}</stEvt:parameters>'
        '</rdf:li></rdf:Seq></xmpMM:History>'
        '</rdf:Description></rdf:RDF></x:xmpmeta>'
        '<?xpacket end="w"?>'
    ).encode("utf-8")


def jpegsave_tagged(image, dst: str, quality: int, steps: list, uuid: str) -> None:
    """jpegsave (Q, optimize_coding, progressif) avec trace de la generation :
    - EXIF Software = outil et versions ;
    - XMP neuf : dc:identifier (uuid du JPEG, au format du ref : 32 hexa),
      xmp:CreatorTool, xmp:CreateDate et un evenement xmpMM:History dont
      stEvt:parameters porte la chaine complete (cf. processing_actions).
    Le XMP et l'IPTC herites du TIFF sont remplaces/supprimes : ils decrivent le
    master (logiciel de numerisation...) et contrediraient la trace du JPEG.
    Le JPEG ne stocke pas Q (seules les tables de quantification en decoulent).
    """
    from datetime import datetime
    from xml.sax.saxutils import escape

    import pyvips

    tools = tool_versions()
    when = datetime.now().astimezone().isoformat(timespec="seconds")
    image = image.copy()  # ne pas modifier l'image de l'appelant
    if image.get_typeof("iptc-data"):
        image.remove("iptc-data")
    image.set_type(pyvips.GValue.gstr_type, "exif-ifd0-Software", tools)
    image.set_type(pyvips.GValue.blob_type, "xmp-data",
                   _xmp_packet(escape(tools), escape(processing_actions(image, quality, steps)), when, uuid))
    image.jpegsave(dst, Q=quality, optimize_coding=True, interlace=True)


def check_jpeg(path: str, width: int, height: int, label: str = None) -> None:
    """Controle d'un JPEG : relu depuis le disque et redecode en entier
    (fail_on="error"), il doit avoir les dimensions de la source. Leve une erreur
    sinon. label : nom a citer dans le message (defaut : path)."""
    import pyvips

    # relecture en memoire, sans laisser le fichier ouvert par libvips
    with open(path, "rb") as f:
        check = pyvips.Image.new_from_buffer(f.read(), "", access="sequential", fail_on="error")
    if (check.width, check.height) != (width, height):
        raise ValueError(f"dimensions du JPEG {check.width}x{check.height} differentes de la source "
                         f"{width}x{height} : {label or path}")
    check.avg()  # force le decodage de tous les pixels


def jpegsave_checked(image, dst: str, quality: int, steps: list, uuid: str) -> None:
    """jpegsave_tagged en deux temps : ecriture sous <dst>.part, controle
    (check_jpeg), puis renommage. Un traitement interrompu ne laisse donc jamais
    de JPEG partiel sous son nom final, que la reprise prendrait pour un fichier
    deja converti. En cas d'echec le .part est supprime et l'erreur remonte.
    """
    import os

    tmp = dst + ".part"
    try:
        jpegsave_tagged(image, tmp, quality, steps, uuid)
        check_jpeg(tmp, image.width, image.height, label=dst)
        os.replace(tmp, dst)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
