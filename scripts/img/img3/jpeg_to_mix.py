#!/usr/bin/env python3
r"""jpeg_to_mix.py — Extrait des JPEG produits par presse_mets.py (ou par
img2/convert.py / convert_stream.py, meme trace) la trace de leur generation (XMP
ecrit par jpegsave_tagged, cf. jpeg_encode.py) et la restitue en MIX 2.0 (NISO
Z39.87), pret a etre insere dans un METS (amdSec/techMD).

Correspondance :
  xmp:CreatorTool  libvips X / libjpeg-turbo Y
    -> ChangeHistory/ImageProcessing/ProcessingSoftware (un par outil, avec version)
  xmpMM:History stEvt:parameters  (chaine complete, ex. « libvips 8.18.0 /
  libjpeg-turbo 3.1.4.1 ; icc_transform sRGB ; jpegsave Q=80, subsampling 4:2:0
  (auto), optimize_coding, progressive »)
    -> ChangeHistory/ImageProcessing/processingActions (telle quelle)
  stEvt:when -> ChangeHistory/ImageProcessing/dateTimeProcessed
  dc:identifier (uuid du JPEG, s'il en porte un)
    -> BasicDigitalObjectInformation/ObjectIdentifier
  colorSpace : BlackIsZero en niveaux de gris ; en couleur, sRGB si la trace porte
  « icc_transform sRGB », sinon RGB (master sans profil ICC : espace non connu)
  taille, dimensions, bandes -> relues dans le fichier ; resolution (JFIF/EXIF,
  heritee du master) -> ImageAssessmentMetadata/SpatialMetrics ; Compression/compressionRatio
  = octets non compresses (largeur x hauteur x bandes, 8 bits) / taille du fichier.

MIX 2.0 n'a pas de champ « niveau de qualite » : Q n'apparait que dans
processingActions (compressionSchemeLocalValue sert a nommer un schema hors liste).

Un JPEG sans XMP de trace conforme est signale (pas de MIX produit pour lui).

Usage :
  python jpeg_to_mix.py fichier.jpg                 # MIX sur stdout
  python jpeg_to_mix.py dossier/ --out-dir mix/     # un <nom>.mix.xml par JPEG (recursif)
  python jpeg_to_mix.py dossier/ --csv trace.csv    # tableau outil/version/qualite par fichier
"""

import argparse
import csv
import os
import re
import sys
from fractions import Fraction
from os.path import basename, getsize, join, splitext
from xml.etree import ElementTree as ET

os.environ.setdefault("VIPS_CONCURRENCY", "1")
import pyvips  # noqa: E402

NS = "http://www.loc.gov/mix/v20"
ET.register_namespace("mix", NS)

XMP_NS = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "xmp": "http://ns.adobe.com/xap/1.0/",
    "xmpMM": "http://ns.adobe.com/xap/1.0/mm/",
    "stEvt": "http://ns.adobe.com/xap/1.0/sType/ResourceEvent#",
}
Q_RE = re.compile(r"jpegsave Q=(\d+)")


def read_trace(path: str) -> dict:
    """Relit un JPEG : trace XMP + caracteristiques techniques."""
    img = pyvips.Image.new_from_file(path)
    info = {"path": path, "width": img.width, "height": img.height, "bands": img.bands,
            "size": getsize(path)}
    # libvips donne xres/yres en pixels/mm ; 1.0 = valeur par defaut (resolution inconnue)
    unit = img.get("resolution-unit") if img.get_typeof("resolution-unit") else ""
    if unit in ("in", "cm") and (img.xres, img.yres) != (1.0, 1.0):
        factor = 25.4 if unit == "in" else 10
        info["resolution"] = ("in." if unit == "in" else "cm",
                              *(Fraction(v * factor).limit_denominator(1000) for v in (img.xres, img.yres)))
    if not img.get_typeof("xmp-data"):
        return info
    try:
        root = ET.fromstring(img.get("xmp-data").decode("utf-8").strip("\x00"))
    except (ET.ParseError, UnicodeDecodeError):
        return info
    desc = root.find(".//rdf:Description", XMP_NS)
    event = root.find(".//xmpMM:History/rdf:Seq/rdf:li", XMP_NS)
    if desc is None or event is None:
        return info
    tools = desc.get(f"{{{XMP_NS['xmp']}}}CreatorTool", "")
    actions = event.findtext("stEvt:parameters", "", XMP_NS)
    m = Q_RE.search(actions)
    if not (tools.startswith("libvips ") and m):
        return info
    info.update(tools=tools, actions=actions, quality=int(m[1]),
                when=event.findtext("stEvt:when", "", XMP_NS),
                uuid=desc.get(f"{{{XMP_NS['dc']}}}identifier", ""))
    for tool in tools.split(" / "):
        name, _, version = tool.partition(" ")
        info.setdefault("software", []).append((name, version))
    return info


def build_mix(t: dict, source_data: str = None, extra_software=()) -> ET.ElementTree:
    """MIX du JPEG decrit par t (cf. read_trace). source_data : reference du master
    dont il est issu (ImageProcessing/sourceData) ; extra_software : (nom, version)
    ajoutes aux ProcessingSoftware, ex. le script de la chaine qui l'a produit."""
    def sub(parent, tag, text=None):
        el = ET.SubElement(parent, f"{{{NS}}}{tag}")
        if text is not None:
            el.text = str(text)
        return el

    root = ET.Element(f"{{{NS}}}mix")
    bdoi = sub(root, "BasicDigitalObjectInformation")
    if t.get("uuid"):
        oid = sub(bdoi, "ObjectIdentifier")
        sub(oid, "objectIdentifierType", "uuid")
        sub(oid, "objectIdentifierValue", t["uuid"])
    sub(bdoi, "fileSize", t["size"])
    sub(sub(bdoi, "FormatDesignation"), "formatName", "image/jpeg")
    comp = sub(bdoi, "Compression")
    sub(comp, "compressionScheme", "JPEG")
    ratio = sub(comp, "compressionRatio")
    sub(ratio, "numerator", t["width"] * t["height"] * t["bands"])
    sub(ratio, "denominator", t["size"])

    bii = sub(root, "BasicImageInformation")
    bic = sub(bii, "BasicImageCharacteristics")
    sub(bic, "imageWidth", t["width"])
    sub(bic, "imageHeight", t["height"])
    # sRGB seulement si la conversion ICC a eu lieu ; sinon l'espace du master, sans
    # profil, n'est pas connu : RGB
    if t["bands"] < 3:
        color_space = "BlackIsZero"
    else:
        color_space = "sRGB" if "icc_transform sRGB" in t["actions"] else "RGB"
    sub(sub(bic, "PhotometricInterpretation"), "colorSpace", color_space)

    assess = sub(root, "ImageAssessmentMetadata")
    if t.get("resolution"):
        unit, xres, yres = t["resolution"]
        spatial = sub(assess, "SpatialMetrics")
        sub(spatial, "samplingFrequencyUnit", unit)
        for name, v in (("xSamplingFrequency", xres), ("ySamplingFrequency", yres)):
            r = sub(spatial, name)
            sub(r, "numerator", v.numerator)
            sub(r, "denominator", v.denominator)

    # JPEG baseline : toujours 8 bits par echantillon
    enc = sub(assess, "ImageColorEncoding")
    bps = sub(enc, "BitsPerSample")
    for _ in range(t["bands"]):
        sub(bps, "bitsPerSampleValue", 8)
    sub(bps, "bitsPerSampleUnit", "integer")
    sub(enc, "samplesPerPixel", t["bands"])

    proc = sub(sub(root, "ChangeHistory"), "ImageProcessing")
    if t["when"]:
        sub(proc, "dateTimeProcessed", t["when"])
    if source_data:
        sub(proc, "sourceData", source_data)
    for name, version in [*t["software"], *extra_software]:
        sw = sub(proc, "ProcessingSoftware")
        sub(sw, "processingSoftwareName", name)
        if version:
            sub(sw, "processingSoftwareVersion", version)
    sub(proc, "processingActions", t["actions"])
    ET.indent(root)
    return ET.ElementTree(root)


def iter_jpegs(target: str):
    if os.path.isdir(target):
        for d, _, files in os.walk(target):
            for f in sorted(files):
                if f.lower().endswith((".jpg", ".jpeg")):
                    yield join(d, f)
    else:
        yield target


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("target", help="JPEG ou dossier (recursif)")
    ap.add_argument("--out-dir", help="ecrit un <nom>.mix.xml par JPEG")
    ap.add_argument("--csv", help="tableau de trace (outil, versions, qualite) par fichier")
    args = ap.parse_args()

    rows, missing = [], 0
    for path in iter_jpegs(args.target):
        t = read_trace(path)
        if "quality" not in t:
            missing += 1
            print(f"SANS TRACE XMP conforme : {path}", file=sys.stderr)
            continue
        rows.append(t)
        tree = build_mix(t)
        if args.out_dir:
            os.makedirs(args.out_dir, exist_ok=True)
            tree.write(join(args.out_dir, splitext(basename(path))[0] + ".mix.xml"),
                       encoding="utf-8", xml_declaration=True)
        elif not args.csv:
            tree.write(sys.stdout.buffer, encoding="utf-8", xml_declaration=True)
            print()
    if args.csv and rows:
        cols = ["path", "uuid", "tools", "quality", "when", "actions", "width", "height", "size"]
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
    print(f"{len(rows)} JPEG avec trace, {missing} sans.", file=sys.stderr)
    if missing:
        sys.exit(2)


if __name__ == "__main__":
    main()
