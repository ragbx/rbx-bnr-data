#!/usr/bin/env python3
r"""
tif_convert.py — Conversion par lot de TIF vers JP2, JPEG ou TIFF pyramidal pour la diffusion web.

Caractéristiques :
  - parcourt récursivement un dossier d'entrée et recrée l'arborescence en sortie
  - reprend où il s'est arrêté (saute les fichiers déjà convertis)
  - isole les erreurs par fichier (un TIF corrompu n'interrompt pas le lot)
  - parallélise au niveau des fichiers, 1 thread libvips par worker (pas de sursouscription CPU)
  - faible empreinte mémoire (lecture en flux, access="sequential")
  - réduction de résolution optionnelle par niveau (haut/moyen/bas), plancher 2000 px

Prérequis (Windows) :
  pip install "pyvips[binary]"   # installe pyvips ET une libvips autonome : rien d'autre à faire
  pip install tqdm               # optionnel : barre de progression
  # Variante équivalente : conda install -c conda-forge pyvips (libvips incluse)
  # (Sur Linux/macOS : installer libvips via le gestionnaire de paquets, puis « pip install pyvips ».)

Exemples (PowerShell ou cmd, Python 64 bits requis) :
  python tif_convert.py C:\data\masters C:\data\diffusion --format jp2 --quality 60
  python tif_convert.py C:\data\masters C:\data\diffusion --format jp2 --quality 50 --workers 8
  python tif_convert.py C:\data\masters C:\data\diffusion --format ptiff --quality 80   # repli TIFF pyramidal
  python tif_convert.py C:\data\masters C:\data\diffusion --format jpeg --quality 80 --niveau moyen   # iconographie
  python tif_convert.py C:\data\masters C:\data\diffusion --format jpeg --quality 75 --niveau bas     # presse
"""

import argparse
import logging
import os
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

# Sous Windows, libvips est fournie comme DLL autonome : on ajoute son dossier au PATH et
# aux répertoires de recherche de DLL AVANT d'importer pyvips. Sur Linux/macOS, libvips est
# installée par le gestionnaire de paquets (ou conda) et trouvée automatiquement : rien à faire.
if os.name == "nt":
    vips_bin = os.environ.get(
        "VIPS_BIN",
        r"C:\Users\pichenotf\vips-dev-x64-all-8.18.3\vips-dev-8.18\bin",
    )
    if os.path.isdir(vips_bin):
        os.environ["PATH"] = vips_bin + os.pathsep + os.environ["PATH"]
        os.add_dll_directory(vips_bin)
import pyvips

# 1 seul thread libvips par processus : on parallélise au niveau des fichiers (voir ProcessPoolExecutor).
# pyvips n'expose pas de concurrency_set fiable selon les versions ; on passe donc par la variable
# d'environnement, posée AVANT tout import de pyvips. Sur Windows (mode spawn), chaque worker
# ré-exécute ce module, donc la variable est en place avant l'import de pyvips fait dans les workers.
os.environ.setdefault("VIPS_CONCURRENCY", "1")

# Extensions reconnues en entrée
TIFF_EXTS = {".tif", ".tiff", ".TIF", ".TIFF"}

# Taille de tuile : 512 est le défaut libvips et un bon compromis pour le tuilage
DEFAULT_TILE = 512

# Plancher de résolution : la largeur ne descend jamais sous cette valeur (en px),
# et les images déjà plus petites ne sont jamais agrandies.
DEFAULT_PLANCHER = 2000

# Niveaux de réduction de résolution par corpus (facteur f, cf. resolution_corpus.ipynb).
# La formule par image (largeur w) est : s = min(1, max(f, plancher / w)).
#   - haut  (manuscrits/plans, détails fins)  : on conserve l'essentiel de la résolution
#   - moyen (iconographie)
#   - bas   (presse, texte)
NIVEAUX = {"haut": 0.80, "moyen": 0.65, "bas": 0.50}

# Extensions de sortie par format
OUT_EXT = {"jp2": ".jp2", "jpeg": ".jpg", "ptiff": ".tif"}


def convert_one(src_str, dst_str, fmt, quality, tile, facteur, plancher):
    """Convertit un fichier. Retourne (src, statut, message)."""
    import pyvips  # ré-import local (process pool)

    src = Path(src_str)
    dst = Path(dst_str)
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)

        # Lecture en flux : faible mémoire même sur des images de plusieurs centaines de Mpx
        image = pyvips.Image.new_from_file(src_str, access="sequential")

        # Réduction de résolution éventuelle (facteur < 1). Le plancher garantit qu'on ne
        # descend pas sous `plancher` px de large ; les images déjà plus petites sont laissées
        # telles quelles (pas d'agrandissement). resize ajuste aussi la résolution (DPI).
        if facteur < 1.0:
            s = min(1.0, max(facteur, plancher / image.width))
            if s < 1.0:
                image = image.resize(s)

        if fmt == "jp2":
            # jp2ksave écrit TOUJOURS une pyramide ; subsampling désactivé par défaut (4:4:4).
            # On baisse Q pour compresser franchement, le master étant sauvegardé ailleurs.
            image.jp2ksave(
                dst_str,
                Q=quality,
                tile_width=tile,
                tile_height=tile,
                # subsample_mode laissé par défaut => pas de sous-échantillonnage chroma
            )
        elif fmt == "jpeg":
            # JPEG progressif (interlace) : meilleur rendu au chargement web.
            # On garde les métadonnées (profil ICC notamment) pour préserver les couleurs.
            image.jpegsave(
                dst_str,
                Q=quality,
                interlace=True,
            )
        elif fmt == "ptiff":
            # Repli : TIFF pyramidal tuilé à compression JPEG interne.
            image.tiffsave(
                dst_str,
                compression="jpeg",
                Q=quality,
                tile=True,
                tile_width=tile,
                tile_height=tile,
                pyramid=True,
                bigtiff=True,           # sécurité pour les fichiers volumineux
                subifd=True,            # overviews en sous-IFD (lecture plus propre par les serveurs)
            )
        else:
            return (src_str, "error", f"format inconnu : {fmt}")

        return (src_str, "ok", "")
    except Exception:  # noqa: BLE001 — on attrape tout pour ne pas tuer le lot
        # repr(exc) donnait des messages vides (ex. AttributeError()) : on renvoie la trace complète.
        return (src_str, "error", traceback.format_exc())


def check_format_support(fmt):
    """Vérifie que libvips sait écrire le format demandé. Retourne un message d'erreur, ou None si OK."""
    import pyvips
    suffixes = pyvips.get_suffixes()
    if fmt == "jp2" and ".jp2" not in suffixes:
        return (
            "Votre installation de libvips n'a PAS le support JPEG2000 (compilée sans OpenJPEG).\n"
            "  Solutions :\n"
            "   - conda install -c conda-forge pyvips   (build incluant OpenJPEG)\n"
            "   - ou archive « vips-dev-w64-all » depuis libvips.org, dossier bin ajoute au PATH,\n"
            "     puis : pip install pyvips  (sans [binary])\n"
            "   - depannage immediat sans reinstaller : relancez avec --format ptiff"
        )
    return None


def target_path(src, in_root, out_root, fmt, quality, facteur):
    """Calcule le chemin de sortie en miroir de l'arborescence d'entrée.

    Le facteur Q est inséré dans le nom : page001.tif -> page001_q50.jp2
    Si une réduction de résolution est demandée (facteur < 1), elle est aussi
    inscrite : page001.tif -> page001_q50_f65.jpg. Ainsi des runs à des qualités
    ou des niveaux différents ne s'écrasent pas mutuellement.
    """
    rel = src.relative_to(in_root)
    ext = OUT_EXT[fmt]
    suffix = f"_q{quality}"
    if facteur < 1.0:
        suffix += f"_f{round(facteur * 100)}"
    new_name = f"{rel.stem}{suffix}{ext}"
    return out_root / rel.parent / new_name


def build_jobs(in_root, out_root, fmt, quality, facteur, overwrite):
    """Liste les conversions à faire (en sautant celles déjà présentes si overwrite=False)."""
    jobs, skipped = [], 0
    for src in sorted(in_root.rglob("*")):
        if not (src.is_file() and src.suffix in TIFF_EXTS):
            continue
        dst = target_path(src, in_root, out_root, fmt, quality, facteur)
        if dst.exists() and not overwrite:
            skipped += 1
            continue
        jobs.append((str(src), str(dst)))
    return jobs, skipped


def main():
    parser = argparse.ArgumentParser(description="Conversion par lot TIF -> JP2/JPEG/TIFF pyramidal pour la diffusion web.")
    parser.add_argument("input_dir", type=Path, help="dossier des TIF source")
    parser.add_argument("output_dir", type=Path, help="dossier de sortie (diffusion)")
    parser.add_argument("--format", choices=["jp2", "jpeg", "ptiff"], default="jp2",
                        help="format de sortie (défaut : jp2)")
    parser.add_argument("--quality", type=int, default=60,
                        help="facteur Q (défaut : 60 ; baisser pour compresser davantage)")
    parser.add_argument("--niveau", choices=sorted(NIVEAUX),
                        help="niveau de réduction de résolution par corpus : "
                             + ", ".join(f"{k} (f={v})" for k, v in NIVEAUX.items())
                             + " (défaut : aucune réduction)")
    parser.add_argument("--facteur", type=float,
                        help="facteur de réduction f explicite (0 < f <= 1) ; surcharge --niveau")
    parser.add_argument("--plancher", type=int, default=DEFAULT_PLANCHER,
                        help=f"largeur minimale en px sous laquelle on ne réduit pas (défaut : {DEFAULT_PLANCHER})")
    parser.add_argument("--tile", type=int, default=DEFAULT_TILE,
                        help=f"taille de tuile en px (défaut : {DEFAULT_TILE})")
    parser.add_argument("--workers", type=int, default=os.cpu_count(),
                        help="nombre de processus parallèles (défaut : nb de cœurs)")
    parser.add_argument("--overwrite", action="store_true",
                        help="reconvertir même si le fichier de sortie existe")
    parser.add_argument("--log", type=Path, default=Path("tif_convert.log"),
                        help="fichier journal (défaut : tif_convert.log)")
    args = parser.parse_args()

    # Console Windows souvent en cp1252 : on force l'UTF-8 pour les messages accentués.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass  # flux redirigé ou non reconfigurable : on n'insiste pas

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(args.log, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
    # libvips relaie ses messages de debug (« VIPS: residual reducev… ») via le logger Python ;
    # au niveau INFO ils noieraient le journal. On ne garde que les avertissements et au-delà.
    logging.getLogger("pyvips").setLevel(logging.WARNING)

    in_root = args.input_dir.resolve()
    out_root = args.output_dir.resolve()
    if not in_root.is_dir():
        logging.error("Dossier d'entrée introuvable : %s", in_root)
        sys.exit(1)

    # Facteur de réduction : --facteur explicite l'emporte sur --niveau ; sinon, si un niveau
    # est donné on prend sa valeur ; à défaut 1.0 (aucune réduction).
    if args.facteur is not None:
        facteur = args.facteur
    elif args.niveau is not None:
        facteur = NIVEAUX[args.niveau]
    else:
        facteur = 1.0
    if not 0.0 < facteur <= 1.0:
        logging.error("Facteur de réduction invalide : %s (attendu 0 < f <= 1).", facteur)
        sys.exit(1)
    if facteur < 1.0:
        logging.info("Réduction de résolution : facteur f=%.2f, plancher %d px.", facteur, args.plancher)

    # Contrôle en amont : on échoue clairement si libvips ne sait pas écrire le format demandé,
    # plutôt que de laisser chaque fichier planter avec une erreur opaque.
    problem = check_format_support(args.format)
    if problem:
        logging.error(problem)
        sys.exit(1)

    jobs, skipped = build_jobs(in_root, out_root, args.format, args.quality, facteur, args.overwrite)
    logging.info("%d fichier(s) à convertir, %d déjà présent(s) et sauté(s).", len(jobs), skipped)
    if not jobs:
        logging.info("Rien à faire.")
        return

    # Barre de progression optionnelle
    try:
        from tqdm import tqdm
        progress = tqdm(total=len(jobs), unit="img")
    except ImportError:
        progress = None

    ok = err = 0
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(convert_one, src, dst, args.format, args.quality, args.tile, facteur, args.plancher)
            for src, dst in jobs
        ]
        for fut in as_completed(futures):
            src, status, msg = fut.result()
            if status == "ok":
                ok += 1
            else:
                err += 1
                logging.error("ECHEC %s :\n%s", src, msg)
            if progress:
                progress.update(1)

    if progress:
        progress.close()

    dt = time.time() - t0
    logging.info("Terminé : %d réussis, %d en erreur, en %.1f s (%.2f img/s).",
                 ok, err, dt, ok / dt if dt else 0)
    if err:
        logging.warning("Des fichiers ont échoué — voir %s. Relancez : les réussites seront sautées.", args.log)
        sys.exit(2)


if __name__ == "__main__":
    main()
