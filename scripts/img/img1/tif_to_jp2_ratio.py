#!/usr/bin/env python3
r"""
tif_to_jp2_ratio.py — Conversion par lot de TIF vers JP2 lossy à RATIO de compression cible.

Variante de tif_convert.py : au lieu d'un facteur de qualité Q fixe, on CALIBRE Q
par image, par dichotomie, pour viser un ratio de compression cible (par défaut ~1:15).

Le ratio est défini par rapport à la taille BRUTE non compressée de l'image
(largeur · hauteur · bandes · octets/échantillon) — convention JPEG2000 usuelle.
Ce n'est PAS le rapport au poids du TIF source (qui peut déjà être compressé).

Caractéristiques héritées de tif_convert.py :
  - parcourt récursivement un dossier d'entrée et recrée l'arborescence en sortie
  - reprend où il s'est arrêté (saute les fichiers déjà convertis)
  - isole les erreurs par fichier (un TIF corrompu n'interrompt pas le lot)
  - parallélise au niveau des fichiers, 1 thread libvips par worker (pas de sursouscription CPU)
  - réduction de résolution optionnelle par niveau (haut/moyen/bas), plancher 2000 px

Spécificités de la calibration :
  - dichotomie sur Q (bornes --qmin/--qmax, max --iter itérations) ; encodage en mémoire
    (jp2ksave_buffer) pour éviter N écritures disque ; une seule écriture finale
  - on garde le Q dont le ratio obtenu est le PLUS PROCHE de la cible (et non le dernier essayé)
  - arrêt anticipé dès que |ratio - cible| / cible <= --tol
  - l'image est matérialisée en mémoire après le resize (copy_memory) pour ne pas re-décoder
    le TIF à chaque itération : coût mémoire ≈ une image décodée par worker

Prérequis : voir tif_convert.py (libvips AVEC support OpenJPEG, ex. conda-forge pyvips).

Exemples :
  python tif_to_jp2_ratio.py /data/masters /data/diffusion --ratio 15
  python tif_to_jp2_ratio.py /data/masters /data/diffusion --ratio 15 --niveau bas --workers 8
  python tif_to_jp2_ratio.py /data/masters /data/diffusion --ratio 12 --tol 0.05 --qmin 20 --qmax 90
"""

import argparse
import csv
import logging
import os
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# Sous Windows, libvips est fournie comme DLL autonome : on l'ajoute au PATH et aux
# répertoires de recherche de DLL AVANT d'importer pyvips (cf. tif_convert.py).
if os.name == "nt":
    vips_bin = os.environ.get(
        "VIPS_BIN",
        r"C:\Users\pichenotf\vips-dev-x64-all-8.18.3\vips-dev-8.18\bin",
    )
    if os.path.isdir(vips_bin):
        os.environ["PATH"] = vips_bin + os.pathsep + os.environ["PATH"]
        os.add_dll_directory(vips_bin)
import pyvips

# 1 seul thread libvips par processus : on parallélise au niveau des fichiers.
os.environ.setdefault("VIPS_CONCURRENCY", "1")

# Extensions reconnues en entrée
TIFF_EXTS = {".tif", ".tiff", ".TIF", ".TIFF"}

# Taille de tuile : 512 est le défaut libvips, bon compromis pour le tuilage.
DEFAULT_TILE = 512

# Plancher de résolution : la largeur ne descend jamais sous cette valeur (en px) ;
# les images déjà plus petites ne sont jamais agrandies.
DEFAULT_PLANCHER = 2000

# Niveaux de réduction de résolution par corpus (cf. tif_convert.py / resolution_corpus.ipynb).
NIVEAUX = {"haut": 0.80, "moyen": 0.65, "bas": 0.50}

# Octets par échantillon selon le format pixel libvips (pour la taille brute non compressée).
FORMAT_BYTES = {
    "uchar": 1, "char": 1,
    "ushort": 2, "short": 2,
    "uint": 4, "int": 4, "float": 4,
    "double": 8, "complex": 8, "dpcomplex": 16,
}

# Cible par défaut : JP2 lossy ~1:15.
DEFAULT_RATIO = 15.0
DEFAULT_TOL = 0.10      # tolérance relative sur le ratio (10 %)
DEFAULT_QMIN = 1
DEFAULT_QMAX = 100
DEFAULT_ITER = 8        # ~8 itérations de dichotomie suffisent sur [1, 100]


def calibrate_jp2(image, target_ratio, tol, q_lo, q_hi, max_iter, tile):
    """Calibre Q par dichotomie pour viser `target_ratio` (ratio = brut / encodé).

    Encode en mémoire à chaque itération et retourne le meilleur candidat trouvé
    (le plus proche de la cible), sous forme d'un dict :
        {"q", "ratio", "size", "raw", "buf", "iters"}
    `buf` est le contenu JP2 prêt à écrire sur disque.
    """
    bps = FORMAT_BYTES.get(image.format, 1)
    raw = image.width * image.height * image.bands * bps

    lo, hi = q_lo, q_hi
    best = None
    iters = 0
    while lo <= hi and iters < max_iter:
        q = (lo + hi) // 2
        # subsample_mode laissé par défaut => pas de sous-échantillonnage chroma (4:4:4),
        # cohérent avec tif_convert.py.
        buf = image.jp2ksave_buffer(Q=q, tile_width=tile, tile_height=tile)
        size = len(buf)
        ratio = raw / size if size else float("inf")
        err = abs(ratio - target_ratio)
        iters += 1

        # On retient le candidat le PLUS PROCHE de la cible, pas le dernier essayé.
        if best is None or err < best["err"]:
            best = {"err": err, "q": q, "ratio": ratio, "size": size, "raw": raw, "buf": buf}

        if err / target_ratio <= tol:
            break
        if ratio > target_ratio:   # trop compressé (fichier trop petit) -> monter Q
            lo = q + 1
        else:                      # pas assez compressé -> baisser Q
            hi = q - 1

    best["iters"] = iters
    return best


def convert_one(src_str, dst_str, target_ratio, tol, q_lo, q_hi, max_iter, tile, facteur, plancher):
    """Convertit un fichier TIF -> JP2 calibré au ratio cible.

    Retourne un dict décrivant l'entrée/sortie (statut, message, Q retenu, ratio obtenu…).
    """
    import pyvips  # ré-import local (process pool)

    src = Path(src_str)
    dst = Path(dst_str)
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)

        src_size = src.stat().st_size
        src_format = src.suffix.lstrip(".").lower()

        # access="random" : la dichotomie ré-encode plusieurs fois la même image, ce qui
        # exclut le mode "sequential" (lecture unique en flux) de tif_convert.py.
        image = pyvips.Image.new_from_file(src_str, access="random")
        src_width, src_height = image.width, image.height

        # Réduction de résolution éventuelle (facteur < 1), avec plancher (cf. tif_convert.py).
        if facteur < 1.0:
            s = min(1.0, max(facteur, plancher / image.width))
            if s < 1.0:
                image = image.resize(s)

        # On matérialise les pixels en RAM une fois : les itérations de dichotomie encodent
        # alors depuis la mémoire au lieu de re-décoder le TIF à chaque passe.
        image = image.copy_memory()
        width, height = image.width, image.height

        best = calibrate_jp2(image, target_ratio, tol, q_lo, q_hi, max_iter, tile)

        # Une seule écriture disque : le meilleur buffer trouvé.
        dst.write_bytes(best["buf"])

        return {
            "src": src_str, "dst": dst_str, "status": "ok", "msg": "",
            "src_format": src_format, "src_size": src_size,
            "src_width": src_width, "src_height": src_height,
            "dst_width": width, "dst_height": height, "dst_size": dst.stat().st_size,
            "q": best["q"], "ratio": best["ratio"], "iters": best["iters"],
        }
    except Exception:  # noqa: BLE001 — on attrape tout pour ne pas tuer le lot
        return {"src": src_str, "dst": dst_str, "status": "error", "msg": traceback.format_exc()}


def check_jp2_support():
    """Vérifie que libvips sait écrire le JPEG2000. Retourne un message d'erreur, ou None si OK."""
    import pyvips
    if ".jp2" not in pyvips.get_suffixes():
        return (
            "Votre installation de libvips n'a PAS le support JPEG2000 (compilée sans OpenJPEG).\n"
            "  Solution : conda install -c conda-forge pyvips   (build incluant OpenJPEG)"
        )
    return None


def target_path(src, in_root, out_root, target_ratio, facteur, plancher):
    """Chemin de sortie en miroir de l'arborescence d'entrée.

    Le ratio cible est inséré dans le nom : page001.tif -> page001_r15.jp2
    Si une réduction de résolution est demandée, le facteur et le plancher le sont aussi :
        page001.tif -> page001_r15_f50_rmin2000.jp2
    (Le Q retenu varie d'une image à l'autre ; il est consigné dans le CSV, pas dans le nom.)
    """
    rel = src.relative_to(in_root)
    suffix = f"_r{round(target_ratio)}"
    if facteur < 1.0:
        suffix += f"_f{round(facteur * 100)}_rmin{plancher}"
    new_name = f"{rel.stem}{suffix}.jp2"
    return out_root / rel.parent / new_name


def build_jobs(in_root, out_root, target_ratio, facteur, plancher, overwrite):
    """Liste les conversions à faire (en sautant celles déjà présentes si overwrite=False)."""
    jobs, skipped = [], 0
    for src in sorted(in_root.rglob("*")):
        if not (src.is_file() and src.suffix in TIFF_EXTS):
            continue
        dst = target_path(src, in_root, out_root, target_ratio, facteur, plancher)
        if dst.exists() and not overwrite:
            skipped += 1
            continue
        jobs.append((str(src), str(dst)))
    return jobs, skipped


class _SimpleProgress:
    """Barre de progression minimale (repli quand tqdm n'est pas installé)."""

    def __init__(self, total, width=30):
        self.total = total
        self.width = width
        self.n = 0
        self.t0 = time.time()
        self._render()

    def _render(self):
        frac = self.n / self.total if self.total else 1.0
        filled = int(self.width * frac)
        bar = "#" * filled + "-" * (self.width - filled)
        dt = time.time() - self.t0
        rate = self.n / dt if dt else 0.0
        eta = (self.total - self.n) / rate if rate else 0.0
        sys.stdout.write(
            f"\r[{bar}] {frac * 100:5.1f}% {self.n}/{self.total} "
            f"{rate:5.2f} img/s ETA {eta:4.0f}s"
        )
        sys.stdout.flush()

    def update(self, k=1):
        self.n += k
        self._render()

    def close(self):
        sys.stdout.write("\n")
        sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(
        description="Conversion par lot TIF -> JP2 lossy à ratio de compression cible (Q calibré par image)."
    )
    parser.add_argument("input_dir", type=Path, help="dossier des TIF source")
    parser.add_argument("output_dir", type=Path, help="dossier de sortie (diffusion)")
    parser.add_argument("--ratio", type=float, default=DEFAULT_RATIO,
                        help=f"ratio de compression cible, brut/encodé (défaut : {DEFAULT_RATIO:g}, soit ~1:15)")
    parser.add_argument("--tol", type=float, default=DEFAULT_TOL,
                        help=f"tolérance relative sur le ratio, arrêt anticipé (défaut : {DEFAULT_TOL})")
    parser.add_argument("--qmin", type=int, default=DEFAULT_QMIN,
                        help=f"borne basse de Q pour la dichotomie (défaut : {DEFAULT_QMIN})")
    parser.add_argument("--qmax", type=int, default=DEFAULT_QMAX,
                        help=f"borne haute de Q pour la dichotomie (défaut : {DEFAULT_QMAX})")
    parser.add_argument("--iter", dest="max_iter", type=int, default=DEFAULT_ITER,
                        help=f"nombre max d'itérations de dichotomie par image (défaut : {DEFAULT_ITER})")
    parser.add_argument("--niveau", choices=sorted(NIVEAUX),
                        help="niveau de réduction de résolution par corpus : "
                             + ", ".join(f"{k} (f={v})" for k, v in NIVEAUX.items())
                             + " (défaut : aucune réduction)")
    parser.add_argument("--facteur", type=float,
                        help="facteur de réduction f explicite (0 < f <= 1) ; surcharge --niveau")
    parser.add_argument("--plancher", "--resolution-min", dest="plancher", type=int, default=DEFAULT_PLANCHER,
                        help=f"largeur minimale en px sous laquelle on ne réduit pas (défaut : {DEFAULT_PLANCHER})")
    parser.add_argument("--tile", type=int, default=DEFAULT_TILE,
                        help=f"taille de tuile en px (défaut : {DEFAULT_TILE})")
    parser.add_argument("--workers", type=int, default=os.cpu_count(),
                        help="nombre de processus parallèles (défaut : nb de cœurs)")
    parser.add_argument("--overwrite", action="store_true",
                        help="reconvertir même si le fichier de sortie existe")
    parser.add_argument("--log", type=Path, default=Path("tif_to_jp2_ratio.log"),
                        help="fichier journal (défaut : tif_to_jp2_ratio.log)")
    parser.add_argument("--csv", type=Path, default=None,
                        help="récapitulatif CSV des fichiers produits "
                             "(défaut : tif_to_jp2_ratio_AAAAMMJJ_HHMMSS_mmm.csv à la racine du dossier de sortie)")
    args = parser.parse_args()

    # Console Windows souvent en cp1252 : on force l'UTF-8 pour les messages accentués.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(args.log, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
    logging.getLogger("pyvips").setLevel(logging.WARNING)

    in_root = args.input_dir.resolve()
    out_root = args.output_dir.resolve()
    if not in_root.is_dir():
        logging.error("Dossier d'entrée introuvable : %s", in_root)
        sys.exit(1)

    if args.ratio <= 1.0:
        logging.error("Ratio cible invalide : %s (attendu > 1).", args.ratio)
        sys.exit(1)
    if not (1 <= args.qmin <= args.qmax <= 100):
        logging.error("Bornes Q invalides : qmin=%s qmax=%s (attendu 1 <= qmin <= qmax <= 100).",
                      args.qmin, args.qmax)
        sys.exit(1)

    if args.csv is None:
        now = datetime.now()
        stamp = now.strftime("%Y%m%d_%H%M%S_") + f"{now.microsecond // 1000:03d}"
        args.csv = out_root / f"tif_to_jp2_ratio_{stamp}.csv"

    # Facteur de réduction : --facteur l'emporte sur --niveau ; sinon 1.0 (aucune réduction).
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

    problem = check_jp2_support()
    if problem:
        logging.error(problem)
        sys.exit(1)

    logging.info("Cible : ratio de compression ~1:%g (tol %.0f %%), Q dans [%d, %d], max %d itérations.",
                 args.ratio, args.tol * 100, args.qmin, args.qmax, args.max_iter)

    jobs, skipped = build_jobs(in_root, out_root, args.ratio, facteur, args.plancher, args.overwrite)
    logging.info("%d fichier(s) à convertir, %d déjà présent(s) et sauté(s).", len(jobs), skipped)
    if not jobs:
        logging.info("Rien à faire.")
        return

    try:
        from tqdm import tqdm
        progress = tqdm(total=len(jobs), unit="img")
    except ImportError:
        progress = _SimpleProgress(len(jobs))

    ok = err = 0
    rows = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(convert_one, src, dst, args.ratio, args.tol, args.qmin, args.qmax,
                        args.max_iter, args.tile, facteur, args.plancher)
            for src, dst in jobs
        ]
        for fut in as_completed(futures):
            res = fut.result()
            if res["status"] == "ok":
                ok += 1
                rows.append({
                    "source": res["src"],
                    "source_format": res["src_format"],
                    "source_taille_octets": res["src_size"],
                    "source_largeur_px": res["src_width"],
                    "source_hauteur_px": res["src_height"],
                    "fichier": res["dst"],
                    "format": "jp2",
                    "ratio_cible": round(args.ratio, 2),
                    "q_retenu": res["q"],
                    "ratio_obtenu": round(res["ratio"], 2),
                    "iterations": res["iters"],
                    "taille_octets": res["dst_size"],
                    "largeur_px": res["dst_width"],
                    "hauteur_px": res["dst_height"],
                })
            else:
                err += 1
                logging.error("ECHEC %s :\n%s", res["src"], res["msg"])
            if progress:
                progress.update(1)

    if progress:
        progress.close()

    if rows:
        rows.sort(key=lambda r: r["fichier"])
        fieldnames = [
            "source", "source_format", "source_taille_octets",
            "source_largeur_px", "source_hauteur_px",
            "fichier", "format", "ratio_cible", "q_retenu", "ratio_obtenu", "iterations",
            "taille_octets", "largeur_px", "hauteur_px",
        ]
        with open(args.csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        logging.info("Récapitulatif CSV écrit : %s (%d ligne(s)).", args.csv, len(rows))

    dt = time.time() - t0
    logging.info("Terminé : %d réussis, %d en erreur, en %.1f s (%.2f img/s).",
                 ok, err, dt, ok / dt if dt else 0)
    if err:
        logging.warning("Des fichiers ont échoué — voir %s. Relancez : les réussites seront sautées.", args.log)
        sys.exit(2)


if __name__ == "__main__":
    main()
