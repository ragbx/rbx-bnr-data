# Script : tif_convert.py

**Emplacement :** `scripts/img/tif_convert.py`

Conversion par lot de TIFF vers **JP2** (JPEG 2000), **JPEG**, ou TIFF pyramidal
en repli, pour produire les images de diffusion web (voir la
[chaîne images de diffusion](images_diffusion.md)). Le script parcourt
récursivement un dossier d'entrée et recrée la même arborescence en sortie. Il
peut au passage **réduire la résolution** des images selon le corpus (voir plus
bas).

---

## Environnement

À lancer avec l'environnement unifié du projet **`conda run -n rbx-bnr-data`** :
il dispose d'une `libvips` compilée avec le support **JPEG2000**. Le script vérifie
ce support au démarrage et s'arrête avec un message explicite si `.jp2` n'est pas
disponible (replier alors sur `--format ptiff`).

Sous Windows, la `libvips` autonome est cherchée dans le dossier indiqué par la
variable d'environnement `VIPS_BIN` (sinon un chemin par défaut) ; ce bloc est
ignoré hors Windows.

---

## Entrée / sortie

- **Entrée** : un dossier de TIFF (`.tif`, `.tiff`), parcouru récursivement.
- **Sortie** : l'arborescence d'entrée reproduite dans le dossier de sortie.
  L'extension dépend du format (`.jp2`, `.jpg`, `.tif`). Le facteur de qualité est
  inséré dans le nom : `page001.tif` → `page001_q60.jp2` ; si une réduction de
  résolution est demandée, le facteur s'ajoute aussi : `page001_q80_f65.jpg`. Des
  conversions à des qualités ou des niveaux différents ne s'écrasent donc pas.
- **Journal** : `tif_convert.log` (configurable via `--log`).

---

## Utilisation

Depuis la racine du projet :

    conda run -n rbx-bnr-data python scripts/img/tif_convert.py <dossier_entree> <dossier_sortie> [options]

| Argument / option | Description |
|---|---|
| `input_dir` | Dossier des TIFF source (positionnel, obligatoire). |
| `output_dir` | Dossier de sortie (positionnel, obligatoire). |
| `--format` | `jp2` (défaut), `jpeg`, ou `ptiff` (TIFF pyramidal, repli sans JPEG2000). |
| `--quality` | Facteur Q (défaut : 60 ; baisser pour compresser davantage). |
| `--niveau` | Niveau de réduction de résolution par corpus : `haut` (f=0,80), `moyen` (f=0,65), `bas` (f=0,50). Défaut : aucune réduction. |
| `--facteur` | Facteur de réduction `f` explicite (0 < f ≤ 1) ; surcharge `--niveau`. |
| `--plancher` | Largeur minimale en px sous laquelle on ne réduit pas (défaut : 2000). |
| `--tile` | Taille de tuile en pixels (défaut : 512). |
| `--workers` | Nombre de processus parallèles (défaut : nombre de cœurs). |
| `--overwrite` | Reconvertir même si le fichier de sortie existe déjà. |
| `--log` | Fichier journal (défaut : `tif_convert.log`). |

Exemples :

    conda run -n rbx-bnr-data python scripts/img/tif_convert.py masters/ diffusion/ --format jp2 --quality 50 --workers 8
    conda run -n rbx-bnr-data python scripts/img/tif_convert.py masters/ diffusion/ --format jpeg --quality 80 --niveau moyen

---

## Comportement

- **Reprenable** : par défaut, les fichiers déjà convertis (sortie présente) sont
  sautés ; `--overwrite` force la reconversion.
- **Robuste** : les erreurs sont isolées par fichier (un TIFF corrompu
  n'interrompt pas le lot) et tracées dans le journal. En cas d'échec, relancer
  reprend là où le lot s'était arrêté.
- **Parallélisme** : un processus par fichier (`ProcessPoolExecutor`), avec un
  seul thread `libvips` par processus (`VIPS_CONCURRENCY = 1`) pour ne pas
  sursouscrire le CPU. Lecture en flux (`access="sequential"`) : faible empreinte
  mémoire même sur de très grandes images.
- En sortie, un bilan donne le nombre de réussites, d'erreurs et le débit
  (images/s). Code de sortie `2` si au moins un fichier a échoué.

---

## Réduction de résolution

Activée par `--niveau` (ou `--facteur`), elle réduit chaque image avant la
conversion. Calibrage validé par corpus dans `resolution_corpus.ipynb` :

| Niveau | Facteur `f` | Corpus visé |
|---|---|---|
| `haut` | 0,80 | Manuscrits / plans (détails fins, source 300 dpi) |
| `moyen` | 0,65 | Iconographie (source 300 dpi) |
| `bas` | 0,50 | Presse (texte, source 400 dpi) |

Pour une image de largeur `w`, le facteur d'échelle appliqué est :

    s = min(1, max(f, plancher / w))

- Le **plancher** (défaut 2000 px) garantit que la largeur ne descend jamais sous
  cette valeur, même pour les corpus au facteur le plus agressif.
- Les images déjà plus petites que le plancher ne sont **jamais agrandies**
  (`s = 1`).
- La résolution (DPI) est réduite dans le même rapport que les pixels.

Sans `--niveau` ni `--facteur`, aucune réduction n'est appliquée (`f = 1,0`) :
le comportement par défaut reste une simple conversion de format.

---

## Tests de paramètres

Pour comparer le compromis poids / qualité avant de figer les réglages, le lanceur
[run_tif_convert.sh](run_tif_convert.md) appelle ce script en boucle sur plusieurs
qualités et seuils de résolution, par type de document.
