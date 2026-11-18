"""Zoom automatique sur le point saisi dans lieu_manuel (2026-11-15). Python de QGIS :

    python3 scripts/ead/qgis_zoom_lieu_manuel.py data/geo/validation_sites_sans_point_20261114.qgz

Ajoute au projet donné (copie de sauvegarde .backup_<date>_avant_zoom.qgz faite avant) :
  - une macro de projet : quand le champ lieu_manuel d'une couche reçoit un point WKT
    (« POINT (lon lat) », WGS84, toutes casses), la carte se centre dessus au 1:ECHELLE et le point
    clignote. La macro ne tourne que si QGIS l'autorise (question à l'ouverture du projet, ou
    Préférences > Options > Général > Activer les macros) ;
  - une action « Zoomer sur le point manuel » sur chaque couche qui a un champ lieu_manuel
    (clic droit sur l'entité > Actions, ou bouton Actions du formulaire), utilisable sans macros.

Ni le GeoPackage ni les saisies ne sont touchés ; seul le .qgz est réécrit.
`qgis_validation_sites_sans_point.py` appelle aussi ajouter_zoom() pour les projets qu'il génère.
"""
import os
import shutil
import sys
from datetime import date

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from qgis.core import Qgis, QgsAction, QgsApplication, QgsProject, QgsVectorLayer  # noqa: E402

CHAMP = "lieu_manuel"
ECHELLE = 2500
NOM_ACTION = "Zoomer sur le point manuel"

MACRO = f'''import re
from qgis.core import QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsGeometry, QgsPointXY, QgsProject, QgsVectorLayer
from qgis.utils import iface

_POINT = re.compile(r"^\\s*point\\s*\\(\\s*([-\\d.]+)\\s+([-\\d.]+)\\s*\\)\\s*$", re.I)
_LIENS = []


def _zoom(couche, idx, valeur):
    if couche.fields().at(idx).name() != "{CHAMP}":
        return
    m = _POINT.match(str(valeur or ""))
    if not m:
        return
    canvas = iface.mapCanvas()
    crs = canvas.mapSettings().destinationCrs()
    tr = QgsCoordinateTransform(QgsCoordinateReferenceSystem("EPSG:4326"), crs, QgsProject.instance())
    pt = tr.transform(QgsPointXY(float(m.group(1)), float(m.group(2))))
    canvas.setCenter(pt)
    canvas.zoomScale({ECHELLE})
    canvas.flashGeometries([QgsGeometry.fromPointXY(pt)], crs)


def openProject():
    for couche in QgsProject.instance().mapLayers().values():
        if isinstance(couche, QgsVectorLayer) and couche.fields().indexOf("{CHAMP}") >= 0:
            f = lambda fid, idx, v, c=couche: _zoom(c, idx, v)
            couche.attributeValueChanged.connect(f)
            _LIENS.append((couche, f))


def saveProject():
    pass


def closeProject():
    for couche, f in _LIENS:
        try:
            couche.attributeValueChanged.disconnect(f)
        except (TypeError, RuntimeError):
            pass
    _LIENS.clear()
'''

ACTION = f'''import re
from qgis.core import QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsGeometry, QgsPointXY, QgsProject
from qgis.utils import iface
m = re.match(r"^\\s*point\\s*\\(\\s*([-\\d.]+)\\s+([-\\d.]+)\\s*\\)\\s*$", """[% "{CHAMP}" %]""", re.I)
if m:
    canvas = iface.mapCanvas()
    crs = canvas.mapSettings().destinationCrs()
    tr = QgsCoordinateTransform(QgsCoordinateReferenceSystem("EPSG:4326"), crs, QgsProject.instance())
    pt = tr.transform(QgsPointXY(float(m.group(1)), float(m.group(2))))
    canvas.setCenter(pt)
    canvas.zoomScale({ECHELLE})
    canvas.flashGeometries([QgsGeometry.fromPointXY(pt)], crs)
else:
    iface.messageBar().pushWarning("{CHAMP}", "pas de point WKT « POINT (lon lat) » dans {CHAMP}")
'''


def ajouter_zoom(projet):
    """Macro + action sur le projet QGIS ouvert (QgsProject) ; renvoie les couches équipées."""
    projet.writeEntry("Macros", "/pythonCode", MACRO)
    equipees = []
    for couche in projet.mapLayers().values():
        if not isinstance(couche, QgsVectorLayer) or couche.fields().indexOf(CHAMP) < 0:
            continue
        am = couche.actions()
        for a in am.actions():
            if a.name() == NOM_ACTION:
                am.removeAction(a.id())
        am.addAction(QgsAction(Qgis.AttributeActionType.GenericPython, NOM_ACTION, ACTION, "", False, "",
                               {"Feature", "Field"}))
        equipees.append(couche.name())
    return equipees


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    chemin = os.path.abspath(sys.argv[1])
    base, ext = os.path.splitext(chemin)
    sauvegarde = f"{base}.backup_{date.today():%Y%m%d}_avant_zoom{ext}"
    if not os.path.exists(sauvegarde):
        shutil.copy2(chemin, sauvegarde)
    app = QgsApplication([], False)
    app.initQgis()
    p = QgsProject.instance()
    if not p.read(chemin):
        sys.exit(f"projet illisible : {chemin}")
    equipees = ajouter_zoom(p)
    if not equipees:
        sys.exit(f"aucune couche avec un champ {CHAMP} : projet non modifié")
    if not p.write(chemin):
        sys.exit("écriture du projet impossible")
    print(f"sauvegarde → {sauvegarde}")
    print(f"macro + action « {NOM_ACTION} » → {', '.join(equipees)}")
    app.exitQgis()


if __name__ == "__main__":
    main()
