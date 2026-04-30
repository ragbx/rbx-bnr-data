# Données de la bn-r

Ce dépôt a vocation à rassembler l'ensemble des données et métadonnées liées à la bn-r.

On dispose de trois ensembles de sources :
- les fichiers issus des différentes campagnes de numérisation,
- les instruments de recherche, encodés en EAD pour le moment dans deux logiciels différents (ead.bn-r.fr et Mnesys),
- le serveur OAI-PMH de la bn-r, qui permet de récupérer l'ensemble des documents publiés sur la bn-r.

## Organisation du dépôt :

### data
Stockage des différentes sources de données :
- az : liste des fichiers présents sur le serveur historique bnr (dit "Azraël"))
- s3 : liste des fichiers présents sur le serveur s3
- ead : instruments de recherches en xml EAD, un répertoire pour chacune des sources
- oai : ensemble des notices bnr au format csv.

### scripts
Scripts qui permettent d'obtenir, manipuler ou analyser les différentes sources

### results
Répertoire qui comprend l'ensemble des analyses ou des transformations effectuées sur les données sources :
- ref : fichier de référence cd ../

## doc
Répertoire qui comprend la documentation du projet
