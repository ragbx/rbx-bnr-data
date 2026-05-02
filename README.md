# Métadonnées et fichiers de la bn-r

## But du dépôt

Ce dépôt a vocation à rassembler l'ensemble des travaux menés sur les métadonnées et fichiers de la bn-r.

Par métadonnées, on entend principalement les descriptions encodées en EAD.

Par fichiers, on entend les fichiers produits au cours des différentes campagnes de numérisation. 

Les objectifs, méthodes et résultats sont détaillés dans la partie [documentation](documentation/accueil.md).

## Organisation du dépôt :

Le dépôt contient 4 répertoires principaux :
- *data* : données sources utilisées dans les différents travaux,
- *scripts* : scripts qui permettent d'obtenir, manipuler ou analyser les différentes sources,
- *results* : ensemble des analyses ou des transformations effectuées sur les données sources :
- *documentation* : documentation du projet

## Aspects techniques
pour cloner le dépôt, il est nécessaire de disposer de [Git LFS](https://docs.github.com/fr/repositories/working-with-files/managing-large-files/installing-git-large-file-storage).

TODO : requirements

Pour lancer un script, se placer toujours à la racine du projet.

Par exemple :
```bash
bash scripts/ead/dao_extraction.sh
```
