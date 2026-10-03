# Schémas XSD

Copies locales, non modifiées, des schémas de la Library of Congress (téléchargées le 2026-10-03),
utilisées par `scripts/img/img3/presse_mets.py --validate`.

| Fichier | Source |
|---|---|
| `mets.xsd` | https://www.loc.gov/standards/mets/mets.xsd |
| `mods-3-8.xsd` | https://www.loc.gov/standards/mods/v3/mods-3-8.xsd |
| `mix.xsd` (MIX 2.0) | https://www.loc.gov/standards/mix/mix.xsd |
| `xlink.xsd` | http://www.loc.gov/standards/xlink/xlink.xsd (importé par METS et MODS ; http uniquement) |
| `xml.xsd` | http://www.loc.gov/mods/xml.xsd (importé par MODS) |

Les imports distants de `mets.xsd` et `mods-3-8.xsd` sont résolus vers ce dossier par le script.
