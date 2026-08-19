"""Utilitaires partages entre download.py et convert.py."""


def relkey(row: dict) -> str:
    """Chemin relatif (calque sur la cle S3) sous lequel un fichier est range
    en conservation/ et en diffusion/.

    Utilise s3_key si present ; sinon reconstruit <prefixe>/<corpus_code>/<name>
    a partir de corpus_code (cas des fichiers non deposes sur S3, ex. MED_PLA).
    """
    s3_key = str(row.get("s3_key") or "").strip()
    if s3_key and s3_key.lower() != "nan":
        return s3_key
    code = str(row["corpus_code"])
    return f"{code.split('_')[0]}/{code}/{row['name']}"
