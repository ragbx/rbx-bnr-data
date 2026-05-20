from bnr.azrael import Azrael2jhove_files
from os.path import join

"""

"""

data_folder = 'data'
azrael = Azrael2exiftool_files()
date_extraction = "20240611"
azrael.create_az(path_az=join(data_folder, f"bnr_azrael_{date_extraction}_detailed.csv.gz", date_extraction=date_extraction),
    path_prefix="/home/kibini/bnr/",
    min_jhove_chunk=1,
    max_jhove_chunk=100)
azrael.exiftool_proc()
