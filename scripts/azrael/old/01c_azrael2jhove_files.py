from bnr.azrael import Azrael2jhove_files
from os.path import join

"""

"""

data_folder = 'data'
azrael = Azrael2jhove_files()
azrael.create_az(path_az=join(data_folder, "bnr_azrael_20240611_detailed.csv.gz"),
    path_prefix="/home/kibini/bnr/",
    min_jhove_chunk=1688,
    max_jhove_chunk=2235)
azrael.jhove_proc()
