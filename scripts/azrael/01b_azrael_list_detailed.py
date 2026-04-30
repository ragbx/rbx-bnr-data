from bnr.azrael import Azrael2analysis, convert_size
from os.path import join

"""

"""

data_folder = 'data'
date = "20251223"
azrael = Azrael2analysis()
azrael.create_az(path_az=join(data_folder, f"bnr_azrael_{date}.csv.gz"))#, path_prefix='../bnr/')
azrael.add_bnr_file_id()
azrael.split_path(n=4)
azrael.dates2dt()
azrael.az['size_mo'] = azrael.az['size'].apply(lambda x : convert_size(x, from_size='o', to_size='mo'))
azrael.get_extension_mimetype()
#azrael.get_jhove_chunk()
azrael.save_az(join(data_folder, f"bnr_azrael_{date}_detailed.csv.gz"))
