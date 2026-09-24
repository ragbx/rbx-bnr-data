from bnr.azrael import Azrael2list

"""

"""

root_path = "D:\Deuxième série"
az2list = Azrael2list(root_path=root_path, code_disk="ARCHIPOP")

az2list.list_files(checksum_md5=True)
az2list.save_list()
