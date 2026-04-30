from bnr.azrael import Azrael2list

"""

"""

root_path = "/home/kibini/bnr"
az2list = Azrael2list(root_path=root_path, code_disk=None)

az2list.list_files(checksum_md5=False)
az2list.save_list()
