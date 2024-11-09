"""
--TO-DO--
* Add posibility of working with client isntead
* Add possibility to altern between devlopmeent node nad production node in get_client 

--verification
* verifiy the path (existence of the file, but foa check if it's already provided by hdf library) 
"""

from helpers.config import get_settings

from hdfs import Config

class BaseDataModel:
    def __init__(self):
        self.app_settings = get_settings()
        self.client = Config(self.app_settings.HDFS_CONFIG_FILE_PATH).get_client() #For the moment it will be set to default  