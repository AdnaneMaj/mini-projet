"""
--TO-DO--
* Before initialiazinz file create empty one first
"""

from helpers.config import get_settings

from hdfs import Config,InsecureClient
import os
import json

class BaseDataModel:

    def __init__(self):
        self.app_settings = get_settings()
        self.client:InsecureClient = Config(self.app_settings.HDFS_CONFIG_FILE_PATH).get_client() #Connect to WebHDFS 
        self.base_dir = os.path.dirname(os.path.dirname(__file__))
        self.metadata_file = os.path.join(
            self.base_dir,
            "assets/metadata.json"
        )
        self.countries_file = os.path.join(
            self.base_dir,
            "assets/countries.json"
        )
        self.cities_file = os.path.join(
            self.base_dir,
            "assets/cities.json"
        )
        self.ids_history_file = os.path.join(
            self.base_dir,
            "assets/ids_history.json"
        )
        self.data_file = os.path.join(
            self.base_dir,
            "assets/data.json"
        )  
        self.root_dir_name = "nooa_dir/"

        self.countries = None
        self.ids_history = None
        self.cities = None

    def save_json(self,file_path,metadata):
        # Save the dictionary as a JSON file
        with open(file_path, "w") as json_file:
            json.dump(metadata, json_file, indent=4)  # `indent=4` makes it pretty-printed

    def load_json(self,file_path):
        # Load the JSON file into a Python dictionary
        with open(file_path, "r") as json_file:     ##########" Handle errors"
            data = json.load(json_file)
        
        return data