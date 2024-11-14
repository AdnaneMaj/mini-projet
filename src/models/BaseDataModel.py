"""
--TO-DO--
* Before initialiazinz file create empty one first
"""

from helpers.config import get_settings

import os
import json

class BaseDataModel:

    def __init__(self):
        self.app_settings = get_settings()
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
        self.subprocess_begin = ['docker','exec','-it','hadoop-master','bash']
        
        self.set_data()

    def set_data(self):
        self.countries = self.load_json(self.countries_file)
        self.ids_history = self.load_json(self.ids_history_file)
        self.cities = self.load_json(self.cities_file)
        self.data = self.load_json(self.data_file)
    
    def save_json(self,file_path,metadata):
        # Save the dictionary as a JSON file
        with open(file_path, "w") as json_file:
            json.dump(metadata, json_file, indent=4)  # `indent=4` makes it pretty-printed

    def load_json(self,file_path):
        # Load the JSON file into a Python dictionary
        try:
            with open(file_path, "r") as json_file:     ##########" Handle errors"
                data = json.load(json_file)
        except FileNotFoundError:
            return
            
        return data