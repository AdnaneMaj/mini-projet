"""
--TO-DO--
* Add possibility to provide the name of hte city

"""

from .BaseDataModel import BaseDataModel
import requests
import subprocess

class NooaDataModel(BaseDataModel):
    def __init__(self):
        super().__init__()

    def check_folder_existence(self):
        # Run the 'hadoop fs -test -d' command to check for the directory
        result = subprocess.run(self.app_settings.SU ['hadoop', 'fs', '-test', '-d', hdfs_path], 
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Check the return code to determine if the directory exists
        if result.returncode == 0:
            print(f"The directory {hdfs_path} exists in HDFS.")
        else:
            print(f"The directory {hdfs_path} does not exist in HDFS.")


    def upload_to_shared_volume(self,station:str):
        pass

    --------------------
    
    def check_folder_existance(self,hdfs_path:str="/user/root/",folder_name:str=None,create_if_not:bool=True)->bool:
        exist = folder_name in self.client.list(hdfs_path=hdfs_path)

        #Create the folder if it dose not exist
        if not exist and create_if_not:
            self.client.makedirs(hdfs_path=hdfs_path+folder_name)

        return exist
    
    def upload_text_file(self,hdfs_target_dir:str,txt_filename:str,txt_content:str):

        # Upload txt to HDFS as a text file
        with self.client.write(f"{hdfs_target_dir}/{txt_filename}", encoding='utf-8') as hdfs_file:
            hdfs_file.write(txt_content)

    def download_in_hdfs(self,station:str):

        #foa, our root dir :
        self.check_folder_existance(folder_name=self.root_dir_name+station)

        #Then get start and years and start downoaidng
        start_year,end_year = list(self.ids_history[station].values())[:2]

        for year in range(int(start_year),int(end_year)+1):
            pass
            


