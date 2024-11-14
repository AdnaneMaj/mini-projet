"""
--TO-DO--
* Add possibility to provide the name of hte city instead of the full number 
( We can suggest some similarity names to choose from )

"""

from .BaseDataModel import BaseDataModel
from .NoaaParserModel import NoaaParserModel
from .enums import Links

import requests
import gzip
import shutil
import subprocess
import os
import csv


class NooaDataModel(BaseDataModel):

    home_directory = os.path.expanduser("~")
    nooa_parser = NoaaParserModel()

    def __init__(self):
        super().__init__()

        #Create the NOAA folder and create it if none
        self.check_hdfs_folder_existence(Links.ROOT_DIR.value)

        #get data and metadata
        #NooaDataModel.nooa_parser.extract_all_links()
        #NooaDataModel.nooa_parser.extract_metadata()

    def check_local_folder_existence(self,local_path:str,create:bool=True):
        # Check if the folder exists
        existed = os.path.exists(local_path)
        if not existed and create:
            # Create the folder
            os.makedirs(local_path)
        
        return existed


    def create_hdfs_directory(self,folder_path:str=""):
        # Run the 'hadoop fs -mkdir' command to create the directory
        result = subprocess.run(self.app_settings.SUBPROCESS_CODE_BEGAN.split() + ['hdfs', 'dfs', '-mkdir', folder_path], 
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            return True
        else :
            raise ValueError(f"Failed to create the folder : {folder_path}")

    def check_hdfs_folder_existence(self,folder_path:str="",create:bool=True):
        # Run the 'hadoop fs -test -d' command to check for the directory
        result = subprocess.run(self.app_settings.SUBPROCESS_CODE_BEGAN.split() + ['hadoop', 'fs', '-test', '-d', folder_path], 
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Check the return code to determine if the directory existse
        if result.returncode !=0 and create:
            self.create_hdfs_directory(folder_path)

        return result.returncode == 0
    
    def empty_hdfs_folder(self,folder_path:str=""):
        result = subprocess.run(self.app_settings.SUBPROCESS_CODE_BEGAN.split()+['hdfs','dfs','-rm','-r','-skipTrash','/user/root/'+folder_path+'*'])
        if result.returncode!=0:
            raise ValueError(f'Failed to empty hdfs folder : {folder_path}')

    def empty_local_folder(self,folder_path:str,recreate:bool=True):
        # Empty the folder
        shutil.rmtree(folder_path)  # Delete the folder and its contents
        if recreate:
            os.makedirs(folder_path)    # Recreate the empty folde

    def download_text_from_link(self,file_name:str,url:str,path):
        """
        Downlad a text file from a link inside the correct folder locally
        """

        station_name = file_name[:-2]     

        # Path to save the compressed and extracted files
        gz_file_path = os.path.join(path,file_name)
        txt_file_path = os.path.join(path,station_name+'txt')

        #Links that failded to donwload
        missing = []

        # Download the .gz file
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(gz_file_path, "wb") as gz_file:
                gz_file.write(response.content)

            # Extract the .txt file
            with gzip.open(gz_file_path, "rb") as gz_file:
                with open(txt_file_path, "wb") as txt_file:
                    shutil.copyfileobj(gz_file, txt_file)

            # Delete the compressed file after extraction
            if os.path.exists(gz_file_path):
                os.remove(gz_file_path)
        else:
            missing.append(url)

    def download_in_shared_volume(self,station:str="029070-99999"):
        """
        Create a folder in the lcoal shared volume in for the specific station to put the files that needs to be moved to hdfs
        ( The assumption here that any found directory was created and preformed it's total task )
        """
        station_folder_path = os.path.join(NooaDataModel.home_directory, self.app_settings.DOCKER_SHARED_VOL_PATH,station)
        #check the folder existence and created if not, else quit ()
        if self.check_local_folder_existence(station_folder_path) or self.check_hdfs_folder_existence(Links.ROOT_DIR.value+station,create=False): #check if exist in NOAA
            return [],station_folder_path

        # URL of the API or file location
        url = Links.BASE_NOAA_LINK.value

        #Then get start and years and start downoaidng
        start_year,end_year = self.ids_history[station]['start'],self.ids_history[station]['end']

        #Get the links to downlaod for that specific station
        links_to_donwload = []
        for year in range(int(start_year),int(end_year)+1):
            file_name = station+"-"+str(year)+".gz"
            links_to_donwload.append((file_name,url+str(year)+"/"+file_name))

        
        #Use request to donwload in the shared volume in order to move later to hdfs
        for name,link in links_to_donwload:
            missing = self.download_text_from_link(name,link,station_folder_path)

        return missing,station_folder_path
    
    def move_to_hdfs(self,station:str):
        """
        The content of shared volume will be moved to HDFS and delted after
        ( Here I'm making the assumption that shared volume contains only what I need )
        """

        #subprocess to move teh station folder to hdfs
        result = subprocess.run(self.app_settings.SUBPROCESS_CODE_BEGAN.split()+['hdfs','dfs','-put','/shared_volume/'+station,Links.ROOT_DIR.value+station])
        if result.returncode == 0:
            return True
        else :
            raise ValueError(f"Failed to move : {station} to hdfs")

    def download_to_hdfs(self,station:str):
        """
        Downlaod to shared volume and then move to HDFS
        """
        #Donwload locally first
        missing,station_folder_path = self.download_in_shared_volume(station)
        if missing:
            print(f"The following links didn't succed to download {missing} ")
        
        #Move the created folder to hdfs
        moved_succefully = self.move_to_hdfs(station)
        if moved_succefully:
            #Delete the folder locally if succefully transmited to hdfs
            self.empty_local_folder(station_folder_path,recreate=False)

        return moved_succefully
    
    #__________________________________________________________________________________________ MapReduce
    def perform_job(self,station:str):
        """
        Perform the map reduce job 
        """
        #Create folder reducing_results if it dose not exist
        if self.check_hdfs_folder_existence(Links.ROOT_DIR.value+"reducing_results/"):
            return

        #hadoop jar /shared_volume/FirstJob.jar mini_project_first.FirstJob NOAA/601400-99999 NOAA/try
        result = subprocess.run(self.app_settings.SUBPROCESS_CODE_BEGAN.split()+["hadoop","jar","/shared_volume/"+self.app_settings.JAR_NAME,self.app_settings.PACKAGE_NAME+"."+self.app_settings.JOB_CLASS_NAME,Links.ROOT_DIR.value+station,Links.ROOT_DIR.value+"reducing_results/"+station])

        if result.returncode == 0:
            print("ggg")
        else:
            raise ValueError(f'Failed to perform map-reduce job on : {station}')
    
        return
    
    #_______________________________________________________________________________________________Fetch data for visualisation (using webhdfs)
    def txt_to_csv(self):
        """
        Result.txt file to a csv file
        """

        # Input and output file paths
        input_file = os.path.join(self.base_dir,'assets','result.txt')
        output_file = os.path.join(self.base_dir,'assets','result.csv')

        # Open the input text file and output CSV file
        with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
            csv_writer = csv.writer(outfile)
            
            # Write the header row
            csv_writer.writerow(['year', 'mean'])
            
            # Process each line in the text file
            for line in infile:
                # Split the line into year and mean parts
                year, mean = line.split('\t')  # Split by tab
                mean_value = mean.split(':')[1].strip()  # Extract the mean value
                # Write the year and mean to the CSV file
                csv_writer.writerow([year, mean_value])

        print(f"Data successfully written to {output_file}")


    def fetch_data(self,station:str):
        """
        Fetch Data from HDFS using Webhdfs
        
        #set the API
        webhdfs_url = f'{self.app_settings.NAME_NODE}:{self.app_settings.PORT}/webhdfs/v1/user/root/{Links.ROOT_DIR.value}reducing_results/{station}/part-r-00000?op=OPEN'

        # Output CSV file path
        output_csv = os.path.join(self.base_dir,'assets','data_to_vis.csv')

        # Fetch the text file from WebHDFS
        response = requests.get(webhdfs_url)

        # Check if the request was successful
        if response.status_code == 200:
            lines = response.text.splitlines()  # Split the file into lines

            # Open the output CSV file and write the data
            with open(output_csv, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(["Year", "Mean"])  # Write header

                for line in lines:
                    year, mean = line.strip().split("\t")  # Split year and mean
                    mean_value = mean.split(":")[1].strip()  # Extract mean value after "mean:"
                    writer.writerow([year, mean_value])  # Write row
        else:
            raise ValueError(f"Failed to fetch the file. Status code: {response.status_code}")
        """

        """
        Using subprocess in a very stupid naif way
        """

        # Example command
        result = subprocess.run(self.app_settings.SUBPROCESS_CODE_BEGAN.split() + ['hdfs','dfs','-get',f'/user/root/{Links.ROOT_DIR.value}reducing_results/{station}/part-r-00000','/shared_volume/result.txt'])

        # Source and destination paths
        source = os.path.join(NooaDataModel.home_directory,self.app_settings.DOCKER_SHARED_VOL_PATH,'result.txt')
        destination = os.path.join(self.base_dir,'assets','result.txt')
        csv_destination = os.path.join(self.base_dir,'assets','result.csv')

        # Move the file to assets
        shutil.move(source, destination)

        #Transform to CSV file of speicifc columns
        self.txt_to_csv(destination,csv_destination)


