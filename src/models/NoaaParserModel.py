from .BaseParserModel import BaseParserModel
from .enums import Links

from typing import Dict
import requests

"""
--To-DO--

-verification:
    * Add year verification
"""

class NoaaParserModel(BaseParserModel):
    def __init__(self):
        super().__init__()
    
    def extract_metadata(self): #--TO-DO-- : The logic of the two download need to be separated
        """
        Metadata (json file):
            * list of year

        Country list :
            * List of codes of countries
        """
        ####Metadata
        metadata:Dict[str,list[int]] = {"years":[]} #initialise empty meta data dict

        content = self.fetch_content(Links.BASE_NOAA_LINK.value) #Fetch the content of the main page
        soup = self.parse_html(content) #get the soup
        links = soup.find_all('a') #find all links
        list_of_years = [int(link['href'][:-1]) for link in links if link['href'][:-1].isdigit()] #get only years

        metadata['years'] = list_of_years

        ####Country list
        url = Links.BASE_NOAA_LINK.value+Links.COUNTRY_LIST_PORT.value #Initialise url
        # Send a GET request to the URL
        response = requests.get(url)
        countries_encode = {}
        countries_decode = {}

        if response.status_code == 200:
            for line in response.text.splitlines()[2:]:
                code,country = line.split(maxsplit=1)
                countries_decode[code]=country.strip()
                countries_encode[country.strip()]=code
        countries = {"encoder":countries_encode,"decoder":countries_decode}

        #### Start-End data
        url = Links.BASE_NOAA_LINK.value+Links.ISD_HISTORY_PORT.value
        response = requests.get(url)
        lines = response.text.splitlines()
        start_index = lines.index('USAF   WBAN  STATION NAME                  CTRY ST CALL  LAT     LON      ELEV(M) BEGIN    END')
        ids_history = {}
        city_decode = {}
        city_encode = {}

        if response.status_code == 200:
            for line in lines[start_index+2:]:
                usaf,wban,station_name,country_code,start_year,end_year = line[:6],line[7:12],line[13:43],line[43:45],line[-17:-13],line[-8:-4]
                ids_history[usaf+'-'+wban]={"start":start_year,"end":end_year,"station":station_name.strip(),"country":country_code}
                city_decode[usaf+'-'+wban]=station_name.strip()
                city_encode[station_name.strip()]=usaf+'-'+wban
        cities = {"encoder":city_encode,"decoder":city_decode}

        self.save_json(self.metadata_file,metadata)
        self.save_json(self.countries_file,countries)
        self.save_json(self.ids_history_file,ids_history)
        self.save_json(self.cities_file,cities)

        #update the files
        self.countries = self.load_json(self.countries_file) #this is a dictionnary
        self.ids_history = self.load_json(self.ids_history_file)
        self.cities = self.load_json(self.cities_file)

    def extract_year_links(self,year:int):

        content = self.fetch_content(Links.BASE_NOAA_LINK.value+str(year)+'/')
        soup = self.parse_html(content) #get the soup
        links = soup.find_all('a') #find all links
        
        usaf_wban = [link['href'][:12] for link in links[5:]]
        return usaf_wban

    def extract_all_links(self,checkpoint_name = "data_checkpoint.json"):
        """
        Get all the links by year in a json file
        """

        # Load the JSON file into a Python dictionary
        metadata = self.load_json(self.metadata_file)

        self.set_checkpoint(checkpoint_name) #Set the checkpoint file path
        data:Dict[int,list[int]] = self.load_checkpoint() #initilaise data
        already_gotten_years = list(data.keys())
        print(already_gotten_years)

        #list of filed years
        failed_years = []

        #Get the list of years
        years_list = metadata['years']
        for year in years_list:
            #skip already scrapped years
            if str(year) in already_gotten_years:
                print(f"Year {year} skipped")
            else :
                try:
                    usaf_wban = self.extract_year_links(year)
                    data[year]=usaf_wban
                    # Save progress after each successful year
                    self.save_checkpoint(data)
                    print(f"Year {year} scraped succefult")
                except:
                    failed_years.append(year)
                    print(f"Year {year} failed to scrap")

        # Save the dictionary as a JSON file
        self.save_json(self.data_file,data)
        self.data = self.load_json(self.data_file)
        return failed_years
