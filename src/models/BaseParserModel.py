from bs4 import BeautifulSoup
import requests
from typing import Dict,Optional,Any
import os
import json

from .BaseDataModel import BaseDataModel
from .enums import ResponseSignal

class BaseParserModel(BaseDataModel):
    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        self.headers = self.app_settings.PARSER_HEADERS

    def set_checkpoint(self,name):
        self.checkpoint_name = name
        print(self.base_dir)
        self.checkpoint_file = os.path.join(self.base_dir,'assets/'+self.checkpoint_name)

    def fetch_content(self, url: str, custom_headers: Optional[Dict[str, str]] = None) -> str:
        """
        Fetch content from URL with error handling and rate limiting
        """
        try:
            headers = {**self.headers, **(custom_headers or {})}
            response = self.session.get(url, headers=headers)
            response.raise_for_status()
            return response.text #this is the html content
        except requests.exceptions.RequestException as e:
            # Log error using your logging configuration
            raise Exception(f"{ResponseSignal.CONTENT_FETCH_FAILED.value} : {str(e)}")

    def parse_html(self, html_content: str, parser_type: str = 'html.parser') -> BeautifulSoup:
        """
        Create BeautifulSoup object from HTML content
        """
        return BeautifulSoup(html_content, parser_type)
    
    def extract_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Override this method in specific parser implementations
        """
        raise NotImplementedError("Implement this method in specific parser class")
    
    def load_checkpoint(self):
        """Load previously saved progress if it exists."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}
        else :
            # Create the file
            with open(self.checkpoint_file, "w") as file:
                pass  # Do nothing, just create the empty file
            return {}
    
    def save_checkpoint(self,data:Dict[int,list[int]]):
        """Save current progress to checkpoint file."""
        with open(self.checkpoint_file, 'w') as f:
            json.dump(data, f,indent=4)