from bs4 import BeautifulSoup
import requests
from typing import Dict,Optional,Any

from .BaseDataModel import BaseDataModel
from .enums import ResponseSignal

class BaseParserModel(BaseDataModel):
    def __init__(self, hdfs_path_client):
        super().__init__(hdfs_path_client)
        self.session = requests.Session()
        self.headers = self.app_settings.PARSER_HEADERS

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
