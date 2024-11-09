from .BaseParserModel import BaseParserModel

from bs4 import BeautifulSoup
from typing import Dict,Any

class NoaaParserModel(BaseParserModel):
    def __init__(self, hdfs_path_client:str):
        super().__init__(hdfs_path_client)

    #overrider extract_data method
    def extract_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Implement specific parsing logic for your target website
        """
        data = {
            'timestamp': datetime.now().isoformat(),
            # Add your specific extraction logic here
            # Example:
            # 'title': soup.find('h1', class_='main-title').text.strip(),
            # 'content': soup.find('div', class_='content').text.strip(),
        }
        return data
        