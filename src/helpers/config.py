from pydantic_settings import BaseSettings
from typing import Dict,List

class Settings(BaseSettings):

    APP_NAME: str
    APP_VERSION: str
    HDFS_CONFIG_FILE_PATH: str
    PARSER_HEADERS: Dict[str,str]
    SUBPROCESS_CODE_BEGAN: List[str]

    class Config:
        env_file = ".env"

def get_settings():
    return Settings()