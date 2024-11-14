from pydantic_settings import BaseSettings
from typing import Dict,List

class Settings(BaseSettings):

    APP_NAME: str
    APP_VERSION: str
    HDFS_CONFIG_FILE_PATH: str
    PARSER_HEADERS: Dict[str,str]
    SUBPROCESS_CODE_BEGAN: str
    DOCKER_SHARED_VOL_PATH: str
    JAR_NAME: str
    PACKAGE_NAME: str
    JOB_CLASS_NAME: str

    class Config:
        env_file = ".env"

def get_settings():
    return Settings()