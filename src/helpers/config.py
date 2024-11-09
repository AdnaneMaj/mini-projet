from pydantic_settings import BaseSettings

class Settings(BaseSettings):

    class Config:
        env_file = "..env"

def get_settings():
    return Settings()