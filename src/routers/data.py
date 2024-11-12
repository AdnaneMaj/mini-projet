from fastapi import APIRouter,Depends,Request
from helpers.config import get_settings,Settings
from models.NoaaParserModel import NoaaParserModel

data_router = APIRouter(
    prefix="/api/v1/data",
    tags = ["api_v1","data"]
)

noaa_parser = NoaaParserModel()

@data_router.post("/metadata")
async def metadata():
    #Get the meatadata
    noaa_parser.extract_metadata()