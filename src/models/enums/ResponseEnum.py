from enum import Enum

class ResponseSignal(Enum):
    
    CONTENT_FETCH_FAILED = 'Failed to fetch content'
    TIME_OUT_ERROR = "Time out error : Took so much to fetch data with failure in the end"
    FAILED_TO_CREATE_FOLDER = "Failed to create folder"
    DIR_DOES_NOT_EXIST = "The provided directory does not exist"
