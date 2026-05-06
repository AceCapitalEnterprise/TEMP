from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # csv_folder_path: str = "/Users/sahilkapoor/SahilData/Workspace/python/strategy-engine/"
    # csv_folder_path: str = "/home/ubuntu/csv/strategy-engine/" #for server
    csv_folder_path: str ="/home/ace/Desktop/strategy-engine/"
    db_path: str = "database.db"


settings = Settings()

