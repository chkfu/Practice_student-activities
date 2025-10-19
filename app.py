import os
from dotenv import load_dotenv
from core import DataLoader, SQLConnector
from core.config.paths import PATH_DATA_USER, PATH_DATA_ACTIVITY, PATH_DATA_COMPONENT


#  ENVIRONMENT

load_dotenv("config.env")
db_user = os.getenv("DB_USER")
db_pw= os.getenv("DB_PW")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT")


#  MAIN

def main():
  
  #  Data Loader
  
  data_loader = DataLoader()
  df_user = data_loader.import_dataset(path=PATH_DATA_USER)
  data_loader.convert_dataset(df_user, "JSON", "new_save")
  
  
  # SQL Connector
  
  sql_connector = SQLConnector(user=db_user, password=db_pw, database=db_name, host=db_host, port=db_port)
  sql_connector.initialise_database()
  sql_connector.initialise_tables("users")
  sql_connector.initialise_tables("activities")
  sql_connector.initialise_tables("components")
  # sql_connector.drop_database()
  # sql_connector.drop_tables("users")
  
  
#  OUTPUT

if __name__ == "__main__":
  main()