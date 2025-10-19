from core import DataLoader
from core.config.paths import PATH_DATA_USER, PATH_DATA_ACTIVITY, PATH_DATA_COMPONENT


#  MAIN

def main():
  
  #  Data Loader
  
  data_loader = DataLoader()
  df_user = data_loader.import_dataset(path=PATH_DATA_USER)
  data_loader.convert_dataset(df_user, "JSON", "new_save")
  
  
#  OUTPUT

if __name__ == "__main__":
  main()