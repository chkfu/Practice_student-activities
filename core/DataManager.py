import pandas as pd


class DataManager:
  
  def __init__(self):
    self.col_name_list = None
    print("[DataManager] initialised successfully.")
    
    
  #  METHOD - BASIC
  
  def print_df(self, target_df: pd.DataFrame):
    if not isinstance(target_df, pd.DataFrame):
      raise TypeError("[DataManager] target dataframe must be a pandas DataFrame.")
    print(target_df)
   
    
    
  #  METHOD - REUSE
  
  def validate_col(self, target_df: pd.DataFrame, target_col: str) -> str:
      output = next((col for col in target_df.columns if col.strip().lower() == target_col.strip().lower()), None)
      if output is None:
        raise ValueError("[DataManager] target column is not found.")
      return output
    
  #  METHOD - CRUD
  
  def remove_col(self, target_df: pd.DataFrame, target_col: str) -> pd.DataFrame:
   
    #  validate types  
    if not isinstance(target_df, pd.DataFrame):
      raise TypeError("[DataManager] target dataframe must be a pandas DataFrame.")
    if not isinstance(target_col, str):
      raise TypeError("[DataManager] target column must be a string.")
    
    #  validate column
    valid_col = self.validate_col(target_df=target_df, target_col=target_col)
    
    #  output
    output = target_df.drop(columns=[valid_col])
    print(f"[DataManager] target column has been removed.")
    return output
    
    
  def remove_rows(self, target_df: pd.DataFrame, target_col: str, target_rows:list) -> pd.DataFrame:
    
    #  validate types
    if not isinstance(target_df, pd.DataFrame):
      raise TypeError("[DataManager] target dataframe must be a pandas DataFrame.")
    if not isinstance(target_col, str):
      raise TypeError("[DataManager] target column must be a string.")
    if not isinstance(target_rows, list):
      raise TypeError("[DataManager] target input must be a list of strings.")
    
    #  validate column
    valid_col = self.validate_col(target_df=target_df, target_col=target_col)
    
    #  validate row
    rows_removal: list = [el.strip().lower() for el in target_rows]
    matched_list: list = target_df[valid_col].isin(rows_removal)  # isin() extracts rows with matched criteria
    
    #  output
    output = target_df[~matched_list]   # learnt: ~ sign as NOT operator
    print(f"[DataManager] target row(s) has/have been removed.")
    return output
  
      
    
    
    
    
    
    
    
