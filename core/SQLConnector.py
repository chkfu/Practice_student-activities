import re
import mysql.connector
from mysql.connector import Error
from core.config.SQLCommands import SQL_COMMANDS


#  CLASS

class SQLConnector:
  
  #  Constructor
  
  def __init__(self, user, password, database, host, port):
      #  learnt: store config first for reuse
      self.config = {
          "user": user,
          "password": password,
          "host": host,
          "port": port
      }
      self.db_name = "student_activities"
      self.initialise_database()
      self.initialise_tables()


  #  METHODS - SQL OPERATION

  def connect_db(self, use_db=False):
      try:
          config = self.config.copy()
          if use_db:
              config["database"] = self.db_name
          connection = mysql.connector.connect(**config)  #  learnt: **config, for dict. unpacking
          cursor = connection.cursor()
          return connection, cursor
      except Error as err:
          print(f"[SQLConnector] Connection error: {err}")
          return None, None

 
  def terminate_db(self, connection, cursor):
      if cursor:
          cursor.close()
      if connection:
          connection.close()
          print("[SQLConnector] terminated MySQL connection.")
          

  #  learnt: the fn as wrappper, bringing simplicity with sql code adopted
  def execute_db(self, sql, db_initialised=False):
    #  learnt: extract connection and cursor
    #  connection
    connection, cursor = self.connect_db(db_initialised)
    if not connection:
        return
    #  execution
    try:
        cursor.execute(sql)
        connection.commit()
    except Error as ex:
        print(f"[SQLConnector] Execution error: {ex}")
    #  disconnection
    finally:
        self.terminate_db(connection, cursor)



  # METHODS - COMMAND-BASED (CRUD concepts)


  #  create methods


  def initialise_database(self, db_init=False):
      self.execute_db(SQL_COMMANDS["create_database"], db_initialised=db_init)
      print("[SQLConnector] Database is created.")


  def initialise_tables(self, db_init=True):
      self.execute_db(SQL_COMMANDS["create_table_users"], db_initialised=db_init)
      self.execute_db(SQL_COMMANDS["create_table_activities"], db_initialised=db_init)
      self.execute_db(SQL_COMMANDS["create_table_components"], db_initialised=db_init)
      print("[SQLConnector] Tables are created.")
      
      
  #  delete methods
  
  
  def drop_database(self, db_init=False):
      self.execute_db(SQL_COMMANDS["drop_database"], db_initialised=db_init)
      print("[SQLConnector] Database is dropped.")
      
      
  def drop_tables(self, table: str, db_init=True):
    
    table_r = re.sub(r'[^a-zA-Z]', "", table).lower()
    if table_r not in ["users", "activities", "components"]:
      raise ValueError(f"[SQLConnector] \"{table_r}\" is not valid.")
    
    if (table_r == "users"):
      self.execute_db(SQL_COMMANDS["drop_table_users"], db_initialised=db_init)
      print(f"[SQLConnector] drop_tables: \"{table_r}\" is dropped.")
    elif (table_r == "activities"):
      self.execute_db(SQL_COMMANDS["drop_table_activities"], db_initialised=db_init)
      print(f"[SQLConnector] drop_tables: \"{table_r}\" is dropped.")
    elif (table_r == "components"):
      self.execute_db(SQL_COMMANDS["drop_table_components"], db_initialised=db_init)
      print(f"[SQLConnector] drop_tables: \"{table_r}\" is dropped.")
    else:
      raise ValueError("[SQLConnector] drop_tables: table name does not match.")
    
