#! /usr/bin/python
import ConfigParser
import psycopg2

def create_connection():
  """
  Create PostgreSQL connection given configuration file.
  """
  config = ConfigParser.ConfigParser()
  config.read("config.ini")
  localhost = config.get('postgresql','localhost')
  database = config.get('postgresql','database')
  username = config.get('postgresql','username')
  password = config.get('postgresql','password')
  port = config.get('postgresql', 'port')
  conn_string = "host='" + localhost + "' port='" + port + "' dbname='" + database + "' user='" + username + "' password='" + password + "'"
  print("Connecting to database...")
  conn = psycopg2.connect(conn_string)
  print("Connection established!")
  return(conn)


def create_staging_schema(cursor,table_schema):
  """
  Create schema if it doesn't exist, then execute on cursor object.
  """
  create_schema = "CREATE SCHEMA IF NOT EXISTS " + table_schema + ";"
  cursor.execute(create_schema)


def create_staging_table(cursor,table_schema,table_name,column_names):
  """
  Create staging table with column names of data type text if it 
  doesn't exist, then execute on cursor object. Replace TO with TOV as TO is 
  a Python reserved word.
  """
  if not column_names:
    create_table = "CREATE TABLE IF NOT EXISTS " + table_schema + "." + table_name + "();"
  else:
    create_table = "CREATE TABLE IF NOT EXISTS " + table_schema + "." + table_name + " (" + " text,".join(column_names) + " text);"
    create_table = create_table.replace(",TO ",",TOV ")
  cursor.execute(create_table)


def insert_records(cursor,table_schema,table_name,column_names,records, checkCols = True):
  """
  Insert multiple records with one INSERT statement into table, then execute 
  on cursor object.
  """
  # Check if each column exists and add missing ones
  if checkCols:
    for cn in column_names:
      add_column_to_staging_table(cursor, table_schema, table_name, cn)

  insert_base = "INSERT INTO " + table_schema + "." + table_name + " (" + ",".join(column_names) + ") VALUES "
  insert_values = []
  for record in records:
    insert_value = "('" + "','".join(str(x).replace("'","''") for x in record) + "')"  
    insert_values.append(insert_value)
  insert_record = insert_base + ",".join(insert_values) + ";"
  if records != []:
    cursor.execute(insert_record.replace(",TO,",",TOV,"))
    print("Inserted " + str(len(records)) + " records into " + table_schema + "." + table_name)

  
def check_if_column_exists(cursor,table_schema,table_name,column_name):
  """
  Check if column exists in table given column name.
  """
  query = "SELECT * FROM information_schema.columns WHERE table_schema = '" + table_schema + "' AND table_name = '" + table_name + "' AND column_name = '" + column_name.lower() + "';"
  cursor.execute(query)
  rows = cursor.fetchall()
  if len(rows) == 1:
    return(rows)


def add_column_to_staging_table(cursor,table_schema,table_name,column_name):
  """
  Add column to staging table as long as column does not already exist.
  """
  if not check_if_column_exists(cursor, table_schema, table_name, column_name):
    add_column = "ALTER TABLE " + table_schema + "." + table_name + " ADD COLUMN " + column_name + " text;"
    cursor.execute(add_column)
    

def update_records(cursor,table_schema,table_name,column_name,value):
  """
  Update records for column in table with value where column = ''.
  """
  update_records = "UPDATE " + table_schema + "." + table_name + " SET " + column_name + "='" + value + "' WHERE COALESCE(" + column_name + ",'')='';"
  cursor.execute(update_records)

