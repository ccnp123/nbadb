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

def create_schema(cursor,table_schema):
  """
  Create schema if it doesn't exist, then execute on cursor object.
  """
  create_schema = "CREATE SCHEMA IF NOT EXISTS " + table_schema + ";"
  #print(create_schema)
  cursor.execute(create_schema)
  #print("Created " + table_schema + " schema")

def create_table(cursor,table_schema,table_name,column_names):
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
  #print(create_table)
  cursor.execute(create_table)
  #print("Created " + table_schema + "." + table_name + " table")

def insert_records(cursor,table_schema,table_name,column_names,records):
  """
  Insert multiple records with one INSERT statement into table, then execute 
  on cursor object.
  """
  # Check if each column exists and add missing ones
  for cn in column_names:
    add_column_to_staging_table(cursor, table_schema, table_name, cn)

  insert_base = "INSERT INTO " + table_schema + "." + table_name + " (" + ",".join(column_names) + ") VALUES "
  insert_values = []
  for record in records:
    insert_value = "('" + "','".join(str(x).replace("'","''") for x in record) + "')"  
    insert_values.append(insert_value)
  insert_record = insert_base + ",".join(insert_values) + ";"
  if records != []:
    #print(insert_record)
    cursor.execute(insert_record.replace(",TO,",",TOV,"))
    print("Inserted " + str(len(records)) + " records into " + table_schema + "." + table_name)
  #else:
  #  print("No records to insert into %s.%s" % (table_schema,table_name))
  
def check_if_column_exists(cursor,table_schema,table_name,column_name):
  """
  Check if column exists in table given column name.
  """
  query = "SELECT * FROM information_schema.columns WHERE table_schema = '" + table_schema + "' AND table_name = '" + table_name + "' AND column_name = '" + column_name.lower() + "';"
  #print(query)
  cursor.execute(query)
  rows = cursor.fetchall()
  if len(rows) == 1:
    return(rows)

def add_column_to_staging_table(cursor,table_schema,table_name,column_name):
  """
  Add column to staging table as long as column does not already exist.
  """
  #print "Adding column " + column_name
  if not check_if_column_exists(cursor, table_schema, table_name, column_name):
    #print "Column " + column_name + " does not exist"
    add_column = "ALTER TABLE " + table_schema + "." + table_name + " ADD COLUMN " + column_name + " text;"
    #print add_column
    cursor.execute(add_column)
    

  #if check_if_column_exists(cursor,table_schema,table_name,column_name):
    #print("Column " + column_name + " already exists in " + table_schema + "." + table_name)
  #else:
    #print table_name
    #print column_name
    #add_column = "ALTER TABLE " + table_schema + "." + table_name + " ADD COLUMN " + column_name + " text;"
    #print(add_column)
    #cursor.execute(add_column)
    #print("Added " + column_name + " to " + table_schema + "." + table_name)

def update_records(cursor,table_schema,table_name,column_name,value):
  """
  Update records for column in table with value where column = ''.
  """
  update_records = "UPDATE " + table_schema + "." + table_name + " SET " + column_name + "='" + value + "' WHERE COALESCE(" + column_name + ",'')='';"
  #print(update_records)
  cursor.execute(update_records)
  #print("Updated records in " + table_schema + "." + table_name + " where " + column_name + " is empty")