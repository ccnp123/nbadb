

class DataAggregator():
  """
  Object to compile many json records into bulk SQL statements
  """

  def __init__(self):
    self.schema_tables = {}
    self.table_records = {}
    self.table_columns = {}


  def add(self, data):
    for i in range(0, len(data['resultSets'])):
      table_schema = "staging_" + data["resource"].lower() + "_2agg"
      table_name = data["resultSets"][i]["name"].lower()
      column_names = data["resultSets"][i]["headers"]
      records = data["resultSets"][i]["rowSet"]

      comb_name = table_schema + "." + table_name

      if self.schema_tables.has_key(table_schema):
        if not table_name in self.schema_tables[table_schema]:
          self.schema_tables[table_schema].append(table_name)
      else:
        self.schema_tables[table_schema] = [table_name]
      
      self.table_columns[comb_name] = column_names
      
      if self.table_records.has_key(comb_name):
        self.table_records[comb_name] += records
      else:
        self.table_records[comb_name] = records


  def makeSQL(self):
    sql = []
    print "schemas: " + str(self.schema_tables)
    for schema, tables in self.schema_tables.iteritems():
      print "working on schema " + schema
      # create this schema if it does not exist
      create_schema = "CREATE SCHEMA IF NOT EXISTS " + schema + ";"
      sql.append(create_schema)
      for table in tables:
        # create table if it does not exist
        comb_name = schema + "." + table
        col_names = self.table_columns[comb_name]
        create_table = "create table if not exists " + comb_name + " (" + " text,".join(col_names) + " text);"
        sql.append(create_table)
        # create bulk insert statement for records
        header = "insert into " + comb_name + " (" + ",".join(col_names) + ") values "
        insert_values = []
        for r in self.table_records[comb_name]:
          insert_value = "('" + "','".join(str(x).replace("'","''") for x in r) + "')"  
          insert_values.append(insert_value)
        insert_records = header + ",".join(insert_values) + ";"
        insert_records = insert_records.replace(",TO,",",TOV")
        sql.append(insert_records)
    print "Generated " + str(len(sql)) + " statements"
    return sql



    









