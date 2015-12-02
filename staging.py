#! /usr/bin/python
import sys
import simplejson as json
import urllib
from datetime import datetime, timedelta
import database as db
import urls as fetch


def retrieve_and_dump_data(conn, url):
  """
  Retrieve json from the given url and dump it into the database.
  Return the data for further processing.
  """
  cursor = conn.cursor()
  data = json.loads(urllib.urlopen(url).read())
  # dump every piece of data into the database
  for i in range(0, len(data["resultSets"])):
    table_schema = "staging_" + data["resource"].lower() + "_2"
    table_name = data["resultSets"][i]["name"].lower()
    column_names = data["resultSets"][i]["headers"]
    records = data["resultSets"][i]["rowSet"]
    # Create schema, table, and insert records
    db.create_staging_schema(cursor,table_schema)
    db.create_staging_table(cursor,table_schema,table_name,column_names)
    db.insert_records(cursor,table_schema,table_name,column_names,records)
    conn.commit()
  cursor.close()
  return data


def load_date(conn, date):
  """
  Retrieve and load all info for a given date
  """
  print "Loading info for " + str(date)
  scoreboard_url = fetch.make_scoreboard_url(date)
  if fetch.validate_url(scoreboard_url):
    data = retrieve_and_dump_data(conn, scoreboard_url)
    # look for games
    game_header = filter((lambda r: r["name"] == "GameHeader"), data["resultSets"])
    if game_header:
      game_ids = map((lambda r: r[2]), game_header[0]["rowSet"])
      if not game_ids:
        print "No Games"
      else:
        print "Found " + str(len(game_ids)) + " games."
        for game_id in game_ids:
          game_urls = fetch.make_urls_for_game_id(game_id)
          for url in game_urls:
            #print url
            retrieve_and_dump_data(conn, url)



if __name__ == "__main__":
  conn = db.create_connection()
  if len(sys.argv) == 1:  # default to yesterday
    dates = [datetime.now() - timedelta(days=1)]
  elif len(sys.argv) == 2:  # Single specified date
    dates = [datetime.strptime(sys.argv[1], "%Y-%m-%d")]
  elif len(sys.argv) == 3:   # Range of dates
    start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")
    dates = [start_date + timedelta(days=x) for x in range((end_date-start_date).days+1)]
  else:
    print "Usage: python staging.py | <> | <YYYY-MM-DD> | <YYYY-MM-DD> <YYYY-MM-DD>"  
    sys.exit()

  for date in dates:
    # TODO break this up into jobs
    load_date(conn, date)
  print "Done!"
  conn.close()


