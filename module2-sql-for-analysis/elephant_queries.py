# import psycopg2

# DB_NAME = 'fwrgqjss'
# DB_USER = 'fwrgqjss'
# DB_PASSWORD = 'p18xHH8m9B3Hmal-Dr5QoVNUR0kX5uEW'
# DB_HOST = 'ruby.db.elephantsql.com'



# connection = psycopg2.connect(dbname = DB_NAME, user=DB_USER, password = DB_PASSWORD, host = DB_HOST)
# print("Connection", connection)

# cursor = connection.cursor()
# cursor.execute("select * from test_table")

# result = cursor.fetchall()
# print(result)


#2

import os
from dotenv import load_dotenv
import json
from psycopg2.extras import execute_values
import psycopg2

load_dotenv()

# reference environment variable
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

print(DB_HOST, DB_NAME, DB_PASSWORD, DB_HOST) # testing


connection = psycopg2.connect(dbname = DB_NAME, user=DB_USER, password = DB_PASSWORD, host = DB_HOST)
# print("Connection", connection)

cursor = connection.cursor()
# print("cursor", cursor)
cursor.execute("select * from test_table")

result = cursor.fetchall()
# print(result)

data_to_insert = '{"a": 1, "b", ["dog", "cat", 42,"c": true]'

# insertion_sql = """
# INSERT INTO test_table (name, data) VALUES
# (
#   'A row name',
#   null
# ),
# (
#   'Another row, with JSON',
#   '{ "a": 1, "b": ["dog", "cat", 42], "c": true }'::JSONB
# );
# """

my_dict = { "a": 1, "b": ["dog", "cat", 42], "c": 'true' }

# insertion_query = f"INSERT INTO test_table (name, data) VALUES (%s, %s)"
# cursor.execute(insertion_query,
#  ('A rowwwww', 'null')
# )
# cursor.execute(insertion_query,
#  ('Another row, with JSONNNNN', json.dumps(my_dict))
# )

insertion_query = "INSERT INTO test_table (name, data) VALUES %s"
execute_values(cursor, insertion_query, [
  ('A rowwwww', 'null'),
  ('Another row, with JSONNNNN', json.dumps(my_dict)),
  ('Third row', "6")
]) # data must be in a list of tuples!!!!!
# cursor.execute(insertion_query)

connection.commit()

cursor.close()
connection.close()
