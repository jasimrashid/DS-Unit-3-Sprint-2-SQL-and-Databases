import os
import sqlite3
import pandas as pd
import json
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import psycopg2

# ------ Inbound #1 - from local database ------

# DB_FILEPATH_IN = "module2-sql-for-analysis/rpg_db.sqlite3"
DB_FILEPATH_IN = os.path.join(os.path.dirname(__file__),"..","module1-introduction-to-sql", "rpg_db.sqlite3")
# breakpoint()
print(DB_FILEPATH_IN)
connection_in = sqlite3.connect(DB_FILEPATH_IN)
cursor_in = connection_in.cursor()
query = "select name, strength from charactercreator_character"
result = cursor_in.execute(query).fetchall()
# print(result)
# quit()
print("1")

# ------ Inbound #2 - from CSV file ------

# CSV_FILEPATH = os.path.join(os.path.dirname(__file__), "buddymove_holidayiq.csv")
# CSV_FILEPATH = "module2-sql-for-analysis/titanic.csv"
CSV_FILEPATH = os.path.join(os.path.dirname(__file__),"titanic.csv")
df = pd.read_csv(CSV_FILEPATH)
np_list = df.to_records(index=False)
native_list = np_list.tolist()

print("2")

# ------ Outbound: from inbound 1 to "charactercreator_character"

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

connection = psycopg2.connect(dbname = DB_NAME, user=DB_USER, password = DB_PASSWORD, host = DB_HOST)

cursor = connection.cursor()

table_name = "charactercreator_character"

creation_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  character_id SERIAL PRIMARY KEY,
  name varchar(40) NOT NULL,
  strength float8
);
"""
cursor.execute(creation_query)



# cursor.execute("select * from charactercreator_character")
# cursor.execute("delete from test_table")
cursor.execute("TRUNCATE charactercreator_character")
connection.commit()


insertion_query = "INSERT INTO charactercreator_character (name, strength) VALUES %s"
execute_values(cursor, insertion_query,result) # data must be in a list of tuples!!!!!

connection.commit()
# cursor.execute(insertion_query)

print("3")

# ------ Output #2: from Inbound #2 to "titanic" in Postgre 

# df table headers: Survived, Pclass, Name, Sex, Age, Siblings/Spouses Aboard, Parents/Children Aboard, Fare

# DROP TABLE IF EXISTS
# cursor.execute("DROP TABLE IF EXISTS titanic")

# CREATE TABLE IF NOT EXISTS
table_name = "titanic"
creation_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  Survived integer NOT NULL,
  Pclass integer NOT NULL,
  Name text,
  Sex text,
  Age float8,
  Siblings_or_spouses_aboard integer,
  Parents_or_children_aboard integer,
  Fare float8
);
"""
# print("SQL:", creation_query)
cursor.execute(creation_query)

#Test
# cursor.execute("select * from charactercreator_character")

cursor.execute("TRUNCATE titanic")
connection.commit()

table_name = "titanic"
insertion_query = f"""INSERT INTO {table_name} 
(Survived, Pclass, Name, Sex, Age, Siblings_or_spouses_aboard, Parents_or_children_aboard, Fare) VALUES %s; """


# quit()
# breakpoint()
execute_values(cursor, insertion_query,native_list); # data must be in a list of tuples!!!!!

connection.commit()
cursor.close()
connection.close()

