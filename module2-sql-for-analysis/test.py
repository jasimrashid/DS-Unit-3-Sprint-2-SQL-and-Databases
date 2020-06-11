#!/usr/bin/env python
import os
import sqlite3
import pandas as pd

# construct a path to wherever your database exists
# DB_FILEPATH = "rpg_db.sqlite3"
# DB_FILEPATH = "module1-introduction-to-sql/rpg_db.sqlite3"
#DB_FILEPATH = os.path.join("module1-introduction-to-sql", "chinook.db")
#DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "module2-0...", "chinook.db")
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")
DB_FILEPATH2 = os.path.join(os.path.dirname(__file__), "buddymove_holidayiq.sqlite3")
# DB_FILEPATH2 = "buddymove_holidayiq.sqlite3"
CSV_FILEPATH = os.path.join(os.path.dirname(__file__), "buddymove_holidayiq.csv")

connection = sqlite3.connect(DB_FILEPATH)
connection2 = sqlite3.connect(DB_FILEPATH2)
# print("CONNECTION:", connection)

cursor = connection.cursor()
# print("CURSOR", cursor)

# breakpoint()

# 1 How many total Characters are there?
# -- A: 302 **note: duplicate names

# query = "select * from charactercreator_character;"

# query = "select count(distinct character_id) from charactercreator_character"
query = "select * from charactercreator_character limit 10"
query1 = "select count(distinct character_id) from charactercreator_character"
query2 = "select count(DISTINCT character_ptr_id) from charactercreator_mage m join charactercreator_character cc on m.character_ptr_id = cc.character_id"
query3 = "select COUNT(DISTINCT 'name') from armory_item"
query4 = "select count(DISTINCT item_ptr_id) from armory_item i join armory_weapon w on i.item_id = w.item_ptr_id"
query41 = "select count(DISTINCT item_id) from armory_item"
query5 = "select cc.character_id, cc.name, count(*) as [number of ite] FROM charactercreator_character cc JOIN charactercreator_character_inventory ci on cc.character_id = ci.character_id join armory_item ai on ci.item_id = ai.item_id group by cc.character_id limit 20"
query6 = "select cc.character_id, cc.name, count(*) as count FROM charactercreator_character cc JOIN charactercreator_character_inventory ci on cc.character_id = ci.character_id join armory_item ai on ci.item_id = ai.item_id join armory_weapon aw on ai.item_id = aw.item_ptr_id group by cc.character_id limit 20"
query7 = "select sum(COUNT)/count(*) from (select cc.character_id as 'character_id', cc.name as 'name', count(*) as count FROM charactercreator_character cc JOIN charactercreator_character_inventory ci on cc.character_id = ci.character_id join armory_item ai on ci.item_id = ai.item_id group by cc.character_id)"
query8 = "select sum(count)/count(*) from (select cc.character_id as 'character_id', cc.name as 'name', count(*) as count FROM charactercreator_character cc JOIN charactercreator_character_inventory ci on cc.character_id = ci.character_id join armory_item ai on ci.item_id = ai.item_id join armory_weapon aw on ai.item_id = aw.item_ptr_id group by cc.character_id)"

result2 = cursor.execute(query1).fetchall()
print("How many total Characters are there?")
print(result2[0][0])
print()



result2 = cursor.execute(query2).fetchall()
print("How many of each specific subclass?")
print(result2[0][0])

result2 = cursor.execute(query3).fetchall()
print("How many total items?")
print(result2[0][0])
print()

result2 = cursor.execute(query4).fetchall()
result3 = cursor.execute(query41).fetchall()
print("How many of the Items are weapons? How many are not?")
print("Items thsat are weapons: ", result2[0][0])
print("Items that are not weapons:", result3[0][0]-result2[0][0])
print()

# rpg_df = pd.DataFrame()
rpg_df = pd.read_sql_query(query5, connection)
result2 = cursor.execute(query5).fetchall()
print("How many Items does each character have? (Return first 20 rows")
# for i, row in enumerate(result2):
#     # print(i)
#     print(row[0])
#     # rpg_df[i] = row
print()
print(rpg_df)
print()

rpg_df = pd.read_sql_query(query6, connection)
print("How many Weapons does each character have? (Return first 20 rows)")
print(rpg_df)
print()

result2 = cursor.execute(query7).fetchall()
print("On average, how many Items does each Character have?")
print(result2[0][0])
print()


result2 = cursor.execute(query8).fetchall()
print("On average, how many Weapons does each character have?")
print(result2[0][0])
print()

print("Assignment 2")


df = pd.read_csv(CSV_FILEPATH)
print(df.head(10))
cursor = connection2.cursor()

# query = "drop table buddymove"
# cursor.execute(query)

df.to_sql("buddymove",connection2)





query = "select count(*) from buddymove"
result2 = cursor.execute(query).fetchall()
print("Buddymove rowcount: ",result2[0][0])



query = "select count(DISTINCT [User Id]) from buddymove where Nature>=100 and Shopping>=100"

result2 = cursor.execute(query).fetchall()
print("Users that reviewed at least 100 in Nature and Shopping categories: ",result2[0][0])

query = "select sum(Sports)/count(*) as Sports, sum(Religious)/count(*) as Religious, sum(Nature)/count(*) as Nature, sum(Theatre)/count(*) as Theatre, sum(Shopping)/count(*) as Shopping, sum(Picnic)/count(*) as Picnic from buddymove"
result2 = cursor.execute(query).fetchall()
print("Average # of reviews for each category: ",result2[0])



# #result = cursor.execute(query)
# #print("RESULT", result) #> returns cursor object w/o results (need to fetch the results)
# result3 = cursor.execute(query).fetchone()
# print("RESULT 2", type(result3), result3)