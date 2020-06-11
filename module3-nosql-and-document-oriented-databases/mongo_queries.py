'''
# PostgreSQL vs MongoDB:

MongoDB:

    Pros>>
        Mongdo DB seems to be preferable if we are not certain about the structure of our input dataset. 
        The database does not require us to specify the format of our data, nor does it force the column
        headers to match. This feature might be beneficial when we are mining and collecting data from 
        disparate sources and at a larger scale. 

        Another way to put this "You don't know exactly what you want, but you have a slight idea. You'll
        collect as much as you can and place it in one location. Then filter it, clean it and produce more
        structured dataset"....?

        Since MongoDB is more inclusive, we are less likely to lose data while migrating from other sources.

    Cons>>
        Querying and analysing data is difficult, especially for non-technical user who is acquainted with 
        the standard syntax for relationait SQL. Not a good choice for a use-case in an environment where 
        users are business users.
        

        (to confirm) MongoDB seems(?) optimized to store name/value pairs. If we are certain about
        the datatype/format and the quality of the data, it might be more efficient to manage the data in a
        database with predefined data model/attributes

        (to confirm) optimized for storing and managing text data

PostgreSQL:

    Pros>>
        Better when data model and structure is well understood, and can be predefined

        Syntax for querying is more standard, thus better suited for non-technical users

    Cons>>
        Might not be suitable when mining less structured data from disparate sources.

        Since the data model is less flexible, building and maintaining the ETL pipeline may take more effort
        

'''



import pymongo
import os
import sqlite3
from pprint import pprint
from dotenv import load_dotenv


load_dotenv()

# ------ Inbound - from local database SQLLite ------

# DB_FILEPATH_IN = "module2-sql-for-analysis/rpg_db.sqlite3"
DB_FILEPATH_IN = os.path.join(os.path.dirname(__file__),"..","module1-introduction-to-sql", "rpg_db.sqlite3")
print(DB_FILEPATH_IN)
connection_in = sqlite3.connect(DB_FILEPATH_IN)
cursor_in = connection_in.cursor()
query = "select * from charactercreator_character"
result = cursor_in.execute(query).fetchall()
# pprint(result)
# quit()

print("1")


# ------ Outbound - to Mongo DB ------
DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME", default="OOPS")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
print("----------------")
print("URI:", connection_uri)


client = pymongo.MongoClient(connection_uri)
print("----------------")
print("CLIENT:", type(client), client)
print("DATABASES", client.list_database_names())



db = client.rpg_db # "test_database" or whatever you want to call it
print("----------------")
print("DB:", type(db), db)
print("COLLECTIONS", db.list_collection_names())

collection = db.charactercreator_character # "pokemon_test" or whatever you want to call it
print("----------------")
print("COLLECTION:", type(collection), collection)
print("DOCUMENTS COUNT:", collection.count_documents({}))



# # for doc in collection.find({"level": {"$gt": 20}}):
# #     print(doc)


# for i, row in enumerate(result):
#     collection.insert_one({
#         "character_id": row[0],
#         "name": row[1],
#         "level": row[2],
#         "exp": row[3],
#         "hp": row[4],
#         "strength": row[5],
#         "intelligence": row[6],
#         "dexterity": row[7],
#         "wisdom": row[8]
#         }
#     )
# print("DOCS:", collection.count_documents({}))
# # print(collection.count_documents({"name": "Pikachu"}))

rowstoinsert = []
for i, row in enumerate(result):
    

    rowstoinsert.append({
        "character_id": row[0],
        "name": row[1],
        "level": row[2],
        "exp": row[3],
        "hp": row[4],
        "strength": row[5],
        "intelligence": row[6],
        "dexterity": row[7],
        "wisdom": row[8]
        }
    )



breakpoint()

#dir(client) # specifies list of attributes for an object

# from pprint import pprint
# pprint(dir(client)) # dir client transposed format



# bulbasaur = {
#    "name": "Bulbasaur",
#    "type": "grass",
#    "moves":["Leech Seed", "Solar Beam"]
# }
# eevee = {
#     "name": "Eevee",
#     "level": 40,
#     "exp": 7500,
#     "hp": 120,
# }
# chansey = {
#     "name": "Chansey",
#     "Egg Group": "Fairy",
#     "Pokedex Color": "Pink",
# }
# jirachi = {
#     "name": "Jirachi",
#     "Egg Group": "Undiscovered",
#     "Pokedex Color": "Yellow",
# }
# snorlax = {}
# charizard = {
#     "lvl": 100,
#     "power": "UNLIMITED",
#     "favorite item": "Pokeflute"
# }
# team = [bulbasaur, eevee, chansey, jirachi, snorlax, charizard]

# collection.insert_many(team)

# breakpoint()
#> list of dict

print("DOCUMENTS COUNT:", collection.count_documents({}))