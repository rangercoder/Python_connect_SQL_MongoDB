import json
import pyodbc
import os
from pymongo import MongoClient
import traceback
from datetime import datetime

# To create a file to input all credentials
def create_file(filename):
    my_information = {
        'SQL_ID': '',
        'SQL_PASSWORD': '',
        'MONGODB_DATABASE': 'ShipPalmShore',
        'MONGODB_PASSWORD': 'password',
        'SQL_INSTANCE': 'SPV3-DB-MIG-VM2',
        'SQL_DATABASE': 'SingaporeBC18',
        'MONGO_HOST': 'localhost',
        'MONGO_USERNAME': 'root',
        'MONGO_PORT': '27017',
        'TABLE1_NAME': 'dbo.[GLI File Storage$24086ca3-09d8-4164-b484-a42e96e1d3fa]',
        'MONGODB_COLLECTION': 'FileStorage'
    }
    for key in my_information:
        new_value = input("Enter a new value for {}: ".format(key))
        my_information[key] = new_value
    file = open(filename, 'w')
    file.write(json.dumps(my_information))

# To read from the credential file
def read_file(filename):
    file = open(filename, "r")
    dictionary = json.load(file)
    file.close()
    return dictionary

filename = "credential.txt"

if not os.path.exists(filename):
    create_file(filename)

data = read_file(filename)
SQL_ID = data['SQL_ID']
SQL_PASSWORD = data['SQL_PASSWORD']
SQL_INSTANCE = data['SQL_INSTANCE']
SQL_DATABASE = data['SQL_DATABASE']
MONGODB_USERNAME = data['MONGO_USERNAME']
MONGODB_PASSWORD = data['MONGODB_PASSWORD']
MONGO_HOST = data['MONGO_HOST']
MONGO_PORT = data['MONGO_PORT']
MONGODB_DATABASE = data['MONGODB_DATABASE']
MONGODB_COLLECTION = data['MONGODB_COLLECTION']
TABLE1_NAME = data['TABLE1_NAME']

try:
    # SQL Connection string
    server = SQL_INSTANCE
    database = SQL_DATABASE
    username = SQL_ID
    password = SQL_PASSWORD
    conn = pyodbc.connect('DRIVER=SQL Server;' +
                          'SERVER=' + server + ';' +
                          'DATABASE=' + database + ';' +
                          'Trusted_Connection=yes;''UID=' + username + ';' +
                          'PWD=' + password + ';')
    cursor = conn.cursor()
    table1_name = TABLE1_NAME
    cursor.execute('SELECT * FROM ' + table1_name + ';')
    # Fetch all rows
    rows = cursor.fetchall()
    # Count number of rows fetched
    row_count = len(rows)
    # MongoDB connection details
    mongo_host = MONGO_HOST
    mongo_port = MONGO_PORT
    mongo_username = MONGODB_USERNAME
    mongo_password = MONGODB_PASSWORD
    mongo_db_name = MONGODB_DATABASE
    mongo_uri = 'mongodb://' + mongo_username + ':' + mongo_password + '@' + mongo_host + ':' + mongo_port + '/?authMechanism=DEFAULT'
    mongo_client = MongoClient(mongo_uri)
    mongodb = mongo_client[mongo_db_name]
    # MongoDB collection name
    collection_name = MONGODB_COLLECTION

    # Count number of documents pushed
    document_count = 0
    # List to store failed documents
    failed_documents = []
    successful_documents = []
    # Insert each row into MongoDB
    for row in rows:
        try:
            # Convert the row to a dictionary
            column_names = [column[0] for column in cursor.description]
            document = dict(zip(column_names, row))
            # Insert document into MongoDB collection
            mongodb[collection_name].insert_one(document)
            document_count += 1
            successful_documents.append(document.get('Guid'))
        except Exception as e:
            # Record failed document details
            failed_document = {
                'Guid': document.get('Guid'),
                'Column': column_names,
                'Timestamp': str(datetime.now())  # Add current timestamp to the document
            }
            failed_documents.append(failed_document)

    if row_count == document_count:
        print("Successful")
        for guid in successful_documents:
            cursor.execute('UPDATE ' + table1_name + ' SET [File] = NULL WHERE Guid = ?', [guid])
            conn.commit()
    else:
        print("Collection might have inconsistencies")
        with open("exception_log.txt", "a") as file:
            file.write("Collection might have inconsistencies\n")
        with open("Inconsistent_record.txt","a") as file1:
            for failed_document in failed_documents:
                file1.write("Failed Document: GUID={}, Column={}, Timestamp={}\n".format(failed_document['Guid'], failed_document['Column'], failed_document['Timestamp']))
        
        # Delete documents that were already moved to MongoDB
        
        if failed_documents:
            timestamp_to_delete = str(datetime.now())  # Get the current timestamp for deletion
            mongodb[collection_name].delete_many({'Timestamp': timestamp_to_delete})
except Exception as e:
    print("Error:", str(e))
    # Write the exception to a file
    with open("exception_log.txt", "a") as file:
        file.write("Error:\n")
        file.write(traceback.format_exc())
        file.write("\n")
