import os
from multiprocessing import Pool
from pymarc import parse_xml_to_array
from pymongo import MongoClient, UpdateOne
from configparser import ConfigParser

DB = "norbok"
COLLECTION = "records"
username = "admin"
directory_path = "data/norbok"

def connect(username, config_file="config.cfg"):
    config = ConfigParser()
    
    if not os.file.pa
    
    try:
        config.read(config_file)
    except
    
    
    client = MongoClient(
        config[username]["host"],
        username=config[username]["username"],
        password=config[username]["pwd"],
        authSource=config[username]["authSource"],
    )

    return client


def main():
    
    collection = connect(username)[DB][COLLECTION]

    # Prepare a list for bulk update operations
    bulk_operations = []

    # Iterate over all documents in the collection
    for doc in collection.find():
        update_fields = {}
        
        # Copy values from fields.856.subfields.u to urn
        if 'fields' in doc and '856' in doc['fields'] and 'subfields' in doc['fields']['856'] and 'u' in doc['fields']['856']['subfields']:
            urls = doc['fields']['856']['subfields']['u']
            if isinstance(urls, list):
                update_fields['urn'] = urls
            else:
                update_fields['urn'] = [urls]
        
        # Copy value from fields.001 to mmsid
        if 'fields' in doc and '001' in doc['fields']:
            update_fields['mmsid'] = doc['fields']['001']
        
        # Extract year from fields.008 and check if it's an integer
        year_str = doc['fields']['008'][7:12]
        update_fields['year'] = year_str
        try:
            year_int = int(year_str)
            update_fields['year_int'] = year_int
        except ValueError:
            # The extracted year is not an integer, do not add year_int field
            pass

        # Update the document with new fields if there are any changes
        if update_fields:
            bulk_operations.append(UpdateOne({'_id': doc['_id']}, {'$set': update_fields}))

            
        # Execute bulk operation in batches of 100 (or another batch size you prefer)
        if len(bulk_operations) == 10000:
            collection.bulk_write(bulk_operations)
            bulk_operations = []  # Reset the list of operations after executing

    # Perform any remaining operations that didn't fill up the last batch
    if bulk_operations:
        collection.bulk_write(bulk_operations)

    print("Documents have been updated.")
