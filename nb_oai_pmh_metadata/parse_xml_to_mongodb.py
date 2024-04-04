import os
from multiprocessing import Pool
from pymarc import parse_xml_to_array
from pymongo import MongoClient, InsertOne
from configparser import ConfigParser

DB = "norbok"
COLLECTION = "records"
username = "admin"
directory_path = "data/norbok"

def connect(username, config_file="config.cfg"):
    config = ConfigParser()
    config.read(config_file)
    
    client = MongoClient(
        config[username]["host"],
        username=config[username]["username"],
        password=config[username]["pwd"],
        authSource=config[username]["authSource"],
    )

    return client


def process_file(file_path):
    # Initialize MongoDB client
    collection = connect(username)[DB][COLLECTION] 
    
    # Parse MARC-XML to PyMARC objects
    records = parse_xml_to_array(file_path)
    records = [record for record in records if record]  # Remove any empty records
    
    
    # Prepare a list for bulk write operations
    operations = []
    
    for record in records:
        record_dict = record.as_dict()
        # Prepare the insert operation for this record
        operations.append(InsertOne(record_dict))
        
        # Check if we have accumulated 100 operations and execute the bulk write
        if len(operations) == 100:
            collection.bulk_write(operations)
            operations = []  # Reset the list of operations after bulk write
    
    # Perform any remaining operations that didn't fill up the last batch
    if operations:
        collection.bulk_write(operations)

    print(f"Processed and uploaded records from {file_path}")


def main():
    # List all MARC-XML files in a directory
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.xml')]

    # Adjust the number of processes according to your machine's capabilities
    with Pool(processes=10) as pool:
        pool.map(process_file, files)

    print("All files have been processed and uploaded to MongoDB.")

if __name__ == "__main__":
    main()