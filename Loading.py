# Importing Necessary Libraries
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
import os


# Load environment variables from .env file
load_dotenv()

def run_loading():
    #Loading the dataset
    data = pd.read_csv(r'data\clean_data.csv')
    products = pd.read_csv(r'data\products.csv')
    customers = pd.read_csv(r'data\customers.csv')
    staff = pd.read_csv(r'data\staff.csv')
    transaction = pd.read_csv(r'data\transaction.csv')
    
    # Create a BlobServiceClient object
    connect_str = os.getenv('AZURE_CONNECTION_STRING_VALUE')
    container_name = os.getenv('CONTAINER_NAME')
    # create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)


    # Load data to Azure Blob Storage
    # List of tuples (DataFrame, Blob Name)
    files = [
        (data, 'rawdata/cleaned_zipco_data.csv'), 
        (products, 'cleaneddata/products.csv'),
        (customers,'cleaneddata/customers.csv'),
        (staff, 'cleaneddata/staff.csv'),
        (transaction, 'cleaneddata/transaction.csv')
    ]

    # Load data to Azure Blob Storage
    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into Azure Blob storage')