# Importing Necessary Libraries
import pandas as pd

# Data Extraction

def run_extraction():
    try:
        data = pd.read_csv(r'zipco_transaction.csv')
        print('Data Extracted Successfully')
    except Exception as e:
        print(f"An error occured: {e}")