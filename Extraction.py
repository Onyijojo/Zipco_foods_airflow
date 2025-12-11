# Importing Necessary Libraries
import pandas as pd
from pathlib import Path

# Data Extraction
def run_extraction():
    # folder where THIS file lives (inside the container: /opt/airflow/dags)
    base_dir = Path(__file__).resolve().parent
    csv_path = base_dir / "zipco_transaction.csv"

    # Let Airflow see real errors – don't swallow them
    data = pd.read_csv(csv_path)
    print("Data Extracted Successfully")
