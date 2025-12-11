import pandas as pd
from pathlib import Path

def run_transformation():
    base_dir = Path(__file__).resolve().parent
    csv_path = base_dir / "zipco_transaction.csv"

    data = pd.read_csv(csv_path)

    # Remove duplicates
    data.drop_duplicates(inplace=True)

    # Handling Missing Data by filling numeric values with mean or median
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        #data[col].fillna(data[col].mean(), inplace=True)
        data.fillna({col: data[col].mean()}, inplace=True)

    # Handling Missing Data by filling categorical values with unknown
    cate_columns = data.select_dtypes(include=['object']).columns
    for col in cate_columns:
        data.fillna({col: 'Unknown'}, inplace=True)
    
    # Cleaning the date column to the right date type
    data['Date'] = pd.to_datetime(data['Date'])

    # Creating fact and dimensions tables
    # Create Products Table
    products = data[['ProductName']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'ProductID'
    products = products.reset_index()

    # Create Staff Table
    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'StaffID'
    staff = staff.reset_index()

    # Create Customers Table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'CustomerID'
    customers = customers.reset_index()


    # Create the Transactions table
    transaction = data.merge(products, on=['ProductName'], how='left') \
                    .merge(customers, on=['CustomerName','CustomerAddress','Customer_PhoneNumber','CustomerEmail'], how='left') \
                    .merge(staff, on=['Staff_Name','Staff_Email'], how='left') \

    transaction.index.name = 'TransactionID'
    transaction =  transaction.reset_index() \
                            [['Date','TransactionID','ProductID','Quantity', 'UnitPrice', 'StoreLocation', 'PaymentType', 'PromotionApplied', \
                                    'Weather', 'Temperature', 'StaffPerformanceRating', 'CustomerFeedback', 'DeliveryTime_min', 'OrderType', \
                                        'CustomerID', 'StaffID', 'DayOfWeek', 'TotalSales']]


    # ---------- Save normalized tables ----------

    data_dir = base_dir / "data"
    data_dir.mkdir(exist_ok=True)  

    data.to_csv(data_dir / "clean_data.csv", index=False)
    customers.to_csv(data_dir / "customers.csv", index=False)
    products.to_csv(data_dir / "products.csv", index=False)
    staff.to_csv(data_dir / "staff.csv", index=False)
    transaction.to_csv(data_dir / "transaction.csv", index=False)

    print("Normalised data saved successfully!")
