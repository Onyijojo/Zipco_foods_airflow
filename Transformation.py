import pandas as pd


def run_transformation():
    data = pd.read_csv(r'zipco_transaction.csv')
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


    
    # Save normalized tables to new CSV files
    data.to_csv(r'data\clean_data.csv', index=False)
    customers.to_csv(r'data\customers.csv', index=False)
    products.to_csv(r'data\products.csv', index=False)
    staff.to_csv(r'data\staff.csv', index=False)
    transaction.to_csv(r'data\transaction.csv', index=False)
    
    print('Normalised data saved successfully!')
