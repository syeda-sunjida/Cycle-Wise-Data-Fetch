import pandas as pd
from sqlalchemy import create_engine

# Database connection details
db_config = {
    'user': 'readonly_user',
    'password': 'password123',
    'host': 'fn-prod-db-cluster.cluster-ro-cqtlpb5sm2vt.ap-northeast-1.rds.amazonaws.com',
    'database': 'api_backend',
    'port': 3306
}

# Create a connection string
connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# Connect to the database
engine = create_engine(connection_string)

# Set cycle start date and cycle end date
cycle_start_date = '2024-08-06 00:00:00'
cycle_end_date = '2024-10-31 23:59:59'
# SQL query to fetch data based on the date range and include starting_balance from the accounts table
query = f"""
SELECT
    s.*, a.type, a.starting_balance
FROM
    subscriptions s
JOIN
    accounts a ON s.account_id = a.id
WHERE
    s.ending_at >= '{cycle_start_date}'
    AND s.ending_at <= '{cycle_end_date}'
    AND LOWER(a.type) LIKE '%real%'  -- Filter only real accounts
"""

# Execute the query and fetch the data into a DataFrame
try:
    df = pd.read_sql(query, engine)
    print(f"Data fetched successfully with {len(df)} rows.")

    # Remove invalid characters from the file name (such as colons in time)
    sanitized_start_date = cycle_start_date.replace(':', '-').replace(' ', '_')
    sanitized_end_date = cycle_end_date.replace(':', '-').replace(' ', '_')

    # Save the result to a CSV file with a sanitized file name
    csv_file_name = f"real_accounts_data_{sanitized_start_date}_to_{sanitized_end_date}.csv"
    df.to_csv(csv_file_name, index=False)
    print(f"Data has been saved to {csv_file_name}")

except Exception as e:
    print(f"An error occurred: {e}")
