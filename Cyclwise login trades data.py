import pandas as pd
from sqlalchemy import create_engine

# Database connection details
db_config = {
      'user': ****
    'password': ****
    'host': ****
    'database': ****
    'port': ****
}

# Create the connection string
connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(connection_string)

# Load the account cycles CSV
csv_file = 'merged_real_accounts_metrics_with_PnL.csv'
df_csv = pd.read_csv(csv_file)

# Convert created_at and ending_at columns to datetime
df_csv['created_at'] = pd.to_datetime(df_csv['created_at'])
df_csv['ending_at'] = pd.to_datetime(df_csv['ending_at'])

# Get the broadest date range for fetching trades
oldest_date = df_csv['created_at'].min().strftime('%Y-%m-%d %H:%M:%S')
latest_date = df_csv['ending_at'].max().strftime('%Y-%m-%d %H:%M:%S')

# Unique account IDs as a string for SQL query
account_ids = df_csv['account_id'].dropna().unique()
account_ids_str = ",".join([str(int(id)) for id in account_ids])

# File path for output
output_file = "filtered_trades_by_cycle_dates.csv"

# Fetch all trades data within the broadest range and filter for each cycle
trades_query = f"""
SELECT id, account_id, close_price, close_time, close_time_str, login, lots, open_price, open_time,
       open_time_str, profit, sl, symbol, commission, ticket, tp, type_str, volume 
FROM trades
WHERE account_id IN ({account_ids_str})
AND open_time BETWEEN UNIX_TIMESTAMP('{oldest_date}') AND UNIX_TIMESTAMP('{latest_date}');
"""
 
# Load trades data
trades_df = pd.read_sql(trades_query, engine)
trades_df['open_time'] = pd.to_datetime(trades_df['open_time'], unit='s')

# Initialize CSV file for saving results
first_write = True

# Filter trades data based on each cycle and append to CSV
try:
    for account_id, account_cycles in df_csv.groupby('account_id'):
        print(f"Processing cycles for account {account_id}...")

        account_trades = trades_df[trades_df['account_id'] == account_id]
        filtered_trades = []

        for _, cycle_row in account_cycles.iterrows():
            start_date = cycle_row['created_at']
            end_date = cycle_row['ending_at']
            cycle_trades = account_trades[(account_trades['open_time'] >= start_date) &
                                          (account_trades['open_time'] <= end_date)]
            filtered_trades.append(cycle_trades)

        # Concatenate and save trades data for each account cycle
        filtered_trades_df = pd.concat(filtered_trades, ignore_index=True)
        filtered_trades_df.to_csv(output_file, mode='a', header=first_write, index=False)
        first_write = False  # Only write header once
        print(f"Completed cycles for account {account_id}.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Ensure the connection is closed
    engine.dispose()
    print("Database connection closed.")
