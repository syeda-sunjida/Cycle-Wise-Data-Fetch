import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# Database connection details
db_config = {
    'user': 'readonly_user',
    'password': 'password123',
    'host': 'fn-prod-db-cluster.cluster-ro-cqtlpb5sm2vt.ap-northeast-1.rds.amazonaws.com',
    'database': 'api_backend',
    'port': 3306
}

# Create the connection string
connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Read the CSV file
csv_file = 'merged_real_accounts_metrics_with_PnL.csv'
df_csv = pd.read_csv(csv_file)

# Convert created_at and ending_at columns to datetime
df_csv['created_at'] = pd.to_datetime(df_csv['created_at'])
df_csv['ending_at'] = pd.to_datetime(df_csv['ending_at'])

# Get the oldest date from created_at and the latest date from ending_at
oldest_date = df_csv['created_at'].min().strftime('%Y-%m-%d %H:%M:%S')
latest_date = df_csv['ending_at'].max().strftime('%Y-%m-%d %H:%M:%S')

# Get the unique account_ids from the CSV and remove any NaN values
account_ids = df_csv['account_id'].dropna().unique()  # Removes NaN values from the account_id column
account_ids_str = ",".join([str(id) for id in account_ids])

# SQL query to fetch trades data based on the account_ids and the date range
trades_query = f"""
SELECT account_id, symbol, volume, profit, login, lots
FROM trades
WHERE account_id IN ({account_ids_str})
AND open_time BETWEEN UNIX_TIMESTAMP('{oldest_date}') AND UNIX_TIMESTAMP('{latest_date}');
"""

try:
    print("Fetching trades data...")
    trades_df = pd.read_sql(trades_query, engine)

    if trades_df.empty:
        print("No trades found within the specified time range and account IDs.")
    else:
        print("Processing trades data...")

        # Calculate Lots based on the login
        trades_df['Lots'] = np.where(
            trades_df['login'].astype(str).str.startswith('7'),
            trades_df['lots'],  # If login starts with '7', use the 'lots' column as is
            trades_df['volume'] / 100  # Otherwise, calculate Lots as volume / 100
        )

        # Group by symbol and calculate the required metrics
        summary_df = trades_df.groupby('symbol').agg(
            total_pnl=('profit', 'sum'),                              # Total PnL
            positive_pnl=('profit', lambda x: x[x > 0].sum()),        # Positive PnL summation
            negative_pnl=('profit', lambda x: x[x < 0].sum()),        # Negative PnL summation
            login_count=('login', 'nunique'),                         # Count of unique logins
            trades_count=('symbol', 'count'),                         # Count of trades
            total_Lots=('Lots', 'sum')                                # Sum of Lots
        ).reset_index()

        print("Trades data summary:")
        print(summary_df.head(10))  # Show the first 10 rows of the summary

        # Save the summary to a new CSV file
        summary_csv_file = "trades_summary_grouped_by_symbol.csv"
        summary_df.to_csv(summary_csv_file, index=False)
        print(f"Grouped trades summary has been saved to {summary_csv_file}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Ensure the connection is closed
    engine.dispose()
    print("Database connection closed.")
