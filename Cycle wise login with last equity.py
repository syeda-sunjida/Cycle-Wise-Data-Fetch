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

# Read the previously generated CSV file
csv_file = 'real_accounts_data_2024-08-06_00-00-00_to_2024-10-31_23-59-59.csv'
df_csv = pd.read_csv(csv_file)

# Extract the account_ids and dates from the CSV
account_ids = df_csv['account_id'].unique()  # Get unique account_ids from the CSV

# Convert ending_at to date only (ignore time for comparison)
df_csv['ending_at_date'] = pd.to_datetime(df_csv['ending_at']).dt.date

# Create a string with account_ids for the SQL query
account_ids_str = ",".join([str(id) for id in account_ids])

# SQL query to fetch matching data from account_metrics based on account_id and metricDate
query = f"""
SELECT account_id, metricDate, lastequity 
FROM account_metrics
WHERE account_id IN ({account_ids_str})
AND DATE(metricDate) <= '2024-10-31'
"""

# Fetch data from account_metrics table
df_metrics = pd.read_sql(query, engine)

# Convert metricDate to date for comparison
df_metrics['metricDate'] = pd.to_datetime(df_metrics['metricDate']).dt.date

# Merge the CSV data with the account_metrics data on account_id and date
df_merged = pd.merge(df_csv, df_metrics, left_on=['account_id', 'ending_at_date'],
                     right_on=['account_id', 'metricDate'], how='left')

# Find rows where the merge did not find a matching metricDate
missing_data = df_merged[df_merged['metricDate'].isnull()]


# For those rows without matching metricDate, find the latest metricDate and lastEquity
def get_latest_lastequity(row):
    account_id = row['account_id']
    metrics_for_account = df_metrics[df_metrics['account_id'] == account_id]

    if not metrics_for_account.empty:
        # Sort by metricDate in descending order to get the latest
        latest_row = metrics_for_account.sort_values(by='metricDate', ascending=False).iloc[0]
        return latest_row['metricDate'], latest_row['lastequity']
    else:
        return None, None


# Update missing rows with the latest metricDate and lastequity
for idx, row in missing_data.iterrows():
    latest_metric_date, latest_lastequity = get_latest_lastequity(row)
    df_merged.at[idx, 'metricDate'] = latest_metric_date
    df_merged.at[idx, 'lastequity'] = latest_lastequity

# Filter out rows where lastEquity equals starting_balance
df_merged = df_merged[df_merged['lastequity'] != df_merged['starting_balance']]

# Calculate PnL (lastEquity - starting_balance) for the remaining rows
df_merged['PnL'] = df_merged['lastequity'] - df_merged['starting_balance']

# Save the merged DataFrame with PnL to a new CSV file
merged_csv_file = "merged_real_accounts_metrics_with_PnL.csv"
df_merged.to_csv(merged_csv_file, index=False)

print(f"Merged data with PnL has been saved to {merged_csv_file}")
