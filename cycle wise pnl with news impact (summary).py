import pandas as pd
from sqlalchemy import create_engine
from tkinter import Tk
from tkinter.filedialog import askopenfilename

# Database connection details
db_config = {
      'user': ****
    'password': ****
    'host': ****
    'database': ****
    'port': ****
}

# Create the connection string
connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Function to select and load the CSV files
def select_csv_file(prompt):
    Tk().withdraw()  # We don't want a full GUI, so keep the root window from appearing
    file_path = askopenfilename(title=prompt, filetypes=[("CSV files", "*.csv")])
    return file_path

# Load the two CSV files
file1_path = select_csv_file('Select File 1 (Trades)')
file2_path = select_csv_file('Select File 2 (News Events)')

if not file1_path or not file2_path:
    print("No file selected. Exiting...")
    exit()

# Load the trades dataset with error handling to skip problematic rows
trades_df = pd.read_csv(file1_path, on_bad_lines='skip', low_memory=False)

# Convert columns to the appropriate data types
trades_df['profit'] = pd.to_numeric(trades_df['profit'], errors='coerce')
trades_df['volume'] = pd.to_numeric(trades_df['volume'], errors='coerce')
trades_df['lots'] = pd.to_numeric(trades_df['lots'], errors='coerce')

# Load the news events dataset
news_df = pd.read_csv(file2_path)

# Convert the time columns to datetime format
trades_df['open_time'] = pd.to_datetime(trades_df['open_time'], errors='coerce')
trades_df['close_time'] = pd.to_datetime(trades_df['close_time_str'], format='%Y.%m.%d %H:%M:%S', errors='coerce')
news_df['news_start'] = pd.to_datetime(news_df['news_start'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
news_df['news_end'] = pd.to_datetime(news_df['news_end'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

# Define a mapping of currencies to symbols
currency_to_symbols = {
    "USD": ["USD", "SPX500", "NDX100", "US30"],
    "AUD": ["AUD", "AUS200"],
    "CAD": ["CAD", "UKOUSD", "USOUSD"],
    "CHF": ["CHF"],
    "CNY": ["CNY", "HK50"],
    "EUR": ["EUR", "GER30", "FRA40"],
    "GBP": ["GBP", "UK100"],
    "JPY": ["JPY", "JP225"],
    "NZD": ["NZD"]
}

# Function to match rows based on time ranges and symbol comparison
def match_trades_with_news(trades, news):
    matched_rows = []
    for index, news_row in news.iterrows():
        news_currency = news_row['Currency']
        news_start = news_row['news_start']
        news_end = news_row['news_end']

        # Find additional symbols based on the currency mapping
        additional_symbols = currency_to_symbols.get(news_currency, [news_currency])

        # Find matching trades within the news time window and symbol containing the currency or additional symbols
        matching_trades = trades.loc[
            (
                (trades['open_time'] >= news_start) & (trades['open_time'] <= news_end) |
                (trades['close_time'] >= news_start) & (trades['close_time'] <= news_end)
            ) &
            (trades['symbol'].apply(lambda sym: any(symbol in str(sym) for symbol in additional_symbols)))
        ]

        if not matching_trades.empty:
            matching_trades = matching_trades.copy()  # Avoid SettingWithCopyWarning
            matching_trades['news_event'] = news_row['Event Name']
            matched_rows.append(matching_trades)

    # Concatenate all matching rows and drop duplicates to avoid double counting
    if matched_rows:
        matched_df = pd.concat(matched_rows).drop_duplicates(subset=['id'])  # Ensure unique trades by dropping duplicates
    else:
        matched_df = pd.DataFrame()  # Empty DataFrame if no matches found

    return matched_df

# Apply the matching function
matched_trades_df = match_trades_with_news(trades_df, news_df)

# Generate summary login-wise for total trades
total_summary = trades_df.groupby('login').agg(
    total_trade_count=('id', 'count'),
    total_profit_sum=('profit', 'sum'),
    positive_profit_sum=('profit', lambda x: x[x > 0].sum(skipna=True)),
    negative_profit_sum=('profit', lambda x: x[x < 0].sum(skipna=True)),
    symbols_used=('symbol', lambda x: ', '.join(x.dropna().unique())),
    total_lot_sum=('lots', lambda x: x.sum(skipna=True))
).reset_index()

# Generate summary login-wise for matched trades
matched_summary = matched_trades_df.groupby('login').agg(
    matched_trade_count=('id', 'count'),
    matched_profit_sum=('profit', 'sum'),
    matched_positive_profit_sum=('profit', lambda x: x[x > 0].sum(skipna=True)),
    matched_negative_profit_sum=('profit', lambda x: x[x < 0].sum(skipna=True)),
    matched_symbols_used=('symbol', lambda x: ', '.join(x.dropna().unique())),
    matched_event_names=('news_event', lambda x: ', '.join(x.dropna().unique())),
    matched_lot_sum=('lots', lambda x: x.sum(skipna=True))
).reset_index()

# Merge both summaries
final_summary = pd.merge(total_summary, matched_summary, on='login', how='left')

# Calculate percentage metrics for PnL and trade count
final_summary['pnl_percentage'] = (final_summary['matched_profit_sum'] / final_summary['total_profit_sum']) * 100
final_summary['trade_count_percentage'] = (final_summary['matched_trade_count'] / final_summary['total_trade_count']) * 100

# Save the matched rows and summary to a new Excel file
output_file_name = "trades_with_news_impact_summary.xlsx"
with pd.ExcelWriter(output_file_name, engine='openpyxl') as writer:
    matched_trades_df.to_excel(writer, sheet_name='Matched Trades', index=False)
    final_summary.to_excel(writer, sheet_name='Login-wise Summary', index=False)

print(f"Data has been saved to {output_file_name}")

# Close the database connection
engine.dispose()
