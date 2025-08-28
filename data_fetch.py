import os
import sqlite3
import logging
from datetime import datetime
import pytz
import yfinance as yf
import pandas as pd
from tabulate import tabulate

# ----------------------------
# CONFIGURATIONS
# ----------------------------
DB_NAME = "nifty50_top20.db"
README_FILE = "README.md"

# Top 20 NIFTY50 stocks (symbols must match Yahoo Finance format, ".NS" for NSE India)
STOCKS = [
    "RELIANCE.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS", "TCS.NS",
    "ITC.NS", "HINDUNILVR.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
    "LT.NS", "AXISBANK.NS", "BAJFINANCE.NS", "ASIANPAINT.NS", "MARUTI.NS",
    "SUNPHARMA.NS", "WIPRO.NS", "POWERGRID.NS", "NTPC.NS", "ONGC.NS"
]

# Setup logging
logging.basicConfig(
    filename="data_fetch.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# IST timezone
IST = pytz.timezone("Asia/Kolkata")


# ----------------------------
# DATABASE FUNCTIONS
# ----------------------------
def init_db():
    """Ensure database exists with tables for each stock."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for stock in STOCKS:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS '{stock}' (
                datetime TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER
            )
        """)
    conn.commit()
    conn.close()


def insert_data(stock, df):
    """Insert stock data into database with ON CONFLICT IGNORE to avoid duplicates."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    rows = df.to_records(index=False)  # Convert DataFrame to iterable of tuples
    cursor.executemany(
        f"""
        INSERT OR IGNORE INTO '{stock}' (datetime, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows
    )

    conn.commit()
    conn.close()



# ----------------------------
# DATA FETCHING
# ----------------------------
def fetch_stock_data(stock):
    """Fetch 1-min data for the past 15 minutes for a stock."""
    try:
        df = yf.download(
            tickers=stock,
            interval="1m",
            period="15m",
            progress=False
        )

        if df.empty:
            logging.warning(f"No data returned for {stock}")
            return None

        # Reset index to get datetime as column
        df.reset_index(inplace=True)

        # Convert timezone to IST
        df["Datetime"] = df["Datetime"].dt.tz_convert(IST)
        df.rename(columns={
            "Datetime": "datetime",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }, inplace=True)

        return df[["datetime", "open", "high", "low", "close", "volume"]]

    except Exception as e:
        logging.error(f"Error fetching data for {stock}: {e}")
        return None


# ----------------------------
# README UPDATE
# ----------------------------
def update_readme():
    """Append last 2 rows from each stock table to README.md."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    with open(README_FILE, "w", encoding="utf-8") as f:
        f.write("# 📈 NIFTY50 Top 20 Data Snapshot\n\n")
        f.write(f"Last updated: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n")

        for stock in STOCKS:
            try:
                df = pd.read_sql_query(
                    f"SELECT * FROM '{stock}' ORDER BY datetime DESC LIMIT 2", conn
                )
                if df.empty:
                    continue

                table = tabulate(df, headers="keys", tablefmt="github", showindex=False)
                f.write(f"## {stock}\n\n{table}\n\n")
            except Exception as e:
                logging.error(f"Error updating README for {stock}: {e}")

    conn.close()


# ----------------------------
# MAIN WORKFLOW
# ----------------------------
def main():
    logging.info("Starting data fetch cycle...")
    init_db()

    for stock in STOCKS:
        df = fetch_stock_data(stock)
        if df is not None and not df.empty:
            insert_data(stock, df)
            logging.info(f"Inserted {len(df)} rows for {stock}")
        else:
            logging.warning(f"No data to insert for {stock}")

    update_readme()
    logging.info("Cycle complete. README updated.")


if __name__ == "__main__":
    main()
