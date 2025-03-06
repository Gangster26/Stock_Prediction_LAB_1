import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime , timedelta
import yfinance as yf
import logging


def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


def full_refresh_stock_prices():
    tickers = ['NFLX', 'PINS']
    logging.info("📅 Downloading data from yfinance...")
    data = yf.download(tickers, period="180d", group_by='ticker')

    if data.empty:
        raise ValueError("❌ No data fetched from yfinance.")

    records = []
    for ticker in tickers:
        if ticker not in data.columns.levels[0]:
            logging.warning(f"❌ No data found for {ticker}")
            continue

        temp_df = data[ticker].reset_index()
        temp_df['STOCK_SYMBOL'] = ticker
        temp_df['DATE'] = temp_df['Date'].dt.strftime('%Y-%m-%d')
        temp_df = temp_df.rename(columns={
            'Open': 'OPEN',
            'Close': 'CLOSE',
            'High': 'HIGH',
            'Low': 'LOW',
            'Volume': 'VOLUME'
        })
        temp_df = temp_df[['STOCK_SYMBOL', 'DATE', 'OPEN', 'CLOSE', 'HIGH', 'LOW', 'VOLUME']]
        temp_df = temp_df.fillna(0)
        logging.info(f"✅ Data for {ticker}:\n{temp_df.head()}")
        records.extend(temp_df.values.tolist())

    if not records:
        raise ValueError("❌ No records to insert into Snowflake.")

    delete_sql = "DELETE FROM STOCK_LAB1.PUBLIC.STOCK_PRICES"
    insert_sql = """
        INSERT INTO STOCK_LAB1.PUBLIC.STOCK_PRICES (STOCK_SYMBOL, DATE, OPEN, CLOSE, HIGH, LOW, VOLUME)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    cursor = None
    try:
        cursor = get_snowflake_cursor()
        logging.info("🚀 Deleting existing data...")
        cursor.execute(delete_sql)
        logging.info("✅ Existing data deleted.")

        logging.info(f"🚀 Inserting {len(records)} new records...")
        cursor.executemany(insert_sql, records)
        cursor.connection.commit()
        logging.info("✅ Data successfully inserted into Snowflake!")

    except Exception as e:
        logging.error(f"❌ Error during DB operations: {e}")
        raise  

    finally:
        if cursor:
            cursor.close()


with DAG(
    dag_id='etl_full_refresh_stock_prices',
    start_date=datetime(2024, 3, 1),
    schedule_interval='0 23 * * 1-5', 
    catchup=False,
    tags=["stocks", "snowflake"],
) as dag:

    refresh_stock_data = PythonOperator(
        task_id='full_refresh_stock_prices',
        python_callable=full_refresh_stock_prices
    )

    refresh_stock_data
