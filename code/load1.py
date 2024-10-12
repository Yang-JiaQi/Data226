from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import pandas as pd

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def extractTransform():
    r = requests.get(url, params = params)
    data = r.json() # convert json to python dict
    data = data.get('Time Series (Daily)', {})  # extract specific part of the JSON response

    df = pd.DataFrame.from_dict(data, orient='index')
    df.index = pd.to_datetime(df.index) # make sure the date is in datetime format
    df.reset_index(inplace = True)
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume'] # rename columns
    df['symbol'] = params['symbol']
    df = df.sort_values(by='date', ascending=False).head(90) # only need last 90 days
    df['date'] = df['date'].astype(str)  # airflow requires converting timestamp to string.

    return df.to_dict(orient='records')

@task
def load(records):
    # create table of replace it if exist
    sql1 = f"""
    CREATE OR REPLACE TABLE {target_table} (
        date varchar(32) PRIMARY KEY,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT,
        symbol VARCHAR(32)
    )
    """
    cur.execute(sql1)

    try:
        cur.execute("BEGIN")

        # incrumental update
        for row in records:
            sql2 = f"""
            MERGE INTO {target_table} AS target
            USING (SELECT '{row['date']}' AS date,
                          {row['open']} AS open,
                          {row['high']} AS high,
                          {row['low']} AS low,
                          {row['close']} AS close,
                          {row['volume']} AS volume,
                          '{row['symbol']}' AS symbol) AS source
            ON target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET
                    open = source.open,
                    high = source.high,
                    low = source.low,
                    close = source.close,
                    volume = source.volume,
                    symbol = source.symbol
            WHEN NOT MATCHED THEN
                INSERT (date, open, high, low, close, volume, symbol)
                VALUES (source.date, source.open, source.high, source.low, source.close, source.volume, source.symbol);
            """
            cur.execute(sql2)

        cur.execute("COMMIT")
        print("Data upserted successfully and transaction committed.")

    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"Transaction rolled back due to error: {str(e)}")


with DAG(
    dag_id = 'APPL_DATA_ETL',
    start_date = datetime(2024,10,10),
    catchup=False,
    tags=['ETL'],
    schedule='33 6 * * *'
) as dag:
    target_table = "TRIP_DB.LAB1.STOCK_AAPL"
    api = Variable.get("api_key")
    url = Variable.get('alphavantage_url')
    cur = return_snowflake_conn()
    symbol = 'AAPL'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': api,  
        'outputsize': 'compact'
    }

    records = extractTransform()
    load(records)