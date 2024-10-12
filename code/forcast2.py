from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def train(cur, train_input_table, train_view, forecast_function_name):

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS 
                            SELECT 
                                TO_TIMESTAMP(DATE, 'YYYY-MM-DD') AS DATE, 
                                CLOSE, 
                                SYMBOL
                            FROM {train_input_table}
                            ORDER BY DATE;"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table}
        ORDER BY DATE;"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise

with DAG(
    dag_id = 'IBM_STOCK_PRICE_TrainPredict',
    start_date = datetime(2024,10,10),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule = '50 6 * * *'
) as dag:

    train_input_table = "TRIP_DB.LAB1.STOCK_IBM"
    train_view = "TRIP_DB.LAB1.STOCK_IBM_ViEW"
    forecast_table = "TRIP_DB.LAB1.STOCK_IBM_FORECAST"
    forecast_function_name = "TRIP_DB.LAB1.IBM_STOCK_PRICE_PREDICT"
    final_table = "TRIP_DB.LAB1.IBM_FINAL"
    cur = return_snowflake_conn()

    train(cur, train_input_table, train_view, forecast_function_name)
    predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)