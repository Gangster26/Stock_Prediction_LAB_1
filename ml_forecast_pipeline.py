from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

SNOWFLAKE_CONN_ID = "snowflake_conn"   
FORECASTING_PERIODS = 7 

def get_snowflake_cursor():
    
    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        return hook.get_conn().cursor()
    except Exception as e:
        print(f"Snowflake connection failed: {str(e)}")
        raise

@task
def train_model():
    cursor = get_snowflake_cursor()
    try:
        cursor.execute("BEGIN")
        
        cursor.execute("""
            CREATE OR REPLACE VIEW STOCK_LAB1.PUBLIC.STOCK_DATA_VIEW AS
            SELECT DATE, CLOSE, STOCK_SYMBOL
            FROM STOCK_LAB1.PUBLIC.STOCK_PRICES
        """)
        
        cursor.execute("""
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST STOCK_LAB1.PUBLIC.PREDICT_STOCK_PRICE (
                INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'STOCK_LAB1.PUBLIC.STOCK_DATA_VIEW'),
                SERIES_COLNAME => 'STOCK_SYMBOL',
                TIMESTAMP_COLNAME => 'DATE',
                TARGET_COLNAME => 'CLOSE',
                CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
            )
        """)
        
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Model training failed: {str(e)}")
        raise
    finally:
        cursor.close()

@task
def generate_forecast():
    cursor = get_snowflake_cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute(f"""
            CALL STOCK_LAB1.PUBLIC.PREDICT_STOCK_PRICE!FORECAST(
                FORECASTING_PERIODS => {FORECASTING_PERIODS},
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            )
        """)
        cursor.execute("SELECT LAST_QUERY_ID()") 
        forecast_query_id = cursor.fetchone()[0]
        

        cursor.execute(f"""
            CREATE OR REPLACE TABLE STOCK_LAB1.PUBLIC.STOCK_DATA_FINAL AS
            SELECT 
                STOCK_SYMBOL, 
                DATE, 
                CLOSE AS ACTUAL, 
                NULL AS FORECAST, 
                NULL AS LOWER_BOUND, 
                NULL AS UPPER_BOUND
            FROM STOCK_LAB1.PUBLIC.STOCK_PRICES
            WHERE DATE < CURRENT_DATE()  
            UNION ALL
            SELECT 
                REPLACE(SERIES::STRING, '"', '') AS STOCK_SYMBOL, 
                TS::DATE AS DATE, 
                NULL AS ACTUAL, 
                FORECAST::FLOAT, 
                LOWER_BOUND::FLOAT, 
                UPPER_BOUND::FLOAT
            FROM TABLE(RESULT_SCAN('{forecast_query_id}'))
        """)
        
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Forecast generation failed: {str(e)}")
        raise
    finally:
        cursor.close()


with DAG(
    dag_id="ml_forecast_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    train_model() >> generate_forecast()