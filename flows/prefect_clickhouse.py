from prefect import flow, task
from clickhouse_driver import Client
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

@task(retries=3)
def create_clickhouse_connection():
    try:
        client = Client(
            host='clickhouse',  # Use 'clickhouse' if running from another container
            port=9000,
            database='super_vault',
            user='default',
            password=os.getenv('CLICKHOUSE_PASSWORD'),
            settings={
                'max_block_size': 100000,
                'max_insert_block_size': 100000,
            }
        )
        return client
    except Exception as e:
        raise Exception(f"Failed to connect to ClickHouse: {str(e)}")

@task
def write_dataframe_to_clickhouse(client: Client, df: pd.DataFrame, table_name: str):
    try:
        # Convert DataFrame to list of tuples
        data = [tuple(x) for x in df.to_numpy()]
        
        # Generate column names
        columns = ', '.join(df.columns)
        
        # Create the insert query
        query = f'INSERT INTO {table_name} ({columns}) VALUES'
        
        # Execute in batches of 10000
        batch_size = 10000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            client.execute(query, batch)
            
    except Exception as e:
        raise Exception(f"Failed to write data to ClickHouse: {str(e)}")

@flow(name="ClickHouse ETL Pipeline")
def etl_pipeline(data: pd.DataFrame, table_name: str):
    client = create_clickhouse_connection()
    write_dataframe_to_clickhouse(client, data, table_name)

if __name__ == "__main__":
    # Example usage with pandas DataFrame
    df = pd.DataFrame({
        'id': range(1, 1001),
        'name': [f'name_{i}' for i in range(1, 1001)],
        'value': range(1000, 0, -1)
    })
    
    etl_pipeline(df, "your_table")