from prefect import flow, task
from clickhouse_driver import Client
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Global configuration
CLICKHOUSE_HOST = '127.0.0.1'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_DB = 'super_vault'
BATCH_SIZE = 10000

@task(retries=3)
def create_clickhouse_connection():
    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
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
        query = f'INSERT INTO {CLICKHOUSE_DB}.{table_name} ({columns}) VALUES'
        
        # Execute in batches of 10000
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            client.execute(query, batch)
            
    except Exception as e:
        raise Exception(f"Failed to write data to ClickHouse: {str(e)}")

@flow(name="ClickHouse ETL Pipeline")
def etl_pipeline(data: pd.DataFrame, table_name: str):
    client = create_clickhouse_connection()
    write_dataframe_to_clickhouse(client, data, table_name)

if __name__ == "__main__":
    # Test connection and create database/table
    client = create_clickhouse_connection()
    try:
        # Create database if not exists
        client.execute(f'CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}')
        
        # Switch to the database
        client.execute(f'USE {CLICKHOUSE_DB}')
        
        # Create a sample table
        client.execute('''
            CREATE TABLE IF NOT EXISTS your_table (
                id UInt32,
                name String,
                value UInt32
            ) ENGINE = MergeTree()
            ORDER BY id
        ''')
        
        print("Database and table created successfully!")
        result = client.execute(f'SHOW TABLES FROM {CLICKHOUSE_DB}')
        print("Available tables:", result)
        
    except Exception as e:
        print(f"Setup failed: {e}")

    # Example usage with pandas DataFrame
    df = pd.DataFrame({
        'id': range(1, 1001),
        'name': [f'name_{i}' for i in range(1, 1001)],
        'value': range(1000, 0, -1)
    })
    
    etl_pipeline(df, "your_table")