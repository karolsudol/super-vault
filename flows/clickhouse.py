from prefect import flow, task
from clickhouse_driver import Client
import pandas as pd
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global configuration
CLICKHOUSE_HOST = '127.0.0.1'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'
CLICKHOUSE_DB = 'default'
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
        logger.info("Successfully connected to ClickHouse")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {str(e)}")
        raise

@flow(name="Create ClickHouse Table")
def create_table_flow():
    client = create_clickhouse_connection()
    try:
        # Create a sample table
        client.execute('''
            CREATE TABLE IF NOT EXISTS your_table (
                id UInt32,
                name String,
                value UInt32
            ) ENGINE = MergeTree()
            ORDER BY id
        ''')
        logger.info("Table 'your_table' created successfully")
        
        # Verify table creation
        tables = client.execute('SHOW TABLES')
        logger.info(f"Available tables: {tables}")
        
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        raise

@flow(name="Write Data to ClickHouse")
def write_data_flow(data: pd.DataFrame):
    client = create_clickhouse_connection()
    try:
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(x) for x in data.to_numpy()]
        columns = ', '.join(data.columns)
        query = f'INSERT INTO your_table ({columns}) VALUES'
        
        # Execute in batches
        total_rows = 0
        for i in range(0, len(data_tuples), BATCH_SIZE):
            batch = data_tuples[i:i + BATCH_SIZE]
            client.execute(query, batch)
            total_rows += len(batch)
            logger.info(f"Inserted batch of {len(batch)} rows. Total rows inserted: {total_rows}")
            
        logger.info(f"Successfully wrote {total_rows} rows to ClickHouse")
        
    except Exception as e:
        logger.error(f"Failed to write data: {str(e)}")
        raise

@flow(name="Read Data from ClickHouse")
def read_data_flow():
    client = create_clickhouse_connection()
    try:
        # Read and log row count
        count = client.execute('SELECT COUNT(*) FROM your_table')[0][0]
        logger.info(f"Total rows in table: {count}")
        
        # Read sample data
        sample_data = client.execute('SELECT * FROM your_table LIMIT 5')
        logger.info(f"Sample data from table: {sample_data}")
        
        return sample_data
        
    except Exception as e:
        logger.error(f"Failed to read data: {str(e)}")
        raise

if __name__ == "__main__":
    # Create table
    create_table_flow()
    
    # Prepare sample data
    df = pd.DataFrame({
        'id': range(1, 1001),
        'name': [f'name_{i}' for i in range(1, 1001)],
        'value': range(1000, 0, -1)
    })
    
    # Write data
    write_data_flow(df)
    
    # Read data
    read_data_flow()