import json
import os
import requests
from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from prefect import task, flow, get_run_logger
from prefect.exceptions import PrefectException
from prefect.tasks import NO_CACHE
import pandas as pd
from clickhouse import create_clickhouse_connection

load_dotenv(".env")

BATCH_SIZE = 10000 

class SuperformConfig:
    def __init__(self, chain_id):
        if chain_id != 1:
            raise Exception("Only Ethereum chain_id is supported")
        
        self.chain_id = chain_id
        self.chain_name = 'Ethereum'
        self.rpc = 'https://eth.llamarpc.com'
        self.w3 = Web3(Web3.HTTPProvider(self.rpc))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.timeout = 30

        # Load contract addresses and ABI's
        with open("abi/erc20.json") as file:
            self.erc20_abi = json.load(file)
        with open("abi/erc4626.json") as file:
            self.erc4626_abi = json.load(file)
        with open("abi/erc4626_form.json") as file:
            self.erc4626_form_abi = json.load(file)
        with open("abi/super_vault.json") as file:
            self.supervault_abi = json.load(file)

class SuperformAPI:
    def __init__(self):
        self.url = 'https://api.superform.xyz/'
        self.api_key = os.getenv('SUPERFORM_API_KEY')

    def _request(self, action):
        url = self.url + action
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'SF-API-KEY': self.api_key
        }
        response = requests.get(url, headers=headers)
        result = json.loads(response.text)
        return result

    def get_vaults(self):
        action = 'vaults'
        response = self._request(action)
        return response

    def get_supervaults(self):
        action = 'stats/vault/supervaults'
        response = self._request(action)
        return response
    
    def get_vault_data(self, superform_id):
        action = f'vault/{superform_id}'
        response = self._request(action)
        return response

@task(cache_policy=NO_CACHE)
def initialize_supervault(chain_id, vault_address):
    logger = get_run_logger()
    try:
        if not vault_address:
            raise ValueError("VAULT_ADDRESS environment variable is not set")
            
        logger.info("Initializing SuperVault with parameters:")
        logger.info(f"Chain ID: {chain_id}")
        logger.info(f"Vault Address: {vault_address}")
        
        # Check if files exist before loading
        required_files = [
            "abi/erc20.json",
            "abi/erc4626.json",
            "abi/erc4626_form.json",
            "abi/super_vault.json"
        ]
        
        for file_path in required_files:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Required file not found: {file_path}")
        
        config = SuperformConfig(chain_id)
        supervault = config.w3.eth.contract(
            address=vault_address,
            abi=config.supervault_abi
        )
        
        # Verify connection to the blockchain
        try:
            config.w3.eth.get_block('latest')
            logger.info("Successfully connected to blockchain")
        except Exception as e:
            raise Exception(f"Failed to connect to blockchain: {str(e)}")
            
        logger.info("SuperVault initialized successfully.")
        return supervault
    except Exception as e:
        logger.error("Error initializing SuperVault: %s", str(e))
        raise PrefectException(f"Failed to initialize SuperVault: {str(e)}") from e

@task(cache_policy=NO_CACHE)
def print_supervault_info(supervault, vault_address):
    logger = get_run_logger()
    try:
        # Basic contract data
        whitelist = supervault.functions.getWhitelist().call()
        
        # Create DataFrame from whitelist
        data = [
            {
                'form_id': form_id,
                'form_id_hex': hex(form_id),
                'chain_id': supervault.w3.eth.chain_id,
                'vault_address': vault_address
            }
            for form_id in whitelist
        ]
        
        # Create and print DataFrame
        df_supervault = pd.DataFrame(data)
        
        # Print detailed information
        print("\n=== SuperVault Information ===")
        print(f"Vault Address: {vault_address}")
        print("\nWhitelist Data:")
        print(df_supervault.to_string())
        
        # Get additional data
        vault_data = supervault.functions.getSuperVaultData().call()
        deposit_limit = supervault.functions.depositLimit().call()
        available_deposit = supervault.functions.availableDepositLimit(vault_address).call()
        available_withdraw = supervault.functions.availableWithdrawLimit(vault_address).call()
        num_superforms = supervault.functions.numberOfSuperforms().call()
        strategist = supervault.functions.strategist().call()
        vault_manager = supervault.functions.vaultManager().call()
        tokenized_strategy = supervault.functions.tokenizedStrategyAddress().call()
        
        print("\nVault Metrics:")
        print(f"Deposit Limit: {deposit_limit}")
        print(f"Available Deposit Limit: {available_deposit}")
        print(f"Available Withdraw Limit: {available_withdraw}")
        print(f"Number of Superforms: {num_superforms}")
        print(f"Strategist: {strategist}")
        print(f"Vault Manager: {vault_manager}")
        print(f"Tokenized Strategy: {tokenized_strategy}")
        print("===========================\n")
        
        return {
            'whitelist': whitelist,
            'vault_data': vault_data,
            'deposit_limit': deposit_limit,
            'available_deposit_limit': available_deposit,
            'available_withdraw_limit': available_withdraw,
            'number_of_superforms': num_superforms,
            'strategist': strategist,
            'vault_manager': vault_manager,
            'tokenized_strategy': tokenized_strategy
        }
        
    except Exception as e:
        logger.error("Error fetching supervault info: %s", str(e))
        raise PrefectException("Failed to fetch supervault info") from e

@task(cache_policy=NO_CACHE)
def format_supervault_data(contract_info, vault_address):
    logger = get_run_logger()
    try:
        # Create DataFrame for whitelist data
        whitelist_data = pd.DataFrame([{
            'form_id': str(form_id),  # Convert to string to handle large numbers
            'form_id_hex': hex(form_id),
            'vault_address': vault_address,
            'timestamp': pd.Timestamp.now()
        } for form_id in contract_info['whitelist']])

        # Explicitly set data types for whitelist DataFrame
        whitelist_data = whitelist_data.astype({
            'form_id': 'string',  # Changed from uint64 to string
            'form_id_hex': 'string',
            'vault_address': 'string',
            'timestamp': 'datetime64[ns]'
        })

        # Create DataFrame for vault metrics
        vault_metrics = pd.DataFrame([{
            'vault_address': vault_address,
            'deposit_limit': str(contract_info['deposit_limit']),  # Convert to string
            'available_deposit_limit': str(contract_info['available_deposit_limit']),
            'available_withdraw_limit': str(contract_info['available_withdraw_limit']),
            'number_of_superforms': int(contract_info['number_of_superforms']),
            'strategist': contract_info['strategist'],
            'vault_manager': contract_info['vault_manager'],
            'tokenized_strategy': contract_info['tokenized_strategy'],
            'timestamp': pd.Timestamp.now()
        }])

        # Explicitly set data types for metrics DataFrame
        vault_metrics = vault_metrics.astype({
            'vault_address': 'string',
            'deposit_limit': 'string',  # Changed from uint64 to string
            'available_deposit_limit': 'string',  # Changed from uint64 to string
            'available_withdraw_limit': 'string',  # Changed from uint64 to string
            'number_of_superforms': 'uint64',  # This one should be small enough to keep as uint64
            'strategist': 'string',
            'vault_manager': 'string',
            'tokenized_strategy': 'string',
            'timestamp': 'datetime64[ns]'
        })

        return {
            'whitelist': whitelist_data,
            'metrics': vault_metrics
        }
    except Exception as e:
        logger.error("Error formatting supervault data: %s", str(e))
        raise PrefectException("Failed to format supervault data") from e

@flow(name="Create Table Flow")
def create_table_flow(query: str):
    logger = get_run_logger()
    client = create_clickhouse_connection()
    try:
        # Extract table name from the query
        table_name = query.split('CREATE TABLE IF NOT EXISTS ')[1].split(' ')[0]
        
        # Drop existing table
        drop_query = f"DROP TABLE IF EXISTS {table_name}"
        client.execute(drop_query)
        logger.info(f"Dropped table {table_name} if it existed")
        
        # Create new table
        client.execute(query)
        logger.info(f"Table {table_name} created successfully")
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        raise

@flow(name="Write Data Flow")
def write_data_flow(data: pd.DataFrame, table_name: str):
    logger = get_run_logger()
    client = create_clickhouse_connection()
    try:
        # Convert DataFrame to list of dictionaries for better control over data types
        records = data.to_dict('records')
        
        # Insert data using named parameters
        query = f'INSERT INTO {table_name} VALUES'
        client.execute(query, records)
            
        logger.info(f"Successfully wrote {len(records)} rows to ClickHouse")
    except Exception as e:
        logger.error(f"Failed to write data: {str(e)}")
        raise

@flow(name="Get form ids from SuperVault Flow")
def supervault_flow():
    chain_id = int(os.getenv('CHAIN_ID', 1))
    vault_address = os.getenv('VAULT_ADDRESS')
    
    logger = get_run_logger()
    logger.info(f"Starting SuperVault flow with chain_id={chain_id}, vault_address={vault_address}")
    
    # Initialize and get supervault data
    supervault = initialize_supervault(chain_id, vault_address)
    contract_info = print_supervault_info(supervault, vault_address)
    
    # Format data for ClickHouse
    formatted_data = format_supervault_data(contract_info, vault_address)
    
    # Create ClickHouse tables if they don't exist
    whitelist_query = """
        CREATE TABLE IF NOT EXISTS supervault_whitelist (
            form_id UInt64,
            form_id_hex String,
            vault_address String,
            timestamp DateTime
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, form_id)
    """
    create_table_flow(whitelist_query)
    
    # metrics_query = """
    #     CREATE TABLE IF NOT EXISTS supervault_metrics (
    #         vault_address String,
    #         deposit_limit UInt256,
    #         available_deposit_limit UInt256,
    #         available_withdraw_limit UInt256,
    #         number_of_superforms UInt64,
    #         strategist String,
    #         vault_manager String,
    #         tokenized_strategy String,
    #         timestamp DateTime
    #     ) ENGINE = MergeTree()
    #     ORDER BY (timestamp, vault_address)
    # """
    # create_table_flow(metrics_query)
    
    # Write data to ClickHouse
    # write_data_flow(formatted_data['whitelist'], 'supervault_whitelist')
    # write_data_flow(formatted_data['metrics'], 'supervault_metrics')
    
    return contract_info

if __name__ == "__main__":
    supervault_flow()