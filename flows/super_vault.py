import json
import os
import requests
from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from prefect import task, flow, get_run_logger
from prefect.exceptions import PrefectException
from prefect.tasks import NO_CACHE

load_dotenv(".env")

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
        logger.info("Fetching SuperVault contract data...")
        
        # Basic contract data
        whitelist = supervault.functions.getWhitelist().call()
        vault_data = supervault.functions.getSuperVaultData().call()
        
        # Limits and configurations
        deposit_limit = supervault.functions.depositLimit().call()
        available_deposit_limit = supervault.functions.availableDepositLimit(vault_address).call()
        available_withdraw_limit = supervault.functions.availableWithdrawLimit(vault_address).call()
        number_of_superforms = supervault.functions.numberOfSuperforms().call()
        
        # Core addresses
        strategist = supervault.functions.strategist().call()
        vault_manager = supervault.functions.vaultManager().call()
        tokenized_strategy = supervault.functions.tokenizedStrategyAddress().call()
        
        # Print the information
        print("\n=== SuperVault Information ===")
        print(f"\nWhitelist: {whitelist}")
        print("\nVault Data:")
        print(f"- Superform IDs: {vault_data[0]}")
        print(f"- Weights: {vault_data[1]}")
        print("\nLimits:")
        print(f"- Deposit Limit: {deposit_limit}")
        print(f"- Available Deposit Limit: {available_deposit_limit}")
        print(f"- Available Withdraw Limit: {available_withdraw_limit}")
        print(f"- Number of Superforms: {number_of_superforms}")
        print("\nCore Addresses:")
        print(f"- Strategist: {strategist}")
        print(f"- Vault Manager: {vault_manager}")
        print(f"- Tokenized Strategy: {tokenized_strategy}")
        
        return {
            'whitelist': whitelist,
            'vault_data': vault_data,
            'deposit_limit': deposit_limit,
            'available_deposit_limit': available_deposit_limit,
            'available_withdraw_limit': available_withdraw_limit,
            'number_of_superforms': number_of_superforms,
            'strategist': strategist,
            'vault_manager': vault_manager,
            'tokenized_strategy': tokenized_strategy
        }
        
    except Exception as e:
        logger.error("Error fetching supervault info: %s", str(e))
        raise PrefectException("Failed to fetch supervault info") from e

@flow(name="SuperVault Flow")
def supervault_flow():
    chain_id = int(os.getenv('CHAIN_ID', 1))
    vault_address = os.getenv('VAULT_ADDRESS')
    
    logger = get_run_logger()
    logger.info(f"Starting SuperVault flow with chain_id={chain_id}, vault_address={vault_address}")
    
    supervault = initialize_supervault(chain_id, vault_address)
    contract_info = print_supervault_info(supervault, vault_address)
    return contract_info

if __name__ == "__main__":
    supervault_flow()