import json
import os
import requests
from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from prefect import task, Flow, get_run_logger
from prefect.exceptions import PrefectException

load_dotenv(".env.local")

class SuperformConfig:
    def __init__(self, chain_id):
        if chain_id != 1:
            raise Exception("Only Ethereum chain_id is supported")
        
        self.chain_id = chain_id
        self.chain_name = 'Ethereum'
        self.rpc = 'https://eth.public-rpc.com'
        self.w3 = Web3(Web3.HTTPProvider(self.rpc))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.timeout = 30

        # Load contract addresses and ABI's
        with open("addresses.json") as file:
            self.deployments = json.load(file)[str(self.chain_id)]
        with open("abi/erc20.json") as file:
            self.erc20_abi = json.load(file)
        with open("abi/erc4626.json") as file:
            self.erc4626_abi = json.load(file)
        with open("abi/erc4626_form.json") as file:
            self.erc4626_form_abi = json.load(file)
        with open("abi/supervault.json") as file:
            self.supervault_abi = json.load(file)

class SuperVault:
    """
    Read values from SuperVault
    """
    def __init__(self, chain_id, vault_address):
        self.config = SuperformConfig(chain_id)
        self.vault_address = vault_address
        with open("abi/supervault.json") as file:
            self.supervault_abi = json.load(file)
        self.supervault = self.config.w3.eth.contract(
            address=vault_address,
            abi=self.supervault_abi
        )

    def get_whitelisted_vaults(self):
        vaults = self.supervault.functions.getWhitelist().call()
        return vaults
    
    def get_supervault_data(self):
        data = self.supervault.functions.getSuperVaultData().call()
        return data
    
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
        action = f'vaults'
        response = self._request(action)
        return response

    def get_supervaults(self):
        action = f'stats/vault/supervaults'
        response = self._request(action)
        return response
    
    def get_vault_data(self, superform_id):
        action = f'vault/{superform_id}'
        response = self._request(action)
        return response

@task
def initialize_supervault(chain_id, vault_address):
    logger = get_run_logger()
    try:
        logger.info("Initializing SuperVault for chain_id: %s and vault_address: %s", chain_id, vault_address)
        config = SuperformConfig(chain_id)
        supervault = config.w3.eth.contract(
            address=vault_address,
            abi=config.supervault_abi
        )
        logger.info("SuperVault initialized successfully.")
        return supervault
    except Exception as e:
        logger.error("Error initializing SuperVault: %s", str(e))
        raise PrefectException("Failed to initialize SuperVault") from e

@task
def get_whitelisted_vaults(supervault):
    logger = get_run_logger()
    try:
        logger.info("Fetching whitelisted vaults.")
        vaults = supervault.functions.getWhitelist().call()
        logger.info("Whitelisted vaults fetched successfully.")
        return vaults
    except Exception as e:
        logger.error("Error fetching whitelisted vaults: %s", str(e))
        raise PrefectException("Failed to fetch whitelisted vaults") from e

@task
def get_supervault_data(supervault):
    logger = get_run_logger()
    try:
        logger.info("Fetching supervault data.")
        data = supervault.functions.getSuperVaultData().call()
        logger.info("Supervault data fetched successfully.")
        return data
    except Exception as e:
        logger.error("Error fetching supervault data: %s", str(e))
        raise PrefectException("Failed to fetch supervault data") from e

with Flow("SuperVault Flow") as flow:
    chain_id = 1  # Ethereum chain ID
    vault_address = os.getenv('VAULT_ADDRESS')  # Get address from .env.local
    supervault = initialize_supervault(chain_id, vault_address)
    whitelisted_vaults = get_whitelisted_vaults(supervault)
    supervault_data = get_supervault_data(supervault)

# To run the flow
if __name__ == "__main__":
    flow.run()