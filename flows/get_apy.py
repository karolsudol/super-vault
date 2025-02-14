import json
from datetime import datetime
from dotenv import load_dotenv
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from prefect import task, flow, get_run_logger
from prefect.exceptions import PrefectException
import time
from prefect.tasks import NO_CACHE

load_dotenv(".env")

class FormConfig:
    def __init__(self):
        self.rpc = 'https://eth.llamarpc.com'
        self.w3 = Web3(Web3.HTTPProvider(self.rpc))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        # Load contract ABI
        with open("abi/erc4626_form.json") as file:
            self.form_abi = json.load(file)

@task(cache_policy=NO_CACHE)
def initialize_form(form_address):
    logger = get_run_logger()
    try:
        config = FormConfig()
        form = config.w3.eth.contract(
            address=form_address,
            abi=config.form_abi
        )
        
        logger.info(f"Successfully initialized form at {form_address}")
        return form
    except Exception as e:
        logger.error(f"Error initializing form: {str(e)}")
        raise PrefectException(f"Failed to initialize form: {str(e)}") from e

@task(cache_policy=NO_CACHE)
def get_form_metrics(form):
    logger = get_run_logger()
    try:
        # Get basic form information
        vault_name = form.functions.getVaultName().call()
        vault_symbol = form.functions.getVaultSymbol().call()
        vault_decimals = form.functions.getVaultDecimals().call()
        vault_address = form.functions.getVaultAddress().call()
        asset_address = form.functions.getVaultAsset().call()
        
        # Get current metrics
        total_assets = form.functions.getTotalAssets().call()
        total_supply = form.functions.getTotalSupply().call()
        price_per_share = form.functions.getPricePerVaultShare().call()
        
        metrics = {
            'vault_name': vault_name,
            'vault_symbol': vault_symbol,
            'vault_decimals': vault_decimals,
            'vault_address': vault_address,
            'asset_address': asset_address,
            'total_assets': total_assets,
            'total_supply': total_supply,
            'price_per_share': price_per_share,
            'timestamp': datetime.now()
        }
        
        logger.info("Form metrics retrieved successfully")
        logger.info(f"Vault Name: {vault_name}")
        logger.info(f"Price per share: {price_per_share / 10**vault_decimals}")
        
        return metrics
    except Exception as e:
        logger.error(f"Error getting form metrics: {str(e)}")
        raise PrefectException(f"Failed to get form metrics: {str(e)}") from e

@task(cache_policy=NO_CACHE)
def calculate_apy(initial_metrics, final_metrics, blocks_elapsed):
    logger = get_run_logger()
    try:
        # Get price points
        initial_price = initial_metrics['price_per_share']
        final_price = final_metrics['price_per_share']
        decimals = initial_metrics['vault_decimals']
        
        # Convert to float with proper decimals
        initial_price_float = initial_price / 10**decimals
        final_price_float = final_price / 10**decimals
        
        # Calculate return for the period
        period_return = (final_price_float / initial_price_float) - 1
        
        # Estimate blocks per year (Ethereum averages ~7200 blocks per day)
        blocks_per_year = 7200 * 365
        
        # Calculate APY
        apy = ((1 + period_return) ** (blocks_per_year / blocks_elapsed) - 1) * 100
        
        logger.info(f"Calculated APY: {apy:.2f}%")
        return apy
    except Exception as e:
        logger.error(f"Error calculating APY: {str(e)}")
        raise PrefectException(f"Failed to calculate APY: {str(e)}") from e

@flow(name="Calculate Form APY Flow")
def form_apy_flow():
    form_address = "0x473b1CE36Dec21Fc1275c4032731C8469BFf371a"
    blocks_per_day = 7200  # Approximate blocks per day
    days_to_track = 2
    
    logger = get_run_logger()
    logger.info(f"Starting Form APY flow for address {form_address} for last {days_to_track} days")
    
    # Initialize form
    form = initialize_form(form_address)
    
    # Get current block number
    current_block = form.w3.eth.block_number
    
    # Calculate block numbers for the last 2 days
    block_checkpoints = [
        current_block - (blocks_per_day * i) 
        for i in range(days_to_track + 1)
    ]
    
    # Get metrics for each checkpoint
    metrics_by_block = {}
    for block_number in block_checkpoints:
        logger.info(f"Getting metrics for block {block_number}")
        metrics = get_form_metrics(form)
        metrics_by_block[block_number] = metrics
        
    # Calculate APY for each day
    results = []
    for i in range(days_to_track):
        start_block = block_checkpoints[i + 1]
        end_block = block_checkpoints[i]
        blocks_elapsed = end_block - start_block
        
        daily_apy = calculate_apy(
            metrics_by_block[start_block],
            metrics_by_block[end_block],
            blocks_elapsed
        )
        
        results.append({
            'day': i + 1,
            'start_block': start_block,
            'end_block': end_block,
            'blocks_elapsed': blocks_elapsed,
            'apy': daily_apy
        })
        logger.info(f"Day {i + 1} APY: {daily_apy:.2f}%")
    
    return {
        'metrics_by_block': metrics_by_block,
        'daily_results': results
    }

if __name__ == "__main__":
    form_apy_flow()