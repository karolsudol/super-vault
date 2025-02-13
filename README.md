# super-vault
ETH SuperUSDC Data Pipeline

### Quick Setup

1. **Prerequisites**
   - Install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
   - Install [uv](https://github.com/astral-sh/uv):
     ```bash
     curl -LsSf https://astral.sh/uv/install.sh | sh
     ```

2. **Installation**
   ```bash
   git clone git@github.com:karolsudol/super-vault.git
   cd super-vault
   chmod +x setup.sh
   ./setup.sh
   ```

3. **Configuration**
   - Edit the `.env` file with your specific configurations
   - Access the Prefect UI at http://localhost:4200

4. **Cleanup**
   ```bash
   # Stop Prefect server and worker
   pkill -f "prefect"
   
   # Stop ClickHouse
   docker-compose down
   
   # Deactivate virtual environment
   deactivate
   ```

### Additional Information
For detailed information about the pipeline and its components, please refer to the documentation in the `docs/` directory.
