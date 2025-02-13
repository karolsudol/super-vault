# super-vault
ETH SuperUSDC Data Pipeline


### Steps to Run the Pipeline

1. **Install**
   - Clone the repo:
     ```bash
     git clone git@github.com:karolsudol/super-vault.git
     cd super-vault
     ```

   - Install Python dependencies:
     ```bash
     uv sync
     source .venv/bin/activate
     ```

   - Install dbt and ClickHouse:
     ```bash
     docker-compose up -d
     ```

   - Wait for the ClickHouse server to be ready:
     ```bash
      docker-compose exec clickhouse-server clickhouse-client --query "SELECT * FROM system.databases"
     ```

   - Run sql query to check if the ClickHouse server is ready:
     ```sql
     SELECT * FROM system.databases;
     ```

2. **Run the dbt project**
   - Run the dbt project:
     ```bash
     dbt run
     ```

3. **Run the pipeline**
   - Run the pipeline:
     ```bash
     cp .env.example .env
     prefect server start
     python flows/super_vault.py
     ```

4. **Clean up**
   - Clean up:
     ```bash
     deactivate
     docker-compose down
     ```
