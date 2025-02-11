# super-vault
ETH SuperUSDC Data Pipeline

## Setup and Run

### Prerequisites
- Ensure Docker and Docker Compose are installed on your machine.
- Create and configure a `.env` file with necessary environment variables, including `CLICKHOUSE_PASSWORD`, `DBT_PROFILES_DIR`, and `DBT_PROJECT_DIR`.

### Steps to Run the Pipeline

1. **Start Docker Services**
   - Ensure Docker is running.
   - Start the services using Docker Compose:
     ```bash
     docker-compose up -d
     ```

2. **Connect to ClickHouse Server**
   - Connect using the ClickHouse client:
     ```bash
     docker run -it --rm --link super-vault-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server --password $CLICKHOUSE_PASSWORD
     ```

3. **Run Prefect Flow for super_vault**
   - Execute the Prefect flow to interact with the `super_vault` database:
     ```bash
     python flows/prefect_dbt_core.py
     ```

### Additional Commands

- **Activate Virtual Environment** (if needed for local development):
  ```bash
  uv venv
  source .venv/bin/activate
  uv sync
  ```

- **Run a Different Prefect Flow**:
  ```bash
  python flows/hello_world.py
  ```

### Stopping and Removing Docker Containers

- **Stop Docker Services**:
  ```bash
  docker-compose down
  ```

### Notes
- Ensure that your Docker services are running before executing the Prefect and dbt commands.
- This setup allows you to manage your data pipeline effectively using dbt, ClickHouse, and Prefect, specifically targeting the `super_vault` database.


## uv - venv
```
uv venv
source .venv/bin/activate
uv sync
```

## start prefect server
```
uvx prefect server start
```

## run flow
```
python flows/hello_world.py
```


## start dbt
```
docker-compose run dbt init
docker-compose up -d
```




