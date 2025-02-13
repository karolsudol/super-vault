#!/bin/bash

# Exit on error
set -e

echo "🚀 Setting up Super-Vault project..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv is not installed. Please install it first:"
    echo "curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Create and activate virtual environment, install dependencies
echo "📦 Setting up Python environment and installing dependencies..."
uv venv
source .venv/bin/activate
uv sync

# Copy environment file if it doesn't exist
echo "🔑 Setting up environment variables..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "⚠️  Please edit .env file with your configurations"
fi

# Start Docker services
echo "🐳 Starting Docker services..."
docker-compose up -d

# Wait for ClickHouse to be ready
echo "⏳ Waiting for ClickHouse to be ready..."
for i in {1..30}; do
    if docker-compose exec clickhouse-server clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
        echo "✅ ClickHouse is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ ClickHouse failed to start"
        exit 1
    fi
    sleep 1
done

# Start Prefect server in the background
echo "🌟 Starting Prefect server..."
prefect server start &
PREFECT_PID=$!

# Wait for Prefect server to be ready
echo "⏳ Waiting for Prefect server to be ready..."
sleep 10

# Deploy flows
echo "🚀 Deploying Prefect flows..."
python flows/deployments.py

# Start Prefect worker
echo "👷 Starting Prefect worker..."
prefect worker start -p "default" &
WORKER_PID=$!

echo """
🎉 Setup complete! Your environment is ready:
- ClickHouse is running
- Prefect server is running (PID: $PREFECT_PID)
- Prefect worker is running (PID: $WORKER_PID)
- Dependencies are installed
- Flows are deployed

To stop the services:
- Press Ctrl+C to stop Prefect
- Run 'docker-compose down' to stop ClickHouse
- Run 'deactivate' to exit the virtual environment

To view the Prefect UI: http://localhost:4200
""" 