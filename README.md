# super-vault
ETH SuperUSDC Data Pipeline


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
```




