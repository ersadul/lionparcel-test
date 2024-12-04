## Fresh setup airflow

create dir for dags, logs, plugins, and config
```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

initialize the database
`docker-compose up airflow-init`

start all services
`docker compose up`

## databases 
### postgres
expose the port so we can connect from outside (to simplify the task, we use 1 postgres db)  
and create another database named "lp-db". and if you want to connect into postgres docker (use default user:pass [airflow:airflow])

```    
	ports:
	  - 5432:5432
```

setup virtual environment 
```
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
```

### setup datawarehouse (bigquery)

setup bq service account credentials at airflow common env in file docker-compose.yaml
```
environment:
      ...
      GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/keys/service-account-key.json

volumes:
      ...
      - ./keys:/opt/airflow/keys
```

## Airflow UI
access through `http://localhost:8080/home` and login using default credentials  
- user: airflow 
- password: airflow