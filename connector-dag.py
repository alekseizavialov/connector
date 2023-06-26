from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import requests
import psycopg2

def cannabis_connector():
    respone = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=10').content.decode('utf8')
    json_respone = json.loads(respone)
    connection = psycopg2.connect(user="postgres",
                          password="password",
                          host="172.26.128.1",
                          port=5432,
                          database="postgres")
    cursor = connection.cursor()
    postgres_insert_query = """INSERT INTO cannabis (id, uid, strain, cannabinoid_abbreviation, 
    cannabinoid, terpene, medical_use, health_benefit, category, "type", buzzword, brand)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    rows_inserted = 0
    for cannabis in json_respone:
        record_to_insert = (cannabis["id"], cannabis["uid"], cannabis["strain"],
                            cannabis["cannabinoid_abbreviation"], cannabis["cannabinoid"], cannabis["terpene"],
                            cannabis["medical_use"], cannabis["health_benefit"], cannabis["category"],
                            cannabis["type"], cannabis["buzzword"], cannabis["brand"])
        try:
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            row_count = cursor.rowcount
        except:
            row_count = 0
        rows_inserted += row_count

args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='connector-dag',
    default_args=args,
    schedule_interval=timedelta(hours=12)
)

with dag:
    get_cannabis = PythonOperator(
        task_id='cannabis',
        python_callable=cannabis_connector,
    )