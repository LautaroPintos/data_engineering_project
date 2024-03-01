from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='DAG-ExamenUno' ,
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=30),
    catchup=False,
    tags=['ingest_examen_uno', 'transform_uno'],
) as dag:

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    inicia_proceso = DummyOperator(
        task_id='inicia_proceso',
    )

    ingest_examen_uno = BashOperator(
        task_id='ingest_examen_uno',
        bash_command='/usr/bin/sh /home/hadoop/scripts/examen_uno/ingest_examen_uno.sh ',
    )


    transform_uno = BashOperator(
        task_id='transform_uno',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen_uno/transform_uno.py ',
    )


    inicia_proceso >> ingest_examen_uno >> transform_uno >> finaliza_proceso


if __name__ == "__main__":
    dag.cli()