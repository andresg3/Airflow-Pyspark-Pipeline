from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta
import os
from pathlib import Path


default_args = {
    'owner': 'airflow',
    'description': 'Pyspark ETL Pipeline',
    'depend_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

vols = ['/home/andresg3/PycharmProjects/Airflow-Pyspark-Pipeline/airflow/scripts:/home/jovyan']

with DAG('docker_dag', default_args=default_args, schedule_interval="@once", catchup=False) as dag:

    begin = DummyOperator(task_id='Begin')

    waiting_file_task = FileSensor(
        task_id='waiting_file_task',
        fd_conn_id='fs_default',
        filepath='/home/andresg3/PycharmProjects/Airflow-Pyspark-Pipeline/data_files',
        poke_interval=1
    )

    t2 = BashOperator(
        task_id='Start_of_Dag',
        bash_command='echo FOUND FILES *************************************'
    )
    # t2 = DockerOperator(
    #     task_id='spark_submit',
    #     image='jupyter/pyspark-notebook',
    #     # image='jupyter/all-spark-notebook',
    #     api_version='auto',
    #     auto_remove=False,
    #     docker_url="unix://var/run/docker.sock",
    #     host_tmp_dir='/tmp',
    #     tmp_dir='/tmp',
    #     volumes=vols,
    #     command='spark-submit --master local[*] /home/jovyan/pyspark_test01.py'
    # )

    end = DummyOperator(task_id='End')

    begin >> waiting_file_task >> t2 >> end
