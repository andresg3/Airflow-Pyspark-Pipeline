from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.docker_operator import DockerOperator
from operators.upload_files_to_s3 import Upload2S3
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
        poke_interval=60
    )

    # t2 = BashOperator(
    #     task_id='Start_of_Dag',
    #     bash_command='echo FOUND FILES *************************************'
    # )
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

    upload_to_S3_task = Upload2S3(
        task_id='upload_files_to_S3',
        # python_callable=lambda **kwargs: print("Uploading file to S3")
        s3_conn_id = 'my_S3_conn',
        filepath='data_files',
        bucket_name='books-s3-landing'
    )

    end = DummyOperator(task_id='End')

    begin >> upload_to_S3_task >> end