from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.docker_operator import DockerOperator
from operators.upload_files_to_s3 import Upload2S3
from operators.move_bucket_data import MoveS3data
from datetime import datetime, timedelta
import os
from pathlib import Path

default_args = {
    'owner': 'airflow',
    'description': 'Pyspark ETL Pipeline',
    'depend_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}

local_src = os.path.join(Path(__file__).parents[1], 'scripts')

with DAG('docker_dag', default_args=default_args, schedule_interval="@once", catchup=False) as dag:
    begin = DummyOperator(task_id='begin')

    # waiting_file_task = FileSensor(
    #     task_id='waiting_file_task',
    #     fd_conn_id='fs_default',
    #     filepath='/home/andresg3/PycharmProjects/Airflow-Pyspark-Pipeline/data_files',
    #     poke_interval=60
    # )
    cmd = 'sh -c "pip install boto3 && spark-submit --master local[*] /home/jovyan/transform.py"'
    process_data = DockerOperator(
        task_id='process_data',
        image='jupyter/pyspark-notebook:acb539921413',
        # image='jupyter/all-spark-notebook',
        api_version='auto',
        auto_remove=False,
        docker_url="unix://var/run/docker.sock",
        host_tmp_dir='/tmp',
        tmp_dir='/tmp',
        volumes=[f'{local_src}:/home/jovyan'],
        command=cmd
    )

    upload_to_S3_task = Upload2S3(
        task_id='upload_to_s3',
        # python_callable=lambda **kwargs: print("Uploading file to S3")
        s3_conn_id='my_S3_conn',
        filepath='data_files',
        bucket_name='books-s3-landing'
    )

    move_data_in_s3 = MoveS3data(
        task_id='copy_from_landing_to_working_zone',
        s3_conn_id='my_S3_conn',
        src_bucket='books-s3-landing',
        dest_bucket='books-s3-working'
    )

    end = DummyOperator(task_id='end')

    begin >> upload_to_S3_task >> move_data_in_s3 >> process_data >> end