from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from operators.upload_files_to_s3 import Upload2S3
from operators.move_bucket_data import MoveS3data
from operators.warehouse_operator import WarehouseOp
from operators.analytics_operator import AnalyticsOperator
from helpers.analytics_queries import AnalyticsQueries as aq
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

    upload_to_S3_task = Upload2S3(
        task_id='upload_to_s3',
        s3_conn_id='my_S3_conn',
        filepath='data_files',
        bucket_name='books-s3-landing'
    )

    s3_landing_to_working = MoveS3data(
        task_id='s3_landing_to_working_copy',
        s3_conn_id='my_S3_conn',
        src_bucket='books-s3-landing',
        dest_bucket='books-s3-working'
    )

    cmd = 'sh -c "pip install boto3 && spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.3\
                                                    --master local[*] /home/jovyan/transform_s3.py"'
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

    warehouse_operations = WarehouseOp(
        task_id='warehouse_ops'
    )

    create_analytics_schema = AnalyticsOperator(
        task_id='create_analytics_schema',
        red_conn_id='redshift_dw',
        sql_query=[aq.create_schema]
    )

    create_author_analytics_tables = AnalyticsOperator(
        task_id='create_author_analytics_tables',
        red_conn_id='redshift_dw',
        sql_query=[aq.create_author_reviews, aq.create_author_rating, aq.create_best_authors]
    )

    create_book_analytics_tables = AnalyticsOperator(
        task_id='create_book_analytics_tables',
        red_conn_id='redshift_dw',
        sql_query=[aq.create_book_reviews, aq.create_book_rating, aq.create_best_books]
    )

    ######## AUTHOR ANALYTICS TASKS  ##########
    # 2020-07-26
    load_author_reviews_tbl = AnalyticsOperator(
        task_id='load_author_reviews_tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_authors_reviews.format('2020-07-01 00:00:00.000000', '2020-08-01 00:00:00.000000')]
    )

    load_author_ratings_tbl = AnalyticsOperator(
        task_id='load_author_ratings_tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_authors_ratings.format('2020-07-01 00:00:00.000000', '2020-08-01 00:00:00.000000')]
    )

    load_best_authors_tbl = AnalyticsOperator(
        task_id='load_best_authors_tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_best_authors]
    )

    ######## BOOK ANALYTICS TASKS  ##########

    load_book_reviews_tbl = AnalyticsOperator(
        task_id='load_book_reviews_tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_books_reviews.format('2020-07-01 00:00:00.000000', '2020-08-01 00:00:00.000000')]
    )

    load_book_ratings_tbl = AnalyticsOperator(
        task_id='load_book_ratings_tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_books_ratings.format('2020-07-01 00:00:00.000000', '2020-08-01 00:00:00.000000')]
    )

    load_best_books_tbl = AnalyticsOperator(
        task_id='load_best_books_tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_best_books]
    )

    end = DummyOperator(task_id='end')

    # begin >> upload_to_S3_task >> move_data_in_s3 >> process_data >> end
    # begin >> process_data >> end
    # begin >> move_data_in_s3 >> end
    # begin >> upload_to_S3_task >> end
    # begin >> warehouse_operations >> end

    begin >> upload_to_S3_task >> s3_landing_to_working >> process_data >> warehouse_operations >> create_analytics_schema
    create_analytics_schema >> [create_author_analytics_tables, create_book_analytics_tables]
    create_author_analytics_tables >> [load_author_reviews_tbl, load_author_ratings_tbl, load_best_authors_tbl] >> end
    create_book_analytics_tables >> [load_book_reviews_tbl, load_book_ratings_tbl, load_best_books_tbl] >> end

    # begin >> end
