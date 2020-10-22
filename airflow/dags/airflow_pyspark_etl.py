from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator

from operators.upload_files_to_s3 import Upload2S3
from operators.move_bucket_data import MoveS3data
from operators.warehouse_operator import WarehouseOp
from operators.analytics_operator import AnalyticsOperator
from operators.data_quality import DataQualityOperator

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
    begin = DummyOperator(task_id='Begin')

    upload_to_S3_task = Upload2S3(
        task_id='Upload_Data_to_S3',
        s3_conn_id='my_S3_conn',
        filepath='data_files',
        bucket_name='books-s3-landing'
    )

    s3_landing_to_working = MoveS3data(
        task_id='S3_Copy_Landing_to_Working',
        s3_conn_id='my_S3_conn',
        src_bucket='books-s3-landing',
        dest_bucket='books-s3-working'
    )

    cmd = 'sh -c "pip install boto3 && spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.3\
                                                    --master local[*] /home/jovyan/transform_s3.py"'
    process_data = DockerOperator(
        task_id='Data_Processing_in_Spark',
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
        task_id='DW_Operations'
    )

    create_analytics_schema = AnalyticsOperator(
        task_id='Create_Analytics_Schema',
        red_conn_id='redshift_dw',
        sql_query=[aq.create_schema]
    )

    create_author_analytics_tables = AnalyticsOperator(
        task_id='Create_Author_Analytics_Tbls',
        red_conn_id='redshift_dw',
        sql_query=[aq.create_author_reviews, aq.create_author_rating, aq.create_best_authors]
    )

    create_book_analytics_tables = AnalyticsOperator(
        task_id='Create_Book_Analytics_Tbls',
        red_conn_id='redshift_dw',
        sql_query=[aq.create_book_reviews, aq.create_book_rating, aq.create_best_books]
    )

    ######## AUTHOR ANALYTICS TASKS  ##########

    load_author_reviews_tbl = AnalyticsOperator(
        task_id='Load_Author_Reviews_Tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_authors_reviews.format('2020-07-01 00:00:00.000000', '2020-08-01 00:00:00.000000')]
    )

    load_author_ratings_tbl = AnalyticsOperator(
        task_id='Load_Author_Ratings_Tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_authors_ratings.format('2020-07-01 00:00:00.000000', '2020-08-01 00:00:00.000000')]
    )

    load_best_authors_tbl = AnalyticsOperator(
        task_id='Load_Best_Authors_Tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_best_authors]
    )

    ######## BOOK ANALYTICS TASKS  ##########

    load_book_reviews_tbl = AnalyticsOperator(
        task_id='Load_Book_Reviews_Tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_books_reviews.format('2020-07-01 00:00:00.000000', '2020-08-01 00:00:00.000000')]
    )

    load_book_ratings_tbl = AnalyticsOperator(
        task_id='Load_Book_Ratings_Tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_books_ratings.format('2020-07-01 00:00:00.000000', '2020-08-01 00:00:00.000000')]
    )

    load_best_books_tbl = AnalyticsOperator(
        task_id='Load_Best_Books_Tbl',
        red_conn_id='redshift_dw',
        sql_query=[aq.populate_best_books]
    )

    warehouse_data_quality = DataQualityOperator(
        task_id='Data_Quality_Checks',
        redshift_conn_id='redshift_dw',
        tables=["books_warehouse.authors", "books_warehouse.reviews", "books_warehouse.books", "books_warehouse.users"]
    )

    end = DummyOperator(task_id='end')

    begin >> upload_to_S3_task >> s3_landing_to_working >> process_data >> warehouse_operations >> warehouse_data_quality >> create_analytics_schema
    create_analytics_schema >> [create_author_analytics_tables, create_book_analytics_tables]
    create_author_analytics_tables >> [load_author_reviews_tbl, load_author_ratings_tbl, load_best_authors_tbl] >> end
    create_book_analytics_tables >> [load_book_reviews_tbl, load_book_ratings_tbl, load_best_books_tbl] >> end
