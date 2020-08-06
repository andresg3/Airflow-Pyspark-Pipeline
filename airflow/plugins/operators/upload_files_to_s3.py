import airflow.hooks.S3_hook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class Upload2S3(BaseOperator):

    ui_color = '#ffcfdb'

    @apply_defaults
    def __init__(self, filename="", bucket_name="", *args, **kwargs):
        super(Upload2S3, self).__init__(*args, **kwargs)
        self.filename = filename
        self.bucket_name = bucket_name

    def upload_files_to_s3(self, filename, bucket_name):  # filename, key, bucket_name):
        # filename = '/home/andresg3/PycharmProjects/Airflow-Pyspark-Pipeline/data_files/file.csv'
        key = 'file66.csv'
        # bucket_name = 'books-s3-landing'
        hook = airflow.hooks.S3_hook.S3Hook('my_S3_conn')
        hook.load_file(filename, key, bucket_name)
        self.log.info(f'{filename} uploaded to S3')

    def execute(self, context):
        self.upload_files_to_s3(self.filename, self.bucket_name)