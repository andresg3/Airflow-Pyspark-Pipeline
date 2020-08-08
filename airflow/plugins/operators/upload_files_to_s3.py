import airflow.hooks.S3_hook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import time
import glob
import os


class Upload2S3(BaseOperator):
    ui_color = '#ffcfdb'

    @apply_defaults
    def __init__(self, s3_conn_id="", filepath="", bucket_name="", *args, **kwargs):
        super(Upload2S3, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.filepath = filepath
        self.bucket_name = bucket_name
        self.hook = self.get_hook()

    def get_hook(self):
        return airflow.hooks.S3_hook.S3Hook(self.s3_conn_id)

    @staticmethod
    def get_key(file, suffix):
        base = (os.path.basename(file))
        # this generates a "key" or filename to upload to s3. example: author_1596807367990.csv
        key = os.path.splitext(base)[0] + '_' + suffix + os.path.splitext(base)[1]
        return key

    def upload_files_to_s3(self, all_files):
        suffix = str(round(time.time() * 1000))
        for file in all_files:
            self.hook.load_file(file, self.get_key(file, suffix), self.bucket_name)
            self.log.info(f'Uploading {file} to S3')

    def execute(self, context):
        all_files = glob.glob(f'{self.filepath}/*.csv')
        self.upload_files_to_s3(all_files)