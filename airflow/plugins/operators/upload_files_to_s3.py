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
        self.archive_filepath = 'data_files/archives'
        self.suffix = str(round(time.time() * 1000))

    def get_hook(self):
        return airflow.hooks.S3_hook.S3Hook(self.s3_conn_id)

    @staticmethod
    def get_key(file, suffix):
        # Generates a new filename for each file being uploaded to s3. example: author_1596807367990.csv
        base = (os.path.basename(file))
        filename = os.path.splitext(base)[0]
        file_type = os.path.splitext(base)[1]
        return filename + '_' + suffix + file_type

    def archive_file(self, file, key):
        dest_filename = os.path.join(self.archive_filepath, key)
        self.log.info(f'Moving {file} to {dest_filename}')

    def upload_files_to_s3(self, all_files):
        for file in all_files:
            key = self.get_key(file, self.suffix)
            self.log.info(f'Uploading {file} to S3')
            # self.hook.load_file(file, key, self.bucket_name)
            # self.archive_file(file, key)

    def execute(self, context):
        all_files = glob.glob(f'{self.filepath}/*.csv')
        self.upload_files_to_s3(all_files)
