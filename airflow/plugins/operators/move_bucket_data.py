import airflow.hooks.S3_hook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class MoveS3data(BaseOperator):
    ui_color = '#fcc200'

    @apply_defaults
    def __init__(self, s3_conn_id="", src_bucket="", dest_bucket="", *args, **kwargs):
        super(MoveS3data, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.source_bucket = src_bucket
        self.dest_bucket = dest_bucket
        self.hook = self.get_hook()

    def get_hook(self):
        return airflow.hooks.S3_hook.S3Hook(self.s3_conn_id)

    def get_files_list(self):
        return self.hook.list_keys(self.source_bucket)

    def copy_files(self, key):
        self.log.info(f'Copying {key} from {self.source_bucket} to {self.dest_bucket}')
        self.hook.copy_object(source_bucket_key=key,
                              dest_bucket_key=key,
                              source_bucket_name=self.source_bucket,
                              dest_bucket_name=self.dest_bucket)

    def clean_bucket(self, bucket_name, keys):
        self.log.info(f'Removing all files from bucket: {bucket_name} ')
        self.hook.delete_objects(bucket_name, keys)

    def execute(self, context):
        keys = self.get_files_list()
        self.clean_bucket(self.dest_bucket, keys)
        for key in keys:
            self.copy_files(key)