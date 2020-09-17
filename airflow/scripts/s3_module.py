import boto3
import configparser
from pathlib import Path

config = configparser.ConfigParser()
# config.read_file(open(os.getcwd() + '/config.cfg'))
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

myAccessKey = config['KEYS']['ACCESS_KEY']
mySecretKey = config['KEYS']['SECRET_KEY']


class S3Module:

    def __init__(self):
        self.s3 = boto3.resource(service_name='s3',
                                 aws_access_key_id=myAccessKey,
                                 aws_secret_access_key=mySecretKey)

    def clean_bucket(self, bucket_name):
        print(f"Cleaning bucket: {bucket_name}")
        self.s3.Bucket(bucket_name).objects.all().delete()

    def get_files(self, bucket_name):
        return [bucket_object.key for bucket_object in self.s3.Bucket(bucket_name).objects.all()]
