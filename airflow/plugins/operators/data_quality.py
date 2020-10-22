from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#b0ffbe'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:

            self.log.info(f"Starting data validation on table {table}")
            records = redshift_hook.get_records(f"SELECT count(*) FROM {table};")
            # records example: [(1283,)]

            if records[0][0] < 1:
                self.log.error("Data Quality validation failed for table : {}.".format(table))
                raise ValueError("Data Quality validation failed for table : {}".format(table))
            self.log.info("Data Quality Validation Passed on table : {}!!!".format(table))
