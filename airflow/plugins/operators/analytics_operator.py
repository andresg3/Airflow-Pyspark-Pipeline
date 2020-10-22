from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class AnalyticsOperator(BaseOperator):
    ui_color = '#35b53d'

    @apply_defaults
    def __init__(self, red_conn_id="", sql_query=[], *args, **kwargs):
        super(AnalyticsOperator, self).__init__(*args, **kwargs)
        self.red_conn_id = red_conn_id
        self.sql_query = sql_query

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.red_conn_id)
        for query in self.sql_query:
            # https://github.com/apache/airflow/blob/master/airflow/hooks/dbapi_hook.py
            self.log.info(f"Running Analytics query : {query}")
            redshift_hook.run(self.sql_query)
            self.log.info("Query ran successfully!!")
