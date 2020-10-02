from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.staging_queries import create_staging_schema, drop_staging_tables, create_staging_tables, copy_staging_tables
from helpers.warehouse_queries import create_warehouse_schema, create_warehouse_tables
from helpers.upsert_queries import upsert_queries
import psycopg2
import configparser
from pathlib import Path
import logging

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[1]}/helpers/warehouse_config.cfg"))

log = logging.getLogger(__name__)

class WarehouseOp(BaseOperator):
    ui_color = '#80bda8'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(WarehouseOp, self).__init__(*args, **kwargs)

    def redshift_conn(self):
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        return conn, cur

    def conn_test(self, cur):
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        print(db_version)

    def execute_query(self, conn, cur, query_list):
        for query in query_list:
            cur.execute(query)
            conn.commit()

    def execute(self, context):
        conn, cur = self.redshift_conn()

        # fix logging. Use: self.log.info("Query ran successfully!!") like in analytics_operator
        log.info("Creating Staging Schema.")
        self.execute_query(conn, cur, [create_staging_schema])

        log.info("Dropping Staging Tables.")
        self.execute_query(conn, cur, drop_staging_tables)

        log.info("Creating Staging Tables.")
        self.execute_query(conn, cur, create_staging_tables)

        log.info("Loading Staging Tables.")
        self.execute_query(conn, cur, copy_staging_tables)

        log.info("Creating Warehouse Schema.")
        self.execute_query(conn, cur, [create_warehouse_schema])

        log.info("Creating Warehouse Tables.")
        self.execute_query(conn, cur, create_warehouse_tables)

        log.info("Running Upsert Queries.")
        self.execute_query(conn, cur, upsert_queries)






