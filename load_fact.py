from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    def __init__(self,
                 conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info(f"Inserting data into fact table {self.table}")
        insert_sql = f"INSERT INTO {self.table} {self.sql}"
        redshift.run(insert_sql)

        self.log.info(f"Fact table {self.table} load complete.")
