from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 sql="",
                 mode="truncate-insert",  
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.mode = mode

    def execute(self, context):
        self.log.info(f"Connecting to Redshift for dimension table {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        if self.mode == 'truncate-insert':
            self.log.info(f"Truncating table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Inserting data into dimension table {self.table}")
        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql}
        """
        redshift.run(insert_sql)

        self.log.info(f"Dimension table {self.table} load complete.")
