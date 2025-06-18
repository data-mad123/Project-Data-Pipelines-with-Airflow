from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info('Starting data quality checks')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        for i, test in enumerate(self.tests):
            sql = test.get('check_sql')
            expected_result = test.get('expected_result')

            if not sql or expected_result is None:
                raise ValueError(f"Test #{i} is missing SQL or expected result.")

            self.log.info(f"Running test #{i + 1}: {sql}")
            records = redshift.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Test #{i + 1} failed: No results returned for query: {sql}")

            actual_result = records[0][0]

            if actual_result != expected_result:
                raise ValueError(f"Test #{i + 1} failed: {sql} returned {actual_result}, expected {expected_result}")

            self.log.info(f"Test #{i + 1} passed: {actual_result} == {expected_result}")
