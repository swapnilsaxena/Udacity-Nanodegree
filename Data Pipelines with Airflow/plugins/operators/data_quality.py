from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.checks = checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
                
        if len(self.checks)<0:
            self.log.info("No data quality checks provided")
            return
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        error_count = 0
        failing_tests = []
            
        for check in self.checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            records = redshift.get_records(sql)[0]

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)   
        
        if error_count > 0:
            raise ValueError('Data quality check failed')
        else:
            self.log.info("All data quality checks passed")