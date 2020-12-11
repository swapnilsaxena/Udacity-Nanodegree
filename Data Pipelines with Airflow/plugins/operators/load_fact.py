from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 sql_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table_name
        self.sql_query = sql_query   

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Running delete statement")
        redshift.run("DELETE FROM {}".format(self.table_name))        
        
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table_name,
            self.sql_query
        )
        self.log.info(f"Executing {formatted_sql} ...")
        redshift.run(formatted_sql)        