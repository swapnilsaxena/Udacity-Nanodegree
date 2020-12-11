from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 delete_load = False,
                 table_name = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.delete_load = delete_load

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.delete_load:
            self.log.info("Running delete statement ")
            redshift_hook.run("DELETE FROM {}".format(self.table_name))
            
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table_name,
            self.sql_squery
        )
        self.log.info(f"Executing {formatted_sql} ...")
        redshift.run(formatted_sql)