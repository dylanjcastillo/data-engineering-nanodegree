from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query="",
                 append=True,
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.append = append
        self.table = table

    def execute(self, context):
        self.log.info(f"Connecting to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
                
        if not self.append:
            self.log.info(f"Deleting contents from table {self.table}")
            redshift.run(f"TRUNCATE {self.table}")
        
        self.log.info(f"Executing insert query: {self.query}")
        redshift.run(self.query)
        
        
        
        
