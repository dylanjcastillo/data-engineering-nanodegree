from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ExecuteQueryFromFileOperator(BaseOperator):
    
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_file="",
                 *args, **kwargs):
        super(ExecuteQueryFromFileOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_file = query_file

    def execute(self, context):
        self.log.info(f"Connecting to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Reading queries from {self.query_file}")        
        with open(self.query_file, "r") as f:
            queries = f.read()
        
        for query in queries.split(";")[:-1]:
            self.log.info(f"Executing query: {query}")
            redshift.run(query)