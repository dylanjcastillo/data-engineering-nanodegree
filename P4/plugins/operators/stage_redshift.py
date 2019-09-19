from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
    COPY {table}
    FROM '{s3_path}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    JSON '{json_format}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 json_format="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.aws_credentials_id = aws_credentials_id
        self.json_format = json_format

    def execute(self, context):
        
        self.log.info("Getting AWS credentials for S3")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        self.log.info("Connecting to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Deleting contents from {self.table} table")
        redshift.run(f"TRUNCATE {self.table}")

        self.log.info(f"Copying data from S3 to {self.table} table")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        if self.json_format != "auto":
            self.json_format = f"s3://{self.json_format}"
            
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                table=self.table,
                s3_path=s3_path,
                access_key=credentials.access_key,
                secret_key=credentials.secret_key,
                json_format=self.json_format,
            )
        
        self.log.info(f"Running copy query: {formatted_sql})
        redshift.run(formatted_sql)


