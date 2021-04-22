from airflow.contrib.hooks.aws_hook import AwsHook #### added this trying to get it working
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#from plugins.operators.s3_to_redshift import S3ToRedshiftOperator  #### this import caues errors with start.sh

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'#########################################################################waht is this for????
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_creds_id="aws_credentials",
                 redshift_table_name="",
                 s3_bucket="udacity-dend",
                 s3_key="",
                 delim=",",
                 ignore_headers=1,
                 *args,
                 **kwargs
                ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id    =   redshift_conn_id
        self.aws_creds_id        =   aws_creds_id
        self.redshift_table_name =   redshift_table_name
        self.s3_bucket           =   s3_bucket
        self.s3_key              =   s3_key
        self.delim               =   delim
        self.ignore_headers      =   ignore_headers

    def execute(self, context):
        self.log.info('StageToRedshiftOperator - Started')
        aws_hook    =   AwsHook(self.aws_creds_id)
        credentials =   aws_hook.get_credentials()
        redshift    =   PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.redshift_table_name))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f'S3 path: {s3_path}')
        
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delim
        )
        
        redshift.run(formatted_sql)



