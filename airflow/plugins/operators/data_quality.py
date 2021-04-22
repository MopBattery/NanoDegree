from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name       = table_name
        
    def execute(self, context):
        self.log.info(f"DataQualityOperator : {self.table_name} started")      
        #        table = kwargs["params"]["table"]
        redshift_hook = PostgresHook(self.redshift_conn_id)
        tab = self.table_name
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {tab}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {tab} returned no results")
        else:
            rowcount = records[0][0]

        self.log.info(f"Data quality on table {tab} check passed with {rowcount} rows, {records} records")   
        self.log.info("DataQualityOperator : "+self.table_name+" finished")