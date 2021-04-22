from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    #sql_template_dim_table = """
    #DROP TABLE IF EXISTS {destination_table};
    #/**/
    #CREATE TABLE {destination_table} AS
    #SELECT distinct {columns_csv_delim},
    #FROM {origin_table};
    #"""
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 source_table="",
                 dest_table="",
                 sql_dml_insert="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id,
        self.source_table=source_table,
        self.dest_table=dest_table,
        self.sql_dml_insert=sql_dml_insert

    def execute(self, context):
        self.log.info('LoadDimensionOperator - {source_table}\t-->\t{dest_table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = LoadDimensionOperator.sql_template_dim_table.format(
            origin_table=self.origin_table,
            dest_table=self.dest_table,
            fact_column=self.fact_column,
            sql_dml_insert=self.sql_dml_insert
        )
        redshift.run(sql)

        

        
        