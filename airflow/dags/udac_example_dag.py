from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import ( StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator )
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner'               : 'udacity',
    'start_date'          : datetime(2019, 1, 12),
    'end_date'            : datetime(2019, 2, 12),
    #
    'depends_on_past'     : False,
    'retries'             : 0,
    'retry_delay'         : timedelta(minutes=5),
    'catchup'             : False,
    'email_on_retry'      : False
}

schedule_interval='0 * * * *'   #   commented out during build, run dag manually....
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        )

staging_songs_table  = "staging_songs"
staging_events_table = "staging_events"
fact_songplays_table = "songplays"
dim_users_table      = "users"
dim_time_table       = "time"
dim_artists_table    = "artists"
dim_songs_table      = "songs"

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#stage_events_to_redshift = StageToRedshiftOperator(
#    task_id='Stage_events__S3_to_Redshift',
#    dag=dag,
#    s3_bucket="udacity-dend",
#    s3_key="log-data",#>>>>>>>>>>>>>>>>>>>>     Should this be log_data or log-data     ????
#    redshift_conn_id="redshift",
#    aws_creds_id="aws_credentials",
#    redshift_table_name=staging_events_table,
#    ignore_headers=1
#)

s3_bucket="udacity-dend"
s3_key="log-data"                          #>>>>>>>>>>>>>>>>>>>>    Should this be log_data or log-data     ????
s3_key="log-data/2018/11/*.json"      #>>>>>>>>>>>>>>>>>>>>    Should this be song_data or song-data   ????    
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events__S3_to_Redshift',
    dag=dag,
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    redshift_table_name=staging_events_table
)

s3_key="song-data"                                                 #>>>>>>>>>>>>>>>>>>>>    Should this be song_data or song-data   ????
s3_key="song-data/A/A/A/*.json"                                    #>>>>>>>>>>>>>>>>>>>>    Should this be song_data or song-data   ????
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs__S3_to_Redshift',
    dag=dag,
    redshift_conn_id="redshift",
    aws_creds_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    redshift_table_name=staging_songs_table,
    ignore_headers=0
)

load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    source_table=staging_songs_table,
    dest_table=fact_songplays_table,
    time_table_insert=SqlQueries.songplay_table_insert
)

#load_user_dim_table = LoadDimensionOperator(
#    task_id='Load_user_dim_table',
#    dag=dag
#)

#load_song_dim_table = LoadDimensionOperator(
#    task_id='Load_songs_dim_table',
#    dag=dag,
#    redshift_conn_id="redshift",
#    source_table=staging_songs_table,
#    dest_table=dim_time_table,
#    time_table_insert=SqlQueries.time_table_insert
#)

#load_artist_dim_table = LoadDimensionOperator(
#    task_id='Load_artist_dim_table',
#    dag=dag,
#    redshift_conn_id="redshift",
#    source_table=staging_songs_table,
#    dest_table=dim_time_table,
#    time_table_insert=SqlQueries.time_table_insert
#)

load_time_dim_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    source_table=staging_songs_table,
    dest_table=dim_time_table,
    time_table_insert=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_name=dim_time_table
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Dependencies...
start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift
#
stage_events_to_redshift  >> load_songplays_fact_table
#stage_events_to_redshift  >> load_user_dim_table
#
#stage_songs_to_redshift   >> load_song_dim_table
#stage_songs_to_redshift   >> load_artist_dim_table
#
load_songplays_fact_table >> load_time_dim_table
#
load_time_dim_table       >> run_quality_checks
#
run_quality_checks        >> end_operator



