import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

import sys
sys.path.append("/opt/airflow/transformations")
from fbref_squad_data_transformation import _squad_data_transformation
from fbref_players_data_transformation import _players_data_transformation

dag = DAG(
    dag_id="data_transformation_to_snowflake",
    start_date=airflow.utils.dates.days_ago(3),
    schedule=None,
    template_searchpath=["/opt/airflow/sql", "/opt/airflow/transformations"]
)

squad_data_transformation = PythonOperator(
    task_id="squad_data_transformation",
    python_callable=_squad_data_transformation,
    dag=dag,
)

create_table_squad_data_to_postgres = SnowflakeOperator(
    task_id="create_table_squad_data_to_snowflake",
    snowflake_conn_id="snowflake_conn",
    sql="create_table_squad_data_query.sql",
    dag=dag
)

write_squad_data_to_postgres = SnowflakeOperator(
    task_id="write_squad_data_to_snowflake",
    snowflake_conn_id="snowflake_conn",
    sql="insert_squad_data_query.sql",
    dag=dag
)

player_data_transformation = PythonOperator(
    task_id="player_data_transformation",
    python_callable=_players_data_transformation,
    dag=dag,
)

create_table_player_data_to_postgres = SnowflakeOperator(
    task_id="create_table_player_data_to_snowflake",
    snowflake_conn_id="snowflake_conn",
    sql="create_table_player_data_query.sql",
    dag=dag
)

write_player_data_to_postgres = SnowflakeOperator(
    task_id="write_player_data_to_snowflake",
    snowflake_conn_id="snowflake_conn",
    sql="insert_player_data_query.sql",
    dag=dag
)


squad_data_transformation >> create_table_squad_data_to_postgres >> \
write_squad_data_to_postgres >> \
player_data_transformation >> create_table_player_data_to_postgres >> \
write_player_data_to_postgres