import pandas as pd
import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="scraping_dag",
    start_date=airflow.utils.dates.days_ago(1),
    schedule=None,
    )

get_fbref_players = BashOperator(
    task_id="get_fbref_players",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_spider",
    dag=dag,
    )

get_fbref_players_goalkeeping = BashOperator(
    task_id="fbref_players_goalkeeping",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_goalkeeping_spider",
    dag=dag,
    )

get_fbref_advanced_goalkeeping = BashOperator(
    task_id="fbref_advanced_goalkeeping",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_advanced_goalkeeping_spider",
    dag=dag,
    )

get_fbref_players_shooting = BashOperator(
    task_id="fbref_players_shooting",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_shooting_spider",
    dag=dag,
    )

get_fbref_players_passing = BashOperator(
    task_id="fbref_players_passing",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_passing_spider",
    dag=dag,
    )

get_fbref_players_passtype = BashOperator(
    task_id="fbref_players_passtype",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_passtype_spider",
    dag=dag,
    )

get_fbref_players_goal_and_shot = BashOperator(
    task_id="fbref_players_goal_and_shot",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_goal_and_shot_spider",
    dag=dag,
    )

get_fbref_players_defensive = BashOperator(
    task_id="fbref_players_defensive",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_defensive_spider",
    dag=dag,
    )

get_fbref_players_possession = BashOperator(
    task_id="fbref_players_possession",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_possession_spider",
    dag=dag,
    )

get_fbref_players_playing_time = BashOperator(
    task_id="fbref_players_playing_time",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_playing_time_spider",
    dag=dag,
    )

get_fbref_players_miscellaneous = BashOperator(
    task_id="fbref_players_miscellaneous",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fbref_players_miscellaneous_spider",
    dag=dag,
    )


def _get_team_data():
    links = ['stats', 'keepers', 'keepersadv', 'shooting', 'passing',
             'passing_types', 'gca', 'defense', 'possession', 'playingtime',
             'misc']

    for link in links:
        stats = pd.read_html(
            f"https://fbref.com/en/comps/1/{link}/World-Cup-Stats#stats_keeper")
        df_squad = stats[0].droplevel(0, axis=1)

        with open(f'/opt/airflow/data/squad_data_{link}.csv', 'w') as file:
            file.write('')

        df_squad.to_csv(f"/opt/airflow/data/squad_data_{link}.csv", index=False)


get_fbref_teams_data = PythonOperator(
    task_id="get_fbref_teams_data",
    python_callable=_get_team_data,
    dag=dag,
    )

get_fifa_world_ranking = BashOperator(
    task_id="get_fifa_world_ranking",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl fifa_world_ranking_spider",
    dag=dag,
    )

get_transfermarkt_players_value_data = BashOperator(
    task_id="get_transfermarkt_players_value",
    bash_command="cd /opt/airflow/src/src && sleep 10 && scrapy crawl transfermarkt_spider",
    dag=dag,
    )

get_fbref_players >> get_fbref_players_goalkeeping >> \
get_fbref_advanced_goalkeeping >> get_fbref_players_shooting >> \
get_fbref_players_passing >> get_fbref_players_passtype >> \
get_fbref_players_goal_and_shot >> \
get_fbref_players_defensive >> \
get_fbref_players_possession >> get_fbref_players_playing_time >> \
get_fbref_players_miscellaneous >> get_fbref_teams_data >> \
get_fifa_world_ranking >> get_transfermarkt_players_value_data