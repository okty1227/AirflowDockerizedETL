# airflow basic usage
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator

import os
import json
import logging


POSTGRE_TABLE_NAME = 'glsdr_job_catalog'
POSTGRE_ID = 'postgres_default' # Airflow postgre connection id
DEFAULT_AIRFLOW_ARGS = {'retries':1}

def get_working_dir(folder_name:str):
    """
    Get the absolute path of a specified folder within the working directory.

    Parameters:
    - folder_name (str): The name of the folder within the working directory.

    Returns:
    - str: The absolute path of the specified folder within the working directory.
    """
    dir_name = os.path.dirname(os.path.abspath(__file__))
    working_dir = os.path.normpath(os.path.join(dir_name,folder_name))   
     
    return working_dir

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s')


def read_crawler_conf():  
    '''read the position key words from config file conf_cralwerJobs.json'''
    conf_folder = get_working_dir(folder_name='../config')
    file_full_name = os.path.normpath(os.path.join(conf_folder,'conf_cralwerJobs.json'))
    
    with open(file=file_full_name) as conf_file:
        cf = json.load(conf_file)
        
        logging.info(f'Successfully read crawlerJob conifg')
        
    return cf


def read_json(file_name):
    '''read lownloaded json file'''
    input_dir = get_working_dir(folder_name='../input_files')
    file_full_name = os.path.normpath(os.path.join(input_dir,file_name))
    
    with open(file=file_full_name,mode='r') as file:
        data = file.read()
        return json.loads(data)

def load_json(*args):
    '''import json files into postgreSQL'''
    for job in args:
        
        raw_df = read_json(file_name=job)

        for rw in range(len(raw_df)):
            
            info = raw_df[rw]
            job_name = job
            comp_name = info['position_info']['company_name'],
            comp_rate = info['position_info']['company_rate'],
            position_name = info['position_info']['position_name'],
            comp_loc_state = info['position_info']['company_location_state'],
            salary_yr = info['avg_salary']['salary'],
            salary_unit = info['avg_salary']['hourly_or_yearly_rate'],
            if_easy_apply = info['if_easy_apply'],
            can_python = info['jd']['Python'],
            can_ml_1 = info['jd']['machine learning'],
            can_ml_2 = info['jd']['ML'],
            can_ai = info['jd']['AI'],
            can_sql = info['jd']['SQL'],
            can_spark = info['jd']['Spark'],
            can_tableau = info['jd']['Tableau'],
            can_r = info['jd']['R'],
            can_java = info['jd']['Java'],
            can_scala = info['jd']['Scala'],
            size = info['size'],
            foundYear = info['found_year'],
            sector = info['sector']
            
            row = (job_name, comp_name, comp_rate, position_name, comp_loc_state,salary_yr,salary_unit, if_easy_apply,
                    can_python, can_ml_1, can_ml_2, can_ai, can_sql, can_spark, can_tableau,
                    can_r, can_java, can_scala, size, foundYear, sector)

            postg_hook = PostgresHook(postgres_conn_id=POSTGRE_ID)
            
            insert_cmd = f"""
                INSERT INTO {POSTGRE_TABLE_NAME} (
                    job_name, comp_name, comp_rate, position_name, comp_loc_state, salary_yr,salary_unit, if_easy_apply,
                    can_python, can_ml_1, can_ml_2, can_ai, can_sql, can_spark, can_tableau,
                    can_r, can_java, can_scala, size, foundYear, sector
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::BOOLEAN, %s::BOOLEAN, %s::BOOLEAN, %s::BOOLEAN, %s::BOOLEAN,
                        %s::BOOLEAN, %s::BOOLEAN, %s::BOOLEAN, %s::BOOLEAN, %s::BOOLEAN, %s::BOOLEAN, %s,%s,%s);
            """
            postg_hook.run(insert_cmd,parameters=row)

def get_file_names():
    '''load file list from lownloaded json files'''
    input_dir = get_working_dir(folder_name='../input_files')
    
    return os.listdir(input_dir)


with DAG(
    dag_id = 'glsdr_etl',
    schedule_interval = '@weekly',
    start_date = datetime(2023,12,29),
    default_args = DEFAULT_AIRFLOW_ARGS,
    dagrun_timeout= timedelta(minutes=120)
    
)as dag:
    
    say_start = BashOperator(
        task_id='start_call',
        bash_command="pip install selenium"  ## pending: test deletion
    )
    
    job_tasks = []
    
    for searched_key in read_crawler_conf():
        
        searched_key_name = searched_key['job_name']
        searched_key_url = searched_key['position_url']
        searched_limit = searched_key["search_amt"]
        
        exe_start = BashOperator(
            task_id=f"scrape_{searched_key_name}",
            bash_command=f'export job_name="{searched_key_name}" && export job_link="{searched_key_url}" && export searchlimit="{searched_limit}" '
                        f'&& python /opt/airflow/crawler_util/main.py',
        )

        job_tasks.append(exe_start)
        
    
    
    say_start >> job_tasks
