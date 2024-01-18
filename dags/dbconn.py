# airflow basic usage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# airflow external usage
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import os
import json
import logging

def get_working_dir(folder_name):
    
    dir_name = os.path.dirname(os.path.abspath(__file__))
    working_dir = os.path.normpath(os.path.join(dir_name,folder_name))   
     
    return working_dir


postgre_table_name = 'glsdr_job_catalog'
postgre_id = 'postgres_default' # airflow postgre connection id
default_airflow_args = {'retries':0}


logfile_pth = get_working_dir('../config') # set logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s')

def read_crawler_conf():  
    '''read the position key words from config file conf_cralwer_jobs.json'''
    conf_folder = get_working_dir(folder_name='../config')
    file_full_name = os.path.normpath(os.path.join(conf_folder,'conf_cralwer_jobs.json'))
    
    with open(file=file_full_name) as conf_file:
        cf = json.load(conf_file)
        
        logging.info(cf)
        # logging.shutdown()  
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

            postg_hook = PostgresHook(postgres_conn_id=postgre_id)
            
            insert_cmd = f"""
                INSERT INTO {postgre_table_name} (
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
    dag_id = 'glsdr_dbconn',
    schedule_interval = '@weekly',
    start_date = datetime(2023,12,29),
    default_args = default_airflow_args,
    dagrun_timeout= timedelta(minutes=120)
    
)as dag:
    
    start_conn = BashOperator(
        task_id='start_conn',
        bash_command="echo db connection started"  ## pending: move to dockerfile
    )
        
    init_postgre = PostgresOperator(
        task_id = 'init_postgre',
        postgres_conn_id = postgre_id,
            sql=f''' CREATE TABLE IF NOT EXISTS {postgre_table_name} (
                    job_name VARCHAR(255),
                    comp_name VARCHAR(255),
                    comp_rate VARCHAR(255),
                    position_name VARCHAR(255),
                    comp_loc_state VARCHAR(255),
                    salary_unit VARCHAR(255),
                    salary_yr VARCHAR(255),
                    if_easy_apply BOOLEAN,
                    can_python BOOLEAN,
                    can_ml_1 BOOLEAN,
                    can_ml_2 BOOLEAN,
                    can_ai BOOLEAN,
                    can_sql BOOLEAN,
                    can_spark BOOLEAN,
                    can_tableau BOOLEAN,
                    can_r BOOLEAN,
                    can_java BOOLEAN,
                    can_scala BOOLEAN,
                    size VARCHAR(255),
                    foundYear VARCHAR(255),
                    sector VARCHAR(255)
                    );''' )
    
    load_data = PythonOperator(
        task_id='load_output',
        python_callable=load_json,
        op_args = get_file_names()
    )
    
    
    
    
    # ## pending:: replace with dbt
    # add_dup_rows = PostgresOperator(
    #     task_id = 'add_dup_vals',
    #     postgres_conn_id = postgre_id,
    #     sql = '''
    #             DROP TABLE addedrownum;
                
    #             CREATE TABLE addedrownum AS                    
    #             SELECT job_name, comp_name, salary_yr
    #             FROM postgre_glsd
    #             WHERE (job_name, comp_name, salary_yr) IN (
    #             SELECT job_name, comp_name, salary_yr
    #             FROM (
    #                 SELECT
    #                 job_name, comp_name, salary_yr,
    #                 ROW_NUMBER() OVER (PARTITION BY job_name, comp_name, salary_yr) AS row_num
    #                 FROM postgre_glsd
    #             ) AS dup_rk_table
    #             WHERE row_num = 1
    #             )
                
    #           '''
    # )
    # clean_na_val = PostgresOperator(
    #     task_id = 'clean_na',
    #     postgres_conn_id = postgre_id,
    #     sql = f'''

    #     DELETE FROM postgre_glsd
    #     WHERE foundYear='none' OR foundYear='--' 
    #           OR size='Unknown' OR salary_yr='none'
    #           OR comp_loc_state = 'none' ;
        
    #     '''
    # )
    
    # process_bar_chart = PostgresOperator(
    #     task_id = 'execute_bar_postgre',
    #     sql = '''
    #             ALTER TABLE postgre_glsd 
    #             ADD COLUMN IF NOT EXISTS foundYear_int INT;
    #                     UPDATE postgre_glsd
                        
    #             SET foundYear_int = CAST(foundYear AS INTEGER);
                
    #             CREATE TABLE count_year_sec AS
    #             SELECT
    #             COUNT(*) AS count,
    #             CASE
    #                 WHEN foundYear_int < 1750 THEN 'LESS THAN 1750'
    #                 WHEN foundYear_int BETWEEN 1750 AND 1799 THEN '1750-1799'
    #                 WHEN foundYear_int BETWEEN 1800 AND 1849 THEN '1850-1849'
    #                 WHEN foundYear_int BETWEEN 1850 AND 1899 THEN '1850-1899'
    #                 WHEN foundYear_int BETWEEN 1900 AND 1909 THEN '1910-1919'
    #                 WHEN foundYear_int BETWEEN 1910 AND 1919 THEN '1910-1919'
    #                 WHEN foundYear_int BETWEEN 1920 AND 1929 THEN '1920-1929'
    #                 WHEN foundYear_int BETWEEN 1930 AND 1939 THEN '1930-1939'
    #                 WHEN foundYear_int BETWEEN 1940 AND 1949 THEN '1940-1949'
    #                 WHEN foundYear_int BETWEEN 1950 AND 1959 THEN '1950-1959'
    #                 WHEN foundYear_int BETWEEN 1960 AND 1969 THEN '1960-1969'
    #                 WHEN foundYear_int BETWEEN 1970 AND 1979 THEN '1970-1979'
    #                 WHEN foundYear_int BETWEEN 1980 AND 1989 THEN '1980-1989'
    #                 WHEN foundYear_int BETWEEN 1990 AND 1999 THEN '1990-1999'
    #                 WHEN foundYear_int BETWEEN 2000 AND 2009 THEN '2000-2009'
    #                 WHEN foundYear_int BETWEEN 2010 AND 2019 THEN '2010-2019'
    #                 WHEN foundYear_int BETWEEN 2020 AND 2029 THEN '2020-2029'
    #                 ELSE 'Unknown'
    #             END AS decade, sector
    #             FROM postgre_glsd 
    #             GROUP BY decade, sector
    #             ORDER BY decade, sector;
    #         '''
    # )
    # say_start >> init_postgre >> job_tasks >> load_data
    # say_start >> job_tasks

    init_postgre >> load_data