import os
import json
# import math
from crawler import CONNECTION
from preprocess import PREPROCESS
# from loadmongo import load_to_mongo_db
import sys

jobs_catalog = {
    'data_scientist':'https://www.glassdoor.com/Job/united-states-data-scientist-jobs-SRCH_IL.0,13_IN1_KO14,28.htm',
    'bi_analyst': 'https://www.glassdoor.com/Job/united-states-business-intelligence-analyst-jobs-SRCH_IL.0,13_IN1_KO14,43.htm',
    'bi_engineer': 'https://www.glassdoor.com/Job/united-states-business-intelligence-engineer-jobs-SRCH_IL.0,13_IN1_KO14,44.htm',
    'dataspecialist':'https://www.glassdoor.com/Job/united-states-data-specialist-jobs-SRCH_IL.0,13_IN1_KO14,29.htm',
    'data_engineer':'https://www.glassdoor.com/Job/united-states-data-engineer-jobs-SRCH_IL.0,13_IN1_KO14,27.htm'
}

def run_main(job_link,job_name):
        glassdoor_connect = CONNECTION(url=job_link,full_search=False,search_limit=10)
        result = glassdoor_connect.main()
        # print(result)
        print(os.path.dirname(os.path.abspath(__file__)))
        processed = PREPROCESS.run_preprocess(crawed_raw_data=result)
        
        with open(f'/opt/airflow/input_files/{job_name}_glassdoor.json','w') as file:
            json.dump(obj=processed,fp=file) 




if __name__ == '__main__':
    
    joblink = os.environ.get('job_link')
    jobname = os.environ.get('job_name')
    print(f'start running{joblink};{jobname}')
    run_main(job_link=joblink,job_name=jobname)

