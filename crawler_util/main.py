import os
import json

from utils import get_parent_working_dir
from crawler import CONNECTION
from preprocess import PREPROCESS


def run_main(job_link,job_name,search_limit):
        
    glassdoor_connect = CONNECTION(url=job_link,full_search=False,search_limit=search_limit,job_name=job_name)
    result = glassdoor_connect.main()
    
    processed = PREPROCESS.run_preprocess(crawed_raw_data=result)
    input_folder = get_parent_working_dir(folder_name='input_files')
    
    with open(f'{input_folder}/{job_name}_glassdoor.json','w') as file:
        json.dump(obj=processed,fp=file) 


if __name__ == '__main__':
    
    joblink = os.environ.get('job_link')
    jobname = os.environ.get('job_name')
    search_limit = os.environ.get('searchlimit')

    run_main(job_link=joblink,job_name=jobname,search_limit=search_limit)

