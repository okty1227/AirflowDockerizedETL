import re
import json
from typing import List
import os

class PREPROCESS():
    script_directory = os.path.dirname(os.path.abspath(__file__))

    config_path = os.path.join(script_directory, 'config.json')
    flexible_skills = ['SQL', 'Spark' ]
    independen_exist_skills = ['R','AI','ML','Tableau','Java','Scala','Python', 'machine learning']
    
    with open(config_path) as f:
        conf = json.load(f)
    
    flexible_skills = conf['flexible_skills'] if conf['flexible_skills'] else flexible_skills
    independen_exist_skills = conf['independen_exist_skills'] if conf['independen_exist_skills'] else independen_exist_skills
        
    ##skill preprocess
    @staticmethod
    def replace_jd_skill(pos:str):
        ##even if position jd is not found, the return value should be False
        if_skill_exist_stor = {}
        for skill in PREPROCESS.flexible_skills:
            if re.search(skill, pos, re.IGNORECASE):
                if_skill_exist_stor[skill] = True
            else:
                if_skill_exist_stor[skill] = False
                
        for skill in PREPROCESS.independen_exist_skills:
            if re.search(rf'\b{skill}\b', pos, re.IGNORECASE):
                
                if_skill_exist_stor[skill] = True
            else:
                if_skill_exist_stor[skill] = False
        
        return if_skill_exist_stor
    
    @staticmethod
    def check_easy_apply(pos_easy_apply:str):
        ## even if the element is not found, the result should be False
        return True if 'Easy Apply' in pos_easy_apply else False
            

    ##basic information
    @staticmethod
    def break_breakline(pos:str):
        
        
        if pos!='-1':
            basic_info_store = {}
            rgx_split_position_info = r'(.+)\n(.+)\n(.+)\n(.+)'
            result_comp_info = re.search(rgx_split_position_info,pos)
    
            basic_info_store['company_name'] = result_comp_info[1] 
            basic_info_store['company_rate'] = result_comp_info[2] 
            basic_info_store['position_name'] = result_comp_info[3]
            location = result_comp_info[4] if result_comp_info[4] else '--'            
            
            result_state = re.search(r',\s*([^,]+)$', location)
            basic_info_store['company_location_state'] = result_state.group(1) if result_state else '--'
            
            
            result_city = re.search(r'^(.+),', location)
            basic_info_store['company_location_city'] = result_city.group(1).strip() if result_city else '--'


        else:
            basic_info_store['company_name'], basic_info_store['company_rate'], basic_info_store['company_name'] = '--', '--', '--'

            basic_info_store['company_location_state'] = '--'
            basic_info_store['company_location_state'] = '--'
            
        return basic_info_store
    
    ##process salary into yearly basis
    @staticmethod
    def process_salary(pos_salary:str):
        salary_info_store = {}
        pos_salary = re.sub(r"[$,\/\ns]","",pos_salary) # remove unnecessary mark
        result = re.search(r'(\d+).*?(yr|hr)', pos_salary) # string format is eg:13000/yr
        if result:            
            salary, unit = result.group(1), result.group(2)
            salary_info_store['hourly_or_yearly_rate'] = unit
        
            if unit=='yr':
                salary_info_store['salary'] = float(salary) 
            if unit=='hr': # make hourly salary into year-based salary
                salary_info_store['salary'] = float(salary) * 2080
        else:
            # for those companies not providing salary info
            salary_info_store['hourly_or_yearly_rate'] = '--'
            salary_info_store['salary'] = '--'
                        
            
        return salary_info_store
    
    @staticmethod
    def run_preprocess(crawed_raw_data:List) -> List:
        
        for position in crawed_raw_data:
            
            position['jd'] = PREPROCESS.replace_jd_skill(position['jd'])
            position['avg_salary'] = PREPROCESS.process_salary(position['avg_salary'])
            position['position_info'] = PREPROCESS.break_breakline(position['position_info'])
            position['if_easy_apply'] = PREPROCESS.check_easy_apply(position['if_easy_apply'])
        
        return crawed_raw_data

# if __name__=='__main__':
    
#     processed = PREPROCESS.run_preprocess(crawed_raw_data = test_input)
#     print(processed)