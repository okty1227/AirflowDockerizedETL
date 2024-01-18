import re
import json
import logging
from typing import List
from utils import get_parent_working_dir

logs_folder = get_parent_working_dir(folder_name="logs")
logging.basicConfig(filename=f'{logs_folder}/crawlerOperation.log',level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class PREPROCESS():

    conf_folder = get_parent_working_dir(folder_name='config')

    with open(f'{conf_folder}/conf_baseSetting.json') as f:
        baseCongig = json.load(f)
    
    flexible_skills = baseCongig['flexible_skills'] 
    independent_exist_skills = baseCongig['independent_exist_skills'] 
    
    unfound_value = baseCongig["default_unfound_val"]    
        
    # Skill preprocess
    @staticmethod
    def replace_jd_skill(pos:str):
        # Even if position jd is not found, the return value should be False
        if_skill_exist_stor = {}
        for skill in PREPROCESS.flexible_skills:
            if re.search(skill, pos, re.IGNORECASE):
                if_skill_exist_stor[skill] = True
            else:
                if_skill_exist_stor[skill] = False
                
        for skill in PREPROCESS.independent_exist_skills:
            if re.search(rf'\b{skill}\b', pos, re.IGNORECASE):
                
                if_skill_exist_stor[skill] = True
            else:
                if_skill_exist_stor[skill] = False
        
        return if_skill_exist_stor
    
    @staticmethod
    def check_easy_apply(pos_easy_apply:str):
        # Even if the element is not found, the result should be False
        return True if 'Easy Apply' in pos_easy_apply else False
            

    # Basic information
    @staticmethod
    def break_breakline(pos:str):
        
        
        if pos != PREPROCESS.unfound_value:
            basic_info_store = {}
            rgx_split_position_info = r'(.+)\n(.+)\n(.+)\n(.+)'
            result_comp_info = re.search(rgx_split_position_info,pos)
    
            basic_info_store['company_name'] = result_comp_info[1] 
            basic_info_store['company_rate'] = result_comp_info[2] 
            basic_info_store['position_name'] = result_comp_info[3]
            location = result_comp_info[4] if result_comp_info[4] else PREPROCESS.unfound_value           
            
            result_state = re.search(r',\s*([^,]+)$', location)
            basic_info_store['company_location_state'] = result_state.group(1) if result_state else PREPROCESS.unfound_value
            
            
            result_city = re.search(r'^(.+),', location)
            basic_info_store['company_location_city'] = result_city.group(1).strip() if result_city else PREPROCESS.unfound_value


        else:
            basic_info_store['company_name'] = PREPROCESS.unfound_value
            basic_info_store['company_rate'] = PREPROCESS.unfound_value
            basic_info_store['company_name'] = PREPROCESS.unfound_value

            basic_info_store['company_location_state'] = PREPROCESS.unfound_value
            basic_info_store['company_location_state'] = PREPROCESS.unfound_value
            
        return basic_info_store
    
    # Process salary into yearly basis
    @staticmethod
    def process_salary(pos_salary:str):
        salary_info_store = {}
        pos_salary = re.sub(r"[$,\/\ns]","",pos_salary) # Remove unnecessary mark
        result = re.search(r'(\d+).*?(yr|hr)', pos_salary) # String format is eg:13000/yr
        if result:            
            salary, unit = result.group(1), result.group(2)
            salary_info_store['hourly_or_yearly_rate'] = unit
        
            if unit=='yr':
                salary_info_store['salary'] = float(salary) 
            if unit=='hr': # Make hourly salary into year-based salary
                salary_info_store['salary'] = float(salary) * 2080
        else:
            # For those companies not providing salary info
            salary_info_store['hourly_or_yearly_rate'] = PREPROCESS.unfound_value
            salary_info_store['salary'] = PREPROCESS.unfound_value
                        
            
        return salary_info_store
    
    @staticmethod
    def run_preprocess(crawed_raw_data:List) -> List:
        logging.info(msg='Start Preprocessing')
        for position in crawed_raw_data:
            
            position['jd'] = PREPROCESS.replace_jd_skill(position['jd'])
            position['avg_salary'] = PREPROCESS.process_salary(position['avg_salary'])
            position['position_info'] = PREPROCESS.break_breakline(position['position_info'])
            position['if_easy_apply'] = PREPROCESS.check_easy_apply(position['if_easy_apply'])
        
        return crawed_raw_data

# if __name__=='__main__':
    
#     processed = PREPROCESS.run_preprocess(crawed_raw_data = test_input)
#     print(processed)