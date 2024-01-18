from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from utils import get_parent_working_dir

import time
import json
import math
import re
import logging


logs_folder = get_parent_working_dir(folder_name="logs")

logging.basicConfig(filename=f'{logs_folder}/crawlerOperation.log',level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

conf_folder = get_parent_working_dir(folder_name="config")

with open(file=f'{conf_folder}/conf_baseSetting.json') as conf:
    
    baseCongig = json.load(conf)
    
class CONFIG():
    
    def check_read_conf(conf_search_preference:str):

        if 'easy_apply_only' not in baseCongig or not isinstance(baseCongig['easy_apply_only'], bool):
            logging.warning(f"easy_apply_only does not fullfill expected format, {baseCongig['easy_apply_only']}")

        if 'last_date_posted' not in baseCongig or not baseCongig['last_date_posted'] in ['one day','three days','week','month'] :
            logging.warning(f"last_date_posted does not fullfill expected format, {baseCongig['easy_apply_only']}")

        logging.info("Config File Check Passed")
    
class CONNECTION():
    
    exist_close_view = baseCongig["xpath"]["exist_close_view"]
    exist_close_invitation = baseCongig["xpath"]["exist_close_invitation"]
    show_more_job = baseCongig["xpath"]["show_more_job"]           
        
    basicInfo_Xpath = baseCongig["xpath"]["basic_info"] 
    ifEasyApply_Xpath = baseCongig["xpath"]["if_easy_apply"] 
    company_all_sect = baseCongig["xpath"]["company_all_section"] 
    jd_selector = baseCongig["xpath"]["jd_selector"] 
    salary_selector = baseCongig["xpath"]["salary_selector"] 
    size_selector = baseCongig["xpath"]["size_selector"] 
    found_year_selector = baseCongig["xpath"]["found_year_selector"] 
    industry_selector = baseCongig["xpath"]["industry_selector"] 
    
    sector_selector = baseCongig["xpath"]["sector_selector"] 
    num_header_selector = baseCongig["xpath"]["num_header_selector"] 
    click_dynamic_selector = baseCongig["xpath"]["click_dynamic_selector"] 

    validate_salary = baseCongig["scrawlerKeyWord"]["validate_salary"] # String validating the section
    company_overview = baseCongig["scrawlerKeyWord"]["company_overview"] # The key word that can verify the locator of section
         
    def __init__(self,url:str,job_name:str,full_search=False,search_limit:int = 10 ,driver_appear:bool = False, show_web=False,end_with_web=False) -> None:
        
        self.url = url
        self.job_name=job_name
        
        options = webdriver.ChromeOptions()
        if end_with_web:                     
            options.add_experimental_option("detach", True)
        
        if not show_web:
            ## hide web browser
            options.add_argument("--window-size=1920,1080")            
            options.add_argument("--headless=new") 
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument('--disable-dev-shm-usage')         
            user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
            options.add_argument(f'user-agent={user_agent}')
            
        
        self.driver = webdriver.Remote("http://selenium:4444/wd/hub", DesiredCapabilities.CHROME,options=options)
        self.driver.get_screenshot_as_file(f"./logs/retrieve_info_err_{self.job_name}.png")
        self.driver.headless = driver_appear
        self.search_limit = search_limit # Search limit by each job key word
        self.stor_data = list()
        
        self.if_full_search = full_search # Collect every position for each job or not
        
    
    def use_full_searched(self):
        '''The function must be used before loading the crawler function'''
        amount_descript = self.driver.find_element(By.XPATH,self.num_header_selector).text
        
        num_searched = int(re.search(r'(\d+)',re.sub(r',','',amount_descript)).group(1)) # The number of positions available
        
        self.search_limit = num_searched if num_searched > self.search_limit else self.search_limit
        
        return self.search_limit
    
    def connect_glassdoor(self): 
        '''Connect to job searched page'''
        self.driver.get(self.url)
    
    def validate_element(self,selector):
        try:
    
            element = self.driver.find_element(By.CSS_SELECTOR,self.company_all_sect + selector).text
            return element
        
        except NoSuchElementException as e:
        
            logging.info(f'{self.job_name}, {e}, default value {baseCongig["default_unfound_val"]} is given')
            
            return baseCongig["default_unfound_val"]
        
    def validate_section_element(self,selector,val_str):
        
        validated_str_1 = self.driver.find_element(By.CSS_SELECTOR,self.company_all_sect + ' > section:nth-child(2)').text
        validated_str_2 = self.driver.find_element(By.CSS_SELECTOR,self.company_all_sect + ' > section:nth-child(3)').text
        val_bool = True
        
        if val_str in validated_str_1:
            val_bool = False
            return self.driver.find_element(By.CSS_SELECTOR,self.company_all_sect + ' > section:nth-child(2)'+selector).text
        
        if val_str in validated_str_2:
            val_bool = False
            return self.driver.find_element(By.CSS_SELECTOR,self.company_all_sect + ' > section:nth-child(3)'+selector).text
        
        if val_bool:
            return baseCongig["default_unfound_val"]
        
    def retrieve_info(self):
        
        stor_xpathtxt = {}
        trial = 0

        while not self.driver.find_elements(By.CSS_SELECTOR,self.company_all_sect) and trial <5 :
            
            time.sleep(1)
            trial += 1      
            
        if trial <5:
            
            ## only collect the company & position whose location, position, rate, company name information are all provided
            if len(self.driver.find_element(By.XPATH,self.basicInfo_Xpath).text.split('\n'))==4:
                
                stor_xpathtxt['position_info'] = self.driver.find_element(By.XPATH,self.basicInfo_Xpath).text
                stor_xpathtxt['if_easy_apply'] = self.driver.find_element(By.XPATH,self.ifEasyApply_Xpath).text
                            
                stor_xpathtxt['jd'] = self.validate_element(selector=self.jd_selector)
                stor_xpathtxt['avg_salary'] = self.validate_section_element(selector=self.salary_selector,val_str=self.validate_salary)         
                stor_xpathtxt['size'] = self.validate_section_element(selector=self.size_selector,val_str=self.company_overview)       
                stor_xpathtxt['found_year'] = self.validate_section_element(selector=self.found_year_selector,val_str=self.company_overview)       
                stor_xpathtxt['sector'] = self.validate_section_element(selector=self.sector_selector,val_str=self.company_overview)                         
                self.stor_data.append(stor_xpathtxt)
                
            else:               
                
                logging.info(f'{self.job_name} - position is not collected because the basic information is not intact')
        else:
            
            self.driver.get_screenshot_as_file(f"./logs/retrieve_info_err_{self.job_name}.png")
            
        
        
    def load_positions(self):
        
        for idx in range(0,int(self.search_limit)):   
           
            position_sect = self.driver.find_element(By.XPATH, f'/html/body/div[2]/div[1]/div[3]/div[2]/div[1]/div[2]/ul/li[{idx+1}]')
            self.driver.execute_script(self.click_dynamic_selector,position_sect) 

            # close all possible pop up windows
            if self.driver.find_elements(By.XPATH,self.exist_close_view):
                logging.info(f'{self.job_name} - successfully close pop up window')
                self.driver.find_element(By.XPATH,self.exist_close_view).click()
                
            if self.driver.find_elements(By.XPATH,self.exist_close_invitation):
                logging.info(f'{self.job_name} - successfully close pop up window')
                self.driver.find_element(By.XPATH,self.exist_close_invitation).click()
                
            self.retrieve_info()
     
    def click_next_turn(self):
        
        for _ in range(math.floor(int(self.search_limit)/30)): #one fold explores 30 more position
            self.driver.get_screenshot_as_file(f"{logs_folder}/click_next_pg.png")
            next_locator = self.driver.find_element(By.XPATH,self.show_more_job)
            self.driver.execute_script(self.click_dynamic_selector,next_locator)
            time.sleep(3)
            logging.info('click next job page')

    def main(self):

        self.connect_glassdoor()
        logging.info(f' {self.job_name} - process started successfully')
        if self.if_full_search:
            self.use_full_searched()
            logging.info('searched all position result')
            
        self.click_next_turn()
        self.load_positions()
        logging.info(f' {self.job_name} - process ended successfully')
        self.driver.close()
        self.driver.quit()
        return self.stor_data
        







