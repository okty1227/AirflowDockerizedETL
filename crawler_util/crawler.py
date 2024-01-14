from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# from bs4 import BeautifulSoup
import time
import json
import math
import re
import os

class CONFIG():
    '''pending'''
    def check_read_conf(conf_search_preference:str):
        
        with open(conf_search_preference, 'r') as file:
            conf = json.load(file)

        if 'easy_apply_only' not in conf or not isinstance(conf['easy_apply_only'], bool):
            raise ValueError("easy_apply_only does not fullfill expected format")

        if 'Salary_Range' not in conf or not isinstance(conf['Salary_Range'], list) or len(conf['Salary_Range']) != 2:
            raise ValueError("Salary_Range does not fullfill expected format")

        if 'Company_Rating_atleast' not in conf or not isinstance(conf['Company_Rating_atleast'], (int, float)):
            raise ValueError("Company_Rating_atleast does not fullfill expected format")

        if 'last_date_posted' not in conf or not conf['last_date_posted'] in ['one day','three days','week','month'] :
            raise ValueError("last_date_posted does not fullfill expected format")

        print("Config File Check Passed")
        return conf
    
class CONNECTION():
    exist_close_view = '/html/body/div[11]/div[2]/div[1]/button'
    exist_close_envitation = '/html/body/div[8]/div/div[2]/span/svg'
    show_more_job = '/html/body/div[2]/div[1]/div[3]/div[2]/div[1]/div[2]/div/button'           
        
    basicInfo_Xpath = '/html/body/div[2]/div[1]/div[3]/div[2]/div[2]/div[1]/header/div[1]'
    ifEasyApply_Xpath = '/html/body/div[2]/div[1]/div[3]/div[2]/div[2]/div[1]/header/div[3]/div[2]'
    
    company_all_sect = 'section.JobDetails_jobDetailsSection__PJz1h'
    
    jd_selector = ' > div'
    salary_selector = ' > div > div > div:nth-child(1)'
    
    size_selector = ' > div > div > div:nth-child(1) > div'
    found_year_selector = ' > div > div > div:nth-child(2) > div'
    industry_selector = ' > div > div > div:nth-child(4) > div'
    sector_selector = ' > div > div > div:nth-child(5) > div'
    num_header_selector = '/html/body/div[2]/div[1]/div[3]/div[2]/div[1]/div[1]/h1'
    click_dynamic_selector = 'arguments[0].click();'
    def __init__(self,url:str,full_search=True,search_limit:int = 10 ,driver_appear:bool = False, show_web=False,end_with_web=False) -> None:
        self.url = url

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
        self.driver.get_screenshot_as_file("./log/retrieve_info_err.png")
        self.driver.headless = driver_appear
        self.search_limit = search_limit # search limit by each job key word
        self.stor_data = list()
        self.ignored = 0 # for test
        self.if_full_search = full_search # collect every position for each job or not
        
        self.validate_salary = 'Average base salary estimate' # the string validating the section 
        self.company_overview = 'Company overview'  # the string validating the section    
         
    def set_sec_validation_str(self,salary_str:str,company_str:str):
        '''as cralwer locators are dynamics, the key word is set to make sure the
            crawled section has the content we need'''
        self.validate_salary = salary_str if salary_str else self.validate_salary
        self.company_overview = company_str if company_str else self.company_overview

    
    def use_full_searched(self):
        '''this function must be used before loading the crawler function'''
        amount_descript = self.driver.find_element(By.XPATH,self.num_header_selector).text
        
        num_searched = int(re.search(r'(\d+)',re.sub(r',','',amount_descript)).group(1)) # the number of positions available
        
        self.search_limit = num_searched if num_searched > self.search_limit else self.search_limit
        
        return self.search_limit
    
    def connect_glassdoor(self): 
        '''connect to job searched page'''
        self.driver.get(self.url)
    
    def validate_element(self,selector,defaule_val='-1'):
        try:
    
            element = self.driver.find_element(By.CSS_SELECTOR,self.company_all_sect + selector).text
            return element
        
        except NoSuchElementException as e:
        
            print(f'{e}, default value -1 is given')
            return defaule_val
        
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
            return '--'
        
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
                print(self.driver.find_element(By.XPATH,self.basicInfo_Xpath).text.split('\n')[1])
                print('position is not collected because the basic information is not intact')
        else:
            self.driver.get_screenshot_as_file("./log/retrieve_info_err.png")
            pass
        
        
    def load_positions(self):
        
        for idx in range(0,self.search_limit):   
           
            position_sect = self.driver.find_element(By.XPATH, f'/html/body/div[2]/div[1]/div[3]/div[2]/div[1]/div[2]/ul/li[{idx+1}]')
            self.driver.execute_script(self.click_dynamic_selector,position_sect) 

            # close all possible pop up windows
            if self.driver.find_elements(By.XPATH,self.exist_close_view):
                print('successfully close pop up window')
                self.driver.find_element(By.XPATH,self.exist_close_view).click()
                
            if self.driver.find_elements(By.XPATH,self.exist_close_envitation):
                print('successfully close pop up window')
                self.driver.find_element(By.XPATH,self.exist_close_envitation).click()
                
            self.retrieve_info()
     
    def click_next_turn(self):
        
        for _ in range(math.floor(self.search_limit/30)): #one fold explores 30 more position
            time.sleep(1)
            self.driver.get_screenshot_as_file("./log/click_next_pg.png")
            next_locator = self.driver.find_element(By.XPATH,self.show_more_job)
            self.driver.execute_script(self.click_dynamic_selector,next_locator)
            time.sleep(3)
            print('successfully click next job page')

    def main(self):

        self.connect_glassdoor()
        print('process started successfully')
        if self.if_full_search:
            self.use_full_searched()
            print('searched all position result')
            
        self.click_next_turn()
        self.load_positions()
        print(self.ignored)
        self.driver.quit()
        return self.stor_data
        
    def direct_to_search(self): ## currently replaced by url
        pass










