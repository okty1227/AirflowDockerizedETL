import os
def get_parent_working_dir(folder_name:str):
    """
    Get the absolute path of a specified folder within the working directory.

    Parameters:
    - folder_name (str): The name of the folder within the working directory.

    Returns:
    - str: The absolute path of the specified folder within the working directory.
    """
    current_script_path = os.path.abspath(__file__)
    parent_folder_path = os.path.dirname(os.path.dirname(current_script_path)) 
    
    return os.path.join(parent_folder_path,folder_name)