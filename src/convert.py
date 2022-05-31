import pandas as pd
import json
from datetime import datetime

def read_master(file_name:str)->pd.DataFrame:
    """takess in the path and outputs the dataframe

    Parameters
    ----------
    master_path : str
        path for the json file

    Returns
    -------
    pd.DataFrame
        the dataframe
    """
    with open("data/store/"+file_name+".json", encoding="utf-8") as f:
        database = json.load(f)
        return pd.json_normalize(database)


def save_master(master_path:str, df:pd.DataFrame):
    """_summary_

    Parameters
    ----------
    master_path : str
        _description_
    df : pd.DataFrame
        _description_
    """
    with open("data/store/"+master_path+".json", 'w') as file:
        json.dump(df.to_dict(), file)


def exclude_existing(col_name:str, temp_dict:dict, file_name:str = 'master'):
    """checks the temp dict's words against the master json file and returns the list of words that are
    different

    Parameters
    ----------
    temp_dict : dict
        dict of words and desc from the export
    STORE_PATH : str, optional
        path of master json, by default 'data/store/'
    reference_name : str, optional
        name of master json file, by default 'master.json'

    Returns
    -------
    list
        Words of the new export that is not found in master json file.
    """

    master_data = read_master(file_name)
    
    # this will contain all of the words less those found in the new export
    leftover_words = list(set(master_data[col_name].values())- set(temp_dict['name']))

    return leftover_words



def export_to_csv(col_name:str, df:pd.DataFrame, name:str, OUTPUT_PATH:str = 'data/output/', STORE_PATH:str = 'data/store/'):
    """exports file to csv after removing duplicates from the master copy

    Parameters
    ----------
    df : pd.DataFrame
        _description_
    name : str
        _description_
    OUTPUT_PATH : str, optional
        _description_, by default 'data/output/'
    STORE_PATH : str, optional
        _description_, by default 'data/store/'
    """
    name_list = list(df[col_name].values)
    desc_list = list(df[col_name].values)
    df_name_desc = pd.DataFrame( desc_list, name_list, columns=['meaning'])
    df_name_desc = pd.concat((pd.Series(name_list),pd.Series(desc_list)),axis=1).rename(columns={0:'Word',1:'Desc'})
    now = datetime.now().strftime("%m%d%Y%H%M%S")
    
    temp_dict = df_name_desc.to_dict()

    leftover_words = exclude_existing(temp_dict)


def main(col_name:str, master_path:str):
    """takes in the col name and outputs the csv file

    Parameters
    ----------
    col_name : str
        _description_
    master_path : str
        _description_
    """

    df_master = read_master(master_path)



if __name__ == '__main__':
    main()