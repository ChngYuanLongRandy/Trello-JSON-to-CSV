import pandas as pd
import json
from datetime import datetime
import logging
import hydra


FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)

def read_master(file_name:str, file_path:str)->pd.DataFrame:
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
    
    file_name = file_path + file_name
    try: 
        logger.info(f"Attempting to read json file for {file_name}")
        with open(file_name+".json", encoding="utf-8") as f:
            database = json.load(f)
    except:
        logger.warning("unable to read json file")

    return pd.json_normalize(database)

def read_inputs(file_name:str, col_name:str, file_path:str)->pd.DataFrame:
    """reads the input file and exports only the data of the col name

    Parameters
    ----------
    file_name : str
        _description_
    col_name : str
        _description_

    Returns
    -------
    pd.DataFrame
        _description_
    """
    file_name = file_path + file_name
    try: 
        logger.info(f"Attempting to read json file for {file_name}")
        with open(file_name+".json", encoding="utf-8") as f:
            database = json.load(f)
    except:
        logger.warning(f"unable to read json file")

    # dataframe of the cards
    df_cards = pd.json_normalize(database['cards'])

    list_list = pd.json_normalize(database)['lists'][0]

    list_dict = {}

    for a_list in list_list:
        name = a_list['name']
        id_list = a_list['id']
        list_dict[name] = id_list

    try:
        logger.info(f"Attempting to get the data for col name {col_name}")
        df_col = df_cards[df_cards['idList'] == list_dict.get(col_name)]

    except:
        logger.warning('Unable to read the col name')
    
    return df_col




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



def export_to_csv(df:pd.DataFrame, name:str, OUTPUT_PATH:str = 'data/output/', STORE_PATH:str = 'data/store/'):
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
    name_list = list(df.values)
    desc_list = list(df.values)
    df_name_desc = pd.DataFrame( desc_list, name_list, columns=['meaning'])
    df_name_desc = pd.concat((pd.Series(name_list),pd.Series(desc_list)),axis=1).rename(columns={0:'Word',1:'Desc'})
    now = datetime.now().strftime("%m%d%Y%H%M%S")
    
    temp_dict = df_name_desc.to_dict()

    leftover_words = exclude_existing(temp_dict)


@hydra.main(config_path='../conf',config_name='config.yml')
def main(args):
    """takes in the col name and exports the input file.
    this input file will then be compared against the master file.
    The duplicates will be remmoved and the unique elements will
    then be used to export
     - timestamped output file
     - master.json file that will be overwritten with the new
       elements 

    Parameters
    ----------
    col_name : str
        _description_
    master_path : str
        _description_
    """
    input_file = args['files']['input_path']
    input_name = args['files']['input_name']
    master_path = args['files']['master_path']
    master_name = args['files']['master_name']
    col_name = args['files']['col_name']
    output_path = args['files']['output_path']

    df_master = read_master(master_path)
    df_input = read_inputs(input_file, col_name)



if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    main()