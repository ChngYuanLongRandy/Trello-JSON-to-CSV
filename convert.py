import os
import pandas as pd
import json
from datetime import datetime
import logging
import hydra
import warnings

# supress warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

def read_master(file_name:str, file_path:str= 'data/store/')->pd.DataFrame:
    """takes master file name and master  file path and outputs the dataframe

    Parameters
    ----------
    file_name : str
        name of the master file without the csv extension
    file_path : str, optional
        path of the master file, by default 'data/store/'

    Returns
    -------
    pd.DataFrame
        DataFrame fo the Master file
    """
    file_name = file_path + file_name
    try: 
        logger.info(f"Attempting to read json file for {file_name}.json")
        if os.path.getsize(file_name+".json") != 0:
            with open(file_name+".json", encoding="utf-8") as f:
                database = json.load(f)
        else:
            logger.info(f"Json file : {file_name}.json is empty")
            return None
    except Exception as e:
        logger.warning(f"unable to read json file with error message {e}")

    if database is not None: 
        logger.info(f"Shape of master df {pd.DataFrame(database).shape}")
        logger.info(f"input_df master df {pd.DataFrame(database).info()}")
        return pd.DataFrame(database)
    
    else:
        logger.info(f"{file_name} is empty")
        return None


def read_inputs(file_name:str, col_name:str, file_path:str = 'data/input/')->pd.DataFrame:
    """reads the input file and exports only the data of the col name
    Parameters
    ----------
    file_name : str
        file name without the csv extension
    col_name : str
        name of the cols
    file_path : str, optional
        path of the input file, by default 'data/input/'
    Returns
    -------
    pd.DataFrame
        The list with the id as the Index, the words and the description cols
    """
    file_name = file_path + file_name
    try: 
        logger.info(f"Attempting to read json file for {file_name}")
        with open(file_name+".json", encoding="utf-8") as f:
            database = json.load(f)
    except Exception as e:
        logger.warning(f"unable to read json file with error message {e}")

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

    except Exception as e:
        logger.warning(f"unable to read col name with error message {e}")
    
    # we only want the input df with open cards
    df_col = df_col[df_col['closed']==False]
    # we need the id , name and description
    df_col = df_col[['id','name','desc']]
    # we need to replace the index with the id
    df_col.set_index('id', inplace=True)
    
    logger.info(f"Shape of input_df {df_col.shape}")
    logger.info(f"input_df info {df_col.info()}")

    return df_col




def save_master(file_name:str, df:pd.DataFrame, file_path:str= 'data/store/', is_dict:bool=True):
    """saves the master file in json format

    Parameters
    ----------
    file_name : str
        name of the master file with any extension
    df : pd.DataFrame
        pd dataframe of the master file
    file_path : str, optional
        path of the master file, by default 'data/store/'
    """
    if is_dict:
        with open(file_path+file_name+".json", 'w') as file:
            json.dump(df.to_dict(), file)
    else:
        with open(file_path+file_name+".json", 'w') as file:
            json.dump(df, file)


def compare_export_save(col_name:str, input_df:pd.DataFrame, master_df:pd.DataFrame,\
    file_name:str,file_path:str= 'data/store/', output_path:str = 'data/ouput/'):
    """checks against the index of the master file and exports the new words and saves
    them in the master file.
    The master file is then saved under JSON format

    Parameters
    ----------
    col_name : str
        col name of the input file
    input_df : pd.DataFrame
        the pandas dataframe of the list under the col name
    master_df : pd.DataFrame
        master file, this contains all of the original or duplicated entries
    file_path : str, optional
        path of the master file , by default 'data/store/'
    output_path : str, optional
        path of the output file , by default 'data/ouput/'
    """



    now = datetime.now().strftime("%m%d%Y%H%M%S")

    if master_df is not None:

        logger.info(f"In compare export save function with params: \n \
            col_name: {col_name}\n \
            input_df: {input_df.info()}\n \
            master_df: {master_df.info()}\n \
            file_name: {file_name}\n \
            file_path: {file_path}\n \
            output_path: {output_path}\n \
            ")

        master_json = master_df.to_dict()
        new_entries_indicies = set(input_df.to_dict()['name'].keys()) - set(master_json['name'].keys())

        try:
        # export new words
            logger.info(f"Attempting to export input_df to csv")
            input_df.loc[new_entries_indicies].to_csv(output_path+now+col_name+'.csv',index = None, header= None)

        except Exception as e:
            logger.warning(f"unable to export input_df to csv with error message {e}")

        try:
            logger.info(f"Attempting to append to master_json")
        # add new into master json and save
            master_json = pd.DataFrame(master_json).append(input_df.loc[new_entries_indicies])

        except Exception as e:
            logger.warning(f"unable to append to master_json with error message {e}")

        try:
            logger.info(f"Attempting to save master json")
            save_master(file_name, master_json, file_path)
        except Exception as e:
            logger.warning(f"unable to save master json with error message {e}")

    else:
        # if master is empty then master will take everything from
        master_json = input_df.to_dict()

        try:
        # export new words
            logger.info(f"Attempting to export input_df to csv")
            input_df.to_csv(output_path+now+col_name+'.csv',index = None, header= None)

        except Exception as e:
            logger.warning(f"unable to export input_df to csv with error message {e}")

        try:
            logger.info(f"Attempting to save master json")
            save_master(file_name, master_json, file_path, is_dict=False)
        except Exception as e:
            logger.warning(f"unable to save master json with error message {e}")


@hydra.main(config_path='./conf',config_name='config.yml')
def main(args):
    """Compares the input file which will be the new file exported
    from trello against the stored words in master file
    The duplicates found in the input file will be remmoved 
    and the new words/elements will then be exported and saved in the master file

    Parameters
    ----------
    args : _type_
        hydra config file
    """
        
    logger.info(f"Starting main application")
    logger.info(f"Current location {os.getcwd()}")
    input_path = os.path.join(hydra.utils.get_original_cwd(),args['files']['input_path'])
    input_name = args['files']['input_name']
    master_path = os.path.join(hydra.utils.get_original_cwd(),args['files']['master_path'])
    master_name = args['files']['master_name']
    col_name = args['files']['col_name']
    output_path = os.path.join(hydra.utils.get_original_cwd(),args['files']['output_path'])

    logger.info(f"Arguments passed: \n\t input_path: {input_path} \n \
        input_name: {input_name} \n \
        master_path: {master_path} \n \
        master_name: {master_name} \n \
        col_name: {col_name} \n \
        output_path: {output_path}")

    try:
        logger.info('Starting read inputs')
        input_df = read_inputs(input_name, col_name ,input_path)
    except Exception as e:
        logger.warning(f"unable to read inputs with error message {e}")

    try:
        logger.info('Starting read master')
        master_df = read_master(master_name,master_path)
    except Exception as e:
        logger.warning(f"unable to read master with error message {e}")

    try:
        logger.info('Starting compare function')
        compare_export_save(col_name, input_df, master_df, master_name,master_path,output_path)
    except Exception as e:
        logger.warning(f"unable to execute compare function with error message {e}")

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    main()