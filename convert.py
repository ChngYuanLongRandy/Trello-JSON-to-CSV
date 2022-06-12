import pandas as pd
import json
from datetime import datetime
import logging
import hydra


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
        logger.info(f"Attempting to read json file for {file_name}")
        with open(file_name+".json", encoding="utf-8") as f:
            database = json.load(f)
    except:
        logger.warning("unable to read json file")

    return pd.DataFrame(database)

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
    
    # we only want the input df with open cards
    df_col = df_col[df_col['closed']==False]
    # we need the id , name and description
    df_col = df_col[['id','name','desc']]
    # we need to replace the index with the id
    df_col.set_index('id', inplace=True)

    return df_col




def save_master(file_name:str, df:pd.DataFrame, file_path:str= 'data/store/'):
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
    with open(file_path+file_name+".json", 'w') as file:
        json.dump(df.to_dict(), file)


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
    master_json = master_df.to_dict()
    new_entries_indicies = set(input_df.to_dict()['name'].keys()) - set(master_json['name'].keys())

    now = datetime.datetime.now().strftime("%m%d%Y%H%M%S")

    # export new words
    input_df.loc[new_entries_indicies].to_csv(output_path+now+col_name+'.csv',index = None, header= None)

    # add new into master json and save
    master_json = pd.DataFrame(master_json).append(input_df.loc[new_entries_indicies])

    save_master(file_name, master_json, file_path)


@hydra.main(config_path='../conf',config_name='config.yml')
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
    input_path = args['files']['input_path']
    input_name = args['files']['input_name']
    master_path = args['files']['master_path']
    master_name = args['files']['master_name']
    col_name = args['files']['col_name']
    output_path = args['files']['output_path']

    input_df = read_inputs(input_name, col_name ,input_path)
    master_df = read_master(master_name,master_path)
    compare_export_save(col_name, input_df, master_df, master_name,master_path,output_path)


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    main()