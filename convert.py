import json
import pandas as pd
import hydra

def convert(input_path,):
    pass



def read_json(input_path:str):
    f = open(input_path)
    database = json.load(f)

@hydra.main(config_path='conf' , config_name='config')
def main(input_path, col_list):
    print(f'input_path is {input_path}')
    print(f'col_list is {col_list}')

if __name__ == "__main__":
    main()