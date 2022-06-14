# Purpose
This is meant to export new words from JSON into CSV while retaining the existing words in a json file.

The resulting CSV file can then be easily imported into anki app 

# Setup

Install conda environment

`- conda env create --file conda.yml`

Activate conda environment

`- conda activate trello_env`

# To Use

Run python with the filename, tweak the arguments according to hydra's configuration.

if Col name is Chinese, you will need to indicate as such, otherwise the default is Words

The input name will always need to be entered otherwise the script will fail

`python convert.py files.input_name=new_words_12jun2022 files.col_name=Chinese`


`python convert.py files.input_name="new_words_12jun2022"`

Tweak with hydra's configuration:

```    
files:
    master_path: 'data/store/'
    master_name: 'master'
    input_path: 'data/input/'
    input_name: 'new_words'
    col_name : 'Words'
    output_path : 'data/output/'
```

Once run, the csv will be generated under output path affixed with the col name and date time.

The master json will also be updated with the new words.

## At Anki's end

Import CSV and add to folder.