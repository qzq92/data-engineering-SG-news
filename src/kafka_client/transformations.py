from unidecode import unidecode
from src.constants import COLUMNS_TO_NORMALIZE, COLUMNS_TO_KEEP, COLUMNS_TO_EXPAND

def flatten_dict(data_dict, separator='_', prefix=''):
    res = {}
    for key, value in data_dict.items():
        if isinstance(value, dict):
            res.update(flatten_dict(value, separator, prefix + key + separator))
        else:
            res[prefix + separator + key] = value
    return res

def expand_nested_columns(api_row: dict) -> tuple:
    for col in COLUMNS_TO_EXPAND:
        api_row.update(flatten_dict(data_dict=api_row[col], prefix=col))
    
    return api_row


def normalize_one(text: str) -> str:
    """
    Checks if string is chinese hanzi and converts to pinyin. Subsequently remove accents for alphabets 
    """
    return unidecode(text)


def normalize_columns(api_row: dict) -> dict:
    """Function which applies normalisation on columns listed in COLUMNS_TO_NORMALIZE config params. 

    Args:
        api_row (dict): _description_

    Returns:
        dict: _description_
    """
    kafka_row = {}
    for col in COLUMNS_TO_KEEP:
        kafka_row[col] = api_row.get(col)
    for col in COLUMNS_TO_NORMALIZE:
        if not api_row.get(col):
            kafka_row[col] = None
            continue
        kafka_row[col] = normalize_one(api_row[col])

    return kafka_row

# Key transformation work post data pull
def transform_row(api_row: dict) -> dict:
    """Function which executes necessary transformation for kafka retrieved data

    Args:
        api_row (dict): _description_

    Returns:
        dict: _description_
    """
    kafka_row = normalize_columns(api_row)

    kafka_row = expand_nested_columns(api_row)

    return kafka_row
