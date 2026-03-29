import pandas as pd
import json
from pathlib import Path


def load_csv_to_df(file_path: str, delimiter: str=',', encoding: str='utf-8', header: int=0) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            file_path,
            delimiter=delimiter,
            encoding=encoding,
            header=header
        )
        return df
    
    except Exception as e:
        return None
    
def load_and_flatten_json(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        df = pd.json_normalize(data)
        return df
    except Exception as e:
        return None
    
def reader(file_path: str) -> pd.DataFrame:
    if not Path(file_path).exists():
        raise(FileNotFoundError(f"The file {file_path} does not exist."))

    suffix = Path(file_path).suffix.lower()

    if suffix == '.csv':
        return load_csv_to_df(file_path)
    elif suffix == '.json':
        return load_and_flatten_json(file_path)