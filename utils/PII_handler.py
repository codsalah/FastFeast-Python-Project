import pandas as pd
import hashlib
import json
from cryptography.fernet import Fernet


def replace_value(column: pd.Series, value='####') -> pd.Series:
    column = column.apply(
        lambda x: value if pd.notna(x) else x
    )
    return column

def hash_value(column: pd.Series, salt = 'a12b') -> pd.Series:
    def _hash(value):
        if not pd.isna(value):
            value += salt
            return hashlib.sha256(value.encode()).hexdigest()
        else:
            return value

    column = column.apply(_hash)
    return column

def two_layers_encryption(column: pd.Series, keys_path: str = 'encryption_keys.json') -> pd.Series:
    # Reading Keys
    with open(keys_path, 'r') as f:
        keys = json.load(f)

    original_keys = {k: v.encode('utf-8') for k, v in keys.items()}
    f1 = Fernet(original_keys["Key1"])
    f2 = Fernet(original_keys['Key2'])

    def _encryption(value):
        if not pd.isna(value):
            # First Encryption
            value = f1.encrypt(value.encode())

            # Second Encryption
            return f2.encrypt(value)
        else:
            return value

    return column.apply(_encryption)

def decryption(column: pd.Series) -> pd.Series:
    # Reading Keys
    with open('encryption_keys.json', 'r') as f:
        keys = json.load(f)

    original_keys = {k: v.encode('utf-8') for k, v in keys.items()}
    f1 = Fernet(original_keys["Key2"])
    f2 = Fernet(original_keys['Key1'])

    def _decryption(value):
        if not pd.isna(value):
            value = f1.decrypt(value) # First Decryption
            return f2.decrypt(value).decode() # Second Decryption
        
        else:
            return value

    return column.apply(_decryption)

def partial_masking(column: pd.Series, keep_first=3) -> pd.Series:
    def _PMask(value):
        if not pd.isna(value):
            visible = min(keep_first, len(value))
            return value[:visible] + "*" * (len(value)-keep_first)
        else:
            return value

    column = column.apply(_PMask)
    return column