from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


def replace_value(column: pd.Series, value='####') -> pd.Series:
    column = column.apply(
        lambda x: value if pd.notna(x) else x
    )
    return column


def hash_value(column: pd.Series, pepper: str) -> pd.Series:
    def _hash(value):
        if pd.isna(value):
            return value
        try:
            salted = str(value) + pepper
            return hashlib.sha256(salted.encode()).hexdigest()
        except Exception as e:
            logger.error('hash_value_failed', error=str(e))
            return None
    return column.apply(_hash)


def two_layers_encryption(column: pd.Series, keys_path: str = 'encryption_keys.json') -> pd.Series:
    with open(keys_path, 'r') as f:
        keys = json.load(f)
    original_keys = {k: v.encode('utf-8') for k, v in keys.items()}
    f1 = Fernet(original_keys["Key1"])
    f2 = Fernet(original_keys['Key2'])
    
    def _encryption(value):
        if pd.notna(value):
            try:
                value = f1.encrypt(value.encode())
                return f2.encrypt(value)
            except Exception as e:
                logger.error('encryption_failed', error=str(e))
                return None
        else:
            return value
    
    return column.apply(_encryption)


def decryption(column: pd.Series, keys_path: str = 'encryption_keys.json') -> pd.Series:
    with open(keys_path, 'r') as f:
        keys = json.load(f)
    original_keys = {k: v.encode('utf-8') for k, v in keys.items()}
    f1 = Fernet(original_keys["Key2"])
    f2 = Fernet(original_keys['Key1'])
    
    def _decryption(value):
        if pd.notna(value):
            try:
                if isinstance(value, str):
                    value = value.encode('utf-8')
                decrypted1 = f1.decrypt(value)
                return f2.decrypt(decrypted1).decode('utf-8')
            except Exception as e:
                logger.error('decryption_failed', error=str(e))
                return None
        else:
            return value
    
    return column.apply(_decryption)


def partial_masking(column: pd.Series, keep_first: int = 3) -> pd.Series:
    def _PMask(value):
        if pd.notna(value):
            str_value = str(value)
            visible = min(keep_first, len(str_value))
            return str_value[:visible] + "*" * (len(str_value) - visible)
        else:
            return value
    
    return column.apply(_PMask)


def generate_encryption_keys(keys_path: str = 'encryption_keys.json') -> None:
    try:
        key1 = Fernet.generate_key().decode('utf-8')
        key2 = Fernet.generate_key().decode('utf-8')
        
        keys = {'Key1': key1, 'Key2': key2}
        
        Path(keys_path).parent.mkdir(parents=True, exist_ok=True)
        with open(keys_path, 'w') as f:
            json.dump(keys, f, indent=2)
        
        logger.info('encryption_keys_generated', path=keys_path)
    except Exception as e:
        logger.error('encryption_keys_generation_failed', error=str(e))
        raise
