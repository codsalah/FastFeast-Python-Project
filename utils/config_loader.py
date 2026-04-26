# NOT USED IN PROJECT - SAFE TO REMOVE
# utils/config_loader.py
# ─────────────────────────────────────────────
# This file does three things:
# 1. Load .env into Python's environment
# 2. Read config.yaml
# 3. Replace all ${VAR} placeholders with real values
# ─────────────────────────────────────────────
 
from dotenv import load_dotenv
import yaml
import os
from pathlib import Path
 
def load_config(config_path: str = 'config/config.yaml') -> dict:
    # This is the only function other modules need to call
    # It returns the complete config as a Python dictionary
    # Example: config['database']['password'] → 'mypassword'
 
    # STEP 1: Load .env file
    # Looks for .env in the current directory (project root)
    # Puts DB_PASSWORD=abc123 into os.environ
    load_dotenv()
 
    # STEP 2: Read config.yaml
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(
            f'Config file not found: {config_path}'
            # Gives a clear error if someone forgot to create config.yaml
        )
 
    with open(config_file, 'r') as f:
        raw_config = yaml.safe_load(f)
        # yaml.safe_load reads the YAML file into a Python dict
        # At this point config['database']['password'] = '${DB_PASSWORD}'
        # Still just a placeholder string — not the real value yet
 
    # STEP 3: Replace all ${VAR} placeholders with real values
    resolved = _resolve(raw_config)
    # Now config['database']['password'] = 'myactualpassword'
 
    return resolved
 
 
def _resolve(obj):
    # This function walks through every value in the config
    # If it finds a ${VAR} placeholder it replaces it
    # It handles nested dictionaries and lists automatically
 
    if isinstance(obj, dict):
        # obj is a dictionary like {'host': 'localhost', 'port': 5432}
        # Process each value recursively
        return {key: _resolve(value) for key, value in obj.items()}
 
    elif isinstance(obj, list):
        # obj is a list like ['email1@x.com', 'email2@x.com']
        # Process each item recursively
        return [_resolve(item) for item in obj]
 
    elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
        # obj is a placeholder like '${DB_PASSWORD}'
        # Extract the variable name: 'DB_PASSWORD'
        var_name = obj[2:-1]
        # obj[2:-1] cuts off the first 2 chars '${' and last char '}'
 
        # Look up the real value from environment
        value = os.getenv(var_name)
        # os.getenv reads from os.environ which was populated by load_dotenv()
 
        if value is None:
            raise ValueError(
                f"Environment variable '{var_name}' not set. "
                f"Check your .env file."
            )
 
        return value
        # Returns the real password/value
 
    else:
        return obj
