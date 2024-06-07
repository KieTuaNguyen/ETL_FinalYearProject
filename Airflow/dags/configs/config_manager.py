import json
from pathlib import Path

def get_config(env):
    config_file = Path(__file__).parent / 'config.prod.json'

    with open(config_file, 'r') as f:
        config_data = json.load(f)

    config = config_data[env]

    return config