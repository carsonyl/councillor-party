"""
Helper functions for loading configurations.
"""
import pytz
from yaml import safe_load_all


def get_all_configs():
    return safe_load_all(open('config.yaml'))


def get_config(config_id):
    for config in get_all_configs():
        if config['id'] == config_id:
            return config
    raise KeyError(config_id + " configuration not found")


def get_tz(config):
    tz_name = config.get('tz', 'America/Vancouver')
    return pytz.timezone(tz_name)
