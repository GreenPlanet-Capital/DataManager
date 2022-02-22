import configparser
import os
import inspect
from DataManager import config_files

assetConfigFileName = os.path.join(\
        os.path.dirname(inspect.getfile(config_files)),
        'assetConfig.cfg')
if not os.path.exists(f'{assetConfigFileName}'):
    with open(f'{assetConfigFileName}', 'w'):
        pass

def _write_config_file(config):
    with open(assetConfigFileName, 'w+') as configFile:
        config.write(configFile)

def get_config_file_str():
    config = configparser.ConfigParser()
    content = ''
    config.read(assetConfigFileName)
    config_dict = {section: dict(config[section]) 
                for section in config.sections()}
    for section, v in config_dict.items():
        content += f'[{section}]\n'
        for var, val in v.items():
            content += f'{var}={val}\n'
        content += '\n'
    return content

def set_keys(section: str, public_key_var_name: str, public_key: str, private_key_var_name: str, private_key: str):
    config = configparser.ConfigParser()
    config.read(assetConfigFileName)

    if section in config.sections():
        return False, f'Section {section} already exists in the config file\n\
        Use `reset` to remove all sections'

    config.add_section(f'{section}')
    config.set(f'{section}', f'{public_key_var_name}', f'{public_key}')
    config.set(f'{section}', f'{private_key_var_name}', f'{private_key}')
    _write_config_file(config=config)
    msg = get_config_file_str()
    return True, f'Successfully wrote to the config file\n\n{msg}'

def reset_config():
    config = configparser.ConfigParser()
    with open(assetConfigFileName, 'w') as configFile:
        config.write(configFile)
    return True, 'Config file was reset'