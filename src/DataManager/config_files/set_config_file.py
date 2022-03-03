import configparser
import os
import inspect
from typing import List
from DataManager import config_files

assetConfigFileName = os.path.join(\
        os.path.dirname(inspect.getfile(config_files)),
        'assetConfig.cfg')
if not os.path.exists(f'{assetConfigFileName}'):
    with open(f'{assetConfigFileName}', 'w'):
        pass

def gdrive_client_secrets(secrets):
    clientSecretsFilePath = os.path.join(\
        os.path.dirname(inspect.getfile(config_files)),
        'client_secrets.json')
    with open(clientSecretsFilePath, 'w') as f:
        f.write(secrets)
    return True, f'\nSuccessfully written\n\n[client_secrets.json]\n{secrets}'

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

def delete_temp_files():
    msg = 'Temporary files were deleted.\n'
    from DataManager.tempDir import threadDir
    from DataManager import tempDir
    import DataManager
    dirs: List[str] = [
        
    ]
    files = [
        assetConfigFileName,
    ]

    tempDirPath = os.path.dirname(inspect.getfile(tempDir))
    threadDirPath = os.path.dirname(inspect.getfile(threadDir))
    rootDirPath = os.path.dirname(inspect.getfile(DataManager))
    files.extend(_get_files_with_ext(tempDirPath, ".db"))
    files.extend(_get_files_with_ext(threadDirPath, ".db"))
    files.extend(_get_files_with_ext(rootDirPath, ".DS_Store"))

    for dir in dirs:
        if os.path.exists(dir):
            msg += _del_all_files_in_dir(dir)

    for file in files:
        if os.path.exists(file):
            msg += _del_file(file)
    msg += '\n'

    return True, msg
    
def _get_files_with_ext(dirPath, extension):
    files = []
    for file in os.listdir(dirPath):
        if file.endswith(extension):
            files.append(os.path.join(dirPath, file))
    return files

def _del_all_files_in_dir(path):
    msg = ''
    msg += f'Deleted all files in {dir}\n'
    for f in os.listdir(path):
        if os.path.isfile(f):
            os.remove(os.path.join(path, f))
            msg += f'Deleted {f}\n'
    msg += '\n'
    return msg

def _del_file(path):
    msg = ''
    os.remove(path)
    msg += f'Deleted {path}\n'
    return msg