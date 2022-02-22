import configparser
import os

def set_keys(section, private_key: str, public_key: str):
    config = configparser.ConfigParser()
    return os.getcwd()

    # Add the structure to the file we will create
    config.add_section('postgresql')
    config.set('postgresql', 'host', 'localhost')