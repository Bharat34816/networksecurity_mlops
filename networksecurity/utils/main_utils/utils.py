import yaml
import os,sys
import numpy as np
import pandas as pd
import dill
from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException

import pickle

def read_yaml_file(file_path:str)->dict:
    try:
        with open(file_path,"rb") as f:
            return yaml.safe_load(f)
    except Exception as e:
        raise NetworkSecurityException(e,sys) 

def write_yaml_file(file_path: str, content: object, replace: bool = False) -> None:
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            yaml.dump(content, file)
    except Exception as e:
        raise NetworkSecurityException(e, sys)
