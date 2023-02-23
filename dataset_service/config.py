import os
import json
import yaml
import collections.abc

DEFAULT_CONFIG_FILE_PATHS = [os.path.join('.','etc','dataset-service.default.yaml'), '/etc/dataset-service/dataset-service.default.yaml']
CONFIG_FILE_PATHS = [os.path.join('.','etc','dataset-service.yaml'), '/etc/dataset-service/dataset-service.yaml']
CONFIG_ENV_VAR_NAME = "DATASET_SERVICE_CONFIG"


def check_config_items (parent_key, ok_config, config):
    #global LOG
    ok = True
    items = config.keys()
    for k,v in ok_config.items():
        if k not in items:
            #print("k = %s, items = %s"%(k, str(items)))
            print("%s not defined in %s section. Required values=%s, obtained=%s"%(k, parent_key, ok_config.keys(), items)) 
            ok = False
            break
        
        if type(v) == type(config.get(k)):
            if type(v) == dict:
                ok = check_config_items(k, v, config.get(k))
        else:
            ok = False
            print("type of %s['%s'] must be %s" % (parent_key, k, type(v)) )
        
        if not ok:
            break        
    return ok

class Config:
    creation_dict = None
    def __init__(self, dictionary={}):
        self.creation_dict = dictionary
        for k, v in dictionary.items():
            if type(v) == dict:
                setattr(self, k, Config(v))
            else:
                setattr(self, k, v)


def get_first_existing_path(paths):
    for p in paths:
        if os.path.exists(p):
            return p
    return None

def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d

def load_config(config_file_path = None):
    try:
        default_config_file_path = get_first_existing_path(DEFAULT_CONFIG_FILE_PATHS)
        if default_config_file_path is None: raise Exception()
        with open(default_config_file_path) as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
    except:
       print("ERROR: Default configuration file not found or cannot be loaded from ")
       for p in DEFAULT_CONFIG_FILE_PATHS:
           print("    " + p)
       return None
    print("Default configuration loaded from file: " + default_config_file_path)
    
    if config_file_path != None:
        if not os.path.exists(config_file_path):
            print("ERROR: Configuration file not found in " + config_file_path)
    else:
        config_file_path = get_first_existing_path(CONFIG_FILE_PATHS)
    if config_file_path != None and os.path.exists(config_file_path):
        try:
            with open(config_file_path) as f:
                new_config = yaml.load(f, Loader=yaml.SafeLoader)
        except:
            print("ERROR: Configuration file cannot be loaded from " + config_file_path)
            return None
        config = update(config, new_config)
        print("Additional configuration loaded from file: " + config_file_path)
    
    if CONFIG_ENV_VAR_NAME in os.environ:
        new_config = json.loads(os.environ[CONFIG_ENV_VAR_NAME])
        config = update(config, new_config)
        print("Additional configuration loaded from environment variable: " + CONFIG_ENV_VAR_NAME)

    return Config(dictionary=config)
