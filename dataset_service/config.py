import os
import json
import yaml
import collections.abc

DEFAULT_CONFIG_FILE_PATHS = [os.path.join('.','etc','dataset-service.default.yaml'), '/etc/dataset-service/dataset-service.default.yaml']
CONFIG_FILE_PATHS = [os.path.join('.','etc','dataset-service.yaml'), '/etc/dataset-service/dataset-service.yaml']
CONFIG_ENV_VAR_NAME = "DATASET_SERVICE_CONFIG"

def _get_first_existing_path(paths):
    for p in paths:
        if os.path.exists(p):
            return p
    return None

def _update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = _update(d.get(k, {}), v)
        else:
            d[k] = v
    return d

def load_config(config_file_path = None):
    try:
        default_config_file_path = _get_first_existing_path(DEFAULT_CONFIG_FILE_PATHS)
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
        config_file_path = _get_first_existing_path(CONFIG_FILE_PATHS)
    if config_file_path != None and os.path.exists(config_file_path):
        try:
            with open(config_file_path) as f:
                new_config = yaml.load(f, Loader=yaml.SafeLoader)
        except:
            print("ERROR: Configuration file cannot be loaded from " + config_file_path)
            return None
        config = _update(config, new_config)
        print("Additional configuration loaded from file: " + config_file_path)
    
    if CONFIG_ENV_VAR_NAME in os.environ:
        new_config = json.loads(os.environ[CONFIG_ENV_VAR_NAME])
        config = _update(config, new_config)
        print("Additional configuration loaded from environment variable: " + CONFIG_ENV_VAR_NAME)

    # return Config(dictionary=config)
    return Config(config)

# class Config:
#     creation_dict = None
#     def __init__(self, dictionary={}):
#         self.creation_dict = dictionary
#         for k, v in dictionary.items():
#             if type(v) == dict:
#                 setattr(self, k, Config(v))
#             else:
#                 setattr(self, k, v)

class Config:
    def __init__(self, config: dict):
        self.db = Config.DB(config["db"])
        self.auth = Config.Auth(config["auth"])
        self.tracer = Config.Tracer(config["tracer"])
        self.zenodo = Config.Zenodo(config["zenodo"])
        self.self = Config.Self(config["self"])

    class DB:
        def __init__(self, db: dict):
            self.host = db["host"]
            self.port = db["port"]
            self.dbname = db["dbname"]
            self.user = db["user"]
            self.password = db["password"]

    class Auth:
        def __init__(self, auth: dict):
            self.token_validation = Config.Auth.Token_validation(auth["token_validation"])
            self.client = Config.Auth.Client(auth["client"])
            self.admin_api_url = auth["admin_api_url"]

        class Token_validation:
            def __init__(self, token_validation: dict):
                self.token_issuer_public_keys_url = token_validation["token_issuer_public_keys_url"]
                self.kid = token_validation["kid"]
                self.audience = token_validation["audience"]
                self.issuer = token_validation["issuer"]
                self.roles = token_validation["roles"]

        class Client:
            def __init__(self, client: dict):
                self.auth_url = client["auth_url"]
                self.client_id = client["client_id"]
                self.client_secret = client["client_secret"]

    class Tracer:
        def __init__(self, tracer: dict):
            self.url = tracer["url"]

    class Zenodo:
        def __init__(self, zenodo: dict):
            self.url = zenodo["url"]
            self.access_token = zenodo["access_token"]
            self.community = zenodo["community"]
            self.grant = zenodo["grant"]

    class Self:
        def __init__(self, config: dict):
            self.name = config["name"]
            self.host = config["host"]
            self.port = config["port"]
            self.log = Config.Self.Log(config["log"])
            self.static_api_doc_dir_path = config["static_api_doc_dir_path"]
            self.static_files_dir_path = config["static_files_dir_path"]
            self.dev_token = config["dev_token"]
            self.datalakeinfo_dir_path = config["datalakeinfo_dir_path"]
            self.datalakeinfo_token = config["datalakeinfo_token"]
            self.datalake_mount_path = config["datalake_mount_path"]
            self.datasets_mount_path = config["datasets_mount_path"]
            self.eforms_file_name = config["eforms_file_name"]
            self.index_file_name = config["index_file_name"]
            self.dataset_link_format = config["dataset_link_format"]
            self.eucaim_search_token = config["eucaim_search_token"]
            self.default_license_id = config["default_license_id"]

        class Log:
            def __init__(self, log: dict):
                self.main_service = Config.Self.Log.LogConfig(log["main_service"])
                self.dataset_creation_job = Config.Self.Log.LogConfig(log["dataset_creation_job"])

            class LogConfig:
                def __init__(self, log: dict):
                    self.level = log["level"]
                    self.file_path = log["file_path"]
                    self.max_size = log["max_size"]
