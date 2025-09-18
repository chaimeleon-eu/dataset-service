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
        self.on_event_scripts = Config.On_event_scripts(config["on_event_scripts"])
        self.logos = Config.Logos(config["logos"])
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
            self.admin_api = Config.Auth.Admin_api(auth["admin_api"])
            self.user_management = Config.Auth.User_management(auth["user_management"])

        class Token_validation:
            def __init__(self, token_validation: dict):
                self.token_issuer_public_keys_url = token_validation["token_issuer_public_keys_url"]
                self.kid = token_validation["kid"]
                self.client_id = token_validation["client_id"]
                self.issuer = token_validation["issuer"]
                self.roles = token_validation["roles"]
                self.project_group_prefix = token_validation["project_group_prefix"]
                self.project_admins_group_prefix = token_validation["project_admins_group_prefix"]

        class Client:
            def __init__(self, client: dict):
                self.auth_url = client["auth_url"]
                self.client_id = client["client_id"]
                self.client_secret = client["client_secret"]
        
        class Admin_api:
            def __init__(self, admin_api: dict):
                self.url = admin_api["url"]
                self.client_id_to_request_user_tokens = admin_api["client_id_to_request_user_tokens"]
                self.parent_group_of_project_groups = admin_api["parent_group_of_project_groups"]

        class User_management:
            def __init__(self, user_management: dict):
                self.assignable_general_roles = user_management["assignable_general_roles"]
                self.prefix_for_roles_as_groups = user_management["prefix_for_roles_as_groups"]
                self.prefix_for_projects_as_groups = user_management["prefix_for_projects_as_groups"]

    class Tracer:
        def __init__(self, tracer: dict):
            self.url = tracer["url"]

    class Zenodo:
        def __init__(self, zenodo: dict):
            self.url = zenodo["url"]
    
    class On_event_scripts:
        def __init__(self, on_event_scripts: dict):
            self.user_management_job_template_file_path = on_event_scripts["user_management_job_template_file_path"]
            self.site_management_job_template_file_path = on_event_scripts["site_management_job_template_file_path"]
            self.subproject_management_job_template_file_path = on_event_scripts["subproject_management_job_template_file_path"]

    class Logos:
        def __init__(self, logos: dict):
            self.image_size_px = logos["image_size_px"]
            self.max_upload_file_size_mb = logos["max_upload_file_size_mb"]

    class Self:
        def __init__(self, config: dict):
            self.name = config["name"]
            self.host = config["host"]
            self.port = config["port"]
            self.root_url = config["root_url"]
            self.log = Config.Self.Log(config["log"])
            self.static_api_doc_dir_path = config["static_api_doc_dir_path"]
            self.static_files_dir_path = config["static_files_dir_path"]
            self.static_files_logos_dir_path = config["static_files_logos_dir_path"]
            self.static_files_output_dir_path = config["static_files_output_dir_path"]
            self.dev_token = config["dev_token"]
            self.datalakeinfo_dir_path = config["datalakeinfo_dir_path"]
            self.datalakeinfo_token = config["datalakeinfo_token"]
            self.datalake_mount_path = config["datalake_mount_path"]
            self.datasets_mount_path = config["datasets_mount_path"]
            self.datalake_external_subpath = config["datalake_external_subpath"]
            self.external_datasets_project_code = config["external_datasets_project_code"]
            self.eforms_file_name = config["eforms_file_name"]
            self.index_file_name = config["index_file_name"]
            self.studies_tmp_file_name = config["studies_tmp_file_name"]
            self.dataset_link_format = config["dataset_link_format"]
            self.eucaim_search_token = config["eucaim_search_token"]
            self.eucaim_search_filter_by_tag = config["eucaim_search_filter_by_tag"]
            self.dataset_integrity_check_life_days = config["dataset_integrity_check_life_days"]
            self.series_hash_cache_life_days = config["series_hash_cache_life_days"]

        class Log:
            def __init__(self, log: dict):
                self.main_service = Config.Self.Log.LogConfig(log["main_service"])
                self.dataset_creation_job = Config.Self.Log.LogConfig(log["dataset_creation_job"])

            class LogConfig:
                def __init__(self, log: dict):
                    self.level = log["level"]
                    self.file_path = log["file_path"]
                    self.max_size = log["max_size"]
