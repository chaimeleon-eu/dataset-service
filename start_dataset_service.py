#! /usr/bin/env python3

from ntpath import join
import os
import logging
import logging.handlers
import logging.config
import sys
import threading
import signal
import time
import yaml
import json
import collections.abc

import dataset_service.RESTServer as RESTServer
from dataset_service.config import Config, check_config_items
from dataset_service import __version__, __appname__

DEFAULT_CONFIG_FILE_PATHS = [os.path.join('.','etc','dataset-service.default.yaml'), '/etc/dataset-service/dataset-service.default.yaml']
CONFIG_FILE_PATHS = [os.path.join('.','etc','dataset-service.yaml'), '/etc/dataset-service/dataset-service.yaml']
CONFIG_ENV_VAR_NAME = "DATASET_SERVICE_CONFIG"
THREAD = None


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

def load_config():
    DEFAULT_CONFIG_FILE_PATH = get_first_existing_path(DEFAULT_CONFIG_FILE_PATHS)
    try:
        with open(DEFAULT_CONFIG_FILE_PATH) as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
    except:
       print("ERROR: Default configuration file not found or cannot be loaded from ")
       for p in DEFAULT_CONFIG_FILE_PATHS:
           print("    " + p)
       sys.exit(1)
    print("Default configuration loaded from file: " + DEFAULT_CONFIG_FILE_PATH)
    
    if len(sys.argv) > 1:
        CONFIG_FILE_PATH = sys.argv[1]
        if not os.path.exists(CONFIG_FILE_PATH):
            print("ERROR: Configuration file not found in " + CONFIG_FILE_PATH)
    else:
        CONFIG_FILE_PATH = get_first_existing_path(CONFIG_FILE_PATHS)
    if (not CONFIG_FILE_PATH is None) and os.path.exists(CONFIG_FILE_PATH):
        try:
            with open(CONFIG_FILE_PATH) as f:
                new_config = yaml.load(f, Loader=yaml.SafeLoader)
        except:
            print("ERROR: Configuration file cannot be loaded from " + CONFIG_FILE_PATH)
            sys.exit(1)
        config = update(config, new_config)
        print("Additional configuration loaded from file: " + CONFIG_FILE_PATH)
    
    if CONFIG_ENV_VAR_NAME in os.environ:
        new_config = json.loads(os.environ[CONFIG_ENV_VAR_NAME])
        config = update(config, new_config)
        print("Additional configuration loaded from environment variable: " + CONFIG_ENV_VAR_NAME)

    return Config(dictionary=config)

class ExtraInfoFilter(logging.Filter):
    """
    This is a filter which injects extra attributes into the log.
      * hostname
    """
    def filter(self, record):
        import socket
        record.hostname = socket.gethostname()
        return True

def config_logger(CONFIG):
#   try:
#       logging.config.fileConfig(CONFIG.self.log.config_file)
#   except Exception as ex:
#       print(ex)
    log_dir = os.path.dirname(CONFIG.self.log.file)
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    fileh = logging.handlers.RotatingFileHandler(filename=CONFIG.self.log.file, maxBytes=CONFIG.self.log.file_max_size, backupCount=3)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileh.setFormatter(formatter)

    if CONFIG.self.log.level == "DEBUG":
        log_level = logging.DEBUG
    elif CONFIG.self.log.level == "INFO":
        log_level = logging.INFO
    elif CONFIG.self.log.level in ["WARN", "WARNING"]:
        log_level = logging.WARN
    elif CONFIG.self.log.level == "ERROR":
        log_level = logging.ERROR
    elif CONFIG.self.log.level in ["FATAL", "CRITICAL"]:
        log_level = logging.FATAL
    else:
        log_level = logging.WARN

    logging.RootLogger.propagate = 0
    logging.root.setLevel(logging.ERROR)

    #log = logging.getLogger('module1')
    log = logging.root
    log.setLevel(log_level)
    log.propagate = 0
    log.addHandler(fileh)

    # Add the filter to add extra fields
    try:
        filt = ExtraInfoFilter()
        #log = logging.getLogger('module1')
        log = logging.root
        log.addFilter(filt)
    except Exception as ex:
        print(ex)

    print("Log file is being written in: " + CONFIG.self.log.file)

def start_daemon(CONFIG):
    global THREAD
    logging.root.info( '------------- Starting %s %s -------------' % (__appname__, __version__))

    THREAD = RESTServer.run_in_thread(host=CONFIG.self.host, port=CONFIG.self.port, config=CONFIG)
    while THREAD.is_alive():
        time.sleep(0.1)

    
def stop_daemon( ):
    global THREAD
    RESTServer.stop()

    while THREAD.is_alive():
        time.sleep(0.1)

    logging.root.info( '------------- %s stopped -------------' % __appname__ )


def signal_int_handler(signal, frame):
    """ Callback function to catch the system signals """
    stop_daemon()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_int_handler)

    CONFIG = load_config()
    config_logger(CONFIG)
    start_daemon(CONFIG)
