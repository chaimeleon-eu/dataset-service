#! /usr/bin/env python3

import sys
import logging
import signal
import threading
import time

from dataset_service.dataset_creation_worker import dataset_creation_worker
from dataset_service.config import load_config
from dataset_service.logger import config_logger
from dataset_service import __version__, __appname__

THREAD = None
worker = None

def start_job(CONFIG, datasetId):
    global THREAD, worker
    logging.root.info( '------------- Starting %s job v%s -------------' % (__appname__, __version__))
    worker = dataset_creation_worker(CONFIG, datasetId)
    THREAD = threading.Thread(target=dataset_creation_worker.run, args=[worker])
    THREAD.daemon = True
    THREAD.start()
    while THREAD.is_alive():
        time.sleep(0.1)
    logging.root.info( '------------- Finished %s job -------------' % __appname__)

def stop_job( ):
    global THREAD, worker
    if worker is None: return
    worker.stop()
    while THREAD != None and THREAD.is_alive():
        time.sleep(0.1)
    logging.root.info( '------------- %s-job stopped -------------' % __appname__ )

def signal_int_handler(signal, frame):
    """ Callback function to catch the system signals """
    stop_job()

if __name__ == "__main__":
    # capture signals for graceful termination
    signal.signal(signal.SIGINT, signal_int_handler)   # The signal sent by ctrl-c in shell (when the python process is in foreground).
    signal.signal(signal.SIGTERM, signal_int_handler)  # The signal sent by command 'kill' (the default, i.e. no specifying the signal parameter)
                                                       #             and by kubernetes 30 sec before SIGKILL and force the container deletion.
    if len(sys.argv) <= 1:
        print("ERROR: at least 1 parameter is required with the id of dataset already created in DB. \n"
             +"The job will do the hard work based on that entry of DB: \n"
             +"  - Create the dataset in the file system (the symbolic links).\n"
             +"  - Calculate the hashes and send them to the tracer.\n"
             +"It will notify the progress also in the DB (in the corresponding dataset_creation_status entry).\n"
             +"And finally it will remove the dataset_creation_status entry in DB to notify that the job finish successfully.\n")
        sys.exit(1)
    datasetId = sys.argv[1]
    config_file_path = sys.argv[2] if len(sys.argv) > 2 else None
    CONFIG = load_config(config_file_path)
    if CONFIG is None: sys.exit(1)
    log_conf = CONFIG.self.log.dataset_creation_job
    config_logger(log_conf.level, log_conf.file_path_format % datasetId, log_conf.max_size)
    start_job(CONFIG, datasetId)
