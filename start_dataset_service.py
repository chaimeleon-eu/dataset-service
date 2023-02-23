#! /usr/bin/env python3

import sys
import logging
import signal
import time

import dataset_service.RESTServer as RESTServer
from dataset_service.config import load_config
from dataset_service.logger import config_logger
from dataset_service import __version__, __appname__

THREAD = None

def start_daemon(CONFIG):
    global THREAD
    logging.root.info( '------------- Starting %s %s -------------' % (__appname__, __version__))

    THREAD = RESTServer.run_in_thread(host=CONFIG.self.host, port=CONFIG.self.port, config=CONFIG)
    while THREAD.is_alive():
        time.sleep(0.1)

    
def stop_daemon( ):
    global THREAD
    RESTServer.stop()

    while THREAD != None and THREAD.is_alive():
        time.sleep(0.1)

    logging.root.info( '------------- %s stopped -------------' % __appname__ )


def signal_int_handler(signal, frame):
    """ Callback function to catch the system signals """
    stop_daemon()


if __name__ == "__main__":
    # capture signals for graceful termination
    signal.signal(signal.SIGINT, signal_int_handler)   # The signal sent by ctrl-c in shell (when the python process is in foreground).
    signal.signal(signal.SIGTERM, signal_int_handler)  # The signal sent by command 'kill' (the default, i.e. no specifying the signal parameter)
                                                       #             and by kubernetes 30 sec before SIGKILL and force the container deletion.
    config_file_path = sys.argv[1] if len(sys.argv) > 1 else None
    CONFIG = load_config(config_file_path)
    if CONFIG is None: sys.exit(1)
    config_logger(CONFIG)
    start_daemon(CONFIG)
