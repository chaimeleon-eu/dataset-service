import os
import sys
import logging
import logging.handlers
import logging.config

class ExtraInfoFilter(logging.Filter):
    """
    This is a filter which injects extra attributes into the log.
      * hostname
    """
    def filter(self, record):
        import socket
        setattr(record, 'hostname', socket.gethostname())
        return True

def config_logger(level: str, file_path: str, max_size: int):
    log_dir = os.path.dirname(file_path)
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    file_handler = logging.handlers.RotatingFileHandler(filename=file_path, maxBytes=max_size, backupCount=3)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    level = level.strip().upper()
    if level == "DEBUG":   log_level = logging.DEBUG
    elif level == "INFO":  log_level = logging.INFO
    elif level == "WARN":  log_level = logging.WARN
    elif level == "ERROR": log_level = logging.ERROR
    elif level == "FATAL": log_level = logging.FATAL
    else: raise Exception("Unkown level '%s' for log configuration." % level)

    logging.RootLogger.propagate = False
    logging.root.setLevel(logging.ERROR)

    #log = logging.getLogger('module1')
    log = logging.root
    log.setLevel(log_level)
    log.propagate = False
    log.addHandler(file_handler)

    # Add the filter to add extra fields
    try:
        filt = ExtraInfoFilter()
        #log = logging.getLogger('module1')
        log = logging.root
        log.addFilter(filt)
    except Exception as ex:
        print(ex)

    print("Log file is being written in: " + file_path)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    log.addHandler(stdout_handler)
    print("Log file is also being written in the standard output.")
