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

def config_logger(CONFIG):
#   try:
#       logging.config.fileConfig(CONFIG.self.log.config_file)
#   except Exception as ex:
#       print(ex)
    log_dir = os.path.dirname(CONFIG.self.log.file)
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)

    file_handler = logging.handlers.RotatingFileHandler(filename=CONFIG.self.log.file, maxBytes=CONFIG.self.log.file_max_size, backupCount=3)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

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

    print("Log file is being written in: " + CONFIG.self.log.file)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    log.addHandler(stdout_handler)
    print("Log file is also being written in the standard output.")
