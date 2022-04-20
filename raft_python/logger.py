import logging
from raft_python.configs import LOGGER_NAME


def set_up_logger(id: int):
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)

    # create console handler logging info messages
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # create file handler which logs even debug messages
    file_handler = logging.FileHandler('raft_python_debug.log')
    file_handler.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        f'%(asctime)s - Replica {id} - %(levelname)s - %(message)s')

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
