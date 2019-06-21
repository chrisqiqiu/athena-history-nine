import logging
# import sys


def setup_logger(module_name):
    FORMAT = '[%(asctime)s][%(name)s][%(levelname)-8s] (L:%(lineno)s) %(funcName)s: %(message)s'
    logging.basicConfig(format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    # logger.addHandler(logging.StreamHandler(sys.stdout))

    return logger
