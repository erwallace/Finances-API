import logging
import coloredlogs


def get_logger(name):
    return logging.getLogger(name)


logger = get_logger(__name__)

# log to console
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.INFO)
c_format = coloredlogs.ColoredFormatter('%(levelname)s: %(message)s')
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)

'''
# log to file
f_handler = logging.FileHandler('logging_output.txt', 'a')
f_handler.setLevel(logging.DEBUG)
f_format = coloredlogs.ColoredFormatter('%(name)s %(levelname)s: %(message)s')
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)
'''

# install coloredlogs module on the root logger
coloredlogs.install(level='INFO')