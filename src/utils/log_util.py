import logging
import logging.handlers

import config

def get_logger(name: str, level: int|str, console: bool=True, file: bool=True, terminator: str='\n') -> logging.Logger:
    """
    name:
        The module name given by __name__
    level:
        10 - DEBUG
        20 - INFO
        30 - WARNING
        40 - ERROR
        50 - CRITICAL
    """
    formatter = logging.Formatter(**config.LOGGING['formatter'])
    log = logging.getLogger(name)
    if console:
        console_handler = logging.StreamHandler()
        console_handler.terminator = terminator
        console_handler.setFormatter(fmt=formatter)
        log.addHandler(console_handler)
        console_handler.setLevel(config.LOGGING['levels']['console'])
    if file:
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename='logs',
            encoding='utf-8',
            when='D',
            interval=7
        )
        file_handler.setFormatter(fmt=formatter)
        log.addHandler(file_handler)
        file_handler.setLevel(config.LOGGING['levels']['file'])

    return log