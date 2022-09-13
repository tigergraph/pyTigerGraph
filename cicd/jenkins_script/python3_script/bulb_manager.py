#!/usr/bin/python3

from check_issue import main as issue_checker
from datetime import datetime
import os
import time
import sys
import subprocess
import traceback, util
import logging
import logging.handlers

def is_blocking(logger):
    '''
    Parameters
       0 logger: the logging system to use MUST be an instance of class logging

    return
       0 if there are no issues
       1 if there are blocking issues
       2 if there are internet or other code related issues
    '''
    flag = 0
    try:
        issue_checker([__file__, 'check', 'QA', 'Bug', 'QA_HOURLY_FAILURE', logger])
        logger.info("green")
        logger.info("QA Fault check passed. Green Light")
    except util.IssueExistanceFail as e: # detect qa hourly fail
        logger.error(str(sys.exc_info()[0]))
        logger.error(str(traceback.format_exc()))
        flag = 1
        logger.error("red")
    except:
        logger.error("purple")
        logger.error(str(traceback.format_exc()))
        logger.error("Internet issue. Purple Light")
        flag = 2
    return flag

def main():
    color = {"red": "KEY_0", "green": "KEY_1", "purple": "KEY_2"}
    sleep_time = 10 # in seconds
    log_filename = '/home/pi/bulb_logs/bulb.out'
    bulb_logger = logging.getLogger('bulb_logger')
    bulb_logger.setLevel(logging.DEBUG)
    #Rotate the log every hour keeping the logs for the past 72 hours
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.handlers.TimedRotatingFileHandler(log_filename, 'h', 1, 72)
    handler.setFormatter(formatter)
    bulb_logger.addHandler(handler)

    #Check every 5 seconds for hourly failure ticket on JIRA
    while True:
        issue = is_blocking(bulb_logger)
        if  issue == 1:
            key_name = color['red']
        elif issue == 2:
            key_name = color['purple']
        else:
            key_name = color['green']
        os.system("irsend SEND_ONCE /home/pi/lircd0.conf {}".format(key_name))
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
