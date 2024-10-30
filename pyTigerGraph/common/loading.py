"""Loading Job Functions

The functions on this page run loading jobs on the TigerGraph server.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging


logger = logging.getLogger(__name__)

def _prep_run_loading_job_with_file(filePath):
    '''read file contents for runLoadingJobWithFile()'''
    try:
        data = open(filePath, 'rb').read()
        return data
    except OSError as ose:
        logger.error(ose.strerror)
        logger.info("exit: runLoadingJobWithFile")

        return None
        # TODO Should throw exception instead?
