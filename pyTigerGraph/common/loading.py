"""Loading Job Functions

The functions on this page run loading jobs on the TigerGraph server.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging
import warnings

from typing import Union

from pyTigerGraph.common.base import pyTigerGraphBaseBase


logger = logging.getLogger(__name__)


class pyTigerGraphBaseLoading(pyTigerGraphBaseBase):

    def _prepRunLoadingJobWithFile(self, filePath, jobName, fileTag, sep, eol):
        '''read file contents for runLoadingJobWithFile()'''
        try:
            data = open(filePath, 'rb').read()
            params = {
                "tag": jobName,
                "filename": fileTag,
            }
            if sep is not None:
                params["sep"] = sep
            if eol is not None:
                params["eol"] = eol
            return data, params
        except OSError as ose:
            logger.error(ose.strerror)
            logger.info("exit: runLoadingJobWithFile")

            return None, None
            # TODO Should throw exception instead?
