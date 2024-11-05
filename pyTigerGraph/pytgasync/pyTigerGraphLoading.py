"""Loading Job Functions

The functions on this page run loading jobs on the TigerGraph server.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging
import warnings

from typing import TYPE_CHECKING, Union
if TYPE_CHECKING:
    import pandas as pd

from pyTigerGraph.common.loading import _prep_run_loading_job_with_file
from pyTigerGraph.pytgasync.pyTigerGraphBase import AsyncPyTigerGraphBase

logger = logging.getLogger(__name__)


class AsyncPyTigerGraphLoading(AsyncPyTigerGraphBase):

    async def runLoadingJobWithDataFrame(self, df: 'pd.DataFrame', fileTag: str, jobName: str, sep: str = None,
                                         eol: str = None, timeout: int = 16000, sizeLimit: int = 128000000, columns: list = None) -> Union[dict, None]:
        """Execute a loading job with the given pandas DataFrame with optional column list.

        The data string will be posted to the TigerGraph server and the value of the appropriate
        FILENAME definition will be updated to point to the data received.

        NOTE: The argument `USING HEADER="true"` in the GSQL loading job may not be enough to
        load the file correctly. Remove the header from the data file before using this function.

        Args:
            df:
                The pandas DateFrame data structure to be loaded.
            fileTag:
                The name of file variable in the loading job (DEFINE FILENAME <fileTag>).
            jobName:
                The name of the loading job.
            sep:
                Data value separator. If your data is JSON, you do not need to specify this
                parameter. The default separator is a comma `,`.
            eol:
                End-of-line character. Only one or two characters are allowed, except for the
                special case `\\r\\n`. The default value is `\\n`
            timeout:
                Timeout in seconds. If set to `0`, use the system-wide endpoint timeout setting.
            sizeLimit:
                Maximum size for input file in bytes.
            columns:
                The ordered pandas DataFrame columns to be uploaded.

        Endpoint:
            - `POST /ddl/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_a_loading_job[Run a loading job]
        """
        logger.info("entry: runLoadingJobWithDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if columns is None:
            data = df.to_csv(sep = sep, header=False)
        else:
            data = df.to_csv(columns = columns, sep = sep, header=False)

        res = await self.runLoadingJobWithData(data, fileTag, jobName, sep, eol, timeout, sizeLimit)

        logger.info("exit: runLoadingJobWithDataFrame")

        return res

    async def runLoadingJobWithFile(self, filePath: str, fileTag: str, jobName: str, sep: str = None,
                                    eol: str = None, timeout: int = 16000, sizeLimit: int = 128000000) -> Union[dict, None]:
        """Execute a loading job with the referenced file.

        The file will first be uploaded to the TigerGraph server and the value of the appropriate
        FILENAME definition will be updated to point to the freshly uploaded file.

        NOTE: The argument `USING HEADER="true"` in the GSQL loading job may not be enough to
        load the file correctly. Remove the header from the data file before using this function.

        Args:
            filePath:
                File variable name or file path for the file containing the data.
            fileTag:
                The name of file variable in the loading job (DEFINE FILENAME <fileTag>).
            jobName:
                The name of the loading job.
            sep:
                Data value separator. If your data is JSON, you do not need to specify this
                parameter. The default separator is a comma `,`.
            eol:
                End-of-line character. Only one or two characters are allowed, except for the
                special case `\\r\\n`. The default value is `\\n`
            timeout:
                Timeout in seconds. If set to `0`, use the system-wide endpoint timeout setting.
            sizeLimit:
                Maximum size for input file in bytes.

        Endpoint:
            - `POST /ddl/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_a_loading_job[Run a loading job]
        """
        logger.info("entry: runLoadingJobWithFile")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        data = _prep_run_loading_job_with_file(filePath)
        res = await self.runLoadingJobWithData(data, fileTag, jobName, sep, eol, timeout, sizeLimit)

        logger.info("exit: runLoadingJobWithFile")

        return res

    async def runLoadingJobWithData(self, data: str, fileTag: str, jobName: str, sep: str = None,
                                    eol: str = None, timeout: int = 16000, sizeLimit: int = 128000000) -> Union[dict, None]:
        """Execute a loading job with the given data string.

        The data string will be posted to the TigerGraph server and the value of the appropriate
        FILENAME definition will be updated to point to the data received.

        NOTE: The argument `USING HEADER="true"` in the GSQL loading job may not be enough to
        load the file correctly. Remove the header from the data file before using this function.

        Args:
            data:
                The data string to be loaded.
            fileTag:
                The name of file variable in the loading job (DEFINE FILENAME <fileTag>).
            jobName:
                The name of the loading job.
            sep:
                Data value separator. If your data is JSON, you do not need to specify this
                parameter. The default separator is a comma `,`.
            eol:
                End-of-line character. Only one or two characters are allowed, except for the
                special case `\\r\\n`. The default value is `\\n`
            timeout:
                Timeout in seconds. If set to `0`, use the system-wide endpoint timeout setting.
            sizeLimit:
                Maximum size for input file in bytes.

        Endpoint:
            - `POST /ddl/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_a_loading_job[Run a loading job]
        """
        logger.info("entry: runLoadingJobWithData")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not data or not jobName or not fileTag:
            # invalid inputs
            logger.error("Invalid data or params")
            logger.info("exit: runLoadingJobWithData")
            return None

        params = {
            "tag": jobName,
            "filename": fileTag,
        }
        if sep is not None:
            params["sep"] = sep
        if eol is not None:
            params["eol"] = eol

        if isinstance(data, str):
            data = data.encode("utf-8")
            res = await self._req("POST", self.restppUrl + "/ddl/" + self.graphname, params=params, data=data,
                            headers={"Content-Type": "application/x-www-form-urlencoded; Charset=utf-8", "RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})
        else:
            res = await self._req("POST", self.restppUrl + "/ddl/" + self.graphname, params=params, data=data,
                            headers={"RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(res))
        logger.info("exit: runLoadingJobWithData")

        return res

    async def uploadFile(self, filePath, fileTag, jobName="", sep=None, eol=None, timeout=16000,
                         sizeLimit=128000000) -> dict:
        """DEPRECATED

        Use `runLoadingJobWithFile()` instead.
        """
        warnings.warn(
            "The `uploadFile()` function is deprecated; use `runLoadingJobWithFile()` instead.",
            DeprecationWarning)

        return await self.runLoadingJobWithFile(filePath, fileTag, jobName, sep, eol, timeout, sizeLimit)

    # TODO POST /restpploader/{graph_name}
