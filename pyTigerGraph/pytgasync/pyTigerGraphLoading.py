"""Loading Job Functions

The functions on this page run loading jobs on the TigerGraph server.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging
import warnings

from typing import TYPE_CHECKING, Union
if TYPE_CHECKING:
    import pandas as pd

from pyTigerGraph.common.loading import (
    _prep_run_loading_job_with_file,
    _prep_loading_job_url,
    _prep_loading_job_info,
    _prep_run_loading_job,
    _prep_abort_loading_jobs,
    _prep_abort_one_loading_job,
    _prep_resume_loading_job,
    _prep_get_loading_job_status,
    _prep_get_loading_jobs_status
)
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
        logger.debug("entry: runLoadingJobWithDataFrame")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if columns is None:
            data = df.to_csv(sep = sep, header=False)
        else:
            data = df.to_csv(columns = columns, sep = sep, header=False)

        res = await self.runLoadingJobWithData(data, fileTag, jobName, sep, eol, timeout, sizeLimit)

        logger.debug("exit: runLoadingJobWithDataFrame")

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
        logger.debug("entry: runLoadingJobWithFile")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        data = _prep_run_loading_job_with_file(filePath)
        res = await self.runLoadingJobWithData(data, fileTag, jobName, sep, eol, timeout, sizeLimit)

        logger.debug("exit: runLoadingJobWithFile")

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
        logger.debug("entry: runLoadingJobWithData")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if not data or not jobName or not fileTag:
            # invalid inputs
            logger.error("Invalid data or params")
            logger.debug("exit: runLoadingJobWithData")
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
        logger.debug("exit: runLoadingJobWithData")

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
    async def getLoadingJobs(self) -> dict:
        """Get a list of all loading jobs for the current graph.

        Endpoint:
            - `GET /gsql/v1/loading-jobs?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_get_loading_job_names[Get loading jobs]
        """
        logger.debug("entry: getLoadingJobs")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_loading_job_url(self.gsUrl, self.graphname)

        res = await self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: getLoadingJobs")

        return res
    
    async def createLoadingJob(self, job_definition: str) -> dict:
        """Create a new loading job with the given definition.

        Args:
            job_definition:
                The definition of the loading job in GSQL DDL format.

        Endpoint:
            - `POST /gsql/v1/loading-jobs?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_create_loading_job[Create a loading job]
        """
        logger.debug("entry: createLoadingJob")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_loading_job_url(self.gsUrl, self.graphname)

        res = self._req("POST", url, data=job_definition)

        logger.debug("return: " + str(res))
        logger.debug("exit: createLoadingJob")

        return res
    
    async def updateLoadingJob(self, job_definition: str) -> dict:
        """Update an existing loading job with the given definition.

        Args:
            job_definition:
                The definition of the loading job in GSQL DDL format.

        Endpoint:
            - `PUT /gsql/v1/loading-jobs/<job_name>?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_upload_a_loading_job[Upload a loading job]
        """
        logger.debug("entry: updateLoadingJob")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_loading_job_url(self.gsUrl, self.graphname)

        res = await self._req("PUT", url, data=job_definition)

        logger.debug("return: " + str(res))
        logger.debug("exit: updateLoadingJob")

        return res
    
    async def getLoadingJobInfo(self, jobName: str, verbose: bool = False) -> dict:
        """Get information about the specified loading job.

        Args:
            jobName:
                The name of the loading job.
            verbose:
                If `True`, return verbose information about the job.

        Endpoint:
            - `GET /gsql/v1/loading-jobs/<job_name>?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_get_loading_job_info[Get loading job info]
        """
        logger.debug("entry: getLoadingJobInfo")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_loading_job_info(self.gsUrl, jobName, self.graphname, verbose)

        res = await self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: getLoadingJobInfo")

        return res

    async def runLoadingJob(self, jobName: str, data_source_config: dict, sys_data_root: str = None,
                      verbose: bool = False, dryrun: bool = False, interval: int = None,
                      maxNumError: int = None, maxPercentError: float = None) -> dict:
        """Run the specified loading job with the given data source configuration.

        Args:
            jobName:
                The name of the loading job.
            data_source_config:
                The data source configuration in dictionary format.
            sys_data_root:
                The system data root.
            verbose:
                If `True`, return verbose information about the job.
            dryrun:
                If `True`, run the job in dry-run mode.
            interval:
                The interval in seconds between each batch of data.
            maxNumError:
                The maximum number of errors allowed.
            maxPercentError:
                The maximum percentage of errors allowed.

        Endpoint:
            - `POST /gsql/v1/loading-jobs/run?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_run_loading_job[Run a loading job]
        """
        logger.debug("entry: runLoadingJob")
        logger.debug("params: " + self._locals(locals()))

        url, data = _prep_run_loading_job(self.gsUrl, self.graphname, jobName, data_source_config, sys_data_root, verbose, dryrun, interval, maxNumError, maxPercentError)

        res = await self._req("POST", url, data=data)

        logger.debug("return: " + str(res))
        logger.debug("exit: runLoadingJob")

        return res
    
    async def dropLoadingJob(self, jobName: str) -> dict:
        """Drop the specified loading job.

        Args:
            jobName:
                The name of the loading job.

        Endpoint:
            - `DELETE /gsql/v1/loading-jobs/<job_name>?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_drop_a_loading_job[Drop a loading job]
        """
        logger.debug("entry: dropLoadingJob")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_loading_job_info(self.gsUrl, jobName, self.graphname)

        res = await self._req("DELETE", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: dropLoadingJob")

        return res
    
    async def abortLoadingJobs(self, jobIds: list[str], pauseJob: bool = False) -> dict:
        """Abort the specified loading jobs.

        Args:
            jobIds:
                A list of job IDs to abort.
            pauseJob:
                If `True`, pause the job instead of aborting it.

        Endpoint:
            - `POST /gsql/v1/loading-jobs/abort?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_abort_loading_jobs[Abort loading jobs]
        """
        logger.debug("entry: abortLoadingJobs")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_abort_loading_jobs(self.gsUrl, self.graphname, jobIds, pauseJob)

        res = await self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: abortLoadingJobs")

        return res
    
    async def abortLoadingJob(self, jobId: str, pauseJob: bool = False) -> dict:
        """Abort the specified loading job.

        Args:
            jobId:
                The ID of the job to abort.
            pauseJob:
                If `True`, pause the job instead of aborting it.

        Endpoint:
            - `POST /gsql/v1/loading-jobs/abort?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_abort_one_loading_job
        """
        logger.debug("entry: abortLoadingJob")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_abort_one_loading_job(self.gsUrl, self.graphname, jobId, pauseJob)

        res = await self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: abortLoadingJob")

        return res
    
    async def resumeLoadingJob(self, jobId: str) -> dict:
        """Resume the specified loading job.

        Args:
            jobId:
                The ID of the job to resume.

        Endpoint:
            - `POST /gsql/v1/loading-jobs/resume?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_resume_loading_job
        """
        logger.debug("entry: resumeLoadingJob")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_resume_loading_job(self.gsUrl, jobId)

        res = await self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: resumeLoadingJob")

        return res
    
    async def getLoadingJobsStatus(self, jobIds: list[str]) -> dict:
        """Get the status of the specified loading jobs.

        Args:
            jobIds:
                A list of job IDs to get the status of.

        Endpoint:
            - `GET /gsql/v1/loading-jobs/status?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_get_loading_job_status[Get loading job status]
        """
        logger.debug("entry: getLoadingJobsStatus")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_get_loading_jobs_status(self.gsUrl, self.graphname, jobIds)

        res = await self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: getLoadingJobsStatus")

        return res
    
    async def getLoadingJobStatus(self, jobId: str) -> dict:
        """Get the status of the specified loading job.

        Args:
            jobId:
                The ID of the job to get the status of.

        Endpoint:
            - `GET /gsql/v1/loading-jobs/status/<job_id>?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_get_one_loading_job_status[Get one loading job status]
        """
        logger.debug("entry: getLoadingJobStatus")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_get_loading_job_status(self.gsUrl, self.graphname, jobId)

        res = await self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: getLoadingJobStatus")

        return res
