"""Loading Job Functions

The functions on this page run loading jobs on the TigerGraph server.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import logging
import warnings

from typing import TYPE_CHECKING, Union
if TYPE_CHECKING:
    import pandas as pd

import json

from pyTigerGraph.common.loading import (
    _prep_run_loading_job_with_file,
    _prep_loading_job_url,
    _prep_loading_job_info,
    _prep_run_loading_job,
    _prep_abort_loading_jobs,
    _prep_abort_one_loading_job,
    _prep_resume_loading_job,
    _prep_get_loading_jobs_status,
    _prep_get_loading_job_status,
    _prep_data_source_url,
    _prep_data_source_by_name,
    _prep_drop_all_data_sources,
    _prep_sample_data_url,
)

from pyTigerGraph.common.gsql import _wrap_gsql_result
from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase

logger = logging.getLogger(__name__)


class pyTigerGraphLoading(pyTigerGraphBase):

    def runLoadingJobWithDataFrame(self, df: 'pd.DataFrame', fileTag: str, jobName: str, sep: str = None,
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
                Timeout in milliseconds. If set to `0`, use the system-wide endpoint timeout setting.
            sizeLimit:
                Maximum size for input file in bytes.
            columns:
                The ordered pandas DataFrame columns to be uploaded.

        Endpoint:
            - `POST /ddl/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_a_loading_job[Run a loading job]
        """
        logger.debug("entry: runLoadingJobWithDataFrame")

        logger.debug("params: " + self._locals(locals()))

        if columns is None:
            data = df.to_csv(sep = sep, header=False)
        else:
            data = df.to_csv(columns = columns, sep = sep, header=False)

        res = self.runLoadingJobWithData(data, fileTag, jobName, sep, eol, timeout, sizeLimit)

        logger.debug("exit: runLoadingJobWithDataFrame")

        return res

    def runLoadingJobWithFile(self, filePath: str, fileTag: str, jobName: str, sep: str = None,
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
                Timeout in milliseconds. If set to `0`, use the system-wide endpoint timeout setting.
            sizeLimit:
                Maximum size for input file in bytes.

        Endpoint:
            - `POST /ddl/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_a_loading_job[Run a loading job]
        """
        logger.debug("entry: runLoadingJobWithFile")

        logger.debug("params: " + self._locals(locals()))

        data = _prep_run_loading_job_with_file(filePath)
        res = self.runLoadingJobWithData(data, fileTag, jobName, sep, eol, timeout, sizeLimit)

        logger.debug("exit: runLoadingJobWithFile")

        return res

    def runLoadingJobWithData(self, data: str, fileTag: str, jobName: str, sep: str = None,
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
                Timeout in milliseconds. If set to `0`, use the system-wide endpoint timeout setting.
            sizeLimit:
                Maximum size for input file in bytes.

        Endpoint:
            - `POST /ddl/{graph_name}`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_run_a_loading_job[Run a loading job]
        """
        logger.debug("entry: runLoadingJobWithData")

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
            res = self._req("POST", self.restppUrl + "/ddl/" + self.graphname, params=params, data=data,
                            headers={"Content-Type": "application/x-www-form-urlencoded; Charset=utf-8", "RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})
        else:
            res = self._req("POST", self.restppUrl + "/ddl/" + self.graphname, params=params, data=data,
                            headers={"RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})

        logger.debug("return: " + str(res))
        logger.debug("exit: runLoadingJobWithData")

        return res

    def uploadFile(self, filePath, fileTag, jobName="", sep=None, eol=None, timeout=16000,
                   sizeLimit=128000000) -> dict:
        """DEPRECATED

        Use `runLoadingJobWithFile()` instead.
        """
        warnings.warn(
            "The `uploadFile()` function is deprecated; use `runLoadingJobWithFile()` instead.",
            DeprecationWarning)

        return self.runLoadingJobWithFile(filePath, fileTag, jobName, sep, eol, timeout, sizeLimit)

    # TODO POST /restpploader/{graph_name}

    def getLoadingJobs(self) -> dict:
        """Get a list of all loading jobs for the current graph.

        Endpoint:
            - `GET /gsql/v1/loading-jobs?graph=<graph_name>`
                See xref:tigergraph-server:API:gsql-endpoints.adoc#_get_loading_job_names[Get loading jobs]
        """
        logger.debug("entry: getLoadingJobs")

        logger.debug("params: " + self._locals(locals()))

        url = _prep_loading_job_url(self.gsUrl, self.graphname)

        res = self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: getLoadingJobs")

        return res

    def createLoadingJob(self, job_definition: str) -> dict:
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

    def updateLoadingJob(self, job_definition: str) -> dict:
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

        res = self._req("PUT", url, data=job_definition)

        logger.debug("return: " + str(res))
        logger.debug("exit: updateLoadingJob")

        return res

    def getLoadingJobInfo(self, jobName: str, verbose: bool = False) -> dict:
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

        res = self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: getLoadingJobInfo")

        return res

    def runLoadingJob(self, jobName: str, data_source_config: dict, sys_data_root: str = None,
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

        res = self._req("POST", url, data=data)

        logger.debug("return: " + str(res))
        logger.debug("exit: runLoadingJob")

        return res

    def dropLoadingJob(self, jobName: str) -> dict:
        """Drop the specified loading job.

        Args:
            jobName:
                The name of the loading job.

        Returns:
            A dict with at least a ``"message"`` key describing the outcome.

        Endpoint:
            - `DELETE /gsql/v1/loading-jobs/<job_name>?graph=<graph_name>` (In TigerGraph versions >= 4.0)
            - Falls back to GSQL ``DROP JOB`` for TigerGraph versions < 4.0
        """
        logger.debug("entry: dropLoadingJob")
        logger.debug("params: " + self._locals(locals()))

        if self._version_greater_than_4_0():
            url = _prep_loading_job_info(self.gsUrl, jobName, self.graphname)
            res = self._req("DELETE", url)
        else:
            res = _wrap_gsql_result(self.gsql(f"USE GRAPH {self.graphname}\nDROP JOB {jobName}"))

        logger.debug("return: " + str(res))
        logger.debug("exit: dropLoadingJob")

        return res

    def abortLoadingJobs(self, jobIds: list[str], pauseJob: bool = False) -> dict:
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

        res = self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: abortLoadingJobs")

        return res

    def abortLoadingJob(self, jobId: str, pauseJob: bool = False) -> dict:
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

        res = self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: abortLoadingJob")

        return res

    def resumeLoadingJob(self, jobId: str) -> dict:
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

        res = self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: resumeLoadingJob")

        return res

    def getLoadingJobsStatus(self, jobIds: list[str]) -> dict:
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

        res = self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: getLoadingJobsStatus")

        return res

    def getLoadingJobStatus(self, jobId: str) -> dict:
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

        res = self._req("GET", url)

        logger.debug("return: " + str(res))
        logger.debug("exit: getLoadingJobStatus")

        return res

    # =====================================================================
    # Data Source Management
    # =====================================================================

    def createDataSource(self, dsName: str, config: dict, graphName: str = None) -> dict:
        """Create a new data source.

        On TigerGraph 4.x uses REST API ``POST /gsql/v1/data-sources``.
        On 3.x falls back to ``CREATE DATA_SOURCE`` via GSQL console.

        Args:
            dsName:  Name for the data source.
            config:  Configuration dict.  Must contain a ``"type"`` key
                     (e.g. ``"s3"``, ``"gcs"``, ``"kafka"``).
            graphName:  Graph to associate the data source with.
                        Defaults to the connection's current graph.

        Returns:
            A dict with at least a ``"message"`` key describing the outcome.
        """
        graph = graphName or self.graphname
        if self._version_greater_than_4_0():
            url = _prep_data_source_url(self.gsUrl, graph)
            body = {"name": dsName, "config": config}
            return self._req("POST", url, data=body, jsonData=True, resKey=None)

        config_json = json.dumps(config)
        gsql_cmd = f"CREATE DATA_SOURCE {dsName} = '{config_json}'"
        if graph:
            gsql_cmd += f" FOR GRAPH {graph}"
        return _wrap_gsql_result(self.gsql(gsql_cmd))

    def updateDataSource(self, dsName: str, config: dict, graphName: str = None) -> dict:
        """Update an existing data source.

        On TigerGraph 4.x uses REST API ``PUT /gsql/v1/data-sources/<name>``.
        On 3.x falls back to a DROP + CREATE sequence.

        Args:
            dsName:   Name of the data source to update.
            config:   New configuration dict.
            graphName:  Graph context.  Defaults to the connection's current graph.
        """
        graph = graphName or self.graphname
        if self._version_greater_than_4_0():
            url = _prep_data_source_by_name(self.gsUrl, dsName, graph)
            body = {"name": dsName, "config": config}
            return self._req("PUT", url, data=body, jsonData=True, resKey=None)

        self.gsql(f"DROP DATA_SOURCE {dsName}")
        config_json = json.dumps(config)
        gsql_cmd = f"CREATE DATA_SOURCE {dsName} = '{config_json}'"
        if graph:
            gsql_cmd += f" FOR GRAPH {graph}"
        return _wrap_gsql_result(self.gsql(gsql_cmd))

    def getDataSource(self, dsName: str) -> dict:
        """Get information about a specific data source.

        On TigerGraph 4.x uses REST API ``GET /gsql/v1/data-sources/<name>``.
        On 3.x falls back to ``SHOW DATA_SOURCE <name>`` via GSQL console.

        Args:
            dsName:  Name of the data source.

        Returns:
            A dict.  On 4.x contains structured data source info.
            On 3.x contains ``{"message": "<GSQL output>"}``.
        """
        if self._version_greater_than_4_0():
            url = _prep_data_source_by_name(self.gsUrl, dsName)
            return self._req("GET", url, resKey="results")

        return _wrap_gsql_result(self.gsql(f"SHOW DATA_SOURCE {dsName}"))

    def getDataSources(self) -> Union[list, dict]:
        """List all data sources.

        On TigerGraph 4.x uses REST API ``GET /gsql/v1/data-sources``.
        On 3.x falls back to ``SHOW DATA_SOURCE *`` via GSQL console.

        Returns:
            On 4.x: a list of data source dicts.
            On 3.x: ``{"message": "<GSQL output>"}``.
        """
        if self._version_greater_than_4_0():
            url = _prep_data_source_url(self.gsUrl)
            return self._req("GET", url, resKey="results")

        return _wrap_gsql_result(self.gsql("SHOW DATA_SOURCE *"))

    def dropDataSource(self, dsName: str, graphName: str = None) -> dict:
        """Drop a data source.

        On TigerGraph 4.x uses REST API ``DELETE /gsql/v1/data-sources/<name>``.
        On 3.x falls back to ``DROP DATA_SOURCE <name>`` via GSQL console.

        Args:
            dsName:     Name of the data source to drop.
            graphName:  Graph context.

        Returns:
            A dict with at least a ``"message"`` key describing the outcome.
        """
        graph = graphName or self.graphname
        if self._version_greater_than_4_0():
            url = _prep_data_source_by_name(self.gsUrl, dsName, graph)
            return self._req("DELETE", url, resKey=None)

        return _wrap_gsql_result(self.gsql(f"DROP DATA_SOURCE {dsName}"))

    def dropAllDataSources(self, graphName: str = None) -> dict:
        """Drop all data sources.

        On TigerGraph 4.x uses REST API ``DELETE /gsql/v1/data-sources/dropAll``.
        On 3.x falls back to ``DROP DATA_SOURCE *`` via GSQL console.

        Args:
            graphName:  If provided, drops data sources for that graph only.

        Returns:
            A dict with at least a ``"message"`` key describing the outcome.
        """
        if self._version_greater_than_4_0():
            url = _prep_drop_all_data_sources(self.gsUrl, graphName)
            return self._req("DELETE", url, resKey=None)

        return _wrap_gsql_result(self.gsql("DROP DATA_SOURCE *"))

    def previewSampleData(self, dsName: str, path: str, size: int = 10,
                          graphName: str = None) -> dict:
        """Preview sample rows from a file in a data source.

        Available on TigerGraph 4.x only (``POST /gsql/v1/sample-data``).
        On 3.x raises ``NotImplementedError``.

        Args:
            dsName:     Name of the data source.
            path:       Path to the file within the data source.
            size:       Number of rows to preview.
            graphName:  Graph context.
        """
        if not self._version_greater_than_4_0():
            raise NotImplementedError(
                "previewSampleData requires TigerGraph 4.x. "
                "On 3.x, access the file directly via your storage provider."
            )

        graph = graphName or self.graphname
        url = _prep_sample_data_url(self.gsUrl)
        body = {
            "graphName": graph,
            "dataSource": dsName,
            "path": path,
            "size": size,
        }
        return self._req("POST", url, data=body, jsonData=True, resKey="results")

    def getVectorIndexStatus(self, graphName: str = None,
                             vertexType: str = None,
                             vectorName: str = None) -> dict:
        """Get the rebuild status of vector indexes.

        Uses REST++ endpoint ``GET /vector/status/<graph>[/<vertexType>[/<vectorName>]]``.

        Args:
            graphName:   Graph name. Defaults to the connection's current graph.
            vertexType:  Optionally filter by vertex type.
            vectorName:  Optionally filter by vector attribute name.
        """
        graph = graphName or self.graphname
        path = f"/vector/status/{graph}"
        if vertexType:
            path += f"/{vertexType}"
            if vectorName:
                path += f"/{vectorName}"
        return self._req("GET", self.restppUrl + path)