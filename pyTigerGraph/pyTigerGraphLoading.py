"""Loading Job Functions

The functions on this page run loading jobs on the TigerGraph server.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object]. 
"""
from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase


class pyTigerGraphLoading(pyTigerGraphBase):
    def runLoadingJobWithFile(self, filePath: str, fileTag: str, jobName: str, sep: str = None,
            eol: str = None, timeout: int = 16000, sizeLimit: int = 128000000) -> dict:
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
        except:
            return None
        return self._post(self.restppUrl + "/ddl/" + self.graphname, params=params, data=data,
            headers={"RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})

    def uploadFile(self, filePath, fileTag, jobName="", sep=None, eol=None, timeout=16000,
            sizeLimit=128000000) -> dict:
        """DEPRECATED

        Use `runLoadingJobWithFile()` instead.
        TODO Proper depreciation
        """
        return self.runLoadingJobWithFile(filePath, fileTag, jobName, sep, eol, timeout, sizeLimit)

    # TODO POST /restpploader/{graph_name}
