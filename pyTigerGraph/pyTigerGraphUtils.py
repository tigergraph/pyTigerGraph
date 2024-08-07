"""Utility Functions.

Utility functions for pyTigerGraph.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object].
"""
import json
import logging
import urllib
from typing import Any, Union
from urllib.parse import urlparse

import requests
from typing import TYPE_CHECKING, Union

from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase
from pyTigerGraph.pyTigerGraphException import TigerGraphException

logger = logging.getLogger(__name__)


class pyTigerGraphUtils(pyTigerGraphBase):

    def _safeChar(self, inputString: Any) -> str:
        """Replace special characters in string using the %xx escape.

        Args:
            inputString:
                The string to process

        Returns:
            Processed string.

        Documentation:
            https://docs.python.org/3/library/urllib.parse.html#url-quoting
        """
        return urllib.parse.quote(str(inputString), safe='')

    def echo(self, usePost: bool = False) -> str:
        """Pings the database.

        Args:
            usePost:
                Use POST instead of GET

        Returns:
            "Hello GSQL" if everything was OK.

        Endpoint:
            - `GET /echo`
            - `POST /echo`
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_echo[Echo]
        """
        logger.info("entry: echo")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

        if usePost:
            ret = str(self._post(self.restppUrl + "/echo/", resKey="message"))

            if logger.level == logging.DEBUG:
                logger.debug("return: " + str(ret))
            logger.info("exit: echo (POST)")

            return ret

        ret = str(self._get(self.restppUrl + "/echo/", resKey="message"))

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: echo (GET)")

        return ret

    def getLicenseInfo(self) -> dict:
        """Returns the expiration date and remaining days of the license.

        Returns:
            Returns license details. For an evaluation/trial deployment, returns an information message and -1 remaining days.

        """
        logger.info("entry: getLicenseInfo")

        res = self._get(self.restppUrl + "/showlicenseinfo", resKey="", skipCheck=True)
        ret = {}
        if not res["error"]:
            ret["message"] = res["message"]
            ret["expirationDate"] = res["results"][0]["Expiration date"]
            ret["daysRemaining"] = res["results"][0]["Days remaining"]
        elif "code" in res and res["code"] == "REST-5000":
            ret["message"] = \
                "This instance does not have a valid enterprise license. Is this a trial version?"
            ret["daysRemaining"] = -1
        else:
            raise TigerGraphException(res["message"], res["code"])

        if logger.level == logging.DEBUG:
            logger.debug("return: " + str(ret))
        logger.info("exit: getLicenseInfo")

        return ret

    def ping(self) -> dict:
        """Public health check endpoint.

        Returns:
            Returns a JSON object with a key of "message" and a value of "pong"
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: ping")
        res = self._get(self.gsUrl+"/api/ping", resKey="")
        if not res["error"]:
            if logger.level == logging.DEBUG:
                logger.debug("exit: ping")
            return res
        else:
            raise TigerGraphException(res["message"], res["code"])

    def getSystemMetrics(self, from_ts:int = None, to_ts:int = None, latest:int = None, what:str = None, who:str = None, where:str = None):
        """Monitor system usage metrics.
        
        Args:
            from_ts (int, optional):
                The epoch timestamp that indicates the start of the time filter.
            to_ts (int, optional):
                The epoch timestamp that indicates the end of the time filter.
            latest (int, optional):
                Number of datapoints to return. If provided, `from_ts` and `to_ts` will be ignored.
            what (str, optional):
                Name of the metric to filter for. Possible choices are:
                - "cpu": Percentage of CPU usage by component
                - "mem": Memory usage in megabytes by component
                - "diskspace": Disk usage in megabytes by directory
                - "network": Network traffic in bytes since the service started
                - "qps": Number of requests per second by endpoint
                - "servicestate": The state of the service, either online 1 or offline 0  (Only avaliable in version <4.1)
                - "connection": Number of open TCP connections (Only avaliable in version <4.1)
            who (str, optional):
                Name of the component that reported the datapoint. (Only avaliable in version <4.1)
            where (str, optional):
                Name of the node that reported the datapoint.

        Returns:
            JSON object of datapoints collected.
            Note: Output format differs between 3.x and 4.x versions of TigerGraph.

        Endpoints:
            - `GET /ts3/api/datapoints` (In TigerGraph versions 3.x)
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_monitor_system_metrics_ts3_deprecated
            - `POST /informant/metrics/get/{metrics_category}` (In TigerGraph versions 4.x)
                See xref:tigergraph-server:API:built-in-endpoints.adoc#_monitor_system_metrics_by_category
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: getSystemMetrics")
        params = {}
        _json = {} # in >=4.1 we need a json request of different parameter names
        if from_ts or to_ts:
            _json["TimeRange"] = {}
        if from_ts:
            params["from"] = from_ts
            _json['TimeRange']['StartTimestampNS'] = str(from_ts)
        if to_ts:
            params["to"] = to_ts
            _json['TimeRange']['EndTimestampNS'] = str(from_ts)
        if latest:
            params["latest"] = latest
            _json["LatestNum"] = str(latest)
        if what:
            if self._versionGreaterThan4_0():
                if what == "servicestate" or what == "connection":
                    raise TigerGraphException("This 'what' parameter is only supported on versions of TigerGraph < 4.1.0.", 0)
                if what == "cpu" or what == "mem":
                        what = "cpu-memory" # in >=4.1 cpu and mem have been conjoined into one category
            params["what"] = what
        if who:
            params["who"] = who
        if where:
            params["where"] = where
            _json["HostID"] = where
        # in >=4.1 the datapoints endpoint has been removed and replaced
        if self._versionGreaterThan4_0():
            res = self._post(self.gsUrl+"/informant/metrics/get/"+what, data=_json, jsonData=True, resKey="")
        else:
            res = self._get(self.gsUrl+"/ts3/api/datapoints", authMode="pwd", params=params, resKey="")
        if logger.level == logging.DEBUG:
            logger.debug("exit: getSystemMetrics")
        return res


    def getQueryPerformance(self, seconds:int = 10):
        """Returns real-time query performance statistics over the given time period, as specified by the seconds parameter. 
        
        Args:
            seconds (int, optional):
                Seconds are measured up to 60, so the seconds parameter must be a positive integer less than or equal to 60.
                Defaults to 10.
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: getQueryPerformance")
        params = {}
        if seconds:
            params["seconds"] = seconds
        res = self._get(self.restppUrl+"/statistics/"+self.graphname, params=params, resKey="")
        if logger.level == logging.DEBUG:
            logger.debug("exit: getQueryPerformance")
        return res

    def getServiceStatus(self, request_body: dict):
        """Returns the status of the TigerGraph services specified in the request.
        Supported on databases versions 3.4 and above.

        Args:
            request_body (dict):
                Must be formatted as specified here: https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_show_service_status
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: getServiceStatus")
        res = self._post(self.gsUrl+"/informant/current-service-status", data=json.dumps(request_body), resKey="")
        if logger.level == logging.DEBUG:
            logger.debug("exit: getServiceStatus")
        return res

    def rebuildGraph(self, threadnum: int = None, vertextype: str = "", segid: str = "", path: str = "", force: bool = False):
        """Rebuilds the graph engine immediately. See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_rebuild_graph_engine for more information.

        Args:
            threadnum (int, optional): 
                Number of threads to execute the rebuild.
            vertextype (str, optional):
                Vertex type to perform the rebuild for. Will perform for all vertex types if not specified.
            segid (str, optional):
                Segment ID of the segments to rebuild. If not provided, all segments will be rebuilt. 
                In general, it is recommneded not to provide this parameter and rebuild all segments.
            path (str, optional):
                Path to save the summary of the rebuild to. If not provided, the default path is "/tmp/rebuildnow".
            force (bool, optional):
                Boolean value that indicates whether to perform rebuilds for segments for which there are no records of new data.
                Normally, a rebuild would skip such segments, but if force is set true, the segments will not be skipped.
        Returns:
            JSON response with message containing the path to the summary file.
        """
        if logger.level == logging.DEBUG:
            logger.debug("entry: rebuildGraph")
        params = {}
        if threadnum:
            params["threadnum"] = threadnum
        if vertextype:
            params["vertextype"] = vertextype
        if segid:
            params["segid"] = segid
        if path:
            params["path"] = path
        if force:
            params["force"] = force
        res = self._get(self.restppUrl+"/rebuildnow/"+self.graphname, params=params, resKey="")
        if not res["error"]:
            if logger.level == logging.DEBUG:
                logger.debug("exit: rebuildGraph")
            return res
        else:
            raise TigerGraphException(res["message"], res["code"])
        