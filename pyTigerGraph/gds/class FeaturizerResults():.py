import time

class AsyncFeaturizerResults():
    def __init__(self, conn, asyncQuery, query_id = "", results=None):
        self.conn = conn
        self.asyncQuery = asyncQuery
        self.query_id = query_id
        self.results = results

    def wait(self, refresh=1):
        while not(self.results):
            if self.algorithmComplete():
                return self._getAlgorithmResults()
            time.sleep(refresh)

    def algorithmComplete(self):
        res = self.conn.checkQueryStatus(self.query_id)[0]
        if res["status"] == "success":
            return True
        elif res["status"] == "running":
            return False
        elif res["status"] == "aborted":
            raise TigerGraphException("Algorithm was aborted")
        else:
            raise TigerGraphException("Algorithm timed-out. Increase your timeout and try again.")

    def _getAlgorithmResults(self):
        res = self.conn.getQueryResults(self.query_id)
        self.results = res
        return res

    @property
    def result(self):
        if self.results:
            return self.results
        else:
            if self.algorithmComplete():
                return self._getQueryResults()
            else:
                raise TigerGraphException("Algorithm Results not Available Yet")
