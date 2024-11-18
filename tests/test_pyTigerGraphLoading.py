import json
import unittest

import pandas
from pyTigerGraphUnitTest import make_connection
from pyTigerGraph.common.loading import (
    _prep_abort_loading_jobs,
    _prep_abort_one_loading_job,
    _prep_resume_loading_job,
    _prep_run_loading_job,
    _prep_loading_job_url,
    _prep_loading_job_info,
    _prep_get_loading_jobs_status,
    _prep_get_loading_job_status,
)

from pyTigerGraph.common.exception import TigerGraphException


class test_pyTigerGraphLoading(unittest.TestCase):
    def test_prep_loading_job_url(self):
        url = _prep_loading_job_url("http://localhost:14240", "mygraph")
        self.assertEqual(url, "http://localhost:14240/gsql/v1/loading-jobs?graph=mygraph")

    def test_prep_loading_job_info(self):
        url = _prep_loading_job_info("http://localhost:14240", "mygraph", "myjob")
        self.assertEqual(url, "http://localhost:14240/gsql/v1/loading-jobs/myjob?graph=mygraph")
    
    def test_prep_run_loading_job(self):
        url, data = _prep_run_loading_job("http://localhost:14240",
                                          "mygraph",
                                          "myjob", {"source": "file.csv"},
                                          "root", True,
                                          True, 1, 1, 2.1)
        self.assertEqual(url, "http://localhost:14240/gsql/v1/loading-jobs/run?graph=mygraph")
        self.assertEqual(data, {
            "name": "myjob",
            "dataSources": [{"source": "file.csv"}],
            "sys.data_root": "root",
            "verbose": True,
            "dryrun": True,
            "interval": 1,
            "maxNumError": 1,
            "maxPercentError": 2.1
        })

    def test_prep_abort_loading_jobs(self):
        url = _prep_abort_loading_jobs("http://localhost:14240", "mygraph", ["job1", "job2"], True)
        self.assertEqual(url, "http://localhost:14240/gsql/v1/loading-jobs/abort?graph=mygraph&jobId=job1&jobId=job2&isPause=true")

    def test_prep_abort_one_loading_job(self):
        url = _prep_abort_one_loading_job("http://localhost:14240", "mygraph", "job1", True)
        self.assertEqual(url, "http://localhost:14240/gsql/v1/loading-jobs/abort?graph=mygraph&jobId=job1&isPause=true")

    def test_prep_resume_loading_job(self):
        url = _prep_resume_loading_job("http://localhost:14240", "job1")
        self.assertEqual(url, "http://localhost:14240/gsql/v1/loading-jobs/resume/job1")

    def test_prep_get_loading_jobs_status(self):
        url = _prep_get_loading_jobs_status("http://localhost:14240", ["job1", "job2"])
        self.assertEqual(url, "http://localhost:14240/gsql/v1/loading-jobs/status?jobId=job1&jobId=job2")

    def test_prep_get_loading_job_status(self):
        url = _prep_get_loading_job_status("http://localhost:14240", "job1")
        self.assertEqual(url, "http://localhost:14240/gsql/v1/loading-jobs/status/job1")
    

                        