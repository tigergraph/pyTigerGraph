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
        logger.debug("exit: runLoadingJobWithFile")

        return None
        # TODO Should throw exception instead?

def _prep_loading_job_url(gsUrl: str, graphname: str):
    '''url builder for getLoadingJobs(), createLoadingJob(), and updateLoadingJob()'''

    url = gsUrl + "/gsql/v1/loading-jobs?graph=" + graphname

    return url

def _prep_loading_job_info(gsUrl: str, jobName: str, graphname: str, verbose: bool = False):
    '''url builder for getLoadingJobInfo() and dropLoadingJob()'''
    if verbose:
        url = gsUrl + "/gsql/v1/loading-jobs/"+jobName+"?graph=" + graphname + "&verbose=true"
    else:
        url = gsUrl + "/gsql/v1/loading-jobs/"+jobName+"?graph=" + graphname

    return url

def _prep_run_loading_job(gsUrl: str,
                          graphname: str,
                          jobName: str,
                          data_source_config: dict,
                          sys_data_root: str,
                          verbose: bool,
                          dryrun: bool,
                          interval: int,
                          maxNumError: int,
                          maxPercentError: float):
    '''url builder for runLoadingJob()'''
    url = gsUrl + "/gsql/v1/loading-jobs/run?graph=" + graphname
    data = {}
    
    data["name"] = jobName
    data["dataSources"] = [data_source_config]

    if sys_data_root:
        data["sys.data_root"] = sys_data_root
    if verbose:
        data["verbose"] = verbose
    if dryrun:
        data["dryrun"] = dryrun
    if interval:
        data["interval"] = interval
    if maxNumError:
        data["maxNumError"] = maxNumError
    if maxPercentError:
        data["maxPercentError"] = maxPercentError
    
    return url, data

def _prep_abort_loading_jobs(gsUrl: str, graphname: str, jobIds: list[str], pauseJob: bool):
    '''url builder for abortLoadingJob()'''
    url = gsUrl + "/gsql/v1/loading-jobs/abort?graph=" + graphname
    for jobId in jobIds:
        url += "&jobId=" + jobId
    if pauseJob:
        url += "&isPause=true"
    return url

def _prep_abort_one_loading_job(gsUrl: str, graphname: str, jobId: str, pauseJob: bool):
    '''url builder for abortLoadingJob()
    TODO: verify that this is correct
    '''
    url = gsUrl + "/gsql/v1/loading-jobs/abort?graph=" + graphname + "&jobId=" + jobId
    if pauseJob:
        url += "&isPause=true"
    return url

def _prep_resume_loading_job(gsUrl: str, jobId: str):
    '''url builder for resumeLoadingJob()'''
    url = gsUrl + "/gsql/v1/loading-jobs/resume/" + jobId
    return url

def _prep_get_loading_jobs_status(gsUrl: str, jobIds: list[str]):
    '''url builder for getLoadingJobStatus()
    TODO: verify that this is correct
    '''
    url = gsUrl + "/gsql/v1/loading-jobs/status/jobId"
    for jobId in jobIds:
        url += "&jobId=" + jobId
    return url

def _prep_get_loading_job_status(gsUrl: str, jobId: str):
    '''url builder for getLoadingJobStatus()'''
    url = gsUrl + "/gsql/v1/loading-jobs/status/" + jobId
    return url