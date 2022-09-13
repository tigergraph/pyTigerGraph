from jira import JIRA
from jira.exceptions import JIRAError
from util import encode_str
import time, traceback, logging


def log(level, message, logger):
    """
    Prints message to stdout if is_bulb is false or log initilized
    in bulb_manager if is_bulb is true
    Args:
        level: level of the message to print
               (i.e. INFO, ERROR, WARNING, etc.)
        message: message to print
        logger: logging system to use (must be
                instance of python logging class)
                or 'None' to print output to screen.
                If NOT specified 'None' will be used.
    """
    if isinstance(logger, logging.Logger):
        logger.log(level, message)
    else:
        print(message)

def init(logger, retry=30, retry_interval=60):
    """
    Connect to JIRA server retrying every period specified by parameter retry_interval
    for a maximum number of times specified by retry.
    Args:
        logger: logging system to use (must be
                instance of python logging class)
                or 'None' to print output to screen.
                If NOT specified 'None' will be used.
        retry: The maximum number of times to retry (default 30). If this is exceeded
               and connection is still unsuccessful, program will error out.
        retry_interval: period to wait in seconds after each try (default 60)
    """

    while retry >= 0:
        try:
            response = JIRA('https://graphsql.atlassian.net', basic_auth=('qa2@tigergraph.com', 'KYLWDFQ0VaPh2thTAS9xEADF'))
            break
        except JIRAError as e:
            log(40, "Unable to connect to jira, retrying in {} seconds".format(retry_interval), logger)
            time.sleep(retry_interval)
            retry -= 1
            if retry < 0:
                log(40, "Failed to connect to jira, request aborted.", logger)
                log(40, traceback.print_exc(), logger)
                raise JIRAError

    return response

def open_issue(project, issue_type, issue_label, summary, description, assignee):
    """
    Open a bug ticket in jira with the given parameters
    Args:
        project: The project name to open the ticket in (eq QA, GLE ...)
        issue_type: issue type
        issue_label: Labels to be added to the ticket in string format.
                     Multiple labels should be seperated by a space (e.g. "label1 label2 ...")
        summary: The summary for the issue to open
        description: The description for the issue to open
        assignee: The person to assign the issue to.
    """

    jira_server = init("none");

    issue_to_create = {
      'project': {'key': project},
      'summary': summary,
      'description': description,
      'issuetype': {'name': issue_type.capitalize()},
      'assignee': {'name': assignee},
      'labels': ["QA_HOURLY_FAILURE", issue_label],
    }

    issue = jira_server.create_issue(fields=issue_to_create)

    return issue

def get_open_issues(project, issue_type, issue_label, logger="none", max_results=50):
    """
    Get open issues of JIRA which status is Open, Reopened or In Progress
    Args:
        jira_obj: jira server to use
        project: project name
        issue_type: issue type
        issue_label: issue labels in string format.
                     Multiple labels should be seperated by a space (e.g. "label1 label2 ...")
        logger: logging system to use (must be
                instance of python logging class)
                or 'None' to print output to screen.
                If NOT specified 'None' will be used.
        max_results: the maximum number of results to display (default: 50)
    Returns:
        An array of issues
    """

    jira_server = init(logger);

    return jira_server.search_issues('project="' + project + '" AND issuetype="' + issue_type + \
        '" AND labels in (' + issue_label + ') AND (status=Open OR status=Reopened OR status="In Progress")', maxResults=max_results)


# QA-2698
def get_ticket(ticket_id, fields=None):
    """
    Retrieves all fields for specified jira ticket
    Returns:
      ticket fields dict
    """

    jira = init("none")

    try:
        issue = jira.issue(ticket_id)
    except JIRAError as e:
        print(e)
        raise JIRAError

    ticket_fields = { encode_str(k): encode_str(v) for k, v in issue.raw['fields'].iteritems() }

    if fields:
        fields = set(fields.split())
        for field in ticket_fields.keys():
            if field in fields:
                if field == 'labels':
                    ticket_fields[field] = [encode_str(v) for v in eval(ticket_fields[field])]
            else:
                ticket_fields.pop(field, None)

    return ticket_fields
