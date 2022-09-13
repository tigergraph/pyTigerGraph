import os
from jira import JIRA
from jira.exceptions import JIRAError
from copy import deepcopy
from .log import Log


class JiraApi(object):
    def __init__(self, jira_url='', jira_user='', jira_token=''):
        self.jira_url = "https://graphsql.atlassian.net" if jira_url == '' else jira_url
        self.jira_user = "qa2@tigergraph.com" if jira_user == '' else jira_user
        self.jira_token = "KYLWDFQ0VaPh2thTAS9xEADF" if jira_token== '' else jira_token
        self.default_project = "QA"
        self.default_issuetype = "Bug"
        self.default_assignee = "wenbing.sun"
        self.default_lables = ["QA_HOURLY_FAILURE", "MIT_BLOCK"]
        self.default_version = os.getenv("MIT_TG_VERSION") if os.getenv("MIT_TG_VERSION") else "3.3.0"
        self.default_priority = "Critical"
        self.log = Log(log_name="jira.log")
        self._jira_client = None
        self.issues = {}

    @property
    def jira_client(self):
        if not self._jira_client:
            try:
               self._jira_client = JIRA(self.jira_url, basic_auth=(self.jira_user,self.jira_token))
               self.log.info(f"successfully connected with jira {self.jira_url}...")
               return self._jira_client
            except JIRAError as e:
               self.log.error(f'oops something wrong when connecting jira, error as {e}')
        else:
            return self._jira_client

    def get_issue(self, issue_id):
        if 'issue_id' not in self.issues:
            self.issues[issue_id] = self.jira_client.issue(issue_id)
        
        return self.issues[issue_id]

    def get_filter_issues(self, project="", issuetype="", labels=None, version=None, max_results=10):
        """
        return openning jira tickets list which has label MIT_BLOCK and QA_HOURLY_FAILURE
        example output: [<JIRA Issue: key='QA-2053', id='56024'>]
        """

        project = self.default_project if not project else project
        issuetype = self.default_issuetype if not issuetype else issuetype.capitalize()
        filter_labels = self.default_lables if not labels else deepcopy(labels)
        fix_version = self.default_version if not version else version

        # QA-2542
        if fix_version:
            filter_labels.append(fix_version)   # always include fixVersion to labels

        filter = f'project={project} AND issuetype={issuetype}' + ''.join([f" AND labels in ({label})" for label in filter_labels]) + \
                 (f' AND fixVersion in ({fix_version})' if fix_version in self.get_versions(project) else '') + \
                 ' AND status in (Open, Reopened, "In Progress")'

        self.log.info(f"start query jira issue with filter: {filter}")
        try:
            return self.jira_client.search_issues(filter, maxResults=max_results)
        except JIRAError as e:
            self.log.error(f'oops something wrong when connecting jira, error as {e}')

    def create_issue(self, summary, description, version, project="", issuetype="", assignee="", labels=None, priority=""):
        """
        create issue in jira
        required variables: ticket summary, ticket description, hourly 
        return: new issue info <JIRA Issue: key='QA-2053', id='56024'>
        """

        project = self.default_project if not project else project
        issuetype = self.default_issuetype if not issuetype else issuetype.capitalize()
        assignee = self.default_assignee if not assignee else assignee
        labels = self.default_lables if not labels else deepcopy(labels)
        priority = self.default_priority if not priority else priority

        issue_to_create = {
            'project': {'key': project},
            'summary': summary,
            'description': description,
            'issuetype': {'name': issuetype},
            'assignee': {'name': assignee},
            'labels': labels,
            'fixVersions': [{'name': version}],
            'priority': {'name': priority},
        }

        # QA-2542
        issue_to_create['labels'] += [v['name'] for v in issue_to_create['fixVersions']]
        if version not in self.get_versions(project):
            if not self.create_version(version, project):
                del issue_to_create['fixVersions']

        self.log.info(f"start creating jira issue for {issue_to_create}")
        try:
           issue = self.jira_client.create_issue(fields=issue_to_create)
           self.log.info(f"create jira issue {issue} successfully")
           return issue
        except JIRAError as e:
           self.log.error(f'oops something wrong when creating jira issue, error as {e}')
    
    def create_comment(self, issue_id, comment):
        """
        return comment_id
        output: str
        """
        try:
            comment_info = self.jira_client.add_comment(issue_id,comment)
            self.log.info(f"create jira comment {comment_info} successfully")
            return comment_info
        except JIRAError as e:
           self.log.error(f'oops something wrong when creating jira issue, error as {e}')

    def get_url(self, issue):
        return f' {self.jira_url}/browse/' + issue.key + '\n'
        
    def get_urls(self, issues):
        """
        return issues' jira url
        output: str
        """
        urls = ''
        for issue in issues:
            urls += f' {self.jira_url}/browse/' + issue.key + '\n'
        return urls

    def create_version(self, *args):
        """
        create new project version
        return Version 
        output: bool
        """

        try:
            return self.jira_client.create_version(*args)
        except JIRAError as e:
            self.log.error(f'error occured when creating new version, error as {e}')
            return None

    def get_versions(self, project):
        """
        return versions of specified project
        output: set
        """

        versions = self.jira_client.project_versions(project)
        return {v.name for v in versions}

    # QA-2698
    def get_issue_content(self, issue_id, fields=None):
        """
        Retrieves all fields for specified jira ticket
        Returns:
          ticket fields dict
        """

        issue = self.get_issue(issue_id)

        ticket_fields = { k: v for k, v in issue.raw['fields'].items() }
        if fields:
            ticket_fields = { k: v for k, v in ticket_fields.items() if k in set(fields.split()) }

        return ticket_fields


if __name__ == "__main__":
    jira = JiraApi()
    issue = jira.get_filter_issues()
    # issue = jira.get_issue('QA-2042')
    res = jira.create_comment("QA-2042","test from jira api")
    print(res)
    # print(issue.fields.__dict__)
    # print(issue.fields.versions[0])
    # print(issue.fields.priority)
    # issue = jira.create_issue(summary="test jira", description="Test jira api")
    # print(issue.fields.__dict__)
