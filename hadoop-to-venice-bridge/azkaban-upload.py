#!/usr/bin/env python2.6

# Copied from voldemort build and push repository
# This script is mostly used to upload Azkaban zip file to your project
# and setup schedule and SLA based on plan config, which can be passed by the argument

import site
site.addsitedir('/usr/local/linkedin/lib/python2.6/site-packages')

import sys
import os
import os.path
import requests
import json
import re
import getpass
import logging
import pprint
import copy
import datetime
import pytz
import tempfile
import zipfile
# from tzlocal import get_localzone
import linkedin.cli

################################################################################

class RestClient(object):

    def __init__(self, base_url, user=None, password=None):
        self._base_url = base_url
        _log.debug("base_url=%s", self._base_url)
        if user is not None:
            _log.debug("(have user, enabling HTTP Basic Auth)")
            if password is not None:
                _log.debug("(have password)")
            else:
                _log.debug("(no password)")
                password = ''
            self._auth = requests.HTTPBasicAuth(user, password)
        else:
            self._auth = None
        self._requests_session = requests.Session()

    def _format_params_for_log(self, params):
        filtered_params = copy.deepcopy(params)
        # Don't log passwords even with --debug
        for key in [ 'password' ]:
            if key in filtered_params:
                filtered_params[key] = '###FILTERED###'
        return json.dumps(filtered_params)

    def _do_request(self, method, path, params=None, data=None, files=None, headers=None):
        url = self._base_url.rstrip('/') + '/' + path.lstrip('/')
        _log.debug("Doing a %s to %s", method, url)
        if params is not None and _log.isEnabledFor(logging.DEBUG):
            _log.debug("Request params: %s", self._format_params_for_log(params))
        if data is not None and _log.isEnabledFor(logging.DEBUG):
            _log.debug("Request data: %s", self._format_params_for_log(data))
        resp = self._requests_session.request(method, url, params=params, data=data, files=files, auth=self._auth, headers=headers)
        if int(resp.status_code / 100) != 2:
            raise RuntimeError("%s to %s failed: %d %s" % (
                                method, url, resp.status_code, resp.reason))
        content_length = resp.headers['Content-Length']
        content_length = 0 if content_length is None else int(content_length)
        if content_length > 0:
            content_type = resp.headers['Content-Type']
            if content_type is None:
                raise RuntimeError("No Content-Type header")
            if content_type.partition(';')[0] != 'application/json':
                raise RuntimeError("Unexpected Content-Type \"%s\"", content_type)
            rdata = json.loads(resp.content)
        else:
            rdata = ''
        _log.debug("Returned data: %r", rdata)
        return rdata

    def get(self, path, params=None, **kwargs):
        if 'data' in kwargs:
            raise RuntimeError("Cannot pass data= to get()")
        return self._do_request('GET', path, params=params, **kwargs)

    def post(self, path, data=None, files=None, **kwargs):
        return self._do_request('POST', path, data=data, files=files, **kwargs)

    def close(self):
        if self._requests_session is not None:
            self._requests_session.close()
            self._requests_session = None

    # Enable use with the 'with' keyword
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

################################################################################

class AzkabanClient(RestClient):

    def __init__(self, base_url, user, password):
        super(AzkabanClient, self).__init__(base_url=base_url)
        self._azkaban_user = user
        self._azkaban_password = password
        self._is_authenticated = False
        self._session_id = None

    # Login to Azkaban using the username and password
    # from the ctor.  Sets _is_authenticated and _session_id
    def _authenticate(self):
        if not self._is_authenticated:
            _log.debug("Logging in to Azkaban")
            # Azkaban doesn't use an Authorization: header so
            # we don't just pass the username and password to
            # RestClient.  Instead we have to go through a
            # separate login step to get a session id.
            results = super(AzkabanClient, self).post('/', {
                    'action': 'login',
                    'username': self._azkaban_user,
                    'password': self._azkaban_password
                })
            if results is None or results['status'] != 'success':
                error = results['error'] if results is not None and 'error' in results else '???'
                raise RuntimeError("Failed to login to Azkaban: %s" % error)
            self._session_id = results['session.id']
            _log.debug("Session ID is %s", self._session_id)
            self._is_authenticated = True

    # Setup some various magic bits that Azkaban expects in all requests
    def _azkabanize(self, data, kwargs):
        self._authenticate()
        if self._is_authenticated:
            # Set the session ID as a query
            if data is None:
                data = {}
            data['session.id'] = self._session_id
        # Azkaban wants this magic header for no adequately explained reason
        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['X-Requested-With'] = 'XMLHttpRequest'
        return (data, kwargs)

    # Override get() to do magic that Azkaban expects
    def get(self, path, params=None, **kwargs):
        (params, kwargs) = self._azkabanize(params, kwargs)
        return super(AzkabanClient, self).get(path, params, **kwargs)

    # Override post() to do magic that Azkaban expects
    def post(self, path, data=None, files=None, **kwargs):
        (data, kwargs) = self._azkabanize(data, kwargs)
        return super(AzkabanClient, self).post(path, data, files=files, **kwargs)

    def _handle_error(self, results, action):
        error = None
        if results is None or results == '' or not isinstance(results, dict):
            error = 'No results from server'
        elif 'error' in results:
            error = results['error']
        if error:
            raise RuntimeError("Failed to %s: %s" % (action, error))

    # http://azkaban.github.io/azkaban/docs/2.5/#api-upload-a-project-zip
    def upload_project_zip(self, project, zipfile):
        _log.debug("Uploading zipfile %s to Azkaban project %s", zipfile, project)
        results = self.post('/manager', data={
                'ajax': 'upload',
                'project': project,
            }, files={
                'file': ( os.path.basename(zipfile), open(zipfile, 'r'), 'application/zip' )
            })
        self._handle_error(results, "upload zipfile %s to Azkaban project %s" % (zipfile, project))
        _log.info("Uploaded file %s as version %s of project %s", zipfile, results['version'], project)

    # http://azkaban.github.io/azkaban/docs/2.5/#api-fetch-flows-of-a-project
    def fetch_project_flows(self, project_name):
        _log.debug("Listing flows for Azkaban project %s", project_name)
        results = self.get('/manager', { 'ajax': 'fetchprojectflows', 'project': project_name })
        self._handle_error(results, "list flows for Azkaban project %s" % (project_name))
        return results

    # undocumented
    #
    # Lists schedule information for a given flow, or
    # nothing if the flow is not scheduled.
    #
    # Returns a structure like
    # {
    #    'schedule': {
    #        'scheduleId': '44748',
    #        'submitUser': 'sisingh',
    #        'period': '1 hour(s)',
    #        'firstSchedTime': '2015-03-31 20:00:00',
    #        'nextExecTime': '2016-07-17 22:00:00'
    #    }
    # }
    def fetch_schedule(self, project_id, flow_id):
        results = self.get('/schedule', {
            'ajax': 'fetchSchedule',
            'projectId': project_id,
            'flowId': flow_id
        })
        self._handle_error(results, "fetch schedule for Azkaban project_id %d" % (project_id))
        return results

    @staticmethod
    def _aztime(dt):
        return dt.strftime('%I,%M,%p,%Z')

    @staticmethod
    def _azdate(dt):
        return dt.strftime('%m/%d/%Y')

    @staticmethod
    def _azduration(delta):
        if delta is None:
            return None
        elif delta.days >= 30 and delta.days % 30 == 0:
            return '%dM' % (int(delta.days/30))
        elif delta.days >= 7 and delta.days % 7 == 0:
            return '%dw' % (int(delta.days/7))
        elif delta.days > 0:
            return '%dd' % (delta.days)
        elif delta.seconds >= 3600 and delta.seconds % 3600 == 0:
            return '%dh' % (int(delta.seconds/3600))
        elif delta.seconds >= 60 and delta.seconds % 60 == 0:
            return '%dm' % (int(delta.seconds/60))
        elif delta.seconds >= 60 and delta.seconds % 60 == 0:
            return '%ds' % (delta.seconds)
        elif delta.seconds > 0:
            return '%ds' % (delta.seconds)
        else:
            raise RuntimeError("Cannot convert timedelta %r to Azkaban duration" % delta)

    @staticmethod
    def test_azduration():
        testcases = [
            # (days, seconds, expected-result)
            (0, 30, '30s'),
            (0, 60, '1m'),
            (0, 600, '10m'),
            (0, 1800, '30m'),
            (0, 3600, '1h'),
            (0, 7200, '2h'),
            (1, 0, '1d'),
            (7, 0, '1w'),
            (14, 0, '2w'),
            (21, 0, '3w'),
            (28, 0, '4w'),
            (6, 0, '6d'),
            (8, 0, '8d'),
            (8, 0, '8d'),
            (30, 0, '1M'),
            (60, 0, '2M'),
            (90, 0, '3M'),
        ]
        for tc in testcases:
            _log.info("test_azduration: days=%d secs=%d", tc[0], tc[1])
            delta = datetime.timedelta(tc[0], tc[1])
            actual = AzkabanClient._azduration(delta)
            expected = tc[2]
            assert actual == expected

    # http://azkaban.github.io/azkaban/docs/2.5/#api-schedule-a-flow
    def schedule_flow(self, project_id, project_name, flow_id, start=None, period=None):
        _log.debug("schedule_flow(project_name=%s flow_id=%s to start=%r period=%r)", project_name, flow_id, start, period)
        if start is None:
            # The documentation shows an example in PDT.  In fact,
            # unless the string "UTC" appears here the time zone
            # specified is ignored and the server's default time
            # zone is used.  We want some semblance of consistency
            # so we go with UTC.
            start = datetime.datetime.now(pytz.timezone('UTC'))
            _log.debug("Default start time %r", start)
            if period is not None and period.days == 0:
                # Round up the start time to the next period
                start = start.replace(microsecond=0)
                delta_in_day = start - start.replace(hour=0, minute=0, second=0)
                if delta_in_day.seconds % period.seconds > 0:
                    start += datetime.timedelta(0, (period.seconds - (delta_in_day.seconds % period.seconds)))
                    _log.debug("Rounded up start time to %r", start)
        _log.info("Scheduling project_name %s flow_id %s at start %r period %r", project_name, flow_id, start, period)
        params = {
            'ajax': 'scheduleFlow',
            'projectId': project_id,
            'projectName': project_name,
            'flow': flow_id,
            'scheduleTime': AzkabanClient._aztime(start),
            'scheduleDate': AzkabanClient._azdate(start)
        }
        if period is not None:
            params['is_recurring'] = 'on'
            params['period'] = AzkabanClient._azduration(period)
        results = self.get('/schedule', params)
        self._handle_error(results, "schedule project %s flow_id %s" % (project_name, flow_id))

    # http://azkaban.github.io/azkaban/docs/2.5/#api-unschedule-a-flow
    def remove_sched(self, schedule_id):
        _log.info("Unscheduling project %s flow %s", project, flow)
        results = self.get('/schedule', {
            'action': 'removeSched',
            'scheduleId': schedule_id
        })
        self._handle_error(results, "schedule project %s flow %s" % (project, flow))

    # not documented
    #
    # Gets all the SLA rules for a flow and its jobs
    #
    # Returns a structure like
    # {
    #    'allJobNames': [ 'debug-read-only-5' ],
    #    'slaEmails': [ 'voldemort-alerts@linkedin.com' ],
    #    'settings': [ {
    #        'duration': '45m',
    #        'actions': [ 'EMAIL', 'KILL' ],
    #        'id': '',
    #        'rule': 'SUCCESS'
    #    } ]
    # }
    #
    def sla_info(self, schedule_id):
        _log.debug("Getting SLA info for schedule id %d", schedule_id)
        results = self.get('/schedule', {
            'ajax': 'slaInfo',
            'scheduleId': schedule_id
        })
        self._handle_error(results, "get SLA info for schedule id %d" % (schedule_id))
        return results

    #
    # This is a limited subset of what the AJAX API can do.  It allows
    # for multiple SLA rules, each of which can match either the flow as
    # a whole or a specific job.  Here we have a single rule which matches
    # the whole flow, because our use case is a single-job flow.
    #
    def set_sla(self, schedule_id, emails=None, rule=None, duration=None, email_action=None, kill_action=None):
        row0 = ','.join([
                    '',         # job_id="" meaning the whole flow
                    'SUCCESS' if rule is None else rule,
                    "%02d:%02d" % (int(duration.seconds/3600), int(duration.seconds/60)),
                    'true' if email_action is None or email_action is True else 'false',
                    'true' if kill_action is None or kill_action is True else 'false'])
        _log.info("Setting SLA info for schedule id %d to (%s,%s)", schedule_id, emails, row0)
        results = self.get('/schedule', {
            'ajax': 'setSla',
            'scheduleId': schedule_id,
            'slaEmails': emails,
            'settings[0]': row0
        })
        self._handle_error(results, "Set SLA info for schedule id %d" % (schedule_id))

class AzkabanFlow(object):

    def __init__(self, project, client, flow_id):
        self._client = client
        self._project = project
        self._flow_id = flow_id
        self._have_schedule = False
        self._schedule = None
        self._have_sla = False
        self._sla = None

    @property
    def flow_id(self):
        return self._flow_id

    def _get_schedule(self):
        if not self._have_schedule:
            _log.debug("Fetching schedule info for project %s flow %s", (self._project.name, self._flow_id))
            res = self._client.fetch_schedule(self._project.project_id, self._flow_id)
            if res is None or len(res) == 0 or 'schedule' not in res:
                sched = None
            else:
                sched = res['schedule']
            self._schedule = sched
            self._have_schedule = True

    @property
    def schedule_id(self):
        self._get_schedule()
        if self._schedule is None or 'scheduleId' not in self._schedule:
            return None
        return int(self._schedule['scheduleId'])

    @property
    def user(self):
        self._get_schedule()
        if self._schedule is None or 'submitUser' not in self._schedule:
            return None
        return self._schedule['submitUser']

    @property
    def period(self):
        self._get_schedule()
        if self._schedule is None or 'period' not in self._schedule:
            return None
        return parse_duration(self._schedule['period'])

    @property
    def first_time(self):
        self._get_schedule()
        if self._schedule is None or 'firstSchedTime' not in self._schedule:
            return None
        return datetime.strptime(self._schedule['firstSchedTime'], '%Y-%m-%d %H:%M:%S')

    @property
    def next_time(self):
        self._get_schedule()
        if self._schedule is None or 'nextExecTime' not in self._schedule:
            return None
        return datetime.strptime(self._schedule['nextExecTime'], '%Y-%m-%d %H:%M:%S')

    def _get_sla(self):
        schedule_id = self.schedule_id
        if schedule_id is None:
            return None
        if not self._have_sla:
            res = self._client.sla_info(schedule_id)
            if res is None or len(res) == 0:
                sla = None
            else:
                sla = res
            self._sla = sla
            self._have_sla = True

    @property
    def sla_emails(self):
        self._get_sla()
        if self._sla is None or 'slaEmails' not in self._sla:
            return None
        return re.split(r'\s*,\s*', self._schedule['slaEmails'])

    @property
    def sla_duration(self):
        self._get_sla()
        if self._sla is None or 'settings' not in self._sla or len(self._sla['settings']) == 0:
            return None
        return parse_duration(self._sla['settings'][0]['duration'])

    def set_sla(self, emails=None, rule=None, duration=None, email_action=None, kill_action=None):
        schedule_id = self.schedule_id
        if schedule_id is None:
            return None
        self._have_sla = False     # invalidate the cache
        # TODO this only sets the 0th SLA rule
        return self._client.set_sla(schedule_id,
                                    emails=emails,
                                    rule=rule,
                                    duration=duration,
                                    email_action=email_action,
                                    kill_action=kill_action)

    def schedule(self, start=None, period=None):
        self._have_schedule = False     # invalidate the cache
        return self._client.schedule_flow(self._project.project_id,
                                          self._project.name,
                                          self._flow_id,
                                          start=start,
                                          period=period)


# This class exists because parts of the Azkaban REST API
# take string project names and other parts take integer
# project ids.  Yay consistency.  So we grab both and
# cache them along with the list of flows.
class AzkabanProject(object):

    def __init__(self, client, name, project_id, flow_ids):
        self._client = client
        self._name = name
        self._project_id = project_id
        # canonical set of names
        self._flow_ids = set(flow_ids)
        # cached AzkabanFlow objects, keyed by names
        self._flows = {}

    @property
    def name(self):
        return self._name

    @property
    def project_id(self):
        return self._project_id

    @property
    def flow_ids(self):
        return sorted(self._flow_ids)

    def get_flow(self, flow_id):
        if flow_id not in self._flow_ids:
            return None
        if flow_id not in self._flows:
            flow = AzkabanFlow(self, self._client, flow_id)
            self._flows[flow_id] = flow
        return self._flows[flow_id]

    @property
    def flows(self):
        return [ self.get_flow(flow_id) for flow_id in sorted(self._flow_ids) ]

    def upload_zip(self, zipfile):
        return self._client.upload_project_zip(self._name, zipfile)

class Azkaban(object):

    def __init__(self, base_url, user, password):
        self._client = AzkabanClient(base_url, user, password)
        self._projects = {}

    def get_project(self, name):
        if name not in self._projects:
            results = self._client.fetch_project_flows(name)
            proj = AzkabanProject(self._client,
                                  name,
                                  results['projectId'],
                                  [ f['flowId'] for f in results['flows'] ])
            self._projects[name] = proj
        return self._projects[name]

_clients = {}

def get_client(plan):
    base_url = plan['baseurl']
    global _clients
    if base_url not in _clients:
        _clients[base_url] = AzkabanClient(base_url, user=plan['user'], password=plan['password'])
    return _clients[base_url]

class NameMatcher(object):

    def __init__(self, step):
        if 'matches' in step:
            matches = step['matches']
            if isinstance(matches, basestring):
                matches = [ matches ]
            elif isinstance(matches, list):
                pass
            else:
                raise RuntimeError("Unknown type for 'matches'")
        else:
            matches = [ '.*' ]
        self._matches = matches

    def match(self, name):
        _log.debug("Checking name %s", name)
        for match in self._matches:
            if re.match(match, name):
                _log.debug("Matched!")
                return True
        return False


def action_upload(project, plan, step):
    basedir = (step['basedir'] if 'basedir' in step else '.')
    if 'zipfile' in plan:
        zipfile_pathname = plan['zipfile']
        zf = zipfile.ZipFile(zipfile_pathname, "a")
        is_temp = False
    else:
        (zipfile_fd, zipfile_pathname) = tempfile.mkstemp(suffix='.zip')
        zf = zipfile.ZipFile(os.fdopen(zipfile_fd, "w"), "w")
        is_temp = True
    _log.info("Creating zipfile %s", zipfile_pathname)
    matcher = NameMatcher(step)
    for filename in os.listdir(basedir):
        if matcher.match(filename):
            _log.info("    %s", filename)
            zf.write(os.path.join(basedir, filename), filename)
    zf.close()
    _log.info("Uploading zipfile %s to project %s", zipfile_pathname, project.name)
    project.upload_zip(zipfile_pathname)
    if is_temp:
        _log.debug("Removing temporary zipfile %s", zipfile_pathname)
        os.unlink(zipfile_pathname)

DURATION_UNITS = {
    's': 1,
    'sec': 1,
    'secs': 1,
    'second': 1,
    'seconds': 1,
    'second(s)': 1,
    'm': 60,
    'min': 60,
    'mins': 60,
    'minute': 60,
    'minutes': 60,
    'minute(s)': 60,
    'h': 3600,
    'hour': 3600,
    'hours': 3600,
    'hour(s)': 3600,
    'd': 86400,
    'day': 86400,
    'days': 86400,
    'day(s)': 86400,
    'w': 7*86400,
    'week': 7*86400,
    'weeks': 7*86400,
    'week(s)': 7*86400,
    'M': 30*86400,
    'mon': 30*86400,
    'month': 30*86400,
    'months': 30*86400,
    'month(s)': 30*86400
}
def parse_duration(s):
    (n, unit) = re.match(r'(\d+)\s*([a-zA-Z()]*)', s).groups()
    if unit is '':
        # default unit is seconds
        return datetime.timedelta(0, int(n))
    if unit not in DURATION_UNITS:
        raise RuntimeError("Cannot convert duration \"%s\" to timedelta" % (s))
    return datetime.timedelta(0, int(n) * DURATION_UNITS[unit])

def test_parse_duration():
    testcases = [
        # (string, expected-days, expected-seconds)
        ('1', 0, 1),
        ('7', 0, 7),
        ('1m', 0, 60),
        ('1min', 0, 60),
        ('1mins', 0, 60),
        ('1 mins', 0, 60),
        ('1 minute', 0, 60),
        ('1 minutes', 0, 60),
        ('1 week', 0, 7*86400),
        ('1 month', 0, 30*86400),
        ('3 months', 0, 90*86400),
    ]
    for tc in testcases:
        _log.info("test_parse_duration: string=\"%s\"", tc[0])
        actual = parse_duration(tc[0])
        expected = datetime.timedelta(tc[1], tc[2])
        assert actual == expected

def _list_flows_matching(project, step):
    matcher = NameMatcher(step)
    flows = []
    for flow_id in project.flow_ids:
        if matcher.match(flow_id):
            flows.append(project.get_flow(flow_id))
    return flows

def action_schedule(project, plan, step):
    _log.debug("Scheduling")
    if 'period' not in step:
        raise RuntimeError("No \"period\" attribute in action \"schedule\"")
    plan_period = parse_duration(step['period'])
    for flow in _list_flows_matching(project, step):
        flow_period = flow.period
        _log.info("Flow %s current period is %r", flow.flow_id, AzkabanClient._azduration(flow_period))
        if flow_period is None or flow_period != plan_period:
            flow.schedule(period=plan_period)
        else:
            _log.info("No change required")

def action_sla(project, plan, step):
    _log.debug("Setting SLA")
    if 'duration' not in step:
        raise RuntimeError("No \"duration\" attribute in action \"sla\"")
    plan_sla_duration = parse_duration(step['duration'])
    #
    if 'emails' not in step:
        raise RuntimeError("No \"emails\" attribute in action \"sla\"")
    plan_sla_emails = step['emails']
    #
    plan_rule = None if 'rule' not in step else step['rule']
    plan_email_action = None if 'email_action' not in step else step['email_action']
    plan_kill_action = None if 'kill_action' not in step else step['kill_action']
    #
    for flow in _list_flows_matching(project, step):
        flow_sla_duration = flow.sla_duration
        _log.info("Flow %s current SLA duration is %r", flow.flow_id, AzkabanClient._azduration(flow_sla_duration))
        if flow_sla_duration is None or flow_sla_duration != plan_sla_duration:
            flow.set_sla(emails=plan_sla_emails,
                         rule=plan_rule,
                         duration=plan_sla_duration,
                         email_action=plan_email_action,
                         kill_action=plan_kill_action)
        else:
            _log.info("No change required")

_actions = { 'upload': action_upload, 'schedule': action_schedule, 'sla': action_sla }

def run_plan(project, plan):
    _log.info("Running plan %s", plan['name'])
    try:
        for step in plan['steps']:
            action = step['action']
            _log.debug("Action: %s", action)
            if action in _actions:
                _actions[action](project, plan, step)
            else:
                raise RuntimeError("Unknown action '%s'" % action)
    except Exception as e:
        _log.error("%s: %s", plan['name'],  e)
        raise


################################################################################

DOC_SUPPORT_EMAIL = 'ask_voldemort'

@linkedin.cli.entrypoint
def main(cli):
    # cli has undocumented --debug, --silent, --log, and --help options
    # cli uses (undocumented) global variables DOC_SUPPORT_EMAIL
    # actually everything it has is undocumented
    cli.add_argument('-U', '--user',
                     help="Azkaban username, '-' means yourself (default is a specific headless user)")
    cli.add_argument('-P', '--password',
                     help="Azkaban password (default is to prompt)")
    cli.add_argument('-a', '--azkaban',
                     help="Override Azkaban instance nickname or base URL")
    cli.add_argument('-p', '--project',
                     help="Override Azkaban project name")
    cli.add_argument('-z', '--zipfile',
                     help="Zip file of flows to upload, for \"upload\" action")
    cli.add_argument('plan', nargs='+')

    with cli.run():
        # cli sets up global variables args, cli, and log.
        # except that those are in practice unusable
        # and we have to do it ourselves.
        global _log
        _log = cli.log
        args = cli.args

        #####
        # AzkabanClient.test_azduration()
        # test_parse_duration()
        # sys.exit(1)
        #####

        user = args.user
        if user is None or user == '-':
            # No username, or a username of '-' means the calling user
            user = getpass.getuser()
        password = args.password
        if password is None:
            # if no password was specified, prompt for it
            password = getpass.getpass('Azkaban Password for ' + user + ':')

        for filename in args.plan:
            with open(filename, 'r') as fh:
                plan = json.load(fh)
            plan['name'] = filename
            if args.zipfile is not None:
                plan['zipfile'] = args.zipfile
            baseurl = args.azkaban if args.azkaban is not None else plan['baseurl']
            project_name = args.project if args.project is not None else plan['project']
            azkaban = Azkaban(baseurl, user, password)
            project = azkaban.get_project(project_name)
            run_plan(project, plan)
