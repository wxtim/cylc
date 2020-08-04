# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) NIWA & British Crown (Met Office) & Contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Manage task jobs.

This module provides logic to:
* Set up the directory structure on remote job hosts.
  * Copy suite service files to remote job hosts for communication clients.
  * Clean up of service files on suite shutdown.
* Prepare task job files.
* Prepare task jobs submission, and manage the callbacks.
* Prepare task jobs poll/kill, and manage the callbacks.
"""

import json
from logging import DEBUG, CRITICAL, INFO, WARNING
import os
from shutil import rmtree
from time import time
import traceback
from copy import deepcopy

from cylc.flow.parsec.util import pdeepcopy, poverride

from cylc.flow import LOG
from cylc.flow.batch_sys_manager import JobPollContext
from cylc.flow.hostuserutil import (
    get_host, is_remote_platform
)
from cylc.flow.job_file import JobFileWriter
from cylc.flow.pathutil import get_remote_suite_run_job_dir
from cylc.flow.subprocpool import SubProcPool
from cylc.flow.subprocctx import SubProcContext
from cylc.flow.task_action_timer import TaskActionTimer
from cylc.flow.task_events_mgr import TaskEventsManager, log_task_job_activity
from cylc.flow.task_message import FAIL_MESSAGE_PREFIX
from cylc.flow.task_job_logs import (
    JOB_LOG_JOB, get_task_job_log, get_task_job_job_log,
    get_task_job_activity_log, get_task_job_id, NN)
from cylc.flow.task_outputs import (
    TASK_OUTPUT_SUBMITTED, TASK_OUTPUT_STARTED, TASK_OUTPUT_SUCCEEDED,
    TASK_OUTPUT_FAILED)
from cylc.flow.platforms import platform_from_name, get_host_from_platform
from cylc.flow.task_remote_mgr import (
    REMOTE_INIT_FAILED, TaskRemoteMgr)
from cylc.flow.task_state import (
    TASK_STATUSES_ACTIVE, TASK_STATUS_READY, TASK_STATUS_SUBMITTED,
    TASK_STATUS_RUNNING, TASK_STATUS_SUCCEEDED, TASK_STATUS_FAILED,
    TASK_STATUS_SUBMIT_RETRYING, TASK_STATUS_RETRYING)
from cylc.flow.wallclock import get_current_time_string, get_utc_mode
from cylc.flow.remote import construct_platform_ssh_cmd
from cylc.flow.exceptions import PlatformLookupError, TaskRemoteMgmtError


class TaskJobManager(object):
    """Manage task job submit, poll and kill.

    This class provides logic to:
    * Submit task jobs.
    * Poll task jobs.
    * Kill task jobs.
    * Set up the directory structure on job hosts.
    * Install suite communicate client files on job hosts.
    * Remove suite contact files on job hosts.
    """

    JOBS_KILL = 'jobs-kill'
    JOBS_POLL = 'jobs-poll'
    JOBS_SUBMIT = SubProcPool.JOBS_SUBMIT
    POLL_FAIL = 'poll failed'
    REMOTE_SELECT_MSG = 'waiting for remote host selection'
    REMOTE_INIT_MSG = 'remote host initialising'
    KEY_EXECUTE_TIME_LIMIT = TaskEventsManager.KEY_EXECUTE_TIME_LIMIT

    def __init__(self, suite, proc_pool, suite_db_mgr,
                 task_events_mgr, job_pool):
        self.suite = suite
        self.proc_pool = proc_pool
        self.suite_db_mgr = suite_db_mgr
        self.task_events_mgr = task_events_mgr
        self.job_pool = job_pool
        self.job_file_writer = JobFileWriter()
        self.batch_sys_mgr = self.job_file_writer.batch_sys_mgr
        self.task_remote_mgr = TaskRemoteMgr(suite, proc_pool)

    def check_task_jobs(self, suite, task_pool):
        """Check submission and execution timeout and polling timers.

        Poll tasks that have timed out and/or have reached next polling time.
        """
        now = time()
        poll_tasks = set()
        for itask in task_pool.get_tasks():
            if self.task_events_mgr.check_job_time(itask, now):
                poll_tasks.add(itask)
                if itask.poll_timer.delay is not None:
                    LOG.info(
                        '[%s] -poll now, (next in %s)',
                        itask, itask.poll_timer.delay_timeout_as_str())
        if poll_tasks:
            self.poll_task_jobs(suite, poll_tasks)

    def kill_task_jobs(self, suite, itasks):
        """Kill jobs of active tasks, and hold the tasks.

        If items is specified, kill active tasks matching given IDs.

        """
        to_kill_tasks = []
        for itask in itasks:
            if itask.state(*TASK_STATUSES_ACTIVE):
                itask.state.reset(is_held=True)
                to_kill_tasks.append(itask)
            else:
                LOG.warning('skipping %s: task not killable' % itask.identity)
        self._run_job_cmd(
            self.JOBS_KILL, suite, to_kill_tasks,
            self._kill_task_jobs_callback)

    def poll_task_jobs(self, suite, itasks, poll_succ=True, msg=None):
        """Poll jobs of specified tasks.

        Any job that is or was submitted or running can be polled, except for
        retrying tasks - which would poll (correctly) as failed. And don't poll
        succeeded tasks by default.

        This method uses _poll_task_jobs_callback() and
        _manip_task_jobs_callback() as help/callback methods.

        _poll_task_job_callback() executes one specific job.
        """
        to_poll_tasks = []
        pollable_statuses = {
            TASK_STATUS_SUBMITTED, TASK_STATUS_RUNNING, TASK_STATUS_FAILED
        }
        if poll_succ:
            pollable_statuses.add(TASK_STATUS_SUCCEEDED)
        for itask in itasks:
            if itask.state(*pollable_statuses):
                to_poll_tasks.append(itask)
            else:
                LOG.debug("skipping %s: not pollable, "
                          "or skipping 'succeeded' tasks" % itask.identity)
        if to_poll_tasks:
            if msg is not None:
                LOG.info(msg)
            self._run_job_cmd(
                self.JOBS_POLL, suite, to_poll_tasks,
                self._poll_task_jobs_callback)

    def prep_submit_task_jobs(self, suite, itasks, check_syntax=True):
        """Prepare task jobs for submit.

        Prepare tasks where possible. Ignore tasks that are waiting for host
        select command to complete. Bad host select command or error writing to
        a job file will cause a bad task - leading to submission failure.

        Return [list, list]: list of good tasks, list of bad tasks
        """
        prepared_tasks = []
        bad_tasks = []
        for itask in itasks:
            prep_task = self._prep_submit_task_job(suite, itask,
                                                   check_syntax=check_syntax)
            if prep_task:
                prepared_tasks.append(itask)
            elif prep_task is False:
                bad_tasks.append(itask)
        return [prepared_tasks, bad_tasks]

    def submit_task_jobs(self, suite, itasks, curve_auth,
                         client_pub_key_dir, is_simulation=False):
        """Prepare and submit task jobs.

        Submit tasks where possible. Ignore tasks that are waiting for host
        select command to complete, or tasks that are waiting for remote
        initialisation. Bad host select command, error writing to a job file or
        bad remote initialisation will cause a bad task - leading to submission
        failure.

        This method uses prep_submit_task_job() as helper.

        Return (list): list of tasks that attempted submission.
        """
        if is_simulation:
            return self._simulation_submit_task_jobs(itasks)

        # Prepare tasks for job submission
        prepared_tasks, bad_tasks = self.prep_submit_task_jobs(suite, itasks)

        # Reset consumed host selection results
        self.task_remote_mgr.platform_n_from_subshell_reset()

        if not prepared_tasks:
            return bad_tasks

        # Group task jobs by (platform)
        auth_itasks = {}  # {platform: [itask, ...], ...}
        for itask in prepared_tasks:
            platform_name = itask.platform['name']
            auth_itasks.setdefault(platform_name, [])
            auth_itasks[platform_name].append(itask)
        # Submit task jobs for each platform
        done_tasks = bad_tasks
        for platform_name, itasks in sorted(auth_itasks.items()):
            # Re-fetch a copy of platform
            platform = itasks[0].platform
            is_init = self.task_remote_mgr.remote_init(
                platform_name, curve_auth, client_pub_key_dir
            )
            if is_init is None:
                # Remote is waiting to be initialised
                for itask in itasks:
                    itask.set_summary_message(self.REMOTE_INIT_MSG)
                    self.job_pool.add_job_msg(
                        get_task_job_id(
                            itask.point, itask.tdef.name, itask.submit_num),
                        self.REMOTE_INIT_MSG)
                continue
            # Ensure that localhost background/at jobs are recorded as running
            # on the host name of the current suite host, rather than just
            # "localhost". On suite restart on a different suite host, this
            # allows the restart logic to correctly poll the status of the
            # background/at jobs that may still be running on the previous
            # suite host.
            host = get_host_from_platform(platform)
            if (
                self.batch_sys_mgr.is_job_local_to_host(
                    itask.summary['batch_sys_name']
                ) and
                not is_remote_platform(platform)
            ):
                host = get_host()

            now_str = get_current_time_string()
            done_tasks.extend(itasks)
            for itask in itasks:
                # Log and persist
                LOG.info(
                    '[%s] -submit-num=%02d, owner@host=%s',
                    itask, itask.submit_num, host)
                self.suite_db_mgr.put_insert_task_jobs(itask, {
                    'is_manual_submit': itask.is_manual_submit,
                    'try_num': itask.get_try_num(),
                    'time_submit': now_str,
                    'platform_name': platform['name'],
                    'batch_sys_name': itask.summary['batch_sys_name'],
                })
                itask.is_manual_submit = False
            if is_init == REMOTE_INIT_FAILED:
                # Remote has failed to initialise
                # Set submit-failed for all affected tasks
                for itask in itasks:
                    itask.local_job_file_path = None  # reset for retry
                    log_task_job_activity(
                        SubProcContext(
                            self.JOBS_SUBMIT,
                            '(init %s)' % host,
                            err=REMOTE_INIT_FAILED,
                            ret_code=1),
                        suite, itask.point, itask.tdef.name)
                    self.task_events_mgr.process_message(
                        itask, CRITICAL,
                        self.task_events_mgr.EVENT_SUBMIT_FAILED)
                continue
            # Build the "cylc jobs-submit" command
            cmd = [self.JOBS_SUBMIT]
            if LOG.isEnabledFor(DEBUG):
                cmd.append('--debug')
            if get_utc_mode():
                cmd.append('--utc-mode')
            if is_remote_platform(itask.platform):
                remote_mode = True
                cmd.append('--remote-mode')
            else:
                remote_mode = False
            cmd.append('--')
            cmd.append(
                get_remote_suite_run_job_dir(
                    platform, suite
                )
            )
            # Chop itasks into a series of shorter lists if it's very big
            # to prevent overloading of stdout and stderr pipes.
            itasks = sorted(itasks, key=lambda itask: itask.identity)
            chunk_size = len(itasks) // ((len(itasks) // 100) + 1) + 1
            itasks_batches = [
                itasks[i:i + chunk_size] for i in range(0,
                                                        len(itasks),
                                                        chunk_size)]
            LOG.debug(
                '%s ... # will invoke in batches, sizes=%s',
                cmd, [len(b) for b in itasks_batches])

            if remote_mode:
                cmd = construct_platform_ssh_cmd(cmd, platform)
            else:
                cmd = ['cylc'] + cmd

            for i, itasks_batch in enumerate(itasks_batches):
                stdin_files = []
                job_log_dirs = []
                for itask in itasks_batch:
                    if remote_mode:
                        stdin_files.append(
                            os.path.expandvars(
                                get_task_job_job_log(
                                    suite, itask.point, itask.tdef.name,
                                    itask.submit_num
                                )
                            )
                        )
                    job_log_dirs.append(get_task_job_id(
                        itask.point, itask.tdef.name, itask.submit_num))
                    # The job file is now (about to be) used: reset the file
                    # write flag so that subsequent manual retrigger will
                    # generate a new job file.
                    itask.local_job_file_path = None
                    itask.state.reset(TASK_STATUS_READY)
                    if itask.state.outputs.has_custom_triggers():
                        self.suite_db_mgr.put_update_task_outputs(itask)

                self.proc_pool.put_command(
                    SubProcContext(
                        self.JOBS_SUBMIT,
                        cmd + job_log_dirs,
                        stdin_files=stdin_files,
                        job_log_dirs=job_log_dirs,
                    ),
                    self._submit_task_jobs_callback, [suite, itasks_batch])
        return done_tasks

    @staticmethod
    def _create_job_log_path(suite, itask):
        """Create job log directory for a task job, etc.

        Create local job directory, and NN symbolic link.
        If NN => 01, remove numbered directories with submit numbers greater
        than 01.
        Return a string in the form "POINT/NAME/SUBMIT_NUM".

        """
        job_file_dir = get_task_job_log(
            suite, itask.point, itask.tdef.name, itask.submit_num)
        job_file_dir = os.path.expandvars(job_file_dir)
        task_log_dir = os.path.dirname(job_file_dir)
        if itask.submit_num == 1:
            try:
                names = os.listdir(task_log_dir)
            except OSError:
                pass
            else:
                for name in names:
                    if name not in ["01", NN]:
                        rmtree(
                            os.path.join(task_log_dir, name),
                            ignore_errors=True)
        else:
            rmtree(job_file_dir, ignore_errors=True)

        os.makedirs(job_file_dir, exist_ok=True)
        target = os.path.join(task_log_dir, NN)
        source = os.path.basename(job_file_dir)
        try:
            prev_source = os.readlink(target)
        except OSError:
            prev_source = None
        if prev_source == source:
            return
        try:
            if prev_source:
                os.unlink(target)
            os.symlink(source, target)
        except OSError as exc:
            if not exc.filename:
                exc.filename = target
            raise exc

    @staticmethod
    def _get_job_scripts(itask, rtconfig):
        """Return pre-script, script, post-script for a job."""
        script = rtconfig['script']
        pre_script = rtconfig['pre-script']
        post_script = rtconfig['post-script']
        if itask.tdef.suite_polling_cfg:
            # Automatic suite state polling script
            comstr = "cylc suite-state " + \
                     " --task=" + itask.tdef.suite_polling_cfg['task'] + \
                     " --point=" + str(itask.point)
            if LOG.isEnabledFor(DEBUG):
                comstr += ' --debug'
            for key, fmt in [
                    ('user', ' --%s=%s'),
                    ('host', ' --%s=%s'),
                    ('interval', ' --%s=%d'),
                    ('max-polls', ' --%s=%s'),
                    ('run-dir', ' --%s=%s')]:
                if rtconfig['suite state polling'][key]:
                    comstr += fmt % (key, rtconfig['suite state polling'][key])
            if rtconfig['suite state polling']['message']:
                comstr += " --message='%s'" % (
                    rtconfig['suite state polling']['message'])
            else:
                comstr += " --status=" + itask.tdef.suite_polling_cfg['status']
            comstr += " " + itask.tdef.suite_polling_cfg['suite']
            script = "echo " + comstr + "\n" + comstr
        return pre_script, script, post_script

    @staticmethod
    def _job_cmd_out_callback(suite, itask, cmd_ctx, line):
        """Callback on job command STDOUT/STDERR."""
        if cmd_ctx.cmd_kwargs.get("host") and cmd_ctx.cmd_kwargs.get("user"):
            owner_at_host = "(%(user)s@%(host)s) " % cmd_ctx.cmd_kwargs
        elif cmd_ctx.cmd_kwargs.get("host"):
            owner_at_host = "(%(host)s) " % cmd_ctx.cmd_kwargs
        elif cmd_ctx.cmd_kwargs.get("user"):
            owner_at_host = "(%(user)s@localhost) " % cmd_ctx.cmd_kwargs
        else:
            owner_at_host = ""
        try:
            timestamp, _, content = line.split("|")
        except ValueError:
            pass
        else:
            line = "%s %s" % (timestamp, content)
        job_activity_log = get_task_job_activity_log(
            suite, itask.point, itask.tdef.name)
        try:
            with open(os.path.expandvars(job_activity_log), "ab") as handle:
                if not line.endswith("\n"):
                    line += "\n"
                handle.write((owner_at_host + line).encode())
        except IOError as exc:
            LOG.warning("%s: write failed\n%s" % (job_activity_log, exc))
            LOG.warning("[%s] -%s%s", itask, owner_at_host, line)

    def _kill_task_jobs_callback(self, ctx, suite, itasks):
        """Callback when kill tasks command exits."""
        self._manip_task_jobs_callback(
            ctx,
            suite,
            itasks,
            self._kill_task_job_callback,
            {self.batch_sys_mgr.OUT_PREFIX_COMMAND: self._job_cmd_out_callback}
        )

    def _kill_task_job_callback(self, suite, itask, cmd_ctx, line):
        """Helper for _kill_task_jobs_callback, on one task job."""
        ctx = SubProcContext(self.JOBS_KILL, None)
        ctx.out = line
        try:
            ctx.timestamp, _, ctx.ret_code = line.split("|", 2)
        except ValueError:
            ctx.ret_code = 1
            ctx.cmd = cmd_ctx.cmd  # print original command on failure
        else:
            ctx.ret_code = int(ctx.ret_code)
            if ctx.ret_code:
                ctx.cmd = cmd_ctx.cmd  # print original command on failure
        log_task_job_activity(ctx, suite, itask.point, itask.tdef.name)
        log_lvl = INFO
        log_msg = 'killed'
        if ctx.ret_code:  # non-zero exit status
            log_lvl = WARNING
            log_msg = 'kill failed'
            itask.state.kill_failed = True
        elif itask.state(TASK_STATUS_SUBMITTED):
            self.task_events_mgr.process_message(
                itask, CRITICAL, self.task_events_mgr.EVENT_SUBMIT_FAILED,
                ctx.timestamp)
        elif itask.state(TASK_STATUS_RUNNING):
            self.task_events_mgr.process_message(
                itask, CRITICAL, TASK_OUTPUT_FAILED)
        else:
            log_lvl = DEBUG
            log_msg = (
                'ignoring job kill result, unexpected task state: %s' %
                itask.state.status)
        itask.set_summary_message(log_msg)
        self.job_pool.add_job_msg(
            get_task_job_id(itask.point, itask.tdef.name, itask.submit_num),
            log_msg)
        LOG.log(log_lvl, "[%s] -job(%02d) %s" % (
            itask.identity, itask.submit_num, log_msg))

    def _manip_task_jobs_callback(
            self, ctx, suite, itasks, summary_callback, more_callbacks=None):
        """Callback when submit/poll/kill tasks command exits."""
        if ctx.ret_code:
            LOG.error(ctx)
        else:
            LOG.debug(ctx)
        # A dict for easy reference of (CYCLE, NAME, SUBMIT_NUM) -> TaskProxy
        #
        # Note for "reload": A TaskProxy instance may be replaced on reload, so
        # the "itasks" list may not reference the TaskProxy objects that
        # replace the old ones. The .reload_successor attribute provides the
        # link(s) for us to get to the latest replacement.
        #
        # Note for "kill": It is possible for a job to trigger its trap and
        # report back to the suite back this logic is called. If so, the task
        # will no longer be TASK_STATUS_SUBMITTED or TASK_STATUS_RUNNING, and
        # its output line will be ignored here.
        tasks = {}
        for itask in itasks:
            while itask.reload_successor is not None:
                itask = itask.reload_successor
            if itask.point is not None and itask.submit_num:
                submit_num = "%02d" % (itask.submit_num)
                tasks[(str(itask.point), itask.tdef.name, submit_num)] = itask
        handlers = [(self.batch_sys_mgr.OUT_PREFIX_SUMMARY, summary_callback)]
        if more_callbacks:
            for prefix, callback in more_callbacks.items():
                handlers.append((prefix, callback))
        out = ctx.out
        if not out:
            out = ""
        bad_tasks = dict(tasks)
        for line in out.splitlines(True):
            for prefix, callback in handlers:
                if line.startswith(prefix):
                    line = line[len(prefix):].strip()
                    try:
                        path = line.split("|", 2)[1]  # timestamp, path, status
                        point, name, submit_num = path.split(os.sep, 2)
                        if prefix == self.batch_sys_mgr.OUT_PREFIX_SUMMARY:
                            del bad_tasks[(point, name, submit_num)]
                        itask = tasks[(point, name, submit_num)]
                        callback(suite, itask, ctx, line)
                    except (LookupError, ValueError, KeyError) as exc:
                        LOG.warning(
                            'Unhandled %s output: %s', ctx.cmd_key, line)
                        LOG.exception(exc)
        # Task jobs that are in the original command but did not get a status
        # in the output. Handle as failures.
        for key, itask in sorted(bad_tasks.items()):
            line = (
                "|".join([ctx.timestamp, os.sep.join(key), "1"]) + "\n")
            summary_callback(suite, itask, ctx, line)

    def _poll_task_jobs_callback(self, ctx, suite, itasks):
        """Callback when poll tasks command exits."""
        self._manip_task_jobs_callback(
            ctx,
            suite,
            itasks,
            self._poll_task_job_callback,
            {self.batch_sys_mgr.OUT_PREFIX_MESSAGE:
             self._poll_task_job_message_callback})

    def _poll_task_job_callback(self, suite, itask, cmd_ctx, line):
        """Helper for _poll_task_jobs_callback, on one task job."""
        ctx = SubProcContext(self.JOBS_POLL, None)
        ctx.out = line
        ctx.ret_code = 0

        # See cylc.flow.batch_sys_manager.JobPollContext
        job_d = get_task_job_id(itask.point, itask.tdef.name, itask.submit_num)
        try:
            job_log_dir, context = line.split('|')[1:3]
            items = json.loads(context)
            jp_ctx = JobPollContext(job_log_dir, **items)
        except TypeError:
            itask.set_summary_message(self.POLL_FAIL)
            self.job_pool.add_job_msg(job_d, self.POLL_FAIL)
            ctx.cmd = cmd_ctx.cmd  # print original command on failure
            return
        except ValueError:
            # back compat for cylc 7.7.1 and previous
            try:
                values = line.split('|')
                items = dict(  # done this way to ensure IndexError is raised
                    (key, values[x]) for
                    x, key in enumerate(JobPollContext.CONTEXT_ATTRIBUTES))
                job_log_dir = items.pop('job_log_dir')
            except (ValueError, IndexError):
                itask.set_summary_message(self.POLL_FAIL)
                self.job_pool.add_job_msg(job_d, self.POLL_FAIL)
                ctx.cmd = cmd_ctx.cmd  # print original command on failure
                return
        finally:
            log_task_job_activity(ctx, suite, itask.point, itask.tdef.name)

        flag = self.task_events_mgr.FLAG_POLLED
        if jp_ctx.run_status == 1 and jp_ctx.run_signal in ["ERR", "EXIT"]:
            # Failed normally
            self.task_events_mgr.process_message(
                itask, INFO, TASK_OUTPUT_FAILED, jp_ctx.time_run_exit, flag)
        elif jp_ctx.run_status == 1 and jp_ctx.batch_sys_exit_polled == 1:
            # Failed by a signal, and no longer in batch system
            self.task_events_mgr.process_message(
                itask, INFO, TASK_OUTPUT_FAILED, jp_ctx.time_run_exit, flag)
            self.task_events_mgr.process_message(
                itask, INFO, FAIL_MESSAGE_PREFIX + jp_ctx.run_signal,
                jp_ctx.time_run_exit,
                flag)
        elif jp_ctx.run_status == 1:
            # The job has terminated, but is still managed by batch system.
            # Some batch system may restart a job in this state, so don't
            # mark as failed yet.
            self.task_events_mgr.process_message(
                itask, INFO, TASK_OUTPUT_STARTED, jp_ctx.time_run, flag)
        elif jp_ctx.run_status == 0:
            # The job succeeded
            self.task_events_mgr.process_message(
                itask, INFO, TASK_OUTPUT_SUCCEEDED, jp_ctx.time_run_exit,
                flag)
        elif jp_ctx.time_run and jp_ctx.batch_sys_exit_polled == 1:
            # The job has terminated without executing the error trap
            self.task_events_mgr.process_message(
                itask, INFO, TASK_OUTPUT_FAILED, get_current_time_string(),
                flag)
        elif jp_ctx.time_run:
            # The job has started, and is still managed by batch system
            self.task_events_mgr.process_message(
                itask, INFO, TASK_OUTPUT_STARTED, jp_ctx.time_run, flag)
        elif jp_ctx.batch_sys_exit_polled == 1:
            # The job never ran, and no longer in batch system
            self.task_events_mgr.process_message(
                itask, INFO, self.task_events_mgr.EVENT_SUBMIT_FAILED,
                jp_ctx.time_submit_exit, flag)
        else:
            # The job never ran, and is in batch system
            self.task_events_mgr.process_message(
                itask, INFO, TASK_STATUS_SUBMITTED, jp_ctx.time_submit_exit,
                flag)

    def _poll_task_job_message_callback(self, suite, itask, cmd_ctx, line):
        """Helper for _poll_task_jobs_callback, on message of one task job."""
        ctx = SubProcContext(self.JOBS_POLL, None)
        ctx.out = line
        try:
            event_time, severity, message = line.split("|")[2:5]
        except ValueError:
            ctx.ret_code = 1
            ctx.cmd = cmd_ctx.cmd  # print original command on failure
        else:
            ctx.ret_code = 0
            self.task_events_mgr.process_message(
                itask, severity, message, event_time,
                self.task_events_mgr.FLAG_POLLED)
        log_task_job_activity(ctx, suite, itask.point, itask.tdef.name)

    def _run_job_cmd(self, cmd_key, suite, itasks, callback):
        """Run job commands, e.g. poll, kill, etc.

        Group itasks with their platform_name, host and owner.
        Put a job command for each group to the multiprocess pool.
        """
        if not itasks:
            return
        # sort itasks into lists based upon where they were run.
        auth_itasks = {}
        for itask in itasks:
            platform_n = itask.platform['name']
            if platform_n not in auth_itasks:
                auth_itasks[platform_n] = []
            auth_itasks[platform_n].append(itask)

        # Go through each list of itasks and carry out commands as required.
        for platform_n, itasks in sorted(auth_itasks.items()):
            platform = platform_from_name(platform_n)
            if is_remote_platform(platform):
                remote_mode = True
                cmd = [cmd_key]
            else:
                cmd = ["cylc", cmd_key]
                remote_mode = False
            if LOG.isEnabledFor(DEBUG):
                cmd.append("--debug")
            cmd.append("--")
            cmd.append(get_remote_suite_run_job_dir(platform, suite))
            job_log_dirs = []
            if remote_mode:
                cmd = construct_platform_ssh_cmd(cmd, platform)
            for itask in sorted(itasks, key=lambda itask: itask.identity):
                job_log_dirs.append(get_task_job_id(
                    itask.point, itask.tdef.name, itask.submit_num))
            cmd += job_log_dirs
            self.proc_pool.put_command(
                SubProcContext(cmd_key, cmd), callback, [suite, itasks])

    @staticmethod
    def _set_retry_timers(itask, rtconfig=None):
        """Set try number and retry delays."""
        if rtconfig is None:
            rtconfig = itask.tdef.rtconfig
        try:
            _ = rtconfig[itask.tdef.run_mode + ' mode']['disable retries']
        except KeyError:
            retry = True
        if retry:
            if rtconfig['job']['submission retry delays']:
                submit_delays = rtconfig['job']['submission retry delays']
            else:
                submit_delays = itask.platform['submission retry delays']
            for key, delays in [
                    (
                        TASK_STATUS_SUBMIT_RETRYING,
                        submit_delays
                    ),
                    (
                        TASK_STATUS_RETRYING,
                        rtconfig['job']['execution retry delays']
                    )
            ]:
                if delays is None:
                    delays = []
                try:
                    itask.try_timers[key].set_delays(delays)
                except KeyError:
                    itask.try_timers[key] = TaskActionTimer(delays=delays)

    def _simulation_submit_task_jobs(self, itasks):
        """Simulation mode task jobs submission."""
        for itask in itasks:
            self._set_retry_timers(itask)
            itask.platform = 'SIMULATION'
            itask.summary['batch_sys_name'] = 'SIMULATION'
            itask.summary[self.KEY_EXECUTE_TIME_LIMIT] = (
                itask.tdef.rtconfig['job']['simulated run length'])
            self.task_events_mgr.process_message(
                itask, INFO, TASK_OUTPUT_SUBMITTED)
        return itasks

    def _submit_task_jobs_callback(self, ctx, suite, itasks):
        """Callback when submit task jobs command exits."""
        self._manip_task_jobs_callback(
            ctx,
            suite,
            itasks,
            self._submit_task_job_callback,
            {self.batch_sys_mgr.OUT_PREFIX_COMMAND: self._job_cmd_out_callback}
        )

    def _submit_task_job_callback(self, suite, itask, cmd_ctx, line):
        """Helper for _submit_task_jobs_callback, on one task job."""
        ctx = SubProcContext(self.JOBS_SUBMIT, None)
        ctx.out = line
        items = line.split("|")
        try:
            ctx.timestamp, _, ctx.ret_code = items[0:3]
        except ValueError:
            ctx.ret_code = 1
            ctx.cmd = cmd_ctx.cmd  # print original command on failure
        else:
            ctx.ret_code = int(ctx.ret_code)
            if ctx.ret_code:
                ctx.cmd = cmd_ctx.cmd  # print original command on failure
        log_task_job_activity(ctx, suite, itask.point, itask.tdef.name)

        if ctx.ret_code == SubProcPool.RET_CODE_SUITE_STOPPING:
            return

        job_d = get_task_job_id(itask.point, itask.tdef.name, itask.submit_num)
        try:
            itask.summary['submit_method_id'] = items[3]
            self.job_pool.set_job_attr(job_d, 'batch_sys_job_id', items[3])
        except IndexError:
            itask.summary['submit_method_id'] = None
        if itask.summary['submit_method_id'] == "None":
            itask.summary['submit_method_id'] = None
        if itask.summary['submit_method_id'] and ctx.ret_code == 0:
            self.task_events_mgr.process_message(
                itask, INFO, TASK_OUTPUT_SUBMITTED, ctx.timestamp)
        else:
            self.task_events_mgr.process_message(
                itask, CRITICAL, self.task_events_mgr.EVENT_SUBMIT_FAILED,
                ctx.timestamp)

    def _prep_submit_task_job(self, suite, itask, check_syntax=True):
        """Prepare a task job submission.

        Return itask on a good preparation.

        """
        if itask.local_job_file_path:
            return itask

        # Handle broadcasts
        overrides = self.task_events_mgr.broadcast_mgr.get_broadcast(
            itask.identity)
        if overrides:
            rtconfig = pdeepcopy(itask.tdef.rtconfig)
            poverride(rtconfig, overrides, prepend=True)
        else:
            rtconfig = itask.tdef.rtconfig

        # Determine platform to be used now, just before job submission,
        # because dynamic platform selection `$(echo platformX)` may be used.
        try:
            platform_n = self.task_remote_mgr.platform_n_from_subshell(
                rtconfig['platform']
            )
        except (PlatformLookupError, TaskRemoteMgmtError) as exc:
            # Submit number not yet incremented
            itask.submit_num += 1
            itask.summary['platforms_used'][itask.submit_num] = ''
            # Retry delays, needed for the try_num
            self._set_retry_timers(itask, rtconfig)
            self._prep_submit_task_job_error(
                suite, itask, '(remote host select)', exc)
            return False
        else:
            if platform_n is None:  # platform select not ready
                itask.set_summary_message(self.REMOTE_SELECT_MSG)
                return
            itask.platform = platform_from_name(platform_n)
            # Submit number not yet incremented
            itask.submit_num += 1
            # Retry delays, needed for the try_num
            self._set_retry_timers(itask, rtconfig)

        try:
            job_conf = self._prep_submit_task_job_impl(suite, itask, rtconfig)

            # Job pool insertion
            job_config = deepcopy(job_conf)
            job_config['logfiles'] = deepcopy(itask.summary['logfiles'])
            itask.jobs.append(job_config['job_d'])
            self.job_pool.insert_job(job_config)

            local_job_file_path = get_task_job_job_log(
                suite, itask.point, itask.tdef.name, itask.submit_num)
            self.job_file_writer.write(local_job_file_path, job_conf,
                                       check_syntax=check_syntax)
        except Exception as exc:
            # Could be a bad command template, IOError, etc
            self._prep_submit_task_job_error(
                suite, itask, '(prepare job file)', exc)
            return False
        itask.local_job_file_path = local_job_file_path

        return itask

    def _prep_submit_task_job_error(self, suite, itask, action, exc):
        """Helper for self._prep_submit_task_job. On error."""
        LOG.debug("submit_num %s" % itask.submit_num)
        LOG.debug(traceback.format_exc())
        LOG.error(exc)
        log_task_job_activity(
            SubProcContext(self.JOBS_SUBMIT, action, err=exc, ret_code=1),
            suite, itask.point, itask.tdef.name, submit_num=itask.submit_num)
        # Persist
        self.suite_db_mgr.put_insert_task_jobs(itask, {
            'is_manual_submit': itask.is_manual_submit,
            'try_num': itask.get_try_num(),
            'time_submit': get_current_time_string(),
            'batch_sys_name': itask.summary.get('batch_sys_name'),
        })
        itask.is_manual_submit = False
        self.task_events_mgr.process_message(
            itask, CRITICAL, self.task_events_mgr.EVENT_SUBMIT_FAILED)

    def _prep_submit_task_job_impl(self, suite, itask, rtconfig):
        """Helper for self._prep_submit_task_job."""

        itask.summary['platforms_used'][
            itask.submit_num] = itask.platform['name']

        itask.summary['batch_sys_name'] = itask.platform['batch system']
        for name in rtconfig['extra log files']:
            itask.summary['logfiles'].append(
                os.path.expanduser(os.path.expandvars(name)))
        try:
            batch_sys_conf = self.task_events_mgr.get_host_conf(
                itask, 'batch systems')[itask.summary['batch_sys_name']]
        except (TypeError, KeyError):
            batch_sys_conf = {}
        try:
            itask.summary[self.KEY_EXECUTE_TIME_LIMIT] = float(
                rtconfig['job']['execution time limit'])
        except TypeError:
            pass

        scripts = self._get_job_scripts(itask, rtconfig)

        # Location of job file, etc
        self._create_job_log_path(suite, itask)
        job_d = get_task_job_id(
            itask.point, itask.tdef.name, itask.submit_num)
        job_file_path = get_remote_suite_run_job_dir(
            itask.platform, suite, job_d, JOB_LOG_JOB)
        return {
            'batch_system_name': itask.platform['batch system'],
            'batch_submit_command_template': (
                itask.platform['batch submit command template']
            ),
            'batch_system_conf': batch_sys_conf,
            'dependencies': itask.state.get_resolved_dependencies(),
            'directives': rtconfig['directives'],
            'environment': rtconfig['environment'],
            'execution_time_limit': itask.summary[self.KEY_EXECUTE_TIME_LIMIT],
            'env-script': rtconfig['env-script'],
            'err-script': rtconfig['err-script'],
            'exit-script': rtconfig['exit-script'],
            'platform': itask.platform,
            'init-script': rtconfig['init-script'],
            'job_file_path': job_file_path,
            'job_d': job_d,
            'namespace_hierarchy': itask.tdef.namespace_hierarchy,
            'owner': itask.task_owner,
            'param_env_tmpl': rtconfig['parameter environment templates'],
            'param_var': itask.tdef.param_var,
            'post-script': scripts[2],
            'pre-script': scripts[0],
            'remote_suite_d': itask.platform['suite definition directory'],
            'script': scripts[1],
            'submit_num': itask.submit_num,
            'flow_label': itask.flow_label,
            'suite_name': suite,
            'task_id': itask.identity,
            'try_num': itask.get_try_num(),
            'uuid_str': self.task_remote_mgr.uuid_str,
            'work_d': rtconfig['work sub-directory'],
        }
