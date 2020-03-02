# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2019 NIWA & British Crown (Met Office) & Contributors.
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

"""Wrangle task proxies to manage the workflow.

"""

from fnmatch import fnmatchcase
import json
from time import time
from queue import Queue, Empty

from cylc.flow.parsec.OrderedDict import OrderedDict

from cylc.flow import LOG
from cylc.flow.cycling.loader import get_point, standardise_point_string
from cylc.flow.exceptions import SuiteConfigError, PointParsingError
from cylc.flow.suite_status import StopMode
from cylc.flow.task_action_timer import TaskActionTimer
from cylc.flow.task_events_mgr import (
    CustomTaskEventHandlerContext, TaskEventMailContext,
    TaskJobLogsRetrieveContext)
from cylc.flow.task_id import TaskID
from cylc.flow.task_job_logs import get_task_job_id
from cylc.flow.task_proxy import TaskProxy
from cylc.flow.task_state import (
    TASK_STATUSES_ACTIVE, TASK_STATUSES_FAILURE, TASK_STATUSES_NOT_STALLED,
    TASK_STATUS_WAITING, TASK_STATUS_EXPIRED,
    TASK_STATUS_QUEUED, TASK_STATUS_READY, TASK_STATUS_SUBMITTED,
    TASK_STATUS_SUBMIT_FAILED, TASK_STATUS_SUBMIT_RETRYING,
    TASK_STATUS_RUNNING, TASK_STATUS_SUCCEEDED, TASK_STATUS_FAILED,
    TASK_STATUS_RETRYING)
from cylc.flow.wallclock import get_current_time_string


class TaskPool(object):
    """Task pool of a suite."""

    ERR_PREFIX_TASKID_MATCH = "No matching tasks found: "
    ERR_PREFIX_TASK_NOT_ON_SEQUENCE = "Invalid cycle point for task: "

    def __init__(self, config, suite_db_mgr, task_events_mgr, job_pool):
        self.config = config
        self.stop_point = config.final_point
        self.suite_db_mgr = suite_db_mgr
        self.task_events_mgr = task_events_mgr
        self.job_pool = job_pool

        self.do_reload = False
        self.custom_runahead_limit = self.config.get_custom_runahead_limit()
        self.max_future_offset = None
        self._prev_runahead_base_point = None
        self.max_num_active_cycle_points = (
            self.config.get_max_num_active_cycle_points())
        self._prev_runahead_sequence_points = None

        self.pool = {}
        self.runahead_pool = {}
        self.myq = {}
        self.queues = {}
        self.assign_queues()

        self.pool_list = []
        self.rhpool_list = []
        self.pool_changed = False
        self.rhpool_changed = False
        self.pool_changes = []

        self.is_held = False
        self.hold_point = None
        self.held_future_tasks = []

        self.orphans = []
        self.task_name_list = self.config.get_task_name_list()

        self.finished_tasks_queue = Queue()

    def assign_queues(self):
        """self.myq[taskname] = qfoo"""
        self.myq.clear()
        for queue, qconfig in self.config.cfg['scheduling']['queues'].items():
            self.myq.update((name, queue) for name in qconfig['members'])

    def add_to_runahead_pool(self, itask, is_new=True):
        """Add a new task to the runahead pool if possible.

        Tasks whose recurrences allow them to spawn beyond the suite
        stop point are added to the pool in the held state, ready to be
        released if the suite stop point is changed.

        """

        # do not add if a task with the same ID already exists
        if self.get_task_by_id(itask.identity) is not None:
            LOG.warning(
                '%s cannot be added to pool: task ID already exists' %
                itask.identity)
            return

        # do not add if an inserted task is beyond its own stop point
        # (note this is not the same as recurrence bounds)
        if itask.stop_point and itask.point > itask.stop_point:
            LOG.info(
                '%s not adding to pool: beyond task stop cycle' %
                itask.identity)
            return

        # add in held state if beyond the suite hold point
        if self.hold_point and itask.point > self.hold_point:
            LOG.info(
                "[%s] -holding (beyond suite hold point) %s",
                itask, self.hold_point)
            itask.state.reset(is_held=True)
        elif (self.stop_point and itask.point <= self.stop_point and
                self.task_has_future_trigger_overrun(itask)):
            LOG.info("[%s] -holding (future trigger beyond stop point)", itask)
            self.held_future_tasks.append(itask.identity)
            itask.state.reset(is_held=True)
        elif (
                self.is_held
                and itask.state(
                    TASK_STATUS_WAITING,
                    is_held=False
                )
        ):
            # Hold newly-spawned tasks in a held suite (e.g. due to manual
            # triggering of a held task).
            itask.state.reset(is_held=True)

        # add to the runahead pool
        self.runahead_pool.setdefault(itask.point, OrderedDict())
        self.runahead_pool[itask.point][itask.identity] = itask
        self.rhpool_changed = True

        # add row to "task_states" table
        if is_new and itask.submit_num == 0:
            self.suite_db_mgr.put_insert_task_states(itask, {
                "time_created": get_current_time_string(),
                "time_updated": get_current_time_string(),
                "status": itask.state.status})
            if itask.state.outputs.has_custom_triggers():
                self.suite_db_mgr.put_insert_task_outputs(itask)
        return itask

    def release_runahead_tasks(self):
        """Release tasks from the runahead pool to the main pool.

        SoD: runahead based on active tasks, not stuck waiting ones.
        Return True if any tasks are released, else False.
        """
        released = False
        if not self.runahead_pool:
            return released

        # Any finished tasks can be released immediately (this can happen at
        # restart when all tasks are initially loaded into the runahead pool).
        for itask_id_maps in self.runahead_pool.copy().values():
            for itask in itask_id_maps.copy().values():
                if itask.state(
                    TASK_STATUS_FAILED,
                    TASK_STATUS_SUCCEEDED,
                    TASK_STATUS_EXPIRED
                ):
                    self.release_runahead_task(itask)
                    released = True

        limit = self.max_num_active_cycle_points

        points = []
        if not self.pool:
            # Main pool empty implies start-up: base runahead on waiting tasks.
            for point, itasks in sorted(self.get_tasks_by_point(incl_runahead=True).items()):
                found = False
                for itask in itasks:
                    if itask.state(TASK_STATUS_WAITING):
                        found = True
                        break
                if not points and not found:
                    # We need to begin with an unfinished cycle point.
                    continue
                points.append(point)
        else:
            # Otherwise, base on oldest non-waiting task in the main pool.
            for point, itasks in sorted(self.get_tasks_by_point(incl_runahead=False).items()):
                found = False
                for itask in itasks:
                    if not itask.state(TASK_STATUS_WAITING):
                        found = True
                        break
                if not points and not found:
                    # We need to begin with an unfinished cycle point.
                    continue
                points.append(point)

        if not points:
            return False

        # Get the earliest point with unfinished tasks.
        runahead_base_point = min(points)

        # TODO SoD: how much of the following is still needed?
        # TODO SoD: can we obsolete the old-style runahead limit?

        # Get all cycling points possible after the runahead base point.
        if (self._prev_runahead_base_point is not None and
                runahead_base_point == self._prev_runahead_base_point):
            # Cache for speed.
            sequence_points = self._prev_runahead_sequence_points
        else:
            sequence_points = []
            for sequence in self.config.sequences:
                point = runahead_base_point
                for _ in range(limit):
                    point = sequence.get_next_point(point)
                    if point is None:
                        break
                    sequence_points.append(point)
            sequence_points = set(sequence_points)
            self._prev_runahead_sequence_points = sequence_points
            self._prev_runahead_base_point = runahead_base_point

        points = set(points).union(sequence_points)

        if self.custom_runahead_limit is None:
            # Calculate which tasks to release based on a maximum number of
            # active cycle points (active meaning non-finished tasks).
            latest_allowed_point = sorted(points)[:limit][-1]
            if self.max_future_offset is not None:
                # For the first N points, release their future trigger tasks.
                latest_allowed_point += self.max_future_offset
        else:
            # Calculate which tasks to release based on a maximum duration
            # measured from the oldest non-finished task.
            latest_allowed_point = (
                runahead_base_point + self.custom_runahead_limit)

            if (self._prev_runahead_base_point is None or
                    self._prev_runahead_base_point != runahead_base_point):
                if self.custom_runahead_limit < self.max_future_offset:
                    LOG.warning(
                        ('custom runahead limit of %s is less than ' +
                         'future triggering offset %s: suite may stall.') % (
                            self.custom_runahead_limit,
                            self.max_future_offset
                        )
                    )
            self._prev_runahead_base_point = runahead_base_point
        if self.stop_point and latest_allowed_point > self.stop_point:
            latest_allowed_point = self.stop_point

        for point, itask_id_map in self.runahead_pool.copy().items():
            if point <= latest_allowed_point:
                for itask in itask_id_map.copy().values():
                    self.release_runahead_task(itask)
                    released = True
        return released

    def load_db_task_pool_for_restart(self, row_idx, row):
        """Load a task from previous task pool.

        Output completion status is loaded from the DB, and tasks recorded
        as submitted or running are polled to confirm their true status.

        Prerequisite status (satisfied or not) is inferred from task status:
           WAITING or HELD  - all prerequisites unsatisfied
           status > QUEUED - all prerequisites satisfied.
        TODO - this is not correct, e.g. a held task may have some (but not
        all) satisfied prerequisites; and a running task (etc.) could have
        been manually triggered with unsatisfied prerequisites. See comments
        in GitHub #2329 on how to fix this in the future.

        """
        if row_idx == 0:
            LOG.info("LOADING task proxies")
        (cycle, name, is_late, status, is_held, submit_num, _,
         user_at_host, time_submit, time_run, timeout,
         outputs_str) = row
        try:
            itask = TaskProxy(
                self.config.get_taskdef(name),
                get_point(cycle),
                is_held=is_held,
                submit_num=submit_num,
                is_late=bool(is_late))
        except SuiteConfigError:
            LOG.exception((
                'ignoring task %s from the suite run database\n'
                '(its task definition has probably been deleted).'
            ) % name)
        except Exception:
            LOG.exception('could not load task %s' % name)
        else:
            if status in (
                    TASK_STATUS_SUBMITTED,
                    TASK_STATUS_RUNNING
            ):
                itask.state.set_prerequisites_all_satisfied()
                # update the task proxy with user@host
                try:
                    itask.task_owner, itask.task_host = user_at_host.split(
                        "@", 1)
                except (AttributeError, ValueError):
                    itask.task_owner = None
                    itask.task_host = user_at_host
                if time_submit:
                    itask.set_summary_time('submitted', time_submit)
                if time_run:
                    itask.set_summary_time('started', time_run)
                if timeout is not None:
                    itask.timeout = timeout

            elif status in TASK_STATUSES_FAILURE:
                itask.state.set_prerequisites_all_satisfied()

            elif status in (
                    TASK_STATUS_QUEUED,
                    TASK_STATUS_READY,
            ):
                # reset to waiting as these had not been submitted yet.
                status = TASK_STATUS_WAITING
                itask.state.set_prerequisites_all_satisfied()

            elif status in (
                    TASK_STATUS_SUBMIT_RETRYING,
                    TASK_STATUS_RETRYING,
            ):
                itask.state.set_prerequisites_all_satisfied()

            elif status in (
                    TASK_STATUS_SUCCEEDED,
            ):
                itask.state.set_prerequisites_all_satisfied()

            itask.state.reset(status)

            # Running or finished task can have completed custom outputs.
            if itask.state(
                    TASK_STATUS_RUNNING,
                    TASK_STATUS_FAILED,
                    TASK_STATUS_SUCCEEDED
            ):
                try:
                    for message in json.loads(outputs_str).values():
                        itask.state.outputs.set_completion(message, True)
                except (AttributeError, TypeError, ValueError):
                    # Back compat for <=7.6.X
                    # Each output in separate line as "trigger=message"
                    try:
                        for output in outputs_str.splitlines():
                            itask.state.outputs.set_completion(
                                output.split("=", 1)[1], True)
                    except AttributeError:
                        pass

            if user_at_host:
                itask.summary['job_hosts'][int(submit_num)] = user_at_host
            LOG.info("+ %s.%s %s%s" % (
                name, cycle, status, ' (held)' if is_held else ''))
            self.add_to_runahead_pool(itask, is_new=False)

    def load_db_task_action_timers(self, row_idx, row):
        """Load a task action timer, e.g. event handlers, retry states."""
        if row_idx == 0:
            LOG.info("LOADING task action timers")
        (cycle, name, ctx_key_raw, ctx_raw, delays_raw, num, delay,
         timeout) = row
        id_ = TaskID.get(name, cycle)
        try:
            # Extract type namedtuple variables from JSON strings
            ctx_key = json.loads(str(ctx_key_raw))
            ctx_data = json.loads(str(ctx_raw))
            for known_cls in [
                    CustomTaskEventHandlerContext,
                    TaskEventMailContext,
                    TaskJobLogsRetrieveContext]:
                if ctx_data and ctx_data[0] == known_cls.__name__:
                    ctx = known_cls(*ctx_data[1])
                    break
            else:
                ctx = ctx_data
                if ctx is not None:
                    ctx = tuple(ctx)
            delays = json.loads(str(delays_raw))
        except ValueError:
            LOG.exception(
                "%(id)s: skip action timer %(ctx_key)s" %
                {"id": id_, "ctx_key": ctx_key_raw})
            return
        if ctx_key == "poll_timer" or ctx_key[0] == "poll_timers":
            # "poll_timers" for back compat with <=7.6.X
            itask = self.get_task_by_id(id_)
            if itask is None:
                LOG.warning("%(id)s: task not found, skip" % {"id": id_})
                return
            itask.poll_timer = TaskActionTimer(
                ctx, delays, num, delay, timeout)
        elif ctx_key[0] == "try_timers":
            itask = self.get_task_by_id(id_)
            if itask is None:
                LOG.warning("%(id)s: task not found, skip" % {"id": id_})
                return
            itask.try_timers[ctx_key[1]] = TaskActionTimer(
                ctx, delays, num, delay, timeout)
        elif ctx:
            key1, submit_num = ctx_key
            # Convert key1 to type tuple - JSON restores as type list
            # and this will not previously have been converted back
            if isinstance(key1, list):
                key1 = tuple(key1)
            key = (key1, cycle, name, submit_num)
            self.task_events_mgr.event_timers[key] = TaskActionTimer(
                ctx, delays, num, delay, timeout)
        else:
            LOG.exception(
                "%(id)s: skip action timer %(ctx_key)s" %
                {"id": id_, "ctx_key": ctx_key_raw})
            return
        LOG.info("+ %s.%s %s" % (name, cycle, ctx_key))

    def release_runahead_task(self, itask):
        """Release itask to the appropriate queue in the active pool."""
        # SoD: to keep partially satisfied tasks in the rh pool:
        #if not itask.state.prerequisites_are_all_satisfied():
        #   return
        try:
            queue = self.myq[itask.tdef.name]
        except KeyError:
            queue = self.config.Q_DEFAULT
        self.queues.setdefault(queue, OrderedDict())
        self.queues[queue][itask.identity] = itask
        self.pool.setdefault(itask.point, {})
        self.pool[itask.point][itask.identity] = itask
        self.pool_changed = True
        self.pool_changes.append(itask)
        LOG.debug("[%s] -released to the task pool", itask)
        del self.runahead_pool[itask.point][itask.identity]
        if not self.runahead_pool[itask.point]:
            del self.runahead_pool[itask.point]
        self.rhpool_changed = True
        if itask.tdef.max_future_prereq_offset is not None:
            self.set_max_future_offset()
        # Auto-spawn next instance of tasks with no prereqs at the next point.
        next_point = itask.next_point()
        if next_point is not None:
            if not itask.tdef.get_parent_points(next_point):
                self.spawn(itask.tdef.name, itask.point, itask.tdef.name, next_point)

    def remove_finished_tasks(self):
        """Remove finished tasks if there are any active ones.

        """
        if self.finished_tasks_queue.empty():
           return

        stalled = True
        for itask in self.get_tasks():
           if itask.state(
                 TASK_STATUS_QUEUED,
                 TASK_STATUS_READY,
                 TASK_STATUS_SUBMITTED,
                 TASK_STATUS_RUNNING,
                 TASK_STATUS_RETRYING,
                 TASK_STATUS_SUBMIT_RETRYING):
              stalled = False
              break
        if stalled:
           LOG.warning("Not removed finished tasks: workflow stalled")
           return
        
        removed = False
        finished_tasks = []
        while True:
           try:
              itask = self.finished_tasks_queue.get(block=False)
           except Empty:
              break
           else:
               finished_tasks.append(itask)

        # Iterate over finished tasks twice: once to update downstreams'
        # list of parents; then again to remove if parents are finished.

        # TODO SoD - can we avoid updating children and doing the "parents
        # finished?" check, for tasks that don't need it?
        # (It's only for conditional reflow prevention).
        for itask in finished_tasks:
              # Tell my children (if they exist) I've finished (conditional
              # housekeeping).
              for msg, children in itask.children.items():
                  for name, point in children:
                      # TODO SoD - this iterates over the task pool.
                      # Only update children if they exist in the pool.
                      # 
                      ctask = self.get_task_by_id(TaskID.get(name, point))
                      if ctask:
                          ctask.parents[(itask.tdef.name, itask.point)] = True

        put_back = []
        for itask in finished_tasks:
            # Remove if all my parents have finished.
            if all(itask.parents.values()):
                self.remove(itask)
                removed = True
            else:
                put_back.append(itask)
        for itask in put_back:
            self.finished_tasks_queue.put(itask)
        return removed
 
    def remove(self, itask, reason=None):
        """Remove finished task proxies.
        
        """
        print('REMOVING', itask.identity)
        try:
            del self.runahead_pool[itask.point][itask.identity]
        except KeyError:
            pass
        else:
            if not self.runahead_pool[itask.point]:
                del self.runahead_pool[itask.point]
            self.rhpool_changed = True
            return

        # remove from queue
        if itask.tdef.name in self.myq:  # A reload can remove a task
            del self.queues[self.myq[itask.tdef.name]][itask.identity]
        del self.pool[itask.point][itask.identity]
        if not self.pool[itask.point]:
            del self.pool[itask.point]
        self.pool_changed = True
        msg = "task proxy removed"
        if reason:
            msg += " (%s)" % reason
        LOG.debug("[%s] -%s", itask, msg)
        if itask.tdef.max_future_prereq_offset is not None:
            self.set_max_future_offset()
        del itask

    def get_all_tasks(self):
        """Return a list of all task proxies."""
        return self.get_rh_tasks() + self.get_tasks()

    def get_tasks(self):
        """Return a list of task proxies in the main task pool."""
        if self.pool_changed:
            self.pool_changed = False
            self.pool_list = []
            for itask_id_maps in self.queues.values():
                self.pool_list.extend(list(itask_id_maps.values()))
        return self.pool_list

    def get_rh_tasks(self):
        """Return a list of task proxies in the runahead pool."""
        if self.rhpool_changed:
            self.rhpool_changed = False
            self.rhpool_list = []
            for itask_id_maps in self.runahead_pool.values():
                self.rhpool_list.extend(list(itask_id_maps.values()))
        return self.rhpool_list

    def get_pool_change_tasks(self):
        """Return a list of task proxies that changed pool."""
        results = self.pool_changes
        self.pool_changes = []
        return results

    def get_tasks_by_point(self, incl_runahead):
        """Return a map of task proxies by cycle point."""
        point_itasks = {}
        for point, itask_id_map in self.pool.items():
            point_itasks[point] = list(itask_id_map.values())

        if not incl_runahead:
            return point_itasks

        for point, itask_id_map in self.runahead_pool.items():
            point_itasks.setdefault(point, [])
            point_itasks[point].extend(list(itask_id_map.values()))
        return point_itasks

    def get_task_by_id(self, id_):
        """Return task by ID is in the runahead_pool or pool.

        Return None if task does not exist.
        """
        for itask_ids in (
                list(self.queues.values())
                + list(self.runahead_pool.values())):
            try:
                return itask_ids[id_]
            except KeyError:
                pass

    def get_ready_tasks(self):
        """
        1) queue tasks that are ready to run (prerequisites satisfied,
        clock-trigger time up) or if their manual trigger flag is set.

        2) then submit queued tasks if their queue limit has not been
        reached or their manual trigger flag is set.

        If TASK_STATUS_QUEUED the task will submit as soon as its internal
        queue allows (or immediately if manually triggered first).

        Use of "cylc trigger" sets a task's manual trigger flag. Then,
        below, an unqueued task will be queued whether or not it is
        ready to run; and a queued task will be submitted whether or not
        its queue limit has been reached. The flag is immediately unset
        after use so that two manual trigger ops are required to submit
        an initially unqueued task that is queue-limited.

        Return the tasks that are dequeued.
        """

        now = time()
        ready_tasks = []
        qconfig = self.config.cfg['scheduling']['queues']

        for queue in self.queues:
            # 1) queue unqueued tasks that are ready to run or manually forced
            for itask in list(self.queues[queue].values()):
                if not itask.state(TASK_STATUS_QUEUED):
                    # only need to check that unqueued tasks are ready
                    if itask.is_ready(now):
                        # queue the task
                        itask.state.reset(TASK_STATUS_QUEUED)
                        itask.reset_manual_trigger()
                        # move the task to the back of the queue
                        self.queues[queue][itask.identity] = \
                            self.queues[queue].pop(itask.identity)

            # 2) submit queued tasks if manually forced or not queue-limited
            n_active = 0
            n_release = 0
            n_limit = qconfig[queue]['limit']
            tasks = list(self.queues[queue].values())

            # 2.1) count active tasks and compare to queue limit
            if n_limit:
                for itask in tasks:
                    if itask.state(
                            TASK_STATUS_READY,
                            TASK_STATUS_SUBMITTED,
                            TASK_STATUS_RUNNING,
                            is_held=False
                    ):
                        n_active += 1
                n_release = n_limit - n_active

            # 2.2) release queued tasks if not limited or if manually forced
            for itask in tasks:
                if not itask.state(TASK_STATUS_QUEUED):
                    # (This excludes tasks remaining TASK_STATUS_READY because
                    # job submission has been stopped with 'cylc shutdown').
                    continue
                if itask.manual_trigger or not n_limit or n_release > 0:
                    # manual release, or no limit, or not currently limited
                    n_release -= 1
                    ready_tasks.append(itask)
                    itask.reset_manual_trigger()
                    # (Set to 'ready' is done just before job submission).
                # else leaved queued

        LOG.debug('%d task(s) de-queued' % len(ready_tasks))

        return ready_tasks

    def task_has_future_trigger_overrun(self, itask):
        """Check for future triggers extending beyond the final cycle."""
        if not self.stop_point:
            return False
        for pct in itask.state.prerequisites_get_target_points():
            if pct > self.stop_point:
                return True
        return False

    def get_min_point(self):
        """Return the minimum cycle point currently in the pool."""
        cycles = list(self.pool)
        minc = None
        if cycles:
            minc = min(cycles)
        return minc

    def get_max_point(self):
        """Return the maximum cycle point currently in the pool."""
        cycles = list(self.pool)
        maxc = None
        if cycles:
            maxc = max(cycles)
        return maxc

    def get_max_point_runahead(self):
        """Return the maximum cycle point currently in the runahead pool."""
        cycles = list(self.runahead_pool)
        maxc = None
        if cycles:
            maxc = max(cycles)
        return maxc

    def set_max_future_offset(self):
        """Calculate the latest required future trigger offset."""
        max_offset = None
        for itask in self.get_tasks():
            if (itask.tdef.max_future_prereq_offset is not None and
                    (max_offset is None or
                     itask.tdef.max_future_prereq_offset > max_offset)):
                max_offset = itask.tdef.max_future_prereq_offset
        self.max_future_offset = max_offset

    def set_do_reload(self, config):
        """Set the task pool to reload mode."""
        self.config = config
        if config.options.stopcp:
            self.stop_point = get_point(config.options.stopcp)
        else:
            self.stop_point = config.final_point
        self.do_reload = True

        self.custom_runahead_limit = self.config.get_custom_runahead_limit()
        self.max_num_active_cycle_points = (
            self.config.get_max_num_active_cycle_points())

        # find any old tasks that have been removed from the suite
        old_task_name_list = self.task_name_list
        self.task_name_list = self.config.get_task_name_list()
        for name in old_task_name_list:
            if name not in self.task_name_list:
                self.orphans.append(name)
        for name in self.task_name_list:
            if name in self.orphans:
                self.orphans.remove(name)
        # adjust the new suite config to handle the orphans
        self.config.adopt_orphans(self.orphans)

        # reassign live tasks from the old queues to the new.
        # self.queues[queue][id_] = task
        self.assign_queues()
        new_queues = {}
        for queue in self.queues:
            for id_, itask in self.queues[queue].items():
                if itask.tdef.name not in self.myq:
                    continue
                key = self.myq[itask.tdef.name]
                new_queues.setdefault(key, OrderedDict())
                new_queues[key][id_] = itask
        self.queues = new_queues

    def reload_taskdefs(self):
        """Reload task definitions."""
        LOG.info("Reloading task definitions.")
        tasks = self.get_all_tasks()
        # Log tasks orphaned by a reload that were not in the task pool.
        for name in self.orphans:
            if name not in (itask.tdef.name for itask in tasks):
                LOG.warning("Removed task: '%s'", name)
        for itask in tasks:
            if itask.tdef.name in self.orphans:
                if (
                        itask.state(
                            TASK_STATUS_WAITING,
                            TASK_STATUS_QUEUED,
                            TASK_STATUS_SUBMIT_RETRYING,
                            TASK_STATUS_RETRYING,
                        )
                        or itask.state.is_held
                ):
                    # Remove orphaned task if it hasn't started running yet.
                    LOG.warning("[%s] -(task orphaned by suite reload)", itask)
                    self.remove(itask)
                else:
                    # Keep active orphaned task, but stop it from spawning.
                    # TODO SoD: remove downstreams from the proxy?
                    LOG.warning(
                        "[%s] -last instance (orphaned by reload)", itask)
            else:
                self.remove(itask, '(suite definition reload)')
                new_task = self.add_to_runahead_pool(TaskProxy(
                    self.config.get_taskdef(itask.tdef.name), itask.point,
                    itask.state.status, stop_point=itask.stop_point,
                    submit_num=itask.submit_num))
                itask.copy_to_reload_successor(new_task)
                LOG.info('[%s] -reloaded task definition', itask)
                if itask.state(*TASK_STATUSES_ACTIVE):
                    LOG.warning(
                        "[%s] -job(%02d) active with pre-reload settings",
                        itask,
                        itask.submit_num)
        LOG.info("Reload completed.")
        self.do_reload = False

    def set_stop_point(self, stop_point):
        """Set the global suite stop point."""
        if self.stop_point == stop_point:
            return
        LOG.info("Setting stop cycle point: %s", stop_point)
        self.stop_point = stop_point
        for itask in self.get_tasks():
            # check cycle stop or hold conditions
            if (
                    self.stop_point
                    and itask.point > self.stop_point
                    and itask.state(
                        TASK_STATUS_WAITING,
                        TASK_STATUS_QUEUED,
                        is_held=False
                    )
            ):
                LOG.warning(
                    "[%s] -not running (beyond suite stop cycle) %s",
                    itask,
                    self.stop_point)
                itask.state.reset(is_held=True)
        return self.stop_point

    def can_stop(self, stop_mode):
        """Return True if suite can stop.

        A task is considered active if:
        * It is in the active state and not marked with a kill failure.
        * It has pending event handlers.
        """
        if stop_mode is None:
            return False
        if stop_mode == StopMode.REQUEST_NOW_NOW:
            return True
        if self.task_events_mgr.event_timers:
            return False
        for itask in self.get_tasks():
            if (
                    stop_mode == StopMode.REQUEST_CLEAN
                    and itask.state(*TASK_STATUSES_ACTIVE)
                    and not itask.state.kill_failed
            ):
                return False
        return True

    def warn_stop_orphans(self):
        """Log (warning) orphaned tasks on suite stop."""
        for itask in self.get_tasks():
            if (
                    itask.state(*TASK_STATUSES_ACTIVE)
                    and itask.state.kill_failed
            ):
                LOG.warning("%s: orphaned task (%s, kill failed)" % (
                    itask.identity, itask.state.status))
            elif itask.state(*TASK_STATUSES_ACTIVE):
                LOG.warning("%s: orphaned task (%s)" % (
                    itask.identity, itask.state.status))
        for key1, point, name, submit_num in self.task_events_mgr.event_timers:
            LOG.warning("%s/%s/%s: incomplete task event handler %s" % (
                point, name, submit_num, key1))

    def is_stalled(self):
        """Return True if the suite is stalled.

        A suite is stalled when:
        * It is not held.
        * It has no active tasks.
        * It has waiting tasks with unmet prerequisites
          (ignoring clock triggers).
        """
        if self.is_held:
            return False
        can_be_stalled = False
        for itask in self.get_tasks():
            if (
                    self.stop_point
                    and itask.point > self.stop_point
                    or itask.state(
                        TASK_STATUS_SUCCEEDED,
                        TASK_STATUS_EXPIRED,
                    )
            ):
                # Ignore: Task beyond stop point.
                # Ignore: Succeeded and expired tasks.
                continue
            if itask.state(*TASK_STATUSES_NOT_STALLED):
                # Pool contains active tasks (or held active tasks)
                # Return "not stalled" immediately.
                return False
            if (
                    itask.state(TASK_STATUS_WAITING)
                    and itask.state.prerequisites_are_all_satisfied()
            ):
                # Waiting tasks with all prerequisites satisfied,
                # probably waiting for clock trigger only.
                # This task can be considered active.
                # Return "not stalled" immediately.
                return False
            # We should be left with (submission) failed tasks and
            # waiting tasks with unsatisfied prerequisites.
            can_be_stalled = True
        return can_be_stalled

    def report_stalled_task_deps(self):
        """Log unmet dependencies on stalled."""
        prereqs_map = {}
        for itask in self.get_tasks():
            if (
                    itask.state(TASK_STATUS_WAITING)
                    and itask.state.prerequisites_are_not_all_satisfied()
            ):
                prereqs_map[itask.identity] = []
                for prereq_str, is_met in itask.state.prerequisites_dump():
                    if not is_met:
                        prereqs_map[itask.identity].append(prereq_str)

        # prune tree to ignore items that are elsewhere in it
        for id_, prereqs in list(prereqs_map.copy().items()):
            for prereq in prereqs:
                prereq_strs = prereq.split()
                if prereq_strs[0] == "LABEL:":
                    unsatisfied_id = prereq_strs[3]
                elif prereq_strs[0] == "CONDITION:":
                    continue
                else:
                    unsatisfied_id = prereq_strs[0]
                # Clear out tasks with dependencies on other waiting tasks
                if unsatisfied_id in prereqs_map:
                    del prereqs_map[id_]
                    break

        for id_, prereqs in prereqs_map.items():
            LOG.warning("Unmet prerequisites for %s:" % id_)
            for prereq in prereqs:
                LOG.warning(" * %s" % prereq)

    def set_hold_point(self, point):
        """Set the point after which tasks must be held."""
        self.hold_point = point
        if point is not None:
            for itask in self.get_all_tasks():
                if itask.point > point:
                    itask.state.reset(is_held=True)

    def hold_tasks(self, items):
        """Hold tasks with IDs matching any item in "ids"."""
        itasks, bad_items = self.filter_task_proxies(items)
        for itask in itasks:
            itask.state.reset(is_held=True)
        return len(bad_items)

    def release_tasks(self, items):
        """Release held tasks with IDs matching any item in "ids"."""
        itasks, bad_items = self.filter_task_proxies(items)
        for itask in itasks:
            itask.state.reset(is_held=False)
        return len(bad_items)

    def hold_all_tasks(self):
        """Hold all tasks."""
        LOG.info("Holding all waiting or queued tasks now")
        self.is_held = True
        for itask in self.get_all_tasks():
            itask.state.reset(is_held=True)

    def release_all_tasks(self):
        """Release all held tasks."""
        self.is_held = False
        self.release_tasks(None)

    def check_abort_on_task_fails(self):
        """Check whether suite should abort on task failure.

        Return True if:
        * There are failed tasks and `abort if any task fails` is specified.
        * There are unexpected failed tasks in a reference test.
        """
        expected_failed_tasks = self.config.get_expected_failed_tasks()
        if expected_failed_tasks is None:
            return False
        return any(
            (
                itask.state.status in TASK_STATUSES_FAILURE
                and itask.identity not in expected_failed_tasks
            )
            for itask in self.get_tasks())

    def spawn(self, up_name, up_point, name, point, message=None, go=False):
        """Spawn a new tasks proxy."""
        LOG.info('[%s.%s] spawning %s.%s (%s)',
                 up_name, up_point, name, point, message)
        itask = None
        for jtask in self.get_all_tasks():
            if jtask.tdef.name == name and jtask.point == point:
                itask = jtask
                break
        if itask is None:
            for tname in self.config.get_task_name_list():
                if tname == name:
                   itask = TaskProxy(
                       self.config.get_taskdef(tname), point)
                   self.add_to_runahead_pool(itask)
                   break
        # TODO itask not found? (shouldn't happen)
        if go:
           itask.state.set_prerequisites_all_satisfied()
        elif message is not None:
           outputs = set([])
           outputs.add((up_name, str(up_point), message))
           itask.state.satisfy_me(outputs)

    def remove_suiciding_tasks(self):
        """Remove any tasks that have suicide-triggered.

        Return the number of removed tasks.
        """
        num_removed = 0
        for itask in self.get_all_tasks():
            if (itask.state.suicide_prerequisites and
                    itask.state.suicide_prerequisites_are_all_satisfied()):
                if itask.state(
                        TASK_STATUS_READY,
                        TASK_STATUS_SUBMITTED,
                        TASK_STATUS_RUNNING,
                        is_held=False
                ):
                    LOG.warning('[%s] -suiciding while active', itask)
                else:
                    LOG.info('[%s] -suiciding', itask)
                self.remove(itask, 'suicide')
                num_removed += 1
        return num_removed

    def spawn_tasks(self, items, failed, non_failed):
        """Spawn downstream children of given task outputs on user command.

        TODO allow user-specified specific outputs.
        """
        n_warnings = 0
        task_items = {}
        select_args = []
        for item in items:
            point_str, name_str = self._parse_task_item(item)[:2]
            if point_str is None:
                LOG.warning(
                    "%s: task ID for insert must contain cycle point" % (item))
                n_warnings += 1
                continue
            try:
                point_str = standardise_point_string(point_str)
            except PointParsingError as exc:
                LOG.warning(
                    self.ERR_PREFIX_TASKID_MATCH + ("%s (%s)" % (item, exc)))
                n_warnings += 1
                continue
            taskdefs = self.config.find_taskdefs(name_str)
            if not taskdefs:
                LOG.warning(self.ERR_PREFIX_TASKID_MATCH + item)
                n_warnings += 1
                continue
            for taskdef in taskdefs:
                task_items[(taskdef.name, point_str)] = taskdef
                select_args.append((taskdef.name, point_str))
        # TODO - this only works for the initial spawning!
        submit_nums = self.suite_db_mgr.pri_dao.select_submit_nums_for_insert(
            select_args)
        for key, taskdef in sorted(task_items.items()):
            # Check that the cycle point is on one of the tasks sequences.
            point = get_point(key[1])
            # Check if cycle point is on the tasks sequence.
            for sequence in taskdef.sequences:
                if sequence.is_on_sequence(point):
                    break
            else:
                LOG.warning("%s%s, %s" % (
                    self.ERR_PREFIX_TASK_NOT_ON_SEQUENCE, taskdef.name,
                    key[1]))
                continue
            submit_num = submit_nums.get(key, 0)

            # This the upstream target task:
            itask = TaskProxy(taskdef, point)

            LOG.info("[%s] - forced spawning", itask)

            msgs = []
            if failed:
                msgs.append('failed')
            elif non_failed:
                for trig, msg, status in itask.state.outputs.get_all(): 
                    if trig not in ["submit-failed", "failed", "expired"]:
                        msgs.append(msg)
            else:
                 msgs.append('succeeded')

            # Now spawn downstream on chosen outputs.
            for msg in msgs:
                try:
                    children = itask.children[msg]
                except KeyError:
                    pass
                else:
                    for child_name, child_point in children:
                        self.spawn(itask.tdef.name, itask.point,
                                   child_name, child_point, msg)

    def trigger_tasks(self, items, back_out=False):
        """Operator-forced task triggering."""
        #------
        # SoD COUP
        for item in items:
            name, str_point = TaskID.split(item)
            self.spawn('None', 'None', name, get_point(str_point), go=True)
        return
        # ------

        itasks, bad_items = self.filter_task_proxies(items)
        n_warnings = len(bad_items)
        for itask in itasks:
            if back_out:
                # (Aborted edit-run, reset for next trigger attempt).
                try:
                    del itask.summary['job_hosts'][itask.submit_num]
                except KeyError:
                    pass
                job_d = get_task_job_id(
                    itask.point, itask.tdef.name, itask.submit_num)
                self.job_pool.remove_job(job_d)
                itask.submit_num -= 1
                itask.summary['submit_num'] = itask.submit_num
                itask.local_job_file_path = None
                continue
            if itask.state(*TASK_STATUSES_ACTIVE):
                LOG.warning('%s: already triggered' % itask.identity)
                n_warnings += 1
                continue
            itask.manual_trigger = True
            if not itask.state(
                    TASK_STATUS_QUEUED,
                    is_held=False
            ):
                itask.state.reset(TASK_STATUS_READY, is_held=False)
        return n_warnings

    def sim_time_check(self, message_queue):
        """Simulation mode: simulate task run times and set states."""
        sim_task_state_changed = False
        now = time()
        for itask in self.get_tasks():
            if itask.state.status != TASK_STATUS_RUNNING:
                continue
            # Started time is not set on restart
            if itask.summary['started_time'] is None:
                itask.summary['started_time'] = now
            timeout = (itask.summary['started_time'] +
                       itask.tdef.rtconfig['job']['simulated run length'])
            if now > timeout:
                conf = itask.tdef.rtconfig['simulation']
                job_d = get_task_job_id(
                    itask.point, itask.tdef.name, itask.submit_num)
                now_str = get_current_time_string()
                if (itask.point in conf['fail cycle points'] and
                        (itask.get_try_num() == 1 or
                         not conf['fail try 1 only'])):
                    message_queue.put(
                        (job_d, now_str, 'CRITICAL', TASK_STATUS_FAILED))
                else:
                    # Simulate message outputs.
                    for msg in itask.tdef.rtconfig['outputs'].values():
                        message_queue.put((job_d, now_str, 'INFO', msg))
                    message_queue.put(
                        (job_d, now_str, 'INFO', TASK_STATUS_SUCCEEDED))
                sim_task_state_changed = True
        return sim_task_state_changed

    def set_expired_task(self, itask, now):
        """Check if task has expired. Set state and event handler if so.

        Return True if task has expired.
        """
        if (
                not itask.state(
                    TASK_STATUS_WAITING,
                    is_held=False
                )
                or itask.tdef.expiration_offset is None
        ):
            return False
        if itask.expire_time is None:
            itask.expire_time = (
                itask.get_point_as_seconds() +
                itask.get_offset_as_seconds(itask.tdef.expiration_offset))
        if now > itask.expire_time:
            msg = 'Task expired (skipping job).'
            LOG.warning('[%s] -%s', itask, msg)
            self.task_events_mgr.setup_event_handlers(itask, "expired", msg)
            itask.state.reset(TASK_STATUS_EXPIRED, is_held=False)
            return True
        return False

    def task_succeeded(self, id_):
        """Return True if task with id_ is in the succeeded state."""
        for itask in self.get_tasks():
            if (
                    itask.identity == id_
                    and itask.state(TASK_STATUS_SUCCEEDED)
            ):
                return True
        return False

    def ping_task(self, id_, exists_only=False):
        """Return message to indicate if task exists and/or is running."""
        found = False
        running = False
        for itask in self.get_tasks():
            if itask.identity == id_:
                found = True
                if itask.state(TASK_STATUS_RUNNING):
                    running = True
                break
        if found and exists_only:
            return True, "task found"
        elif running:
            return True, "task running"
        elif found:
            return False, "task not running"
        else:
            return False, "task not found"

    def filter_task_proxies(self, items):
        """Return task proxies that match names, points, states in items.

        Return (itasks, bad_items).
        In the new form, the arguments should look like:
        items -- a list of strings for matching task proxies, each with
                 the general form name[.point][:state] or [point/]name[:state]
                 where name is a glob-like pattern for matching a task name or
                 a family name.

        """
        itasks = []
        bad_items = []
        if not items:
            itasks += self.get_all_tasks()
        else:
            for item in items:
                point_str, name_str, status = self._parse_task_item(item)
                if point_str is None:
                    point_str = "*"
                else:
                    try:
                        point_str = standardise_point_string(point_str)
                    except PointParsingError:
                        # point_str may be a glob
                        pass
                tasks_found = False
                for itask in self.get_all_tasks():
                    nss = itask.tdef.namespace_hierarchy
                    if (fnmatchcase(str(itask.point), point_str) and
                            (not status or itask.state.status == status) and
                            (fnmatchcase(itask.tdef.name, name_str) or
                             any(fnmatchcase(ns, name_str) for ns in nss))):
                        itasks.append(itask)
                        tasks_found = True
                if not tasks_found:
                    LOG.warning(self.ERR_PREFIX_TASKID_MATCH + item)
                    bad_items.append(item)
        return itasks, bad_items

    @staticmethod
    def _parse_task_item(item):
        """Parse point/name:state or name.point:state syntax."""
        if ":" in item:
            head, state_str = item.rsplit(":", 1)
        else:
            head, state_str = (item, None)
        if "/" in head:
            point_str, name_str = head.split("/", 1)
        elif "." in head:
            name_str, point_str = head.split(".", 1)
        else:
            name_str, point_str = (head, None)
        return (point_str, name_str, state_str)
