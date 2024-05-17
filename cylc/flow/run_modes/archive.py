# THIS FILE IS PART OF THE CYLC WORKFLOW ENGINE.
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
"""Utilities supporting skip modes
"""
from ansimarkup import parse as cparse
from logging import INFO
from typing import (
    TYPE_CHECKING, Any, Dict, List, Set, Tuple)

from cylc.flow import LOG
from cylc.flow.exceptions import WorkflowConfigError
from cylc.flow.platforms import get_platform
from cylc.flow.task_outputs import (
    TASK_OUTPUT_SUBMITTED,
    TASK_OUTPUT_SUCCEEDED,
    TASK_OUTPUT_FAILED,
    TASK_OUTPUT_STARTED
)
from cylc.flow.task_state import RunMode

if TYPE_CHECKING:
    from cylc.flow.taskdef import TaskDef
    from cylc.flow.task_job_mgr import TaskJobManager
    from cylc.flow.task_proxy import TaskProxy
    from typing_extensions import Literal


def submit_task_job(
    task_job_mgr: 'TaskJobManager',
    itask: 'TaskProxy',
    rtconfig: Dict[str, Any],
    workflow: str,
    now: Tuple[float, str]
) -> 'Literal[True]':
    """Submit a task in skip mode.

    Returns:
        True - indicating that TaskJobManager need take no further action.
    """
    itask.summary['started_time'] = now[0]
    # TODO - do we need this? I don't thing so?
    task_job_mgr._set_retry_timers(itask, rtconfig)
    itask.waiting_on_job_prep = False
    itask.submit_num += 1

    itask.platform = get_platform()
    itask.platform['name'] = RunMode.ARCHIVE
    itask.summary['job_runner_name'] = RunMode.ARCHIVE
    itask.tdef.run_mode = RunMode.ARCHIVE
    task_job_mgr.task_events_mgr.process_message(
        itask, INFO, TASK_OUTPUT_SUBMITTED,
    )
    task_job_mgr.workflow_db_mgr.put_insert_task_jobs(
        itask, {
            'time_submit': now[1],
            'try_num': itask.get_try_num(),
        }
    )
    try:
        archive(itask, task_job_mgr)
    except Exception as exc: # noqa
        task_job_mgr.task_events_mgr.process_message(
            itask, INFO, TASK_OUTPUT_FAILED,
        )
    else:
        task_job_mgr.task_events_mgr.process_message(
            itask, INFO, TASK_OUTPUT_SUCCEEDED,
        )
    return True


def archive(itask, task_job_mgr):
    from cylc.flow.pathutil import get_workflow_run_dir
    from pathlib import Path
    from cylc.flow.cycling.loader import get_point
    from shutil import rmtree
    workflow_dir = Path(get_workflow_run_dir(task_job_mgr.workflow))
    for old_cycle in (workflow_dir / 'log/job').glob('*'):
        if get_point(old_cycle.name) < itask.point:
            rmtree(old_cycle)
            LOG.info(f'Removed log files for {old_cycle.name}')
    for old_cycle in (workflow_dir / 'work').glob('*'):
        if get_point(old_cycle.name) < itask.point:
            rmtree(old_cycle)
            LOG.info(f'Removed work for {old_cycle.name}')