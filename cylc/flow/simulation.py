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
"""Utilities supporting simulation and skip modes
"""

from typing import (
    TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, ValuesView)
from time import time


from cylc.flow import LOG
from cylc.flow.cycling.loader import get_point
from cylc.flow.exceptions import CylcError
from cylc.flow.network.resolvers import TaskMsg
from cylc.flow.platforms import FORBIDDEN_WITH_PLATFORM
from cylc.flow.task_outputs import (
    TASK_OUTPUT_EXPIRED,
    TASK_OUTPUT_SUBMITTED,
    TASK_OUTPUT_SUBMIT_FAILED,
    TASK_OUTPUT_STARTED,
    TASK_OUTPUT_SUCCEEDED,
    TASK_OUTPUT_FAILED,
    TASK_OUTPUT_FINISHED,
)
from cylc.flow.task_state import (
    TASK_STATUS_RUNNING,
    TASK_STATUS_FAILED,
    TASK_STATUS_SUCCEEDED,
)
from cylc.flow.wallclock import get_current_time_string

from metomi.isodatetime.parsers import DurationParser

if TYPE_CHECKING:
    from queue import Queue
    from cylc.flow.taskdef import TaskDef
    from cylc.flow.cycling import PointBase
    from cylc.flow.task_proxy import TaskProxy


SIMULATION_CONFIGS = ['simulation', 'execution time limit']


SIMULATION = 'simulation'
SKIP = 'skip'
LIVE = 'live'
DUMMY = 'dummy'
WORKFLOW = 'workflow'


def configure_sim_modes(taskdefs, workflow_mode):
    """Adjust task definitions for simulation and dummy modes.
    """
    for tdef in taskdefs:
        # Compute simulated run time by scaling the execution limit.
        configure_rtc_sim_mode(tdef.rtconfig, workflow_mode)


def configure_rtc_sim_mode(rtc, workflow_mode):
    """Change a task proxy's runtime config to simulation mode settings.
    """
    rtc['run mode'] = rtc.get('run mode', workflow_mode)
    dummy_mode = bool(workflow_mode == DUMMY)

    sleep_sec = get_simulated_run_len(rtc)
    rtc['execution time limit'] = (
        sleep_sec + DurationParser().parse(str(
            rtc['simulation']['time limit buffer'])).get_seconds()
    )

    rtc['simulation']['simulated run length'] = sleep_sec
    rtc['submission retry delays'] = [1]

    if dummy_mode:
        # Generate dummy scripting.
        rtc['init-script'] = ""
        rtc['env-script'] = ""
        rtc['pre-script'] = ""
        rtc['post-script'] = ""
        rtc['err-script'] = ""
        rtc['script'] = build_dummy_script(
            rtc, sleep_sec)

    disable_platforms(rtc)

    rtc['platform'] = 'localhost'

    # Disable environment, in case it depends on env-script.
    rtc['environment'] = {}

    rtc["simulation"][
        "fail cycle points"
    ] = parse_fail_cycle_points(
        rtc["simulation"]["fail cycle points"]
    )


def check_sim_modes(taskdefs: 'ValuesView[TaskDef]'):
    """If running in live mode warn of taskdefs with run-mode set to
    skip or simulation.

    These tasks will appear to run, but won't actually do
    anything.
    """
    warn_for: Dict[str, List[str]] = {}
    for tdef in taskdefs:
        if tdef.rtconfig['run mode'] == SKIP:
            warn_for[tdef.name] = SKIP
        if tdef.rtconfig['run mode'] == SIMULATION:
            warn_for[tdef.name] = SIMULATION
    if warn_for:
        msg = (
            'The following tasks have a non-live mode set'
            ' in their config:\n * ')
        msg += '\n * '.join(
            [f'{k} ({v})' for k, v in warn_for.items()])
        LOG.warning(msg)


def get_simulated_run_len(rtc: Dict[str, Any]) -> int:
    """Get simulated run time.

    Args:
        rtc: run time config

    Returns:
        Number of seconds to sleep for in sim mode.
    """
    # Simulated run length acts as a flag that this is at runtime:
    # If durations have already been parsed, trying to parse them
    # again will result in failures.
    recalc = bool(rtc['simulation'].get('simulated run length', ''))
    limit = rtc['execution time limit']
    speedup = rtc['simulation']['speedup factor']

    if recalc:
        if limit and speedup:
            sleep_sec = limit / speedup
        else:
            sleep_sec = rtc['simulation']['default run length']
    else:
        if limit and speedup and isinstance(limit, float):
            sleep_sec = limit / speedup
        elif limit and speedup:
            sleep_sec = (DurationParser().parse(
                str(limit)).get_seconds() / speedup)
        else:
            default_run_len = str(rtc['simulation']['default run length'])
            sleep_sec = DurationParser().parse(default_run_len).get_seconds()

    return sleep_sec


def build_dummy_script(rtc: Dict[str, Any], sleep_sec: int) -> str:
    """Create fake scripting for dummy mode.

    This is for Dummy mode only.
    """
    script = "sleep %d" % sleep_sec
    # Dummy message outputs.
    for msg in rtc['outputs'].values():
        script += "\ncylc message '%s'" % msg
    if rtc['simulation']['fail try 1 only']:
        arg1 = "true"
    else:
        arg1 = "false"
    arg2 = " ".join(rtc['simulation']['fail cycle points'])
    script += "\ncylc__job__dummy_result %s %s || exit 1" % (arg1, arg2)
    return script


def disable_platforms(
    rtc: Dict[str, Any]
) -> None:
    """Force platform = localhost

    Remove legacy sections [job] and [remote], which would conflict
    with setting platforms.

    This can be simplified when support for the FORBIDDEN_WITH_PLATFORM
    configurations is dropped.
    """
    for section, keys in FORBIDDEN_WITH_PLATFORM.items():
        if section in rtc:
            for key in keys:
                if key in rtc[section]:
                    rtc[section][key] = None
    rtc['platform'] = 'localhost'


def parse_fail_cycle_points(
    f_pts_orig: List[str]
) -> 'Union[None, List[PointBase]]':
    """Parse `[simulation][fail cycle points]`.

    - None for "fail all points".
    - Else a list of cycle point objects.

    Examples:
        >>> this = parse_fail_cycle_points
        >>> this(['all']) is None
        True
        >>> this([])
        []
    """
    f_pts: 'Optional[List[PointBase]]'
    if f_pts_orig is None or 'all' in f_pts_orig:
        f_pts = None
    else:
        f_pts = []
        for point_str in f_pts_orig:
            f_pts.append(get_point(point_str).standardise())
    return f_pts


def unpack_dict(dict_: NestedDict, parent_key: str = '') -> Dict[str, Any]:
    """Unpack a nested dict into a single layer.

    Examples:
        >>> unpack_dict({'foo': 1, 'bar': {'baz': 2, 'qux':3}})
        {'foo': 1, 'bar.baz': 2, 'bar.qux': 3}
        >>> unpack_dict({'foo': {'example': 42}, 'bar': {"1":2, "3":4}})
        {'foo.example': 42, 'bar.1': 2, 'bar.3': 4}

    """
    output = {}
    for key, value in dict_.items():
        new_key = parent_key + '.' + key if parent_key else key
        if isinstance(value, dict):
            output.update(unpack_dict(value, new_key))
        else:
            output[new_key] = value

    return output


def nested_dict_path_update(
    dict_: NestedDict, path: List[Any], value: Any
) -> NestedDict:
    """Set a value in a nested dict.

    Examples:
        >>> nested_dict_path_update({'foo': {'bar': 1}}, ['foo', 'bar'], 42)
        {'foo': {'bar': 42}}
    """
    this = dict_
    for i in range(len(path)):
        if isinstance(this[path[i]], dict):
            this = this[path[i]]
        else:
            this[path[i]] = value
    return dict_


def update_nested_dict(rtc: NestedDict, dict_: NestedDict) -> None:
    """Update one config nested dictionary with the contents of another.

    Examples:
        >>> x = {'foo': {'bar': 12}, 'qux': 77}
        >>> y = {'foo': {'bar': 42}}
        >>> update_nested_dict(x, y)
        >>> print(x)
        {'foo': {'bar': 42}, 'qux': 77}
    """
    for keylist, value in unpack_dict(dict_).items():
        keys = keylist.split('.')
        rtc = nested_dict_path_update(rtc, keys, value)


def internal_task_status_check(
    message_queue: 'Queue[TaskMsg]',
    itasks: 'List[TaskProxy]',
    broadcast_mgr: Optional[Any] = None,
    workflow_mode: Optional[str] = LIVE
):
    """Checks whether simulated or skipped tasks have been updated by a
    broadcast and works out whether we want to view them as being "done".
    """
    internal_task_state_changed = False
    now = time()
    for itask in itasks:
        if broadcast_mgr:
            broadcast = broadcast_mgr.get_broadcast(itask.tokens)
            if broadcast:
                update_nested_dict(
                    itask.tdef.rtconfig, broadcast)
                configure_rtc_sim_mode(itask.tdef.rtconfig, False)
        if (
            itask.tdef.rtconfig['run mode'] == SIMULATION
            and sim_time_check(message_queue, itask, now, broadcast_mgr)
        ):
            internal_task_state_changed = True
        if (
            itask.tdef.rtconfig['run mode'] == SKIP
            and set_skip_outputs(message_queue, itask, broadcast_mgr)
        ):
            internal_task_state_changed = True

    return internal_task_state_changed


def set_skip_outputs(
    message_queue: 'Queue[TaskMsg]',
    itask: 'TaskProxy',
    broadcast_mgr: Optional[Any] = None
) -> bool:
    """Set task output required by skip mode without delay.

    * Submitted, Started and Succeeded will always be generated.
    * All required outputs will be generated.
    * If [runtime][<namespace>][skip]outputs is specified and does not
      include either succeeded or failed then succeeded will be produced.

    """
    job_d = itask.tokens.duplicate(job=str(itask.submit_num))
    now_str = get_current_time_string()

    outputs = filter_skip_outputs(
        itask.tdef.outputs, itask.tdef.rtconfig['skip']['outputs']
    )
    outputs = {'submitted': 'submitted', 'bar': 'dafursargrfaf', 'succeeded': 'succeeded'}
    for _, message in outputs.items():

        message_queue.put(TaskMsg(job_d, now_str, 'WARNING', message))
    return True
    # job_d = itask.tokens.duplicate(job=str(itask.submit_num))
    # now_str = get_current_time_string()
    # if sim_task_failed(
    #     itask.tdef.rtconfig['simulation'],
    #     itask.point,
    #     itask.get_try_num()
    # ):
    #     message_queue.put(
    #         TaskMsg(job_d, now_str, 'CRITICAL', TASK_STATUS_FAILED)
    #     )
    # else:
    #     # Simulate message outputs.
    #     for msg in itask.tdef.rtconfig['outputs'].values():
    #         message_queue.put(
    #             TaskMsg(job_d, now_str, 'DEBUG', msg)
    #         )
    #     message_queue.put(
    #         TaskMsg(job_d, now_str, 'DEBUG', TASK_STATUS_SUCCEEDED)
    #     )


def filter_skip_outputs(
    task_outputs: Dict[str, Tuple[str, Optional[bool]]],
    skip_outputs: List[str]
) -> Dict[str, str]:
    """Decide which outputs should be marked satisfied in skip mode.

    Encapsulate logic for ease of testing.
    """
    outputs = {}
    for output, (message, is_required) in task_outputs.items():
        if (
            # These are always emitted:
            output in [TASK_OUTPUT_SUBMITTED, TASK_OUTPUT_SUBMITTED]
            # This task will fail in skip mode!
            or (
                output == TASK_OUTPUT_FAILED
                and TASK_OUTPUT_FAILED in skip_outputs
            )
            # This task will succeed in skip mode:
            or (
                output == TASK_OUTPUT_SUCCEEDED
                and TASK_OUTPUT_FAILED not in skip_outputs
            )
            or is_required
            or output in skip_outputs
        ):
            outputs[output] = message
    return outputs


def sim_time_check(
    message_queue: 'Queue[TaskMsg]',
    itask: 'TaskProxy',
    now: float,
    broadcast_mgr: Optional[Any] = None
) -> bool:
    """Check if scheduler only tasks have been "running" for
    as long as required.

    If they have change the task state.
    If broadcasts are active and they apply to tasks in itasks update
    itasks.rtconfig.

    Returns:
        True if _any_ simulated task state has changed.
    """
    if broadcast_mgr:
        broadcast = broadcast_mgr.get_broadcast(itask.tokens)
        if broadcast:
            for config in SIMULATION_CONFIGS:
                if (
                    config in broadcast
                    and isinstance(broadcast[config], dict)
                ):
                    itask.tdef.rtconfig[config].update(broadcast[config])
                elif config in broadcast:
                    itask.tdef.rtconfig[config] = broadcast[config]
            configure_rtc_sim_mode(itask.tdef.rtconfig, False)
    if itask.state.status != TASK_STATUS_RUNNING:
        return False
    # Started time is not set on restart
    if itask.summary['started_time'] is None:
        itask.summary['started_time'] = now
    timeout = (
        itask.summary['started_time'] +
        itask.tdef.rtconfig['simulation']['simulated run length']
    )
    if now > timeout:
        job_d = itask.tokens.duplicate(job=str(itask.submit_num))
        now_str = get_current_time_string()
        if sim_task_failed(
            itask.tdef.rtconfig['simulation'],
            itask.point,
            itask.get_try_num()
        ):
            message_queue.put(
                TaskMsg(job_d, now_str, 'CRITICAL', TASK_STATUS_FAILED)
            )
        else:
            # Simulate message outputs.
            for msg in itask.tdef.rtconfig['outputs'].values():
                message_queue.put(
                    TaskMsg(job_d, now_str, 'DEBUG', msg)
                )
            message_queue.put(
                TaskMsg(job_d, now_str, 'DEBUG', TASK_STATUS_SUCCEEDED)
            )
        return True
    return False


def sim_task_failed(
        sim_conf: Dict[str, Any],
        point: 'PointBase',
        try_num: int,
) -> bool:
    """Encapsulate logic for deciding whether a sim task has failed.

    Allows Unit testing.
    """
    return (
        sim_conf['fail cycle points'] is None  # i.e. "all"
        or point in sim_conf['fail cycle points']
    ) and (
        try_num == 1 or not sim_conf['fail try 1 only']
    )
