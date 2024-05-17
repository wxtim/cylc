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
"""Utilities supporting all nonlive modes
"""
from typing import TYPE_CHECKING, Dict, List

from cylc.flow import LOG
from cylc.flow.run_modes.skip import check_task_skip_config
from cylc.flow.task_state import RunMode

if TYPE_CHECKING:
    from cylc.flow.taskdefs import TaskDefs


def mode_validate_checks(taskdefs: 'Dict[str, TaskDefs]'):
    """Warn user if any tasks has "run mode" set to a non-live value.

    Additionally, run specific checks for each mode's config settings.
    """
    warn_nonlive: Dict[str, List[str]] = {
        RunMode.SKIP: [],
        RunMode.SIMULATION: [],
        RunMode.DUMMY: [],
        RunMode.ARCHIVE: [],
    }

    # Run through taskdefs looking for those with nonlive modes
    for taskname, taskdef in taskdefs.items():
        # Add to list of tasks to be run in non-live modes:
        if (
            taskdef.rtconfig.get('run mode', 'workflow')
            not in [RunMode.LIVE, 'workflow']
        ):
            warn_nonlive[taskdef.rtconfig['run mode']].append(taskname)

        # Run any mode specific validation checks:
        check_task_skip_config(taskname, taskdef)

    # Assemble warning message about any tasks in nonlive mode.
    if any(warn_nonlive.values()):
        message = 'The following tasks are set to run in non-live mode:'
        for mode, tasknames in warn_nonlive.items():
            if tasknames:
                message += f'\n{mode} mode:'
            for taskname in tasknames:
                message += f'\n    * {taskname}'
        LOG.warning(message)
