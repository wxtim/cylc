#!/usr/bin/env python3

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
"""Task state properties for display."""

from cylc.flow.task_state import TaskStatus

from colorama import Style, Fore, Back


_STATUS_MAP = {
    TaskStatus.RUNAHEAD: {
        "ascii_ctrl": Style.BRIGHT + Fore.WHITE + Back.BLUE
    },
    TaskStatus.WAITING: {
        "ascii_ctrl": Style.BRIGHT + Fore.CYAN + Back.RESET
    },
    TaskStatus.QUEUED: {
        "ascii_ctrl": Style.BRIGHT + Fore.WHITE + Back.BLUE
    },
    TaskStatus.READY: {
        "ascii_ctrl": Style.BRIGHT + Fore.GREEN + Back.RESET
    },
    TaskStatus.EXPIRED: {
        "ascii_ctrl": Style.BRIGHT + Fore.WHITE + Back.BLACK
    },
    TaskStatus.SUBMITTED: {
        "ascii_ctrl": Style.BRIGHT + Fore.YELLOW + Back.RESET
    },
    TaskStatus.SUBMIT_FAILED: {
        "ascii_ctrl": Style.BRIGHT + Fore.BLUE + Back.RESET
    },
    TaskStatus.SUBMIT_RETRYING: {
        "ascii_ctrl": Style.BRIGHT + Fore.BLUE + Back.RESET
    },
    TaskStatus.RUNNING: {
        "ascii_ctrl": Style.BRIGHT + Fore.WHITE + Back.GREEN
    },
    TaskStatus.SUCCEEDED: {
        "ascii_ctrl": Style.NORMAL + Fore.BLACK + Back.RESET
    },
    TaskStatus.FAILED: {
        "ascii_ctrl": Style.BRIGHT + Fore.WHITE + Back.RED
    },
    TaskStatus.RETRYING: {
        "ascii_ctrl": Style.BRIGHT + Fore.MAGENTA + Back.RESET
    }
}


def extract_group_state(child_states, is_stopped=False):
    """Summarise child states as a group."""
    ordered_states = [TaskStatus.SUBMIT_FAILED, TaskStatus.FAILED,
                      TaskStatus.EXPIRED, TaskStatus.SUBMIT_RETRYING,
                      TaskStatus.RETRYING, TaskStatus.RUNNING,
                      TaskStatus.SUBMITTED, TaskStatus.READY,
                      TaskStatus.QUEUED, TaskStatus.WAITING,
                      TaskStatus.SUCCEEDED,
                      TaskStatus.RUNAHEAD]
    if is_stopped:
        ordered_states = [TaskStatus.SUBMIT_FAILED, TaskStatus.FAILED,
                          TaskStatus.RUNNING, TaskStatus.SUBMITTED,
                          TaskStatus.EXPIRED, TaskStatus.READY,
                          TaskStatus.SUBMIT_RETRYING, TaskStatus.RETRYING,
                          TaskStatus.SUCCEEDED, TaskStatus.QUEUED,
                          TaskStatus.WAITING,
                          TaskStatus.RUNAHEAD]
    for state in ordered_states:
        if state in child_states:
            return state
    return None


def get_status_prop(status, key, subst=None):
    """Return property for a task status."""
    if key == "ascii_ctrl" and subst is not None:
        return "%s%s\033[0m" % (_STATUS_MAP[status][key], subst)
    elif key == "ascii_ctrl":
        return "%s%s\033[0m" % (_STATUS_MAP[status][key], status.value)
    else:
        return _STATUS_MAP[status][key]
