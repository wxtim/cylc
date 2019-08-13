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

import unittest

from cylc.flow.task_state_prop import *


def get_test_extract_group_state_order():
    return [
        (
            [TaskStatus.SUBMIT_FAILED, TaskStatus.FAILED],
            False,
            TaskStatus.SUBMIT_FAILED
        ),
        (
            ["Who?", TaskStatus.FAILED],
            False,
            TaskStatus.FAILED
        ),
        (
            [TaskStatus.RETRYING, TaskStatus.RUNNING],
            False,
            TaskStatus.RETRYING
        ),
        (
            [TaskStatus.RETRYING, TaskStatus.RUNNING],
            True,
            TaskStatus.RUNNING
        ),
    ]


def get_test_get_status_prop():
    return [
        (
            TaskStatus.WAITING,
            "ascii_ctrl",
            "ace",
            "ace"
        ),
        (
            TaskStatus.WAITING,
            "ascii_ctrl",
            None,
            TaskStatus.WAITING.value
        )
    ]


class TestTaskStateProp(unittest.TestCase):

    def test_extract_group_state_childless(self):
        self.assertTrue(extract_group_state(child_states=[]) is None)

    def test_extract_group_state_order(self):
        params = get_test_extract_group_state_order()
        for child_states, is_stopped, expected in params:
            r = extract_group_state(child_states=child_states,
                                    is_stopped=is_stopped)
            self.assertEqual(expected, r)

    def test_get_status_prop(self):
        params = get_test_get_status_prop()
        for status, key, subst, expected in params:
            r = get_status_prop(status=status, key=key, subst=subst)
            self.assertTrue(expected in r)


if __name__ == '__main__':
    unittest.main()
