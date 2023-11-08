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
"""Test Skip Mode

Refers heavily to:
https://github.com/cylc/cylc-admin/blob/master/docs/proposal-skip-mode.md
"""

import re


def test_validate_skip_config(flow, validate, scheduler, start, caplog):
    """The "skip" mode configured by [runtime][<namespace>][skip]
    and [runtime][<namespace>]run mode.

    The valid configurations:
        - outputs
        - disable task event handlers

    If the run mode is set to simulation or skip in the workflow
    configuration, then cylc validate and cylc lint should produce
    a warning.
    """
    id_ = flow({
        'scheduling': {
            'graph': {
                'R1': 'foo & bar & baz'
            }
        },
        'runtime': {
            'foo': {
                'run mode': 'skip',
                'skip': {
                    'outputs': ['foo'],
                    'disable task event handlers': True
                }
            },
            'bar': {'run mode': 'skip'},
            'baz': {'run mode': 'simulation'},
        }
    })
    assert validate(id_)
    assert '\n * foo (skip)' in caplog.messages[0]
    assert '\n * bar (skip)' in caplog.messages[0]
    assert '\n * baz (simulation)' in caplog.messages[0]
