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

"""Tests for the backend method of workflow_state"""


import pytest
from textwrap import dedent
from typing import TYPE_CHECKING

from cylc.flow.dbstatecheck import CylcWorkflowDBChecker as Checker


if TYPE_CHECKING:
    from cylc.flow.dbstatecheck import CylcWorkflowDBChecker


@pytest.fixture(scope='module')
async def checker(mod_flow, mod_scheduler, mod_run, mod_complete) -> 'CylcWorkflowDBChecker':
    """Make a real world database.

    We could just write the database manually but this is a better
    test of the overall working of the function under test.
    """
    wid = mod_flow({
        'scheduling': {
            'graph': {'P1Y': dedent('''
                good:succeeded
                bad:failed?
                output
            ''')},
            'initial cycle point': '1000',
            'final cycle point': '1001'
        },
        'runtime': {
            'bad': {'simulation': {'fail cycle points': '1000'}},
            'output': {'outputs': {'custom_output': 'I do not believe it'}}
        }
    })
    schd = mod_scheduler(wid, paused_start=False)
    async with mod_run(schd):
        await mod_complete(schd)
        yield schd


def test_basic(checker):
    """Pass no args, get unfiltered output"""
    with Checker(
        'somestring', 'utterbunkum',
        checker.workflow_db_mgr.pub_path
    ) as checker:
        result = checker.workflow_state_query()

    expect = [
        ['bad', '10000101T0000Z', 'failed', '[1]'],
        ['bad', '10010101T0000Z', 'succeeded', '[1]'],
        ['good', '10000101T0000Z', 'succeeded', '[1]'],
        ['good', '10010101T0000Z', 'succeeded', '[1]'],
        ['output', '10000101T0000Z', 'succeeded', '[1]'],
        ['output', '10010101T0000Z', 'succeeded', '[1]']
    ]
    assert result == expect
