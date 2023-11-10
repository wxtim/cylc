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
"""Tests for utilities supporting simulation and skip modes
"""
import pytest
from pytest import param

from cylc.flow.cycling.integer import IntegerPoint
from cylc.flow.cycling.iso8601 import ISO8601Point
from cylc.flow.simulation import (
    build_dummy_script,
    disable_platforms,
    filter_skip_outputs,
    get_simulated_run_len,
    parse_fail_cycle_points,
    sim_task_failed,
)


@pytest.mark.parametrize(
    'execution_time_limit, speedup_factor, default_run_length',
    (
        param(None, None, 'PT1H', id='default-run-length'),
        param(None, 10, 'PT1H', id='speedup-factor-alone'),
        param('PT1H', None, 'PT1H', id='execution-time-limit-alone'),
        param('P1D', 24, 'PT1M', id='speed-up-and-execution-tl'),
        param(60 * 60 * 24, 24, 'PT1M', id='recalculation'),
        param(1, None, 3600, id='recalculation'),
    )
)
def test_get_simulated_run_len(
    execution_time_limit, speedup_factor, default_run_length
):
    """Test the logic of the presence or absence of config items.

    Avoid testing the correct workign of DurationParser.
    """
    rtc = {
        'execution time limit': execution_time_limit,
        'simulation': {
            'speedup factor': speedup_factor,
            'default run length': default_run_length,
        },
    }
    if isinstance(execution_time_limit, int):
        rtc['simulation']['simulated run length'] = 30
    assert get_simulated_run_len(rtc) == 3600


@pytest.mark.parametrize(
    'fail_one_time_only', (True, False)
)
def test_set_simulation_script(fail_one_time_only):
    rtc = {
        'outputs': {'foo': '1', 'bar': '2'},
        'simulation': {
            'fail try 1 only': fail_one_time_only,
            'fail cycle points': '1',
        }
    }
    result = build_dummy_script(rtc, 60)
    assert result.split('\n') == [
        'sleep 60',
        "cylc message '1'",
        "cylc message '2'",
        f"cylc__job__dummy_result {str(fail_one_time_only).lower()}"
        " 1 || exit 1"
    ]


@pytest.mark.parametrize(
    'rtc, expect', (
        ({'platform': 'skarloey'}, 'localhost'),
        ({'remote': {'host': 'rheneas'}}, 'localhost'),
        ({'job': {'batch system': 'loaf'}}, 'localhost'),
    )
)
def test_disable_platforms(rtc, expect):
    """A sampling of items FORBIDDEN_WITH_PLATFORMS are removed from a
    config passed to this method.
    """
    disable_platforms(rtc)
    assert rtc['platform'] == expect
    subdicts = [v for v in rtc.values() if isinstance(v, dict)]
    for subdict in subdicts:
        for k, val in subdict.items():
            if k != 'platform':
                assert val is None


def test_parse_fail_cycle_points(set_cycling_type):
    before = ['2', '4']
    set_cycling_type()
    assert parse_fail_cycle_points(before) == [
        IntegerPoint(i) for i in before
    ]


@pytest.mark.parametrize(
    'conf, point, try_, expect',
    (
        param(
            {'fail cycle points': [], 'fail try 1 only': True},
            ISO8601Point('1'),
            1,
            False,
            id='defaults'
        ),
        param(
            {'fail cycle points': None, 'fail try 1 only': True},
            ISO8601Point('1066'),
            1,
            True,
            id='fail-all'
        ),
        param(
            {
                'fail cycle points': [
                    ISO8601Point('1066'), ISO8601Point('1067')],
                'fail try 1 only': True
            },
            ISO8601Point('1067'),
            1,
            True,
            id='point-in-failCP'
        ),
        param(
            {
                'fail cycle points': [
                    ISO8601Point('1066'), ISO8601Point('1067')],
                'fail try 1 only': True
            },
            ISO8601Point('1000'),
            1,
            False,
            id='point-notin-failCP'
        ),
        param(
            {'fail cycle points': None, 'fail try 1 only': True},
            ISO8601Point('1066'),
            2,
            False,
            id='succeed-attempt2'
        ),
        param(
            {'fail cycle points': None, 'fail try 1 only': False},
            ISO8601Point('1066'),
            7,
            True,
            id='fail-attempt7'
        ),
    )
)
def test_sim_task_failed(
    conf, point, try_, expect, set_cycling_type
):
    set_cycling_type('iso8601')
    assert sim_task_failed(conf, point, try_) == expect


@pytest.mark.parametrize(
    'task_outputs, skip_outputs, expect',
    (
        param(
            {
                'succeeded': ('success', False),
                'failed': ('failed', False),
                'started': ('started', True),
                'custom': ('made to order', True),
                'custom2': ('custom2', False),
                'custom3': ('custom3', None),
            },
            [],
            {
                'started': 'started',
                'custom': 'made to order',
                'succeeded': 'success'
            },
            id="all-required-outputs+succeed"
        ),
        param(
            {
                'submitted': ('submitted', None),
                'started': ('started', None),
            },
            [],
            {
                'submitted': 'submitted',
                'started': 'started',
            },
            id="submitted-and-started-are-always-produced"
            # n.b. TaskOuputs.__init__ means that succeeded and
            # started are always in the task_outputs.
        ),
        param(
            {
                'submitted': ('submitted', None),
                'custom': ('custom', None),
                'started': ('started', None),
                'succeeded': ('succeeded', None),
            },
            ['custom'],
            {
                'submitted': 'submitted',
                'started': 'started',
                'custom': 'custom',
                'succeeded': 'succeeded',
            },
            id="outputs-&-no-success-or-failure"
            # n.b. TaskOuputs.__init__ means that succeeded are
            # always in the task_outputs.
        ),
    )
)
def test_filter_skip_outputs(task_outputs, skip_outputs, expect):
    """It produces a list of outputs to return in a sorted
    order.

    From: https://github.com/cylc/cylc-admin/blob/master/docs/
    proposal-skip-mode.md

    * By default, all required outputs will be generated plus
      succeeded if success is optional.
    * The outputs submitted and started are always produced
      and do not need to be defined in outputs.
    * If outputs is specified and does not include either succeeded
      or failed then succeeded will be produced.
    """
    assert filter_skip_outputs(task_outputs, skip_outputs) == expect
