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

from cylc.flow.task_events_mgr import TaskJobLogsRetrieveContext
from cylc.flow.scheduler import Scheduler

from pathlib import Path
from typing import Any as Fixture


async def test_process_job_logs_retrieval_warns_no_platform(
    one_conf: Fixture, flow: Fixture, scheduler: Fixture, run: Fixture,
    db_select: Fixture, caplog: Fixture
):
    """Job log retrieval handles `NoHostsError`"""

    ctx = TaskJobLogsRetrieveContext(
        ctx_type='raa',
        platform_name='skarloey',
        max_size=256,
        key='skarloey'
    )
    reg: str = flow(one_conf)
    schd: 'Scheduler' = scheduler(reg, paused_start=True)
    # Run
    async with run(schd):
        schd.task_events_mgr._process_job_logs_retrieval(
            schd, ctx, 'foo'
        )
        warning = caplog.records[-1]
        assert warning.levelname == 'WARNING'
        assert 'Unable to retrieve' in warning.msg


async def test_process_message_no_repeat(
    one_conf: Fixture, flow: Fixture, scheduler: Fixture, run: Fixture
):
    """Don't log received messages if they are found again."""
    reg: str = flow(one_conf)
    schd: 'Scheduler' = scheduler(reg, paused_start=True)

    async with run(schd) as log:
        # Setup `job-activity.log` path:
        job_activity_log = (
            Path(schd.workflow_run_dir)
            / 'log/job/1/one/NN/job-activity.log'
        )
        job_activity_log.parent.mkdir(parents=True)

        args = {
            'itask': schd.pool.get_tasks()[0],
            'severity': 'comical',
            'message': 'The dead swans lay in the stagnant pool',
            'event_time': 'Thursday',
            'flag': '(received)',
            'submit_num': 0
        }
        # Process message should continue (i.e. check is True):
        assert schd.task_events_mgr._process_message_check(**args) is True
        # We have logged this message.
        assert schd.task_events_mgr.FLAG_RECEIVED in log.records[-1].message

        args = {
            'itask': schd.pool.get_tasks()[0],
            'severity': 'comical',
            'message': 'The dead swans lay in the stagnant pool',
            'event_time': 'Thursday',
            'flag': '(polled)',
            'submit_num': 0
        }
        # Process message should not continue - we've seen it before,
        # albeit with a different flag:
        assert schd.task_events_mgr._process_message_check(**args) is None
        # We haven't logged another message:
        assert schd.task_events_mgr.FLAG_RECEIVED in log.records[-1].message
