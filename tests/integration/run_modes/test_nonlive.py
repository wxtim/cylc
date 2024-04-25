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

# Define here to ensure test doesn't just mirror code:
from typing import Any, Dict


KGO = {
    'live': {
        'flow_nums': '[1]',
        'is_manual_submit': 0,
        'try_num': 1,
        'submit_status': None,
        'run_signal': None,
        'run_status': None,
        'platform_name': 'localhost',
        'job_runner_name': 'background',
        'job_id': None},
    'simulation': {
        'flow_nums': '[1]',
        'is_manual_submit': 0,
        'try_num': 1,
        'submit_status': 0,
        'run_signal': None,
        'run_status': None,
        'platform_name': 'simulation',
        'job_runner_name': 'simulation',
        'job_id': None},
    'skip': {
        'flow_nums': '[1]',
        'is_manual_submit': 0,
        'try_num': 1,
        'submit_status': 0,
        'run_signal': None,
        'run_status': 0,
        'platform_name': 'skip',
        'job_runner_name': 'simulation',
        'job_id': None},
    'dummy': {
        'flow_nums': '[1]',
        'is_manual_submit': 0,
        'try_num': 1,
        'submit_status': None,
        'run_signal': None,
        'run_status': None,
        'platform_name': 'localhost',
        'job_runner_name': 'background',
        'job_id': None},
}


def not_time(data: Dict[str, Any]):
    """Filter out fields containing times to reduce risk of
    flakiness"""
    return {k: v for k, v in data.items() if 'time' not in k}


async def test_task_jobs(flow, scheduler, start):
    """Ensure that task job data is added to the database correctly
    for each run mode.
    """
    schd = scheduler(flow({
        'scheduling': {'graph': {
            'R1': '&'.join(KGO)}},
        'runtime': {
            mode: {'run mode': mode} for mode in KGO}
    }))
    async with start(schd):
        schd.task_job_mgr.submit_task_jobs(
            schd.workflow,
            schd.pool.get_tasks(),
            schd.server.curve_auth,
            schd.server.client_pub_key_dir
        )
        schd.workflow_db_mgr.process_queued_ops()

        for mode, kgo in KGO.items():
            taskdata = not_time(
                schd.workflow_db_mgr.pub_dao.select_task_job(1, mode))
            assert taskdata == kgo, (
                f'Mode {mode}: incorrect db entries.')

        schd.pool.set_prereqs_and_outputs('*', ['failed'], [], [])

        schd.task_job_mgr.submit_task_jobs(
            schd.workflow,
            schd.pool.get_tasks(),
            schd.server.curve_auth,
            schd.server.client_pub_key_dir
        )
        schd.workflow_db_mgr.process_queued_ops()

        for mode, kgo in KGO.items():
            taskdata = not_time(
                schd.workflow_db_mgr.pub_dao.select_task_job(1, mode))
            assert taskdata == kgo, (
                f'Mode {mode}: incorrect db entries.')
