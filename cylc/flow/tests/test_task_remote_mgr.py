# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2020 NIWA & British Crown (Met Office) & Contributors.
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

from os.path import basename
from tempfile import NamedTemporaryFile

import pytest
from pytest_mock import mocker

from cylc.flow.task_remote_mgr import TaskRemoteMgr
from cylc.flow.subprocpool import SubProcPool


@pytest.fixture
def task_remote_mgr(mocker: mocker):
    """Fixture to create a ``TaskRemoteMgr``.
    """
    proc_pool = SubProcPool()
    proc_pool.get_temporary_file = mocker.Mock(side_effect = NamedTemporaryFile)
    return TaskRemoteMgr(suite='suite', proc_pool=proc_pool)


remote_host_select_test_data = [
    ["localhost", "localhost"],
    [None, "localhost"],
    ["cylc.cylc.remote", "cylc.cylc.remote"]
]


@pytest.mark.remote
@pytest.mark.ssh
@pytest.mark.parametrize("host,expected", remote_host_select_test_data)
def test_remote_host_select(task_remote_mgr, host, expected):
    """Test the remote host selection method.

    Args:
        task_remote_mgr (TaskRemoteMgr):
            TaskRemoteMgr fixture object.
        host (str):
            The local or remote host.
        expected (str):
            The expected resolved host.
    """
    resolved_host = task_remote_mgr.remote_host_select(host)
    assert expected == resolved_host


@pytest.mark.remote
@pytest.mark.ssh
def test_remote_init(task_remote_mgr, mocker: mocker):
    """Test the remote host selection method.

    Args:
        task_remote_mgr (TaskRemoteMgr):
            TaskRemoteMgr fixture object.
    """
    task_remote_mgr.uuid_str = task_remote_mgr.suite
    with NamedTemporaryFile(delete=False) as tf:
        # mock workflow communication method, set to SSH
        mocked_glbl_cfg_function = mocker.patch('cylc.flow.task_remote_mgr.glbl_cfg')
        mocked_glbl_cfg = mocker.Mock()
        mocked_glbl_cfg_function.return_value = mocked_glbl_cfg
        mocked_glbl_cfg.get_host_item = mocker.Mock(return_value = 'ssh')


        # mock the returned list of workflow files
        workflow_files = [(tf.name, basename(tf.name))]
        mocked_function = mocker.Mock()
        mocked_function.return_value = workflow_files
        task_remote_mgr._remote_init_items = mocked_function

        # mock call used to check if workflow UUID file exists
        mocker.patch('os.path.exists').return_value = True

        res = task_remote_mgr.remote_init('localhost', 'root')
        task_remote_mgr.proc_pool.process()
        print(res)