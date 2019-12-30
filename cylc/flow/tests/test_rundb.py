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
import sqlite3
import unittest

from unittest import mock
from tempfile import NamedTemporaryFile

from cylc.flow.rundb import *


class TestRunDb(unittest.TestCase):

    def setUp(self):
        self.mocked_connection = mock.Mock()
        self.mocked_connection_cmgr = mock.Mock()
        self.mocked_connection_cmgr.__enter__ = mock.Mock(return_value=(
            self.mocked_connection))
        self.mocked_connection_cmgr.__exit__ = mock.Mock(return_value=None)
        self.dao = CylcSuiteDAO('')
        self.dao.connect = mock.Mock()
        self.dao.connect.return_value = self.mocked_connection_cmgr

    get_select_task_job = [
        ["cycle", "name", "NN"],
        ["cycle", "name", None],
        ["cycle", "name", "02"],
    ]

    def test_select_task_job(self):
        """Test the rundb CylcSuiteDAO select_task_job method"""
        columns = list(task_jobs.columns)[3:]
        expected_values = [[2 for _ in columns]]

        mocked_execute = mock.Mock()
        mocked_execute.fetchall.return_value = expected_values
        self.mocked_connection.execute.return_value = mocked_execute

        # parameterized test
        for cycle, name, submit_num in self.get_select_task_job:
            returned_values = self.dao.select_task_job(cycle, name, submit_num)

            for index, column in enumerate(columns):
                row = returned_values[0]
                self.assertEqual(2, row[index])

    def test_select_task_job_sqlite_error(self):
        """Test that when the rundb CylcSuiteDAO select_task_job method raises
        a SQLite exception, the method returns None"""

        self.mocked_connection.execute.side_effect = sqlite3.DatabaseError

        r = self.dao.select_task_job("it'll", "raise", "an error!")
        self.assertIsNone(r)


def test_remove_columns():
    """Test workaround for dropping columns in sqlite3."""

    with NamedTemporaryFile() as nf:
        dao = CylcSuiteDAO(nf.name, is_public=False)
        dao.remove_columns('broadcast_states', ['namespace', 'value'])
        with dao.connect() as conn:
            data = conn.execute('SELECT * from broadcast_states').keys()
            assert data == ['point', 'key']


def test_upgrade_hold_swap():
    """Pre Cylc8 DB upgrade compatibility test."""
    # FIXME: see upgrade_hold comment, alembic?
    # # test data
    # initial_data = [
    #     # (name, cycle, status, hold_swap)
    #     ('foo', '1', 'waiting', ''),
    #     ('bar', '1', 'held', 'waiting'),
    #     ('baz', '1', 'held', 'running'),
    #     ('pub', '1', 'waiting', 'held')
    # ]
    # expected_data = [
    #     # (name, cycle, status, hold_swap, is_held)
    #     ('foo', '1', 'waiting', 0),
    #     ('bar', '1', 'waiting', 1),
    #     ('baz', '1', 'running', 1),
    #     ('pub', '1', 'waiting', 1)
    # ]
    # tables = [
    #     task_pool,
    #     task_pool_checkpoints
    # ]
    #
    # with create_temp_db() as (temp_db, conn):
    #     # initialise tables
    #     for table in tables:
    #         conn.execute(
    #             rf'''
    #                 CREATE TABLE {table} (
    #                     name varchar(255),
    #                     cycle varchar(255),
    #                     status varchar(255),
    #                     hold_swap varchar(255)
    #                 )
    #             '''
    #         )
    #
    #         conn.executemany(
    #             rf'''
    #                 INSERT INTO {table}
    #                 VALUES (?,?,?,?)
    #             ''',
    #             initial_data
    #         )
    #
    #     # close database
    #     conn.commit()
    #     conn.close()
    #
    #     # open database as cylc dao
    #     dao = CylcSuiteDAO(temp_db)
    #     conn = dao.connect()
    #
    #     # check the initial data was correctly inserted
    #     for table in tables:
    #         dump = [x for x in conn.execute(rf'SELECT * FROM {table}')]
    #         assert dump == initial_data
    #
    #     # upgrade
    #     assert dao.upgrade_is_held()
    #
    #     # check the data was correctly upgraded
    #     for _ in tables:
    #         dump = [x for x in conn.execute(rf'SELECT * FROM task_pool')]
    #         assert dump == expected_data
    #
    #     # make sure the upgrade is skipped on future runs
    #     assert not dao.upgrade_is_held()


if __name__ == '__main__':
    unittest.main()
