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
"""Provide data access object for the suite runtime database."""

import sqlite3
import traceback

from sqlalchemy import Column, INTEGER, REAL, Table, TEXT, MetaData

from cylc.flow import LOG
import cylc.flow.flags
from cylc.flow.wallclock import get_current_time_string

meta = MetaData()


# --- tables

broadcast_events = Table(
    'broadcast_events', meta,
    Column('time', TEXT),
    Column('change', TEXT),
    Column('point', TEXT),
    Column('namespace', TEXT),
    Column('key', TEXT),
    Column('value', TEXT)
)

broadcast_states = Table(
    'broadcast_states', meta,
    Column('point', TEXT, primary_key=True),
    Column('namespace', TEXT, primary_key=True),
    Column('key', TEXT, primary_key=True),
    Column('value', TEXT)
)

broadcast_states_checkpoints = Table(
    'broadcast_states_checkpoints', meta,
    Column('id', INTEGER, primary_key=True),
    Column('point', TEXT, primary_key=True),
    Column('namespace', TEXT, primary_key=True),
    Column('key', TEXT, primary_key=True),
    Column('value', TEXT)
)

inheritance = Table(
    'inheritance', meta,
    Column('namespace', TEXT, primary_key=True),
    Column('inheritance', TEXT)
)

suite_params = Table(
    'suite_params', meta,
    Column('key', TEXT, primary_key=True),
    Column('value', TEXT)
)

suite_params_checkpoints = Table(
    'suite_params_checkpoints', meta,
    Column('id', INTEGER, primary_key=True),
    Column('key', TEXT, primary_key=True),
    Column('value', TEXT)
)

suite_template_vars = Table(
    'suite_template_vars', meta,
    Column('key', TEXT, primary_key=True),
    Column('value', TEXT)
)

task_jobs = Table(
    'task_jobs', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('submit_num', INTEGER, primary_key=True),
    Column('is_manual_submit', INTEGER),
    Column('try_num', INTEGER),
    Column('time_submit', TEXT),
    Column('time_submit_exit', TEXT),
    Column('submit_status', INTEGER),
    Column('time_run', TEXT),
    Column('time_run_exit', TEXT),
    Column('run_signal', TEXT),
    Column('run_status', INTEGER),
    Column('user_at_host', TEXT),
    Column('batch_sys_name', TEXT),
    Column('batch_sys_job_id', TEXT)
)

task_events = Table(
    'task_events', meta,
    Column('name', TEXT),
    Column('cycle', TEXT),
    Column('time', TEXT),
    Column('submit_num', INTEGER),
    Column('event', TEXT),
    Column('message', TEXT)
)

task_action_timers = Table(
    'task_action_timers', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('ctx_key', TEXT, primary_key=True),
    Column('ctx', TEXT),
    Column('delays', TEXT),
    Column('num', INTEGER),
    Column('delay', TEXT),
    Column('timeout', TEXT)
)

checkpoint_id = Table(
    'checkpoint_id', meta,
    Column('id', INTEGER, primary_key=True),
    Column('time', TEXT),
    Column('event', TEXT)
)

task_late_flags = Table(
    'task_late_flags', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('value', INTEGER)
)

task_outputs = Table(
    'task_outputs', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('outputs', INTEGER)
)

task_pool = Table(
    'task_pool', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('spawned', INTEGER),
    Column('status', TEXT),
    Column('is_held', INTEGER)
)

task_pool_checkpoints = Table(
    'task_pool_checkpoints', meta,
    Column('id', INTEGER, primary_key=True),
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('spawned', INTEGER),
    Column('status', TEXT),
    Column('is_held', INTEGER)
)

task_states = Table(
    'task_states', meta,
    Column('name', TEXT, primary_key=True),
    Column('cycle', TEXT, primary_key=True),
    Column('time_created', TEXT),
    Column('time_updated', TEXT),
    Column('submit_num', INTEGER),
    Column('status', TEXT)
)

task_timeout_timers = Table(
    'task_timeout_timers', meta,
    Column('cycle', TEXT, primary_key=True),
    Column('name', TEXT, primary_key=True),
    Column('timeout', REAL)
)

xtriggers = Table(
    'xtriggers', meta,
    Column('signature', TEXT, primary_key=True),
    Column('results', REAL)
)

# ---

class CylcSuiteDAO(object):
    """Data access object for the suite runtime database."""

    CONN_TIMEOUT = 0.2
    DB_FILE_BASE_NAME = "db"
    MAX_TRIES = 100
    CHECKPOINT_LATEST_ID = 0

    def __init__(self, db_file_name=None, is_public=False):
        """Initialise object.

        db_file_name - Path to the database file
        is_public - If True, allow retries, etc

        """
        self.db_file_name = db_file_name
        self.is_public = is_public
        self.conn = None
        self.n_tries = 0

        self.tables = {}
        for name, attrs in sorted(self.TABLES_ATTRS.items()):
            self.tables[name] = CylcSuiteDAOTable(name, attrs)

        if not self.is_public:
            self.create_tables()

    def add_delete_item(self, table_name, where_args=None):
        """Queue a DELETE item for a given table.

        where_args should be a dict, update will only apply to rows matching
        all these items.

        """
        self.tables[table_name].add_delete_item(where_args)

    def add_insert_item(self, table_name, args):
        """Queue an INSERT args for a given table.

        If args is a list, its length will be adjusted to be the same as the
        number of columns. If args is a dict, will return a list with the same
        length as the number of columns, the elements of which are determined
        by matching the column names with the keys in the dict.

        Empty elements are padded with None.

        """
        self.tables[table_name].add_insert_item(args)

    def add_update_item(self, table_name, set_args, where_args=None):
        """Queue an UPDATE item for a given table.

        set_args should be a dict, with column keys and values to be set.
        where_args should be a dict, update will only apply to rows matching
        all these items.

        """
        self.tables[table_name].add_update_item(set_args, where_args)

    def close(self):
        """Explicitly close the connection."""
        if self.conn is not None:
            try:
                self.conn.close()
            except sqlite3.Error:
                pass
            self.conn = None

    def connect(self):
        """Connect to the database."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_file_name, self.CONN_TIMEOUT)
        return self.conn

    def create_tables(self):
        """Create tables."""
        names = []
        for row in self.connect().execute(
                "SELECT name FROM sqlite_master WHERE type==? ORDER BY name",
                ["table"]):
            names.append(row[0])
        cur = None
        for name, table in self.tables.items():
            if name not in names:
                cur = self.conn.execute(table.get_create_stmt())
        if cur is not None:
            self.conn.commit()

    def execute_queued_items(self):
        """Execute queued items for each table."""
        try:
            for table in self.tables.values():
                # DELETE statements may have varying number of WHERE args so we
                # can only executemany for each identical template statement.
                for stmt, stmt_args_list in table.delete_queues.items():
                    self._execute_stmt(stmt, stmt_args_list)
                # INSERT statements are uniform for each table, so all INSERT
                # statements can be executed using a single "executemany" call.
                if table.insert_queue:
                    self._execute_stmt(
                        table.get_insert_stmt(), table.insert_queue)
                # UPDATE statements can have varying number of SET and WHERE
                # args so we can only executemany for each identical template
                # statement.
                for stmt, stmt_args_list in table.update_queues.items():
                    self._execute_stmt(stmt, stmt_args_list)
            # Connection should only be opened if we have executed something.
            if self.conn is None:
                return
            self.conn.commit()
        except sqlite3.Error:
            if not self.is_public:
                raise
            self.n_tries += 1
            LOG.warning(
                "%(file)s: write attempt (%(attempt)d) did not complete\n" % {
                    "file": self.db_file_name, "attempt": self.n_tries})
            if self.conn is not None:
                try:
                    self.conn.rollback()
                except sqlite3.Error:
                    pass
            return
        else:
            # Clear the queues
            for table in self.tables.values():
                table.delete_queues.clear()
                del table.insert_queue[:]  # list.clear avail from Python 3.3
                table.update_queues.clear()
            # Report public database retry recovery if necessary
            if self.n_tries:
                LOG.warning(
                    "%(file)s: recovered after (%(attempt)d) attempt(s)\n" % {
                        "file": self.db_file_name, "attempt": self.n_tries})
            self.n_tries = 0
        finally:
            # Note: This is not strictly necessary. However, if the suite run
            # directory is removed, a forced reconnection to the private
            # database will ensure that the suite dies.
            self.close()

    def _execute_stmt(self, stmt, stmt_args_list):
        """Helper for "self.execute_queued_items".

        Execute a statement. If this is the public database, return True on
        success and False on failure. If this is the private database, return
        True on success, and raise on failure.
        """
        try:
            self.connect()
            self.conn.executemany(stmt, stmt_args_list)
        except sqlite3.Error:
            if not self.is_public:
                raise
            if cylc.flow.flags.debug:
                traceback.print_exc()
            err_log = (
                "cannot execute database statement:\n"
                "file=%(file)s:\nstmt=%(stmt)s"
            ) % {"file": self.db_file_name, "stmt": stmt}
            for i, stmt_args in enumerate(stmt_args_list):
                err_log += ("\nstmt_args[%(i)d]=%(stmt_args)s" % {
                    "i": i, "stmt_args": stmt_args})
            LOG.warning(err_log)
            raise

    def pre_select_broadcast_states(self, id_key=None, order=None):
        """Query statement and args formation for select_broadcast_states."""
        form_stmt = r"SELECT point,namespace,key,value FROM %s"
        if order == "ASC":
            ordering = " ORDER BY point ASC, namespace ASC, key ASC"
            form_stmt = form_stmt + ordering
        if id_key is None or id_key == self.CHECKPOINT_LATEST_ID:
            return form_stmt % self.TABLE_BROADCAST_STATES, []
        else:
            return (form_stmt % self.TABLE_BROADCAST_STATES_CHECKPOINTS +
                    r" WHERE id==?"), [id_key]

    def select_broadcast_states(self, callback, id_key=None, sort=None):
        """Select from broadcast_states or broadcast_states_checkpoints.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [point, namespace, key, value]

        If id_key is specified,
        select from broadcast_states table if id_key == CHECKPOINT_LATEST_ID.
        Otherwise select from broadcast_states_checkpoints where id == id_key.
        """
        stmt, stmt_args = self.pre_select_broadcast_states(id_key=None,
                                                           order=sort)
        for row_idx, row in enumerate(self.connect().execute(stmt, stmt_args)):
            callback(row_idx, list(row))

    def pre_select_broadcast_events(self, order=None):
        """Query statement and args formation for select_broadcast_events."""
        form_stmt = r"SELECT time,change,point,namespace,key,value FROM %s"
        if order == "DESC":
            ordering = (" ORDER BY " +
                        "time DESC, point DESC, namespace DESC, key DESC")
            form_stmt = form_stmt + ordering
        return form_stmt % self.TABLE_BROADCAST_EVENTS, []

    def select_broadcast_events(self, callback, id_key=None, sort=None):
        """Select from broadcast_events.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [time, change, point, namespace, key, value]
        """
        stmt, stmt_args = self.pre_select_broadcast_events(order=sort)
        for row_idx, row in enumerate(self.connect().execute(stmt, stmt_args)):
            callback(row_idx, list(row))

    def select_checkpoint_id(self, callback, id_key=None):
        """Select from checkpoint_id.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [id, time, event]

        If id_key is specified, add where id == id_key to select.
        """
        stmt = r"SELECT id,time,event FROM checkpoint_id"
        stmt_args = []
        if id_key is not None:
            stmt += r" WHERE id==?"
            stmt_args.append(id_key)
        stmt += r"  ORDER BY time ASC"
        for row_idx, row in enumerate(self.connect().execute(stmt, stmt_args)):
            callback(row_idx, list(row))

    def select_checkpoint_id_restart_count(self):
        """Return number of restart event in checkpoint_id table."""
        stmt = r"SELECT COUNT(event) FROM checkpoint_id WHERE event==?"
        stmt_args = ['restart']
        for row in self.connect().execute(stmt, stmt_args):
            return row[0]
        return 0

    def select_suite_params(self, callback, id_key=None):
        """Select from suite_params or suite_params_checkpoints.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [key,value]

        If id_key is specified,
        select from suite_params table if id_key == CHECKPOINT_LATEST_ID.
        Otherwise select from suite_params_checkpoints where id == id_key.
        """
        form_stmt = r"SELECT key,value FROM %s"
        if id_key is None or id_key == self.CHECKPOINT_LATEST_ID:
            stmt = form_stmt % self.TABLE_SUITE_PARAMS
            stmt_args = []
        else:
            stmt = (form_stmt % self.TABLE_SUITE_PARAMS_CHECKPOINTS +
                    r" WHERE id==?")
            stmt_args = [id_key]
        for row_idx, row in enumerate(self.connect().execute(stmt, stmt_args)):
            callback(row_idx, list(row))

    def select_suite_template_vars(self, callback):
        """Select from suite_template_vars.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [key,value]
        """
        for row_idx, row in enumerate(self.connect().execute(
                r"SELECT key,value FROM %s" % self.TABLE_SUITE_TEMPLATE_VARS)):
            callback(row_idx, list(row))

    def select_table_schema(self, my_type, my_name):
        """Select from task_action_timers for restart.

        Invoke callback(row_idx, row) on each row.
        """
        for sql, in self.connect().execute(
                r"SELECT sql FROM sqlite_master where type==? and name==?",
                [my_type, my_name]):
            return sql

    def select_task_action_timers(self, callback):
        """Select from task_action_timers for restart.

        Invoke callback(row_idx, row) on each row.
        """
        attrs = []
        for item in self.TABLES_ATTRS[self.TABLE_TASK_ACTION_TIMERS]:
            attrs.append(item[0])
        stmt = r"SELECT %s FROM %s" % (
            ",".join(attrs), self.TABLE_TASK_ACTION_TIMERS)
        for row_idx, row in enumerate(self.connect().execute(stmt)):
            callback(row_idx, list(row))

    def select_task_job(self, cycle, name, submit_num=None):
        """Select items from task_jobs by (cycle, name, submit_num).

        :return: a dict for mapping keys to the column values
        :rtype: dict
        """
        keys = []
        for column in self.tables[self.TABLE_TASK_JOBS].columns[3:]:
            keys.append(column.name)
        if submit_num in [None, "NN"]:
            stmt = (r"SELECT %(keys_str)s FROM %(table)s"
                    r" WHERE cycle==? AND name==?"
                    r" ORDER BY submit_num DESC LIMIT 1") % {
                "keys_str": ",".join(keys),
                "table": self.TABLE_TASK_JOBS}
            stmt_args = [cycle, name]
        else:
            stmt = (r"SELECT %(keys_str)s FROM %(table)s"
                    r" WHERE cycle==? AND name==? AND submit_num==?") % {
                "keys_str": ",".join(keys),
                "table": self.TABLE_TASK_JOBS}
            stmt_args = [cycle, name, submit_num]
        try:
            for row in self.connect().execute(stmt, stmt_args):
                ret = {}
                for key, value in zip(keys, row):
                    ret[key] = value
                return ret
        except sqlite3.DatabaseError:
            return None

    def select_task_job_run_times(self, callback):
        """Select run times of succeeded task jobs grouped by task names.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [name, run_times_str]

        where run_times_str is a string containing comma separated list of
        integer run times. This method is used to re-populate elapsed run times
        of each task on restart.
        """
        stmt = (
            r"SELECT"
            r" name,"
            r" GROUP_CONCAT("
            r"     CAST(strftime('%s', time_run_exit) AS NUMERIC) -"
            r"     CAST(strftime('%s', time_run) AS NUMERIC))"
            r" FROM task_jobs"
            r" WHERE run_status==0 GROUP BY name ORDER BY time_run_exit")
        for row_idx, row in enumerate(self.connect().execute(stmt)):
            callback(row_idx, list(row))

    def select_submit_nums_for_insert(self, task_ids):
        """Select name,cycle,submit_num from task_states.

        Fetch submit numbers for tasks on insert.
        Return a data structure like this:

        {
            (name1, point1): submit_num,
            ...,
        }

        task_ids should be specified as [(name-glob, cycle), ...]

        Args:
            task_ids (list): A list of tuples, with the name-glob and cycle
                of a task.
        """
        # Ignore bandit false positive: B608: hardcoded_sql_expressions
        # Not an injection, simply putting the table name in the SQL query
        # expression as a string constant local to this module.
        stmt = (  # nosec
            r"SELECT name,cycle,submit_num FROM %(name)s"
            r" WHERE name==? AND cycle==?"
        ) % {"name": self.TABLE_TASK_STATES}
        ret = {}
        for task_name, task_cycle in task_ids:
            for name, cycle, submit_num in self.connect().execute(
                stmt, (task_name, task_cycle,)
            ):
                ret[(name, cycle)] = submit_num
        return ret

    def select_xtriggers_for_restart(self, callback):
        stm = r"SELECT signature,results FROM %s" % self.TABLE_XTRIGGERS
        for row_idx, row in enumerate(self.connect().execute(stm, [])):
            callback(row_idx, list(row))

    def select_task_pool(self, callback, id_key=None):
        """Select from task_pool or task_pool_checkpoints.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [cycle, name, spawned, status]

        If id_key is specified,
        select from task_pool table if id_key == CHECKPOINT_LATEST_ID.
        Otherwise select from task_pool_checkpoints where id == id_key.
        """
        form_stmt = r"SELECT cycle,name,spawned,status,is_held FROM %s"
        if id_key is None or id_key == self.CHECKPOINT_LATEST_ID:
            stmt = form_stmt % self.TABLE_TASK_POOL
            stmt_args = []
        else:
            stmt = (
                form_stmt % self.TABLE_TASK_POOL_CHECKPOINTS + r" WHERE id==?")
            stmt_args = [id_key]
        for row_idx, row in enumerate(self.connect().execute(stmt, stmt_args)):
            callback(row_idx, list(row))

    def select_task_pool_for_restart(self, callback, id_key=None):
        """Select from task_pool+task_states+task_jobs for restart.

        Invoke callback(row_idx, row) on each row, where each row contains:
            [cycle, name, spawned, is_late, status, is_held, submit_num,
             try_num, user_at_host, time_submit, time_run, timeout, outputs]

        If id_key is specified,
        select from task_pool table if id_key == CHECKPOINT_LATEST_ID.
        Otherwise select from task_pool_checkpoints where id == id_key.
        """
        form_stmt = r"""
            SELECT
                %(task_pool)s.cycle,
                %(task_pool)s.name,
                %(task_pool)s.spawned,
                %(task_late_flags)s.value,
                %(task_pool)s.status,
                %(task_pool)s.is_held,
                %(task_states)s.submit_num,
                %(task_jobs)s.try_num,
                %(task_jobs)s.user_at_host,
                %(task_jobs)s.time_submit,
                %(task_jobs)s.time_run,
                %(task_timeout_timers)s.timeout,
                %(task_outputs)s.outputs
            FROM
                %(task_pool)s
            JOIN
                %(task_states)s
            ON  %(task_pool)s.cycle == %(task_states)s.cycle AND
                %(task_pool)s.name == %(task_states)s.name
            LEFT OUTER JOIN
                %(task_late_flags)s
            ON  %(task_pool)s.cycle == %(task_late_flags)s.cycle AND
                %(task_pool)s.name == %(task_late_flags)s.name
            LEFT OUTER JOIN
                %(task_jobs)s
            ON  %(task_pool)s.cycle == %(task_jobs)s.cycle AND
                %(task_pool)s.name == %(task_jobs)s.name AND
                %(task_states)s.submit_num == %(task_jobs)s.submit_num
            LEFT OUTER JOIN
                %(task_timeout_timers)s
            ON  %(task_pool)s.cycle == %(task_timeout_timers)s.cycle AND
                %(task_pool)s.name == %(task_timeout_timers)s.name
            LEFT OUTER JOIN
                %(task_outputs)s
            ON  %(task_pool)s.cycle == %(task_outputs)s.cycle AND
                %(task_pool)s.name == %(task_outputs)s.name
        """
        form_data = {
            "task_pool": self.TABLE_TASK_POOL,
            "task_states": self.TABLE_TASK_STATES,
            "task_late_flags": self.TABLE_TASK_LATE_FLAGS,
            "task_timeout_timers": self.TABLE_TASK_TIMEOUT_TIMERS,
            "task_jobs": self.TABLE_TASK_JOBS,
            "task_outputs": self.TABLE_TASK_OUTPUTS,
        }
        if id_key is None or id_key == self.CHECKPOINT_LATEST_ID:
            stmt = form_stmt % form_data
            stmt_args = []
        else:
            form_data["task_pool"] = self.TABLE_TASK_POOL_CHECKPOINTS
            stmt = (form_stmt + r" WHERE %(task_pool)s.id==?") % form_data
            stmt_args = [id_key]
        for row_idx, row in enumerate(self.connect().execute(stmt, stmt_args)):
            callback(row_idx, list(row))

    def select_task_times(self):
        """Select submit/start/stop times to compute job timings.

        To make data interpretation easier, choose the most recent succeeded
        task to sample timings from.
        """
        q = """
            SELECT
                name,
                cycle,
                user_at_host,
                batch_sys_name,
                time_submit,
                time_run,
                time_run_exit
            FROM
                %(task_jobs)s
            WHERE
                run_status = %(succeeded)d
        """ % {
            'task_jobs': self.TABLE_TASK_JOBS,
            'succeeded': 0,
        }
        columns = (
            'name', 'cycle', 'host', 'batch_system',
            'submit_time', 'start_time', 'succeed_time'
        )
        return columns, [r for r in self.connect().execute(q)]

    def take_checkpoints(self, event, other_daos=None):
        """Add insert items to *_checkpoints tables.

        Select items in suite_params, broadcast_states and task_pool and
        prepare them for insert into the relevant *_checkpoints tables, and
        prepare an insert into the checkpoint_id table the event and the
        current time.

        If other_daos is a specified, it should be a list of CylcSuiteDAO
        objects.  The logic will prepare insertion of the same items into the
        *_checkpoints tables of these DAOs as well.
        """
        id_ = 1
        for max_id, in self.connect().execute(
                "SELECT MAX(id) FROM checkpoint_id"):
            if max_id is not None and max_id >= id_:
                id_ = max_id + 1
        daos = [self]
        if other_daos:
            daos.extend(other_daos)
        for dao in daos:
            dao.tables[self.TABLE_CHECKPOINT_ID].add_insert_item([
                id_, get_current_time_string(), event])
        for table_name in [
                self.TABLE_SUITE_PARAMS,
                self.TABLE_BROADCAST_STATES,
                self.TABLE_TASK_POOL]:
            for row in self.connect().execute("SELECT * FROM %s" % table_name):
                for dao in daos:
                    dao.tables[table_name + "_checkpoints"].add_insert_item(
                        [id_] + list(row))

    def vacuum(self):
        """Vacuum to the database."""
        return self.connect().execute("VACUUM")

    def remove_columns(self, table, to_drop):
        conn = self.connect()

        # get list of columns to keep
        schema = conn.execute(
            rf'''
                PRAGMA table_info({table})
            '''
        )
        new_cols = [
            name
            for _, name, *_ in schema
            if name not in to_drop
        ]

        # copy table
        conn.execute(
            rf'''
                CREATE TABLE {table}_new AS
                SELECT {', '.join(new_cols)}
                FROM {table}
            '''
        )

        # remove original
        conn.execute(
            rf'''
                DROP TABLE {table}
            '''
        )

        # copy table
        conn.execute(
            rf'''
                CREATE TABLE {table} AS
                SELECT {', '.join(new_cols)}
                FROM {table}_new
            '''
        )

        # done
        conn.commit()

    def upgrade_is_held(self):
        """Upgrade hold_swap => is_held.

        * Add a is_held column.
        * Set status and is_held as per the new schema.
        * Set the swap_hold values to None
          (bacause sqlite3 does not support DROP COLUMN)

        From:
            cylc<8
        To:
            cylc>=8
        PR:
            #3230

        Returns:
            bool - True if upgrade performed, False if upgrade skipped.

        """
        conn = self.connect()

        # check if upgrade required
        schema = conn.execute(rf'PRAGMA table_info({self.TABLE_TASK_POOL})')
        for _, name, *_ in schema:
            if name == 'is_held':
                LOG.debug('is_held column present - skipping db upgrade')
                return False

        # perform upgrade
        for table in [self.TABLE_TASK_POOL, self.TABLE_TASK_POOL_CHECKPOINTS]:
            LOG.info('Upgrade hold_swap => is_held in %s', table)
            conn.execute(
                rf'''
                    ALTER TABLE
                        {table}
                    ADD COLUMN
                        is_held BOOL
                '''
            )
            for cycle, name, status, hold_swap in conn.execute(rf'''
                    SELECT
                        cycle, name, status, hold_swap
                    FROM
                        {table}
            '''):
                if status == 'held':
                    new_status = hold_swap
                    is_held = True
                elif hold_swap == 'held':
                    new_status = status
                    is_held = True
                else:
                    new_status = status
                    is_held = False
                conn.execute(
                    rf'''
                        UPDATE
                            {table}
                        SET
                            status=?,
                            is_held=?,
                            hold_swap=?
                        WHERE
                            cycle==?
                            AND name==?
                    ''',
                    (new_status, is_held, None, cycle, name)
                )
            self.remove_columns(table, ['hold_swap'])
            conn.commit()
        return True
