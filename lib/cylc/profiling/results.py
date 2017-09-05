# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2017 NIWA
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

# NOTE: All fields are of type 'FLOAT' as they may be averaged values.
# WARNING: These fields are used to draw up the database schema.
RESULT_FIELDS = [
    'elapsed_time', 'cpu_time', 'user_time', 'system_time', 'rss_memory',
    'file_ins', 'file_outs', 'startup_time', 'main_loop_iterations',
    'average_main_loop_iteration_time', 'elapsed_non_sleep_time'
]  # TODO: New standadrised metric system...

import json
import os
import sqlite3
import tempfile
import unittest

import cylc.profiling as prof
import cylc.profiling.git as git  # TODO: Rename utills?


def _results_sorter(one, two):
    """Comparison function for sorting result keys."""
    # Sort by platform.
    ret = cmp(one[0], two[0])
    if ret:
        return ret
    # Sort by version commit date.
    try:
        ret = cmp(git.get_commit_date(two[1]), git.get_commit_date(one[1]))
    except KeyError:
        # Error retrieving commit date - skip.
        pass
    else:
        if ret:
            return ret
    # Sort by experiment id.
    ret = cmp(one[2], two[2])
    if ret:
        return ret
    # Sort by run name.
    try:
        # Attempt to parse as integer.
        return cmp(int(one[3]), int(two[3]))
    except ValueError:
        return cmp(one[3], two[3])


def _sync_experiments(conn, experiments=None, experiment_ids=None):
    """Sync experiments table with results table.

    One of experiments or experiment_ids must be provided. These lists should
    contain only the details of experiments which may have changed (i.e. with
    the addition / removal of results).

    Note:
        New experiments can be entered into the table only if they are provided
        via the experiments keyword.

    Args:
        conn (sqlite3.Connection): Results database connection.
        experiments (list): List of experiment dictionaries.
        experiment_ids (list): List of experiment_id strings.

    """
    if not experiment_ids:
        experiment_ids = [experiment['id'] for experiment in experiments]

    stmt = 'SELECT experiment_id FROM results WHERE experiment_id IN (%s)' % (
        ', '.join(['?'] * len(experiment_ids)))
    curr = conn.cursor()
    curr.execute(stmt, experiment_ids)
    exps_in_results = set(x[0] for x in curr.fetchall())

    stmt = ('SELECT experiment_id FROM experiments WHERE experiment_id IN '
            '(%s)' % (', '.join(['?'] * len(experiment_ids))))
    curr = conn.cursor()
    curr.execute(stmt, experiment_ids)
    exps_in_experiments = set(x[0] for x in curr.fetchall())

    if experiments:
        for experiment_id in exps_in_results - exps_in_experiments:
            # Experiment present in the results table but not in the experiments
            # table. Add an entry for it.
            stmt = 'INSERT INTO experiments VALUES(%s)' % (', '.join(['?'] * 3))
            experiment = prof.get_dict_by_attr(experiments, experiment_id)
            options = _get_experiment_options_from_file(experiment)
            args = (experiment_id, experiment['name'], json.dumps(options))
            with conn:
                conn.execute(stmt, args)

    for experiment_id in exps_in_experiments - exps_in_results:
        # Experiment present in the experiments table but not used in the
        # results tabke. Remove this (unused) entry.
        stmt = 'DELETE FROM experiments WHERE experiment_id = (?)'
        with conn:
            conn.execute(stmt, [experiment_id])


def _results_call(conn, platforms=None, version_ids=None, experiment_ids=None,
                  run_names=None, operator='SELECT', sort=False):
    """Execute a call to the results table.

    platforms, version_ids, experiment_ids, run_names are used as WHERE
    parameters when provided.

    Supported SQL Operators:
        SELECT:
            Return a list of db entry (tuples) matching the query parameters.
        DELETE:
            Return None, remove entries matching the seqrch query parameters.

    Args:
        conn (sqlite3.Connection): Results database connection.
        platforms (list): List of platform names (str).
        version_ids (list): List of version id strings (str).
        experiment_ids (list): List of experiment ids (str).
        run_names (list): List of run names (str).
        operator (str): The operation to perform (i.e. 'SELECT' or 'DELETE').
        sort (bool): If True, when operator=='SELECT' results will be sorted
            by result key.

    Returns:
        list / None: List if operator='SELECT' else None.

    """
    where = []
    args = []
    if platforms:
        if not isinstance(platforms, list):
            platforms = [platforms]
        where.append('platform IN (%s)' % (', '.join(['?'] * len(platforms))))
        args.extend(platforms)
    if version_ids:
        if not isinstance(version_ids, list):
            version_ids = [version_ids]
        where.append(
            'version_id IN (%s)' % (', '.join(['?'] * len(version_ids))))
        args.extend(version_ids)
    if experiment_ids:
        if not isinstance(experiment_ids, list):
            experiment_ids = [experiment_ids]
        where.append(
            'experiment_id IN (%s)' % (', '.join(['?'] * len(experiment_ids))))
        args.extend(experiment_ids)
    if run_names:
        if not isinstance(run_names, list):
            run_names = [run_names]
        where.append(
            'run_name IN (%s)' % (', '.join(['?'] * len(run_names))))
        args.extend(run_names)

    if operator == 'SELECT':
        stmt = 'SELECT * from results'
    elif operator == 'DELETE':
        stmt = 'DELETE from results'
    else:
        raise Exception('Unsupported results operator "%s"' % operator)
    if where:
        stmt += ' WHERE %s' % ' AND '.join(where)

    if operator == 'SELECT':
        curr = conn.cursor()
        curr.execute(stmt, args)
        if sort:
            return sorted(curr.fetchall(), _results_sorter)
        else:
            return curr.fetchall()
    else:
        with conn:
            conn.cursor().execute(stmt, args)


def get(*args, **kwargs):
    """Return a list of results matching the provided parameters.

    For arguments see _results_call().

    """
    return _results_call(*args, **kwargs)


def get_dict(*args, **kwargs):
    """Return a list of the form [keys, results].

    For arguments see _results_call().

    Return:
        list: [keys, results]
            - keys: Tuple of the form (platform, version_id, experiment_id,
              run_name).
            - results: Dictionary of metric name against value.

    """
    ret = []
    for result in get(*args, **kwargs):
        ret.append((result[0:4], dict((RESULT_FIELDS[ind], value) for
                                      ind, value in enumerate(result[4:]))))
    return ret


def get_keys(*args, **kwargs):
    """Return a list of experiment keys (as tuples)

    For arguments see _results_call().

    Return:
        list: [(platform, version_id, experiment_id, run_name), ...]

    """
    return tuple(tuple(row[0:4]) for row in get(*args, **kwargs))


def get_experiment_ids(conn, experiment_names):
    """Obtain experiment ids from experiment name.

    Return:
        dict: {experiment_name: experiment_ids}

    """
    ret = conn.cursor().execute(
        'SELECT name, experiment_id FROM experiments WHERE name IN '
        '(%s)' % (', '.join(['?'] * len(experiment_names))),
        experiment_names).fetchall()
    return dict((exp_name,
                 [exp_id for name, exp_id in ret if name == exp_name]) for
                exp_name in set(x[0] for x in ret))


def get_experiment_names(conn, experiment_ids):
    """Obtain experiment names from experiment ids.

    Return:
        dict: {experiment_id: experiment_name}

    """
    ret = conn.cursor().execute(
        'SELECT experiment_id, name FROM experiments WHERE experiment_id IN '
        '(%s)' % (', '.join(['?'] * len(experiment_ids))),
        experiment_ids).fetchall()
    return dict((x, y) for x, y in ret)


def _get_experiment_options_from_file(experiment):
    """Extract options from an experiment file.

    Args:
        experiment (dict): Experiment dictionary.

    Returns:
        dict: {'option': 'value'}

    """
    options = {}
    for key in prof.EXPERIMENT_OPTIONS:
        if key in experiment['config']:
            options[key] = experiment['config'][key]
    return options


def get_experiment_options_from_db(conn, experiment_id):
    """Return options for a particular experiment.

    Raises:
        IndexError: If experiment_id is not in the database.

    Returns:
        dict: {'option': 'value'}

    """
    return json.loads(conn.cursor().execute(
        'SELECT options FROM experiments WHERE experiment_id = (?)',
        [experiment_id]).fetchone()[0])


def get_conn(db_file):
    """Return database connection.

    Creates profiling directory and database file if not present.

    Returns:
        sqlite3.Connection

    """
    if not os.path.exists(db_file):
        conn = sqlite3.connect(db_file)
        print 'Creating results database.'
        conn.cursor().execute(
            'CREATE TABLE experiments('
            'experiment_id TEXT PRIMARY KEY, '
            'name TEXT, '
            'options TEXT)')
        conn.cursor().execute(
            'CREATE TABLE results('
            'platform TEXT, '
            'version_id TEXT, '
            'experiment_id TEXT, '
            'run_name TEXT, '
            '%s)' % (', '.join('%s FLOAT' % i for i in RESULT_FIELDS)))
        conn.commit()
    else:
        conn = sqlite3.connect(db_file)
    return conn


def add(conn, results, experiments):
    """Insert provided results into the results table.

    Ensure the experiments table is synconised to reflect this.

    Args:
        results (tuple): Tuple of result keys and a results dictionary.
        experiments (list): List of experiment dictionaries for any experiments
            present in results (required to syncronise the experiments table.

    """
    args = []
    for platform, version_id, experiment_id, run_name, result_dict in results:
        exp_results = (result_dict.get(metric, None) for metric in
                       RESULT_FIELDS)
        args.append((platform, version_id, experiment_id, run_name) +
                    tuple(exp_results))
    stmt = 'INSERT INTO results VALUES(%s)' % ', '.join(
        (4 + len(prof.METRICS)) * ['?'])
    with conn:  # Commits automatically if successfull.
        conn.cursor().executemany(stmt, args)

    used_experiments = [prof.get_dict_by_attr(experiments, experiment_id) for
                        experiment_id in set(result[2] for result in results)]
    _sync_experiments(conn, experiments=experiments)


def remove(*args, **kwargs):
    """Delete results matching the provided query.

    Ensures the experiments table is syncronised with this change.

    For arguments see _results_call().

    """
    kwargs['operator'] = 'SELECT'
    changes = get_keys(*args, **kwargs)

    kwargs['operator'] = 'DELETE'
    _results_call(*args, **kwargs)

    _sync_experiments(args[0], experiment_ids=[r[2] for r in changes])


def update_experiment(conn, experiment_id, experiment):
    """Update results for an experment to relfect an updated experiment dict.

    Args:
        experiment_id (str): The id of the experiement to update.
        experiment (dict): The new experiment dictionary to update the old
            experiment_id to.

    """
    new_results = [
        key[0:2] + (experiment['id'],) + key[3:4] + (result,) for
        key, result in get_dict(conn, experiment_ids=[experiment_id])
    ]
    remove(conn, experiment_ids=[experiment_id])
    add(conn, new_results, [experiment])


def tabulate(conn, platforms=None, version_ids=None, experiment_ids=None):
    """Print a list of results present in the DB.

    Args:
        Platforms (list): If provided results be be filtered to the provided
            platform names (str).
        version_ids (list): If provided versions will be filtered to the
            provied version ids (str).
        experiment_ids (list): If provided experiments will be filtered to the
            provided experiment ids (str).

    """
    # Get (platform, version_id, experiment_id) keys from the results DB.
    result_keys = set(x[0:3] for x in get_keys(
        conn,
        platforms or None,
        version_ids or None,
        experiment_ids or None))

    # Get dictionary of {experiment_id: experiment_name} pairs.
    exp_name_dict = get_experiment_names(
        conn,
        list(set([result[2] for result in result_keys])))

    # Get dictionary of experiment names vs the current experiment id.
    current_experiments = dict((exp_name, prof.get_experiment_id(exp_name)) for
                               exp_name in exp_name_dict.values())

    # Sort result keys.
    def sorty(one, two):
        """Sort function for user presentation or result entries."""
        return (
            # Experiment name.
            cmp(exp_name_dict[one[2]], exp_name_dict[two[2]]) or
            # Experiment version.
            cmp(one[2], two[2]) or
            # Platform.
            cmp(one[0], two[0]) or
            # Version commit date.
            cmp(git.get_commit_date(two[1]), git.get_commit_date(one[1]))
        )

    # Make table from results.
    table = [['Experiment Name', 'Experiment ID', 'Platform', 'Version ID']]
    previous = None
    for platform, version_id, experiment_id in sorted(result_keys, sorty):
        # Get the experiment name.
        experiment_name = exp_name_dict[experiment_id]
        # Put an asterix infront of the current version.
        if current_experiments[experiment_name] == experiment_id:
            experiment_id = '* %s' % experiment_id
        else:
            experiment_id = '  %s' % experiment_id
        row = []
        if previous:
            # Treat the table as a tree, don't repeat entries vertically.
            flag = True
            for ind, token in enumerate((
                    experiment_name, experiment_id, platform, version_id)):
                if flag and previous[ind] == token:
                    row.append('')
                else:
                    flag = False
                    row.append(token)
        else:
            row = [experiment_name, experiment_id, platform, version_id]
        table.append(row)
        previous = (experiment_name, experiment_id, platform, version_id)

    # Print table to stdout.
    prof._write_table(table, headers=True)


class TestAddResult(unittest.TestCase):

    def setUp(self):
        self.conn = get_conn(tempfile.mktemp())
        add(self.conn,
            [
                ('a', 'b', 'c', 'd', {}),
                ('a', 'g', 'e', 'd', {}),
                ('f', 'b', 'e', 'd', {})
            ],
            [
                {'id': 'c', 'name': 'C', 'config': {}},
                {'id': 'e', 'name': 'E', 'config': {}}
            ]
        )

    def test_result_added(self):
        curr = self.conn.cursor()
        curr.execute('SELECT * FROM results')
        self.assertEqual(
            curr.fetchall(),
            [
                (u'a', u'b', u'c', u'd') + (None,) * len(RESULT_FIELDS),
                (u'a', u'g', u'e', u'd') + (None,) * len(RESULT_FIELDS),
                (u'f', u'b', u'e', u'd') + (None,) * len(RESULT_FIELDS)
            ]
        )

    def test_experiment_added(self):
        curr = self.conn.cursor()
        curr.execute('SELECT * FROM experiments')
        self.assertEqual(
            curr.fetchall(),
            [
                (u'c', u'C', '{}'),
                (u'e', u'E', '{}'),
            ]
        )

    def test_modified_experiment_result_added(self):
        add(self.conn,
            [
                ('f', 'g', 'h', 'd', {})
            ],
            [
                {'id': 'h', 'name': 'C', 'config': {}}
            ]
        )
        curr = self.conn.cursor()
        curr.execute('SELECT * FROM results')
        self.assertEqual(
            curr.fetchall(),
            [
                (u'a', u'b', u'c', u'd') + (None,) * len(RESULT_FIELDS),
                (u'a', u'g', u'e', u'd') + (None,) * len(RESULT_FIELDS),
                (u'f', u'b', u'e', u'd') + (None,) * len(RESULT_FIELDS),
                (u'f', u'g', u'h', u'd') + (None,) * len(RESULT_FIELDS)
            ]
        )

    def test_modified_experiment_experiment_added(self):
        add(self.conn,
            [
                ('f', 'g', 'h', 'd', {})
            ],
            [
                {'id': 'h', 'name': 'C', 'config': {}}
            ]
        )
        curr = self.conn.cursor()
        curr.execute('SELECT * FROM experiments')
        self.assertEqual(
            curr.fetchall(),
            [
                (u'c', u'C', '{}'),
                (u'e', u'E', '{}'),
                (u'h', u'C', '{}'),
            ]
        )


class TestRemoveResult(unittest.TestCase):

    def setUp(self):
        self.conn = get_conn(tempfile.mktemp())
        add(self.conn,
            [
                ('a', 'b', 'c', 'd', {}),
                ('a', 'b', 'e', 'd', {})
            ],
            [
                {'id': 'c', 'name': 'C', 'config': {}},
                {'id': 'e', 'name': 'E', 'config': {}}
            ]
        )
        remove(self.conn, experiment_ids=['e'])

    def test_result_removed(self):
        curr = self.conn.cursor()
        curr.execute('SELECT experiment_id FROM results')
        self.assertEqual(curr.fetchall(), [(u'c',)])

    def test_experiment_removed(self):
        curr = self.conn.cursor()
        curr.execute('SELECT experiment_id FROM experiments')
        self.assertEqual(curr.fetchall(), [(u'c',)])


class TestGetResult(unittest.TestCase):

    def setUp(self):
        self.conn = get_conn(tempfile.mktemp())
        add(self.conn,
            [
                ('a', 'b', 'c', 'd', {RESULT_FIELDS[0]: '1.0'}),
                ('a', 'g', 'e', 'd', {RESULT_FIELDS[0]: 2.0}),
                ('f', 'b', 'e', 'd', {RESULT_FIELDS[0]: 3.0})
            ],
            [
                {'id': 'c', 'name': 'C', 'config': {}},
                {'id': 'e', 'name': 'E', 'config': {}}
            ]
        )

    def test_get_result_keys(self):
        # Get all.
        self.assertEqual(
            get_keys(self.conn),
            (
                (u'a', u'b', u'c', u'd'),
                (u'a', u'g', u'e', u'd'),
                (u'f', u'b', u'e', u'd')
            )
        )
        # Get by platform
        self.assertEqual(
            get_keys(self.conn, platforms=['a']),
            (
                (u'a', u'b', u'c', u'd'),
                (u'a', u'g', u'e', u'd')
            )
        )
        self.assertEqual(
            get_keys(self.conn, platforms=['f']),
            (
                (u'f', u'b', u'e', u'd'),
            )
        )
        # Get by version.
        self.assertEqual(
            get_keys(self.conn, version_ids=['b']),
            (
                (u'a', u'b', u'c', u'd'),
                (u'f', u'b', u'e', u'd')
            )
        )
        self.assertEqual(
            get_keys(self.conn, version_ids=['g']),
            (
                (u'a', u'g', u'e', u'd'),
            )
        )
        # Get by experiment.
        self.assertEqual(
            get_keys(self.conn, experiment_ids=['c']),
            (
                (u'a', u'b', u'c', u'd'),
            )
        )
        self.assertEqual(
            get_keys(self.conn, experiment_ids=['e']),
            (
                (u'a', u'g', u'e', u'd'),
                (u'f', u'b', u'e', u'd')
            )
        )

    def test_get_result_values(self):
        temp = len(RESULT_FIELDS) - 1
        self.assertEqual(
            get(self.conn),
            [
                (u'a', u'b', u'c', u'd', 1.0) + (None,) * temp,
                (u'a', u'g', u'e', u'd', 2.0) + (None,) * temp,
                (u'f', u'b', u'e', u'd', 3.0) + (None,) * temp
            ]
        )
        result_dict = dict((key, None) for key in RESULT_FIELDS)
        result_dict[RESULT_FIELDS[0]] = 3.0
        self.assertEqual(
            get_dict(self.conn, platforms='f'),
            [
                ((u'f', u'b', u'e', u'd'), result_dict)
            ]
        )

class TestResultSorting(unittest.TestCase):

    def setUp(self):
        self.conn = get_conn(tempfile.mktemp())
        add(self.conn,
            [
                ('b', 'b', 'd', 'd', {}),
                ('b', 'b', 'e', '2', {}),
                ('b', 'b', 'e', '1', {}),
                ('b', 'b', 'e', '3', {}),
                ('b', 'b', 'c', 'd', {}),
                ('a', 'b', 'c', 'd', {}),
            ],
            [
                {'id': 'c', 'name': 'C', 'config': {}},
                {'id': 'd', 'name': 'C', 'config': {}},
                {'id': 'e', 'name': 'E', 'config': {}}
            ]
        )

    def test_sorting(self):
        self.assertEqual(
            get_keys(self.conn, sort=True),
            (
                (u'a', u'b', u'c', u'd'),
                (u'b', u'b', u'c', u'd'),
                (u'b', u'b', u'd', u'd'),
                (u'b', u'b', u'e', u'1'),
                (u'b', u'b', u'e', u'2'),
                (u'b', u'b', u'e', u'3')
            )
        )


class TestResultPromotion(unittest.TestCase):

    def setUp(self):
        self.conn = get_conn(tempfile.mktemp())
        add(self.conn,
            [
                ('a', 'b', 'c', 'd', {RESULT_FIELDS[0]: 1.0})
            ],
            [
                {'id': 'c', 'name': 'C', 'config': {
                    prof.EXPERIMENT_OPTIONS[0]: 1}}
            ]
        )

    def test_promotion(self):
        update_experiment(
            self.conn,
            'c',
            {'id': 'e', 'name': 'C', 'config': {prof.EXPERIMENT_OPTIONS[0]: 2}}
        )
        # Ensure results updated.
        self.assertEqual(
            get(self.conn, experiment_ids=['c']), []
        )
        self.assertEqual(
            get(self.conn, experiment_ids=['e']),
            [
                ('a', 'b', 'e', 'd', 1.0) + (None,) * (len(RESULT_FIELDS) - 1)
            ]
        )
        # Ensure experiment added
        self.assertEqual(
            get_experiment_options_from_db(self.conn, 'e'),
            {prof.EXPERIMENT_OPTIONS[0]: 2}
        )
        self.assertEqual(
            self.conn.cursor().execute(
                'SELECT * FROM experiments WHERE name = (?)', 'C').fetchall(),
                [(u'e', u'C', json.dumps({prof.EXPERIMENT_OPTIONS[0]: 2}))]
        )



if __name__ == '__main__':
    unittest.main()
