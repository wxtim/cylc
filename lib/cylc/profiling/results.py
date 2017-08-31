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

import os
import sqlite3

import cylc.profiling as prof


def get(conn, platforms=None, version_ids=None, experiment_ids=None,
        run_names=None, sorted=False):
    where = []
    args = []
    if platforms:
        if not isinstance(platforms, list):
            platforms = [platforms]
        where.append('platform IN (%s)' % ', '.join((['?'] * len(platforms))))
        args.extend(platforms)
    if version_ids:
        if not isinstance(version_ids, list):
            version_ids = [version_ids]
        where.append(
            'version_id IN (%s)' % ', '.join((['?'] * len(version_ids))))
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

    stmt = 'SELECT * from results'
    if where:
        stmt += ' WHERE %s' % ' AND '.join(where)
    cur = conn.cursor()
    cur.execute(stmt, args)
    results = cur.fetchall()
    results.sort()
    return results


def get_dict(*args, **kwargs):
    ret = []
    for result in get(*args, **kwargs):
        ret.append((result[0:4], dict((RESULT_FIELDS[ind], value) for
                                      ind, value in enumerate(result[4:]))))
    return ret


def get_keys(*args, **kwargs):
    return tuple(tuple(row[0:4]) for row in get(*args, **kwargs))


def get_conn():
    """Return database connection.

    Creates profiling directory and database file if not present.

    Returns:
        sqlite3.Connection

    """
    profile_dir = os.path.join(prof.CYLC_DIR, prof.PROFILE_DIR_NAME)
    if not os.path.exists(profile_dir):
        print 'Creating profiling directory.'
        os.mkdir(profile_dir)
        os.mkdir(os.path.join(profile_dir, prof.PROFILE_PLOT_DIR_NAME))
        os.mkdir(os.path.join(profile_dir, prof.USER_EXPERIMENT_DIR_NAME))

    profile_file = os.path.join(profile_dir, prof.PROFILE_DB)
    if not os.path.exists(profile_file):
        conn = sqlite3.connect(profile_file)
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
        conn = sqlite3.connect(profile_file)
    return conn


def add(conn, results):
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
