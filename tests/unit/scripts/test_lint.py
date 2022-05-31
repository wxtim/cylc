#!/usr/bin/env python3
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
"""Tests `cylc lint` CLI Utility.
"""
from pathlib import Path
import pytest

from cylc.flow.scripts.lint import (
    CHECK78,
    check_cylc_file,
    get_cylc_files,
    parse_checks
)


TEST_FILE = """
[vizualization]
[cylc]
    include at start-up = foo
    exclude at start-up = bar
    log resolved dependencies = True
    required run mode = False
    health check interval = PT10M
    abort if any task fails = true
    suite definition directory = '/woo'
    disable automatic shutdown = false
    reference test = true
    spawn to max active cycle points = false
    [[simulation]]
        disable suite event handlers = true
    [[authentication]]


[scheduling]
    [[dependencies]]
        [[[R1]]]
            graph = foo


[runtime]
    [[MYFAM]]
        extra log files = True
        [[[remote]]]
            host = parasite
            suite definition directory = '/home/bar'
        [[[job]]]
            batch system = slurm
            shell = fish
        [[[events]]]
            mail retry delays = PT30S

"""


@pytest.fixture()
def create_testable_file(monkeypatch, capsys):
    monkeypatch.setattr(Path, 'read_text', lambda _: TEST_FILE)
    check_cylc_file(Path('x'), parse_checks())
    return capsys.readouterr()


@pytest.mark.parametrize(
    'number', range(len(CHECK78['7-to-8']))
)
def test_check_cylc_file(create_testable_file, number):
    try:
        assert f'[{number:03d}:7-to-8]' in create_testable_file.out
    except AssertionError as exc:
        raise AssertionError(
            f'missing error number {number:03d}:7-to-8 - '
            f'{[*CHECK78["7-to-8"].keys()][number]}'
        )


def test_get_cylc_files_get_all_rcs(tmp_path):
    """It returns all paths except `log/**`.
    """
    expect = [('etc', 'foo.rc'), ('bin', 'foo.rc'), ('an_other', 'foo.rc')]

    # Create a fake run directory, including the log file which should not
    # be searched:
    dirs = ['etc', 'bin', 'log', 'an_other']
    for path in dirs:
        thispath = tmp_path / path
        thispath.mkdir()
        (thispath / 'foo.rc').touch()

    # Run the test
    result = [(i.parent.name, i.name) for i in get_cylc_files(tmp_path)]
    assert result == expect
