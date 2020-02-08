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

# This file is used by Travis-CI to start the coverage process.
# In order to make Cylc and Python aware of it, we export PYTHONPATH when
# running the tests.

import coverage
coverage.process_startup()

import cylc.flow.terminal


original_cli_function = cylc.flow.terminal.cli_function

def mocked_cli_function(parser_function=None, **parser_kwargs):
    """Wrap the original cylc.flow.terminal.cli_function with a
    try finally, to save and stop the coverage process.

    See: https://github.com/cylc/cylc-flow/pull/3486
    """
    try:
        return original_cli_function(parser_function, **parser_kwargs)
    finally:
        if getattr(coverage.process_startup, 'coverage', False):
            coverage.process_startup.coverage.save()
            coverage.process_startup.coverage.stop()

cylc.flow.terminal.cli_function = mocked_cli_function
