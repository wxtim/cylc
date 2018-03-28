#!/bin/bash
# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2018 NIWA
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
#-------------------------------------------------------------------------------
# Run doctests in the cylc nameless codebase.
#-------------------------------------------------------------------------------
. "$(dirname "$0")/test_header"
tests 2
#-------------------------------------------------------------------------------
TEST_KEY="${TEST_KEY_BASE}"
run_pass "${TEST_KEY}" python -m doctest "${ROSE_HOME}/lib/python/rose/bush.py"
sed -i /1034h/d "${TEST_KEY}.out"  # Remove some nasty unicode output.
file_cmp "${TEST_KEY}.out" "${TEST_KEY}.out" /dev/null
