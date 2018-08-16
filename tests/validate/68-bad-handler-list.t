#!/bin/bash
# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2018 NIWA & British Crown (Met Office) & Contributors.
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
# Test validation fails on bad "handler events" value.
. "$(dirname "$0")/test_header"

set_test_number 4

TEST_NAME="${TEST_NAME_BASE}-ok"
cat >'suite.rc' <<'__SUITE_RC__'
[scheduling]
    [[dependencies]]
        graph=t1
[runtime]
    [[t1]]
        script=true
        [[[events]]]
            # all valid events
            handler events = submitted, submission failed, submission retry,\
            submission timeout, execution timeout, late, custom, warning,\
            critical, started, succeeded, failed, retry
__SUITE_RC__
run_ok "${TEST_NAME}" cylc validate 'suite.rc'

TEST_NAME="${TEST_NAME_BASE}-bad-value"
cat >'suite.rc' <<'__SUITE_RC__'
[scheduling]
    [[dependencies]]
        graph=t1
[runtime]
    [[t1]]
        script=true
        [[[events]]]
            # all valid events AND an invalid one (biscuit)
            handler events = submitted, submission failed, submission retry,\
            submission timeout, execution timeout, late, custom, warning,\
            critical, started, biscuit, succeeded, failed, retry
__SUITE_RC__
run_fail "${TEST_NAME}" cylc validate 'suite.rc'
cmp_ok "${TEST_NAME}.stderr" <<'__ERR__'
"ERROR: bad task event t1: biscuit"
__ERR__
