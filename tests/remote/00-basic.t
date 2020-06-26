#!/bin/bash
# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
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
#-------------------------------------------------------------------------------
# Test remote host settings.
. "$(dirname "$0")/test_header"
#-------------------------------------------------------------------------------
require_remote_platform
set_test_number 3
#-------------------------------------------------------------------------------
install_suite "${TEST_NAME_BASE}" basic
#-------------------------------------------------------------------------------
TEST_NAME="${TEST_NAME_BASE}-validate"
run_ok "${TEST_NAME}" cylc validate "${SUITE_NAME}"
#-------------------------------------------------------------------------------
TEST_NAME="${TEST_NAME_BASE}-run"
suite_run_ok "${TEST_NAME}" cylc run --reference-test --debug --no-detach "${SUITE_NAME}"
#-------------------------------------------------------------------------------
TEST_NAME=${TEST_NAME_BASE}-platform
sqlite3 "${SUITE_RUN_DIR}/log/db" \
    'select platform_name from task_jobs where name=="foo"' >'foo-host.txt'
cmp_ok 'foo-host.txt' <<<"${CYLC_REMOTE_PLATFORM}"
#-------------------------------------------------------------------------------
purge_suite_remote "${CYLC_REMOTE_PLATFORM}" "${SUITE_NAME}"
purge_suite "${SUITE_NAME}"
exit
