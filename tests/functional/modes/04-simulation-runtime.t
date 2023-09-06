#!/usr/bin/env bash
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

# Test that we can broadcast an alteration to simulation mode.

. "$(dirname "$0")/test_header"
set_test_number 3

install_workflow "${TEST_NAME_BASE}" "${TEST_NAME_BASE}"
run_ok "${TEST_NAME_BASE}-validate" cylc validate "${WORKFLOW_NAME}"
workflow_run_ok "${TEST_NAME_BASE}-run" \
    cylc play "${WORKFLOW_NAME}" --mode=simulation
SCHD_LOG="${WORKFLOW_RUN_DIR}/log/scheduler/log"

# If we speed up the simulated task we
# can make it finish before workflow timeout:
cylc broadcast "${WORKFLOW_NAME}" -s '[simulation]speedup factor = 600'

# Wait for the workflow to finish (it wasn't run in no-detach mode):
poll_grep "INFO - DONE" "${SCHD_LOG}"

# If we hadn't changed the speedup factor using broadcast
# The workflow timeout would have been hit:
grep_fail "WARNING - Orphaned tasks" "${SCHD_LOG}"

purge
exit
