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

# Test that we can broadcast a skip to a task:

. "$(dirname "$0")/test_header"
set_test_number 4

install_workflow "${TEST_NAME_BASE}" "${TEST_NAME_BASE}"
run_ok "${TEST_NAME_BASE}-validate" cylc validate "${WORKFLOW_NAME}"
workflow_run_ok "${TEST_NAME_BASE}-run" \
    cylc play "${WORKFLOW_NAME}" --no-detach --debug

# It only has the job file (created before the broadcast kicks in):
FILES_IN_JOB_DIR=$(ls "${WORKFLOW_RUN_DIR}/log/job/23590101T0000Z/bar/01/" | wc -w)
run_ok "count-of-job-dir-files" test "$FILES_IN_JOB_DIR" == 1

# It starts and ends at about the same time:
SCHD_LOG="${WORKFLOW_RUN_DIR}/log/scheduler/log"
START=$(grep 'bar.*=> running' "${SCHD_LOG}" | awk '{print $1}')
FINISH=$(grep 'bar.*=> succeeded' "${SCHD_LOG}" | awk '{print $1}')
run_ok "task was skipped" test "$START" == "$FINISH"

purge
exit
