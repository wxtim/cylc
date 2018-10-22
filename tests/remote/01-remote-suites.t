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
# Test running of suites on set hosts, including remote hosts.
. $(dirname $0)/test_header
#-------------------------------------------------------------------------------
set_test_remote
REMOTE_HOST="$( \
    cylc get-global-config '--item=[test battery]remote host' 2>'/dev/null')"
if [[ -z "${REMOTE_HOST}" || "${REMOTE_HOST}" == 'localhost' ]]; then
    skip_all '"[test battery]remote host" not defined with remote suite hosts'
fi
set_test_number 25
#-------------------------------------------------------------------------------
# Validate generic suite to run for tests.
install_suite $TEST_NAME_BASE 01-remote-suites
TEST_NAME=$TEST_NAME_BASE-validate
run_ok $TEST_NAME cylc validate $SUITE_NAME
#-------------------------------------------------------------------------------
# Set-up for valid command-line specification formats and host types to test.

# Categories of host to set.
HOST_OPT_NONE=  # do not set host i.e. use default
HOST_OPT_LOCAL="localhost"
HOST_OPT_REMOTE="{REMOTE_HOST}"
HOST_OPT_INVALID="INVALID_HOST"

# Host setting option format: both equals and space positional arg delimiter.
HOST_OPT_START_EQUALS="--host="
HOST_OPT_START_SPACE="--host "

# Command formats varying by ordering of argument and option specification.
USE_RUN_CMD_ROOT="$TEST_NAME cylc run "
# ... For appended arguments and options, see USE_RUN_CMD in for loop below.
#-------------------------------------------------------------------------------
# Cover whole phase space for set-up options above to run a suite by set host.

LABEL=1
for USE_HOST_OPT_START in HOST_OPT_START_EQUALS HOST_OPT_START_SPACE
do
    for USE_HOST_OPT in \
    HOST_OPT_NONE HOST_OPT_LOCAL HOST_OPT_REMOTE HOST_OPT_INVALID
    do
        HOST_OPT=${USE_HOST_OPT_START}${USE_HOST_OPT}
        for USE_RUN_CMD in \
            "--reference-test --debug --no-detach ${HOST_OPT} ${SUITE_NAME}" \
            "${SUITE_NAME} --reference-test --debug --no-detach ${HOST_OPT}" \
            "--reference-test --debug ${SUITE_NAME} ${HOST_OPT} --no-detach"
        do
        if [[ "${USE_HOST_OPT}" == "HOST_OPT_INVALID" ]]
        then
            suite_run_fail "${TEST_NAME_BASE}-${LABEL}" \
                "${USE_RUN_CMD_ROOT}${USE_RUN_CMD}"
        else
            suite_run_ok "${TEST_NAME_BASE}-${LABEL}" \
                "${USE_RUN_CMD_ROOT}${USE_RUN_CMD}"
        fi
        LABEL=$(($LABEL+1))  # increment for distinct sub-test names.
        done
    done
done
#-------------------------------------------------------------------------------
# Clean up
purge_suite_remote "${CYLC_TEST_HOST}" "${SUITE_NAME}"
purge_suite "${SUITE_NAME}"
exit
