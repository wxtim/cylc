#!/bin/bash
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
#-------------------------------------------------------------------------------
# Test running of suites on set hosts, including remote hosts.
. "$(dirname "$0")/test_header"
#-------------------------------------------------------------------------------
set_test_remote
REMOTE_HOST="$( cylc get-global-config '--item=[test battery]remote host' \
    2>'/dev/null')"
if [[ -z "${REMOTE_HOST}" || "${REMOTE_HOST}" == 'localhost' ]]; then
    skip_all '"[test battery]remote host" not defined with remote suite hosts'
fi
set_test_number 25
#-------------------------------------------------------------------------------
# Set-up for valid command-line specification formats and host types to test.

# Categories of host to set.
HOST_OPT_LOCAL="localhost"
HOST_OPT_REMOTE="${REMOTE_HOST}"
HOST_OPT_INVALID="__invalidhost__"  # very unlikely as a host name...
# Don't set, use default (other tests check default amounts to correct host):
HOST_OPT_NONE=

# Host setting option format: both equals and space positional arg delimiter.
HOST_OPT_START_EQUALS="--host="
HOST_OPT_START_SPACE="--host "

# Cylc command to test with. Note "cylc restart" could be used instead (or too,
# but choose to test just one since a lot of suites are run per command).
USE_RUN_CMD_ROOT="cylc run "

#-------------------------------------------------------------------------------
# Cover whole phase space for set-up options above to run a suite by set host.

for USE_HOST_OPT_START in "${HOST_OPT_START_EQUALS}" "${HOST_OPT_START_SPACE}"
do
    for USE_HOST_OPT in "${HOST_OPT_NONE}" "${HOST_OPT_LOCAL}" \
        "${HOST_OPT_REMOTE}" "${HOST_OPT_INVALID}"
    do
        HOST_OPT="${USE_HOST_OPT_START}${USE_HOST_OPT}"
        # Command formats: test various ordering of arguments and options,
        # namely 1. arg then opt, 2. opt then arg, opt then arg then more opt:
        for USE_RUN_CMD in \
            "${SUITE_NAME} --reference-test --debug --no-detach ${HOST_OPT}" \
            "--reference-test --debug --no-detach ${HOST_OPT} ${SUITE_NAME}" \
            "--reference-test --debug ${SUITE_NAME} ${HOST_OPT} --no-detach"
        do
            TEST_IDENTIFIER="${USE_HOST_OPT_START}-${USE_HOST_OPT}"
            TEST_NAME="${TEST_NAME_BASE}-${TEST_IDENTIFIER}"
            # Also use the test name as the suite name, for traceability:
            install_suite "${TEST_NAME}" 01-remote-suites

            if [[ "${USE_HOST_OPT}" == "${HOST_OPT_INVALID}" ]]
            then  # invalid host so suite should fail.
                suite_run_fail "${TEST_NAME}" \
                    "${USE_RUN_CMD_ROOT}${USE_RUN_CMD}"
            else  # otherwise suite should run okay on correct host.
                suite_run_ok "${TEST_NAME}" "${USE_RUN_CMD_ROOT}${USE_RUN_CMD}"
                # Clean-up suite as move onto next (avoid many running at once)
                purge_suite_remote "${USE_HOST_OPT}" "${TEST_NAME}"
                purge_suite "${TEST_NAME}"
            fi
        done
    done
done

exit
