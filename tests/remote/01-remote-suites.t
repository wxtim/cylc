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
# Test remote host settings.
. $(dirname $0)/test_header
#-------------------------------------------------------------------------------
set_test_remote
set_test_number 1
#-------------------------------------------------------------------------------
install_suite $TEST_NAME_BASE 01-remote-suites
#-------------------------------------------------------------------------------
TEST_NAME=$TEST_NAME_BASE-validate
run_ok $TEST_NAME cylc validate $SUITE_NAME
#-------------------------------------------------------------------------------
# Set-up for valid command-line specification formats and host types to test.

# Command formats varying by ordering of argument and option specification.
RUN_CMD_OPTS_THEN_ARGS=$(suite_run_ok $TEST_NAME cylc run --reference-test --debug --no-detach $HOST_OPT $SUITE_NAME)
RUN_CMD_ARGS_THEN_OPTS=$(suite_run_ok $TEST_NAME cylc run $SUITE_NAME --reference-test --debug --no-detach $HOST_OPT)
RUN_CMD_MIXED_OPTS_ARGS=$(suite_run_ok $TEST_NAME cylc run --reference-test --debug $SUITE_NAME $HOST_OPT --no-detach)

# Categories of host to set for both equals and space positional arg delimiter.
HOST_OPT_NONE=
HOST_OPT_LOCAL="localhost"
HOST_OPT_REMOTE="{HOST}"
HOST_OPT_INVALID="INVALID_HOST"
#-------------------------------------------------------------------------------
# No host set.
TEST_NAME=$TEST_NAME_BASE-no-host-set
HOST_OPT="${HOST_OPT_NONE}"
$RUN_CMD_OPTS_THEN_ARGS  # doesn't matter which command format used here
#-------------------------------------------------------------------------------
# Opts then args: specifying a HOST in standard format of '--host=HOST'.
HOST_OPT_STARTER="--host="

TEST_NAME=$TEST_NAME_BASE-equals-localhost
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_LOCAL}"
$RUN_CMD_OPTS_THEN_ARGS

TEST_NAME=$TEST_NAME_BASE-equals-remote-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_REMOTE}"
$RUN_CMD_OPTS_THEN_ARGS

TEST_NAME=$TEST_NAME_BASE-equals-invalid-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_INVALID}"
$RUN_CMD_OPTS_THEN_ARGS
#-------------------------------------------------------------------------------
# Opts then args: Specifying HOST in alternative supported format '--host HOST'.
HOST_OPT_STARTER="--host "

TEST_NAME=$TEST_NAME_BASE-space-localhost
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_LOCAL}"
$RUN_CMD_OPTS_THEN_ARGS

TEST_NAME=$TEST_NAME_BASE-space-remote-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_REMOTE}"
$RUN_CMD_OPTS_THEN_ARGS

TEST_NAME=$TEST_NAME_BASE-space-invalid-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_INVALID}"
$RUN_CMD_OPTS_THEN_ARGS
#-------------------------------------------------------------------------------
# Args then opts: specifying a HOST in standard format of '--host=HOST'.
HOST_OPT_STARTER="--host="

TEST_NAME=$TEST_NAME_BASE-equals-localhost
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_LOCAL}"
$RUN_CMD_ARGS_THEN_OPTS

TEST_NAME=$TEST_NAME_BASE-equals-remote-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_REMOTE}"
$RUN_CMD_ARGS_THEN_OPTS

TEST_NAME=$TEST_NAME_BASE-equals-invalid-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_INVALID}"
$RUN_CMD_ARGS_THEN_OPTS
#-------------------------------------------------------------------------------
# Args then opts: Specifying HOST in alternative supported format '--host HOST'.
HOST_OPT_STARTER="--host "

TEST_NAME=$TEST_NAME_BASE-space-localhost
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_LOCAL}"
$RUN_CMD_ARGS_THEN_OPTS

TEST_NAME=$TEST_NAME_BASE-space-remote-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_REMOTE}"
$RUN_CMD_ARGS_THEN_OPTS

TEST_NAME=$TEST_NAME_BASE-space-invalid-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_INVALID}"
$RUN_CMD_ARGS_THEN_OPTS
#-------------------------------------------------------------------------------
# Mixed: specifying a HOST in standard format of '--host=HOST'.
HOST_OPT_STARTER="--host="

TEST_NAME=$TEST_NAME_BASE-equals-localhost
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_LOCAL}"
$RUN_CMD_MIXED_OPTS_ARGS

TEST_NAME=$TEST_NAME_BASE-equals-remote-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_REMOTE}"
$RUN_CMD_MIXED_OPTS_ARGS

TEST_NAME=$TEST_NAME_BASE-equals-invalid-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_INVALID}"
$RUN_CMD_MIXED_OPTS_ARGS
#-------------------------------------------------------------------------------
# Mixed: Specifying HOST in alternative supported format '--host HOST'.
HOST_OPT_STARTER="--host "

TEST_NAME=$TEST_NAME_BASE-space-localhost
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_LOCAL}"
$RUN_CMD_MIXED_OPTS_ARGS

TEST_NAME=$TEST_NAME_BASE-space-remote-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_REMOTE}"
$RUN_CMD_MIXED_OPTS_ARGS

TEST_NAME=$TEST_NAME_BASE-space-invalid-host
HOST_OPT="${HOST_OPT_STARTER}${HOST_OPT_INVALID}"
$RUN_CMD_MIXED_OPTS_ARGS
#-------------------------------------------------------------------------------
# Clean up
purge_suite_remote "${CYLC_TEST_HOST}" "${SUITE_NAME}"
purge_suite "${SUITE_NAME}"
exit
