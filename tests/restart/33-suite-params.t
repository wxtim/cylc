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
. "$(dirname "$0")/test_header"
set_test_number 7
init_suite "${TEST_NAME_BASE}" <<'__SUITE_RC__'
#!Jinja2
[cylc]
    UTC mode = True  # UTC_mode
[scheduling]
    initial cycle point = {{ icp }}  # start_point
    [[dependencies]]
        [[[P1D]]]
            graph = foo[-P1D] => foo
__SUITE_RC__

dt () {
    python -c "
from datetime import datetime, timedelta
print (datetime.utcnow() + timedelta(minutes=$1)).strftime('%Y%m%dT%H%M%SZ')
"
}

unixtime () {
    python -c "
import isodatetime.parsers
print isodatetime.parsers.TimePointParser().parse('$1').get(
    'seconds_since_unix_epoch')"
}

remove_last_updated () {
    while [[ $# > 0 ]]; do
        sed -i '/last_updated/d' "$1"; shift
    done
}

TEST_NAME="${TEST_NAME_BASE}-stop_mode=request"

# set test suite running
cylc run --debug --no-auto-shutdown --until=2010 --mode=dummy --hold \
    --hold-after=2006 -s 'icp=2000' "${SUITE_NAME}" 2>&1
    # can_auto_stop, final_point, run_mode, is_held, pool_hold_point,
    # template_vars
poll ! test -f "${SUITE_RUN_DIR}/.service/contact"

# modify run state
STOP_TIME="$(dt 60)"; echo "\$\$\$ $STOP_TIME" >&2
run_ok "${TEST_NAME}-set-stop-point" \
    cylc stop "${SUITE_NAME}" 2005  # final_point
run_ok "${TEST_NAME}-set-stop-time" \
    cylc stop "${SUITE_NAME}" -w "${STOP_TIME}"  # stop_clock_time
run_ok "${TEST_NAME}-set-stop-task" \
    cylc stop "${SUITE_NAME}" foo.20050101T0000  # stop_task

# reccord suite parameters
run_ok "${TEST_NAME}-dump1" cylc dump "${SUITE_NAME}" -g
contains_ok "${TEST_NAME}-dump1.stdout" <<__FILE__
UTC_mode=1
can_auto_stop=0
initial_point=20000101T0000Z
final_point=20050101T0000Z
is_held=1
pool_hold_point=2006
start_point=20000101T0000Z
stop_clock_time=$(unixtime "${STOP_TIME}")
stop_mode=None
stop_task=foo.20050101T0000Z
__FILE__
cylc stop "${SUITE_NAME}"
poll test -f "${SUITE_RUN_DIR}/.service/contact"

cylc restart "${SUITE_NAME}" --debug 2>&1
poll ! test -f "${SUITE_RUN_DIR}/.service/contact"
sleep 1
run_ok "${TEST_NAME}-dump2" cylc dump "${SUITE_NAME}" -g

remove_last_updated "${TEST_NAME}-dump1.stdout" "${TEST_NAME}-dump2.stdout"
cmp_ok "${TEST_NAME}-dump2.stdout" "${TEST_NAME}-dump1.stdout"

cylc stop --now --now "${SUITE_NAME}"
sleep 1
purge_suite "${SUITE_NAME}"

# TODO - remove template vars
