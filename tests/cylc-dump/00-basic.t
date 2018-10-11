#!/bin/bash
# this file is part of the cylc suite engine.
# copyright (c) 2008-2018 niwa
#
# this program is free software: you can redistribute it and/or modify
# it under the terms of the gnu general public license as published by
# the free software foundation, either version 3 of the license, or
# (at your option) any later version.
#
# this program is distributed in the hope that it will be useful,
# but without any warranty; without even the implied warranty of
# merchantability or fitness for a particular purpose.  see the
# gnu general public license for more details.
#
# you should have received a copy of the gnu general public license
# along with this program.  if not, see <http://www.gnu.org/licenses/>.
#-------------------------------------------------------------------------------
. "$(dirname "$0")/test_header"
#-------------------------------------------------------------------------------
set_test_number 13
#-------------------------------------------------------------------------------
TEST_NAME="${TEST_NAME_BASE}"
init_suite "${TEST_NAME}" <<< '
[scheduling]
    initial cycle point = 1
    cycling mode = integer
    [[dependencies]]
        graph = foo[-P1] => foo
'
run_ok "${TEST_NAME}-suite-start" cylc run "${SUITE_NAME}" \
    --host=localhost --hold || exit
cylc suite-state "${SUITE_NAME}" --task='foo' --point=1 \
    --status='held' --interval=1 --max-polls=20 >&2

TEST_NAME="${TEST_NAME_BASE}-dump"
run_ok "${TEST_NAME}" cylc dump "${SUITE_NAME}"
sed -i '/\(last_updated\|uuid_str\)/d' "${TEST_NAME}.stdout"
cmp_ok "${TEST_NAME}.stdout" <<__HERE__
UTC_mode=False
can_auto_stop=True
cycle_point_format=None
cylc_version=$(cylc version)
final_point=None
initial_point=1
is_held=True
newest cycle point string=1
newest runahead cycle point string=None
ns_defn_order=['root', 'foo']
oldest cycle point string=1
pool_hold_point=None
reloading=False
run_mode=live
start_point=1
state totals={'held': 1}
states=['held']
status_string=held
stop_clock_time_string=None
stop_mode=None
stop_point=None
stop_task=None
suite_urls={'suite': '', 'foo': '', 'root': ''}
template_vars={}
time zone info={'hours': 1, 'string_basic': '+01', 'string_extended': '+01', 'minutes': 0}
foo, 1, held, unspawned
__HERE__

TEST_NAME="${TEST_NAME_BASE}-dump-tasks"
run_ok "${TEST_NAME}" cylc dump "${SUITE_NAME}" --tasks
sed -i '/\(last_updated\|uuid_str\)/d' "${TEST_NAME}.stdout"
cmp_ok "${TEST_NAME}.stdout" <<'__HERE__'
foo, 1, held, unspawned
__HERE__

TEST_NAME="${TEST_NAME_BASE}-dump-global"
run_ok "${TEST_NAME}" cylc dump "${SUITE_NAME}" --global
sed -i '/\(last_updated\|uuid_str\)/d' "${TEST_NAME}.stdout"
cmp_ok "${TEST_NAME}.stdout" <<__HERE__
UTC_mode=False
can_auto_stop=True
cycle_point_format=None
cylc_version=$(cylc version)
final_point=None
initial_point=1
is_held=True
newest cycle point string=1
newest runahead cycle point string=None
ns_defn_order=['root', 'foo']
oldest cycle point string=1
pool_hold_point=None
reloading=False
run_mode=live
start_point=1
state totals={'held': 1}
states=['held']
status_string=held
stop_clock_time_string=None
stop_mode=None
stop_point=None
stop_task=None
suite_urls={'suite': '', 'foo': '', 'root': ''}
template_vars={}
time zone info={'hours': 1, 'string_basic': '+01', 'string_extended': '+01', 'minutes': 0}
__HERE__

TEST_NAME="${TEST_NAME_BASE}-dump-json"
run_ok "${TEST_NAME}" cylc dump "${SUITE_NAME}" --json
sed -i '/\(last_updated\|uuid_str\)/d' "${TEST_NAME}.stdout"
cmp_ok "${TEST_NAME}.stdout" <<'__HERE__'
{
    "global": {
        "UTC_mode": false, 
        "can_auto_stop": true, 
        "cycle_point_format": null, 
        "cylc_version": "7.7.1-152-g242ad-dirty", 
        "final_point": null, 
        "initial_point": "1", 
        "is_held": true, 
        "newest cycle point string": "1", 
        "newest runahead cycle point string": null, 
        "ns_defn_order": [
            "root", 
            "foo"
        ], 
        "oldest cycle point string": "1", 
        "pool_hold_point": null, 
        "reloading": false, 
        "run_mode": "live", 
        "start_point": "1", 
        "state totals": {
            "held": 1
        }, 
        "states": [
            "held"
        ], 
        "status_string": "held", 
        "stop_clock_time_string": null, 
        "stop_mode": null, 
        "stop_point": null, 
        "stop_task": null, 
        "suite_urls": {
            "foo": "", 
            "root": "", 
            "suite": ""
        }, 
        "template_vars": {}, 
        "time zone info": {
            "hours": 1, 
            "minutes": 0, 
            "string_basic": "+01", 
            "string_extended": "+01"
        }, 
    }, 
    "tasks": {
        "foo.1": {
            "batch_sys_name": null, 
            "description": "", 
            "execution_time_limit": null, 
            "finished_time": null, 
            "finished_time_string": null, 
            "job_hosts": {}, 
            "label": "1", 
            "latest_message": "", 
            "logfiles": [], 
            "mean_elapsed_time": null, 
            "name": "foo", 
            "spawned": "False", 
            "started_time": null, 
            "started_time_string": null, 
            "state": "held", 
            "submit_method_id": null, 
            "submit_num": 0, 
            "submitted_time": null, 
            "submitted_time_string": null, 
            "title": ""
        }
    }
}
__HERE__

TEST_NAME="${TEST_NAME_BASE}-dump-json-tasks"
run_ok "${TEST_NAME}" cylc dump "${SUITE_NAME}" --json --tasks
sed -i '/\(last_updated\|uuid_str\)/d' "${TEST_NAME}.stdout"
cmp_ok "${TEST_NAME}.stdout" <<'__HERE__'
{
    "foo.1": {
        "batch_sys_name": null, 
        "description": "", 
        "execution_time_limit": null, 
        "finished_time": null, 
        "finished_time_string": null, 
        "job_hosts": {}, 
        "label": "1", 
        "latest_message": "", 
        "logfiles": [], 
        "mean_elapsed_time": null, 
        "name": "foo", 
        "spawned": "False", 
        "started_time": null, 
        "started_time_string": null, 
        "state": "held", 
        "submit_method_id": null, 
        "submit_num": 0, 
        "submitted_time": null, 
        "submitted_time_string": null, 
        "title": ""
    }
}
__HERE__

TEST_NAME="${TEST_NAME_BASE}-dump-json-global"
run_ok "${TEST_NAME}" cylc dump "${SUITE_NAME}" --json --global
sed -i '/\(last_updated\|uuid_str\)/d' "${TEST_NAME}.stdout"
cmp_ok "${TEST_NAME}.stdout" <<'__HERE__'
{
    "UTC_mode": false, 
    "can_auto_stop": true, 
    "cycle_point_format": null, 
    "cylc_version": "7.7.1-152-g242ad-dirty", 
    "final_point": null, 
    "initial_point": "1", 
    "is_held": true, 
    "newest cycle point string": "1", 
    "newest runahead cycle point string": null, 
    "ns_defn_order": [
        "root", 
        "foo"
    ], 
    "oldest cycle point string": "1", 
    "pool_hold_point": null, 
    "reloading": false, 
    "run_mode": "live", 
    "start_point": "1", 
    "state totals": {
        "held": 1
    }, 
    "states": [
        "held"
    ], 
    "status_string": "held", 
    "stop_clock_time_string": null, 
    "stop_mode": null, 
    "stop_point": null, 
    "stop_task": null, 
    "suite_urls": {
        "foo": "", 
        "root": "", 
        "suite": ""
    }, 
    "template_vars": {}, 
    "time zone info": {
        "hours": 1, 
        "minutes": 0, 
        "string_basic": "+01", 
        "string_extended": "+01"
    }, 
}
__HERE__
