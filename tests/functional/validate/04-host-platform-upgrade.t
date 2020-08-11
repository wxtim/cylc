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
# Quick double check of a few key cases for the Upgrade logic for
# hosts -=> platforms. These are tested throughly in
# `tests/unit/test_platform_lookup.py`
#
# 1. Very simple case (foo): a job with a single host correctly identifies a
#    platform to use.
# 2. Example with a batch system (bar): A job where the batch system and
#    the remote host togther identify a platform.
# 3. Don't do anything with `host = $(myfunction)` on validation except raise
#    a warning that the upgrade will happen on validation.
# 4. Raise an error if no matching platform can be found.
# 5. Raise an error if Cylc 7 and 8 syntax are mixed in the same task.


. "$(dirname "$0")/test_header"
#-------------------------------------------------------------------------------
set_test_number 8
#-------------------------------------------------------------------------------

create_test_globalrc "" "
[platforms]
    [[beeblebrox]]
        remote hosts = zaph1, zaph2, zaph3
    [[trillian]]
        remote hosts = zarquon, arkleseizure
        batch system = pbs
"

#-------------------------------------------------------------------------------
# Tests of upgrader passing.

cat > suite.rc <<'__HEREDOC__'
[scheduling]
    [[graph]]
        R1 = foo & bar & baz

[runtime]
    [[foo]]
        [[[remote]]]
            host = zaph2

    [[bar]]
        [[[remote]]]
            host = zarquon
        [[[job]]]
            batch system = pbs

    [[baz]]
        [[[remote]]]
            host = $(she scripts shell scripts on the sea shore)
__HEREDOC__

TEST_NAME=${TEST_NAME_BASE}
run_ok "${TEST_NAME}" cylc validate -v suite.rc

declare -A GREPTESTS
GREPTESTS['host=>platform']='platform \"beeblebrox\" .* task \"foo\"'
GREPTESTS['host-and-batch-sys=>platform']='platform \"trillian\" .* task \"bar\"'
GREPTESTS['host_is_fn=>warn-upg-on-submit']="The host setting of 'baz' is a function"

for task in "${!GREPTESTS[@]}"; do
    ln -s "${TEST_NAME}.stderr" "${TEST_NAME}.${task}."
    grep_ok "${GREPTESTS[$task]}" "${TEST_NAME}.${task}."
done

#-------------------------------------------------------------------------------
# Tests of upgrader failing

cat > suite-fail-no-platform.rc <<'__HEREDOC__'
[scheduling]
    [[graph]]
        R1 = qux
[runtime]
    [[qux]]
        [[[remote]]]
            host = antidissestablishmentarianism_is_a_very_long_word
        [[[job]]]
            batch system = can_you_spell_it
__HEREDOC__

cat > suite-fail-mixed-syntax.rc <<'__HEREDOC__'
[scheduling]
    [[graph]]
        R1 = wibble
[runtime]
    [[wibble]]
        platform = beeblebrox
        [[[remote]]]
            host = zaph3
__HEREDOC__

declare -A FAILS
FAILS['no-platform']='PlatformLookupError: for task qux: No platform found'
FAILS['mixed-syntax']='PlatformLookupError.* Task wibble set platform and item'

for cause in "${!FAILS[@]}"; do
    run_fail "${TEST_NAME}.fail-${cause}" cylc validate -v "suite-fail-$cause.rc"
    grep_ok "${FAILS[$cause]}" "${TEST_NAME}.fail-${cause}.stderr"
done
exit
