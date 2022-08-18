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

#------------------------------------------------------------------------------
# Test linting with a toml file present.
. "$(dirname "$0")/test_header"
set_test_number 8

# Set Up:
rm etc/global.cylc

cat > flow.cylc <<__HERE__
# This is definitely not an OK flow.cylc file.
\t[scheduler]

 [cylc]

[[dependencies]]

[runtime]
          [[foo]]
        inherit = hello
     [[[job]]]
something\t
__HERE__

mkdir sites

cat > sites/niwa.cylc <<__HERE__
blatantly = not valid
__HERE__


# Control tests
TEST_NAME="it lints without toml file"
run_ok "${TEST_NAME}" cylc lint
TESTOUT="${TEST_NAME}.stdout"
named_grep_ok "it returns error code" "S004" "${TESTOUT}"
named_grep_ok "it returns error from subdirectory" "niwa.cylc" "${TESTOUT}"
named_grep_ok "it returns a 728 upgrade code" "^\[U" "${TESTOUT}"


# Add a pyproject.toml file
cat > pyproject.toml <<__HERE__
# .cylc-lint.toml
# Check against these rules
rulesets = [
    "style"
]
#  do not check for these errors
ignore = [
    "S004"
]
# do not lint files matching
# these globs:
exceptions = [
    "sites/*.cylc",
]

__HERE__

# Test that results are different:
TEST_NAME="it_lints_with_toml_file"
run_ok "${TEST_NAME}" cylc lint
TESTOUT="${TEST_NAME}.stdout"
grep_fail "S004" "${TESTOUT}"
grep_fail "niwa.cylc" "${TESTOUT}"
grep_fail "^\[U" "${TESTOUT}"
