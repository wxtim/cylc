#!/usr/bin/env python3

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

"""cylc [control] reset [OPTIONS] ARGS

Force task instances to a specified state.
  cylc reset --state=xxx REG - reset all tasks to state xxx
  cylc reset --state=xxx REG TASK_GLOB ... - reset one or more tasks to xxx

Outputs are automatically updated to reflect the new task state, except for
custom message outputs which can be manipulated directly with "--output".

Prerequisites reflect the state of other tasks; they are not changed except
to unset them on resetting state to 'waiting' or earlier.

To hold and release tasks use "cylc hold" and "cylc release", not this command.
"""

import os
import sys

from cylc.flow.option_parsers import CylcOptionParser as COP
from cylc.flow.network.client import SuiteRuntimeClient
from cylc.flow.task_state import TASK_STATUSES_CAN_RESET_TO
from cylc.flow.terminal import prompt, cli_function


def get_option_parser():
    parser = COP(
        __doc__, comms=True, multitask=True,
        argdoc=[
            ('REG', 'Suite name'),
            ('[TASK_GLOB ...]', 'Task matching patterns')])

    parser.add_option(
        "-s", "--state", metavar="STATE",
        help="Reset task state to STATE, can be %s" % (
            ', '.join(TASK_STATUSES_CAN_RESET_TO)),
        choices=list(TASK_STATUSES_CAN_RESET_TO),
        action="store", dest="state")

    parser.add_option(
        "--output", "-O",
        metavar="OUTPUT",
        help=("Find task output by message string or trigger string, " +
              "set complete or incomplete with !OUTPUT, " +
              "'*' to set all complete, '!*' to set all incomplete. " +
              "Can be used more than once to reset multiple task outputs."),
        action="append", default=[], dest="outputs")

    return parser


@cli_function(get_option_parser)
def main(parser, options, suite, *task_globs):
    if not options.state and not options.outputs:
        parser.error("Neither --state=STATE nor --output=OUTPUT is set")

    if options.state == "spawn":
        # Back compat.
        sys.stderr.write(
            "'cylc reset -s spawn' is deprecated; calling 'cylc spawn'\n")
        cmd = sys.argv[0].replace('reset', 'spawn')
        try:
            os.execvp(cmd, [cmd] + task_globs)
        except OSError as exc:
            if exc.filename is None:
                exc.filename = cmd
            raise SystemExit(exc)

    if not options.state:
        options.state = ''

    prompt('Reset task(s) %s in %s' % (task_globs, suite), options.force)
    pclient = SuiteRuntimeClient(suite, timeout=options.comms_timeout)
    pclient(
        'reset_task_states',
        {'tasks': task_globs, 'state': options.state,
         'outputs': options.outputs}
    )


if __name__ == "__main__":
    main()
