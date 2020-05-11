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

"""cylc [prep] edit [OPTIONS] ARGS

Edit suite definitions without having to move to their directory
locations, and with optional reversible inlining of include-files. Note
that Jinja2 suites can only be edited in raw form but the processed
version can be viewed with 'cylc [prep] view -p'.

1/cylc [prep] edit SUITE
Change to the suite definition directory and edit the suite.rc file.

2/ cylc [prep] edit -i,--inline SUITE
Edit the suite with include-files inlined between special markers. The
original suite.rc file is temporarily replaced so that the inlined
version is "live" during editing (i.e. you can run suites during
editing and cylc will pick up changes to the suite definition). The
inlined file is then split into its constituent include-files
again when you exit the editor. Include-files can be nested or
multiply-included; in the latter case only the first inclusion is
inlined (this prevents conflicting changes made to the same file).

3/ cylc [prep] edit --cleanup SUITE
Remove backup files left by previous INLINED edit sessions.

INLINED EDITING SAFETY: The suite.rc file and its include-files are
automatically backed up prior to an inlined editing session. If the
editor dies mid-session just invoke 'cylc edit -i' again to recover from
the last saved inlined file. On exiting the editor, if any of the
original include-files are found to have changed due to external
intervention during editing you will be warned and the affected files
will be written to new backups instead of overwriting the originals.
Finally, the inlined suite.rc file is also backed up on exiting
the editor, to allow recovery in case of accidental corruption of the
include-file boundary markers in the inlined file.

The edit process is spawned in the foreground as follows:
  % <editor> suite.rc
Where <editor> is defined in the cylc site/user config files.

See also 'cylc [prep] view'."""

import sys
from cylc.flow.exceptions import CylcError, UserInputError
from cylc.flow.remote import remrun
if remrun(forward_x11=True):
    sys.exit(0)

import os
import re
from subprocess import call
from shutil import copy


from cylc.flow.parsec.include import inline, \
    split_file, backup, backups, newfiles, cleanup, modtimes


from cylc.flow.cfgspec.glbl_cfg import glbl_cfg
from cylc.flow.option_parsers import CylcOptionParser as COP
from cylc.flow.suite_files import parse_suite_arg
from cylc.flow.terminal import cli_function
from cylc.flow.wallclock import get_current_time_string


def get_option_parser():
    parser = COP(__doc__, prep=True)

    parser.add_option(
        "--inline", "-i",
        help="Edit with include-files inlined as described above.",
        action="store_true", default=False, dest="inline")

    parser.add_option(
        "--cleanup",
        help="Remove backup files left by previous inlined edit sessions.",
        action="store_true", default=False, dest="cleanup")

    parser.add_option(
        "--gui", "-g", help="Force use of the configured GUI editor.",
        action="store_true", default=False, dest="geditor")

    return parser


@cli_function(get_option_parser)
def main(parser, options, *args):
    suiterc = parse_suite_arg(options, args[0])[1]

    if options.geditor:
        editor = glbl_cfg().get(['editors', 'gui'])
    else:
        editor = glbl_cfg().get(['editors', 'terminal'])

    suitedir = os.path.dirname(suiterc)

    if options.cleanup:
        # remove backup files left by inlined editing sessions
        cleanup(suitedir)
        sys.exit(0)

    if not options.inline:
        # plain old editing.
        # move to suite def dir
        os.chdir(suitedir)

        # edit the suite.rc file
        if not os.path.isfile(suiterc):
            raise UserInputError(f'file not found: {suiterc}')

        # in case editor has options, e.g. 'emacs -nw':
        command_list = re.split(' ', editor)
        command_list.append(suiterc)
        command = ' '.join(command_list)
        # THIS BLOCKS UNTIL THE COMMAND COMPLETES
        retcode = call(command_list)
        if retcode != 0:
            # the command returned non-zero exist status
            raise CylcError(f'{command} failed: {retcode}')

        # !!!EDITING FINISHED!!!
        sys.exit(0)

    # read the suite.rc file
    if os.path.isfile(suiterc):
        # back up the original
        backup(suiterc)
        # record original modtime
        modtimes[suiterc] = os.stat(suiterc).st_mtime
        # read the file
        h = open(suiterc, 'r')
        lines0 = h.readlines()
        h.close()
        if lines0[0].startswith('# !WARNING! CYLC EDIT INLINED'):
            print('WARNING: RECOVERING A PREVIOUSLY INLINED FILE')
            recovery = True
            lines = lines0
        else:
            recovery = False
            lines = inline(lines0, suitedir, suiterc, for_edit=True)
    else:
        parser.error("File not found: " + suiterc)

    lines = [i.rstrip() for i in lines]

    # overwrite the (now backed up) original with the inlined file:
    h = open(suiterc, 'wb')
    for line in lines:
        h.write((line + '\n').encode())
    h.close()

    print('PRE-EDIT BACKUPS:')
    for file in backups:
        src = re.sub(suitedir + '/', '', file)
        dst = re.sub(suitedir + '/', '', backups[file])
        print(' + ' + src + ' ---> ' + dst)

    # in case editor has options, e.g. 'emacs -nw':
    command_list = re.split(' ', editor)
    command_list.append(suiterc)
    command = ' '.join(command_list)
    # THIS BLOCKS UNTIL THE COMMAND COMPLETES
    retcode = call(command_list)
    if retcode != 0:
        # the command returned non-zero exist status
        raise CylcError(f'{command} failed: {retcode}')
    print('EDITING DONE')

    # Now back up the inlined file in case of absolute disaster, so as the
    # user or his editor corrupting the inlined-include-file marker lines.
    inlined_suiterc_backup = (
        suitedir + '/suite.rc.INLINED.EDIT.' +
        get_current_time_string(override_use_utc=True, use_basic_format=True)
    )
    copy(suiterc, inlined_suiterc_backup)

    # read in the edited inlined file
    h = open(suiterc, 'r')
    lines = h.readlines()
    h.close()

    # split it back into separate files
    split_file(suitedir, lines, suiterc, recovery)

    print(' + edited:', suiterc)
    print(' + backup:', inlined_suiterc_backup)
    print('INCLUDE-FILES WRITTEN:')
    for file in newfiles:
        f = re.sub(suitedir + '/', '', file)
        if re.search(r'\.EDIT\.NEW\.', f):
            print(' + ' + f + ' (!!! WARNING: original changed on disk !!!)')
        else:
            print(' + ' + f)
    # DONE


if __name__ == "__main__":
    main()
