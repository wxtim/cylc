#!/usr/bin/env python3
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
"""Look through a folder for Cylc 7 syntax ``suite*.rc`` files
"""
from optparse import Values
from pathlib import Path
import re
from typing import List

from cylc.flow import LOG
from cylc.flow.option_parsers import (
    CylcOptionParser as COP,
)
from cylc.flow.terminal import cli_function


FILEGLOBS = ['*.rc']
CHECK78 = {
    '7-to-8': {
        re.compile(r'\[cylc\]'): {
            'short': 'section `[cylc]` is now called `[scheduler]`.',
            'url': 'major-changes/config-changes.html#graph'
        },
        re.compile(r'include at start-up\s?='): {
            'short': '`include at start up` is obselete.',
            'url': 'major-changes/config-changes.html#graph'
        },
        re.compile(r'exclude at start-up\s?='): {
            'short': '`exclude at start up` is obselete.',
            'url': 'major-changes/config-changes.html#graph'
        },
        re.compile(r'log resolved dependencies\s?='): {
            'short': '`exclude at start up` is obselete.',
            'url': 'major-changes/config-changes.html#graph'
        },
        re.compile(r'\[\[dependencies\]\]'): {
            'short': '`[dependencies]` is deprecated.',
            'url': 'major-changes/config-changes.html#graph'
        },
        re.compile(r'graph\s?='): {
            'short': (
                '`[cycle point]graph =` is deprecated, '
                'use `cycle point = <graph>`',
            ),
            'url': 'major-changes/config-changes.html#graph'
        },
        re.compile(r'\[\[remote\]\]'): {
            'short': (
                '`[runtime][<namespace>][remote]host` is deprecated, '
                'use `[runtime][<namespace>]platform`',
            ),
            'url': 'major-changes/config-changes.html#graph'
        },
        re.compile(r'\[\[job\]\]'): {
            'short': (
                '`[runtime][<namespace>][job]` is deprecated, '
                'use `[runtime][<namespace>]platform`',
            ),
            'url': 'major-changes/config-changes.html#graph'
        }
    }
}


def parse_checks():
    """Collapse metadata in checks dicts.
    """
    parsedchecks = {}
    index = 0
    for checkdict in [CHECK78]:
        for purpose, checks in checkdict.items():
            for pattern, meta in checks.items():
                meta.update({'purpose': purpose})
                meta.update({'index': index})
                parsedchecks.update({pattern: meta})
                index += 1   # noqa SIM113
    return parsedchecks


def check_cylc_file(file_, checks, modify=False):
    """Check A Cylc File for Cylc 7 Config"""
    # Set mode as read-write or read only.
    outlines = []

    # Open file, and read it's line to mempory.
    lines = file_.read_text().split('\n')
    count = 0
    for line_no, line in enumerate(lines):
        for check, message in checks.items():
            if check.findall(line) and not line.strip().startswith('#'):
                count += 1
                if modify:
                    outlines.append(
                        f'# {message["purpose"]}: {message["short"]}\n'
                        f'# - see {message["url"]}'
                    )
                else:
                    print(
                        f'[{message["index"]:03d}:{message["purpose"]}]'
                        f'{file_}:{line_no}:{message["short"]}'
                    )
        if modify:
            outlines.append(line)
    if modify:
        file_.write_text('\n'.join(outlines))
    return count


def get_cylc_files(base: Path) -> List[Path]:
    """Given a directory yield paths to check.
    """
    excludes = [Path('log')]

    for rglob in FILEGLOBS:
        for path in base.rglob(rglob):
            # Exclude log directory:
            if path.relative_to(base).parents[0] not in excludes:
                yield path


def get_option_parser() -> COP:
    parser = COP(
        __doc__,
        argdoc=[('targets ...', 'Directories to lint')],
        # auto_add=False,  NOTE: at present auto_add can not be turned off
        color=False
    )
    parser.add_option(
        '--inplace', '-i',
        help=(
            'Modify files in place, adding comments to files'
            'If not set script will work as a linter'
        ),
        action='store_true',
        default=False,
    )

    return parser


@cli_function(get_option_parser)
def main(parser: COP, options: 'Values', *targets) -> None:
    checks = parse_checks()
    count = 0
    for target in targets:
        target = Path(target)
        if not target.exists():
            LOG.warn(f'Path {target} does not exist.')
        else:
            for file_ in get_cylc_files(target):
                LOG.debug(f'Checking {file_}')
                count += check_cylc_file(file_, checks, options.inplace)
    LOG.info(f'Checked and found {count} possible issues.')
