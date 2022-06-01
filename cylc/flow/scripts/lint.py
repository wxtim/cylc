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
"""Look through one or more folders for Cylc 7 syntax ``suite*.rc`` files
and search for syntax which may be problematic at Cylc 8.

Can be run either as a linter or "in place" (``-i``), leaving comments
in files. If used in the "in place" mode it is recommended that you ensure
that you have recorded the state of your workflow in a version control
system before starting.
"""
from colorama import Fore
from optparse import Values
from pathlib import Path
import re
from typing import Generator

from cylc.flow import LOG
from cylc.flow.option_parsers import (
    CylcOptionParser as COP,
)
from cylc.flow.terminal import cli_function

STYLE_GUIDE = (
    'https://cylc.github.io/cylc-doc/latest/html/workflow-design-guide/'
    'style-guide.html#'
)
URL_STUB = "https://cylc.github.io/cylc-doc/latest/html/7-to-8/"
SECTION1 = r'\[{}\]'
SECTION2 = r'\[\[{}\]\]'
SECTION3 = r'\[\[\[{}\]\]\]'
FILEGLOBS = ['*.rc', '*.cylc']
CHECKS = {
    '7-to-8': {
        re.compile(SECTION1.format('vizualization')): {
            'short': 'section ``[vizualization]`` has been removed.',
            'url': 'summary.html#new-web-and-terminal-uis'
        },
        re.compile(SECTION1.format('cylc')): {
            'short': 'section ``[cylc]`` is now called ``[scheduler]``.',
            'url': 'summary.html#terminology'
        },
        re.compile(SECTION2.format('authentication')): {
            'short': '``[cylc][authentication]`` is now obsolete.',
            'url': ''
        },
        re.compile(r'include at start-up\s*?='): {
            'short': '``[cylc]include at start up`` is obsolete.',
            'url': (
                'major-changes/excluding-tasks.html?'
                '#excluding-tasks-at-start-up-is-not-supported'
            ),
        },
        re.compile(r'exclude at start-up\s*?='): {
            'short': '``[cylc]exclude at start up`` is obsolete.',
            'url': (
                'major-changes/excluding-tasks.html?'
                '#excluding-tasks-at-start-up-is-not-supported'
            ),
        },
        re.compile(r'log resolved dependencies\s*?='): {
            # Mainly for testing
            'short': '``[cylc]log resolved dependencies`` is obsolete.',
            'url': ''
        },
        re.compile(r'required run mode\s*?='): {
            # Mainly for testing
            'short': '``[cylc]required run mode`` is obsolete.',
            'url': ''
        },
        re.compile(r'health check interval\s*?='): {
            'short': '``[cylc]health check interval`` is obsolete.',
            'url': ''
        },
        re.compile(r'abort if any task fails\s*?='): {
            'short': '``[cylc]abort if any task fails`` is obsolete.',
            'url': ''
        },
        re.compile(r'disable automatic shutdown\s*?='): {
            'short': '``[cylc]disable automatic shutdown`` is obsolete.',
            'url': ''
        },
        re.compile(r'reference test\s*?='): {
            # Mainly for testing
            'short': '``[cylc]reference test`` is obsolete.',
            'url': ''
        },
        re.compile(r'disable suite event handlers\s*?='): {
            'short': '``[cylc]disable suite event handlers`` is obsolete.',
            'url': ''
        },
        re.compile(SECTION2.format('simulation')): {
            'short': '``[cylc]simulation`` is obsolete.',
            'url': ''
        },
        re.compile(r'spawn to max active cycle points\s*?='): {
            'short': '``[cylc]spawn to max active cycle points`` is obsolete.',
            'url': (
                'https://cylc.github.io/cylc-doc/latest/html/reference'
                '/config/workflow.html#flow.cylc[scheduling]runahead%20limit'
            ),
        },
        re.compile(r'abort on stalled\s*?='): {
            'short':
                '``[cylc][events]abort on stalled`` is obsolete.',
            'url': ''
        },
        re.compile(r'abort if .* handler fails\s*?='): {
            'short': (
                '``[cylc][events]abort on ___ handler fails`` commands are'
                ' obsolete.'
            ),
            'url': ''
        },
        re.compile(r'.* handler\s*?='): {
            'short': (
                '``[cylc][<namespace>][events]___ handler`` commands are'
                ' now "handlers".'
            ),
            'url': ''
        },
        re.compile(r'mail retry delays\s*?='): {
            'short': (
                '``[runtime][<namespace>][events]mail retry delays`` '
                'is obsolete.'
            ),
            'url': ''
        },
        re.compile(r'extra log files\s*?='): {
            'short': (
                '``[runtime][<namespace>][events]extra log files`` '
                'is obsolete.'
            ),
            'url': ''
        },
        re.compile(r'shell\s*?='): {
            'short': (
                '``[runtime][<namespace>]shell`` '
                'is obsolete.'
            ),
            'url': ''
        },
        re.compile(r'suite definition directory\s*?='): {
            'short': (
                '``[runtime][<namespace>][remote]suite definition directory`` '
                'is obsolete.'
            ),
            'url': 'summary.html#symlink-dirs'
        },
        re.compile(SECTION2.format('dependencies')): {
            'short': '``[dependencies]`` is deprecated.',
            'url': 'major-changes/config-changes.html#graph'
        },
        re.compile(r'graph\s*?='): {
            'short': (
                '``[cycle point]graph =`` is deprecated, '
                'use ``cycle point = <graph>``'
            ),
            'url': 'major-changes/config-changes.html#graph'
        },
        re.compile(SECTION2.format('remote')): {
            'short': (
                '``[runtime][<namespace>][remote]host`` is deprecated, '
                'use ``[runtime][<namespace>]platform``'
            ),
            'url': 'major-changes/platforms.html#platforms'
        },
        re.compile(SECTION3.format('job')): {
            'short': (
                '``[runtime][<namespace>][job]`` is deprecated, '
                'use ``[runtime][<namespace>]platform``'
            ),
            'url': 'major-changes/platforms.html#platforms'
        },
        re.compile(SECTION2.format('parameter templates')): {
            'short': (
                '``[cylc][parameter templates]`` is deprecated, '
                'use ``[task parameters][templates]``'
            ),
            'url': ''
        },
        re.compile(SECTION2.format('parameters')): {
            'short': (
                '``[cylc][parameters]`` is deprecated, '
                'use ``[task parameters]``'
            ),
            'url': ''
        },
        re.compile(r'task event mail interval\s*?='): {
            'short': (
                '``[cylc][task event mail interval]`` is deprecated, '
                'use ``[scheduler][mail][task event batch interval]``'
            ),
            'url': ''
        }
    },
    'lint': {
        re.compile(r'^\t'): {
            'short': 'Use multiple spaces, not tabs',
            'url': STYLE_GUIDE + 'tab-characters'
        },
        # Not a full test, but if a non section is not indented...
        re.compile(r'^[^\[|\s]'): {
            'short': 'Item not indented.',
            'url': STYLE_GUIDE + 'indentation'
        },
        #            [section]
        re.compile(r'^\s+\[.*\]'): {
            'short': 'Too many indents for top level section.',
            'url': STYLE_GUIDE + 'indentation'
        },
        # 2 or 4 space indentation both seem reasonable:
        re.compile(r'^(\s|\s{3}|\s{5,})\[\[.*\]\]'): {
            'short': 'wrong number of indents for second level section.',
            'url': STYLE_GUIDE + 'indentation'
        },
        re.compile(r'^(\s{1,3}|\s{5,7}|\s{9,})\[\[\[.*\]\]\]'): {
            'short': 'wrong number of indents for third level section.',
            'url': STYLE_GUIDE + 'indentation'
        },
        re.compile(r'\s$'): {
            'short': 'wrong number of indents for third level section.',
            'url': STYLE_GUIDE + 'trailing-whitespace'
        },
        re.compile(r'^.{80,}'): {
            'short': 'line > 79 characters.',
            'url': STYLE_GUIDE + 'line-length-and-continuation'
        },
        re.compile(r'inherit\s*=\s*.*[a-z].*$'): {
            'short': 'Family name contains lowercase characters.',
            'url': STYLE_GUIDE + 'task-naming-conventions'
        },

    }
}


def parse_checks(check_arg):
    """Collapse metadata in checks dicts.
    """
    parsedchecks = {}
    if check_arg == '8':
        purpose_filters = ['7-to-8']
    elif check_arg == 'lint':
        purpose_filters = ['lint']
    else:
        purpose_filters = ['lint', '7-to-8']

    for purpose, checks in CHECKS.items():
        if purpose in purpose_filters:
            for index, (pattern, meta) in enumerate(checks.items()):
                meta.update({'purpose': purpose})
                meta.update({'index': index})
                parsedchecks.update({pattern: meta})
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
                    if message['url'].startswith('http'):
                        url = message['url']
                    else:
                        url = URL_STUB + message['url']
                    outlines.append(
                        f'# [{message["index"]:03d}: {message["purpose"]}]: '
                        f'{message["short"]}\n'
                        f'# - see {url}'
                    )
                else:
                    print(
                        Fore.YELLOW +
                        f'[{message["index"]:03d}: {message["purpose"]}]'
                        f'{file_}: {line_no}: {message["short"]}'
                    )
        if modify:
            outlines.append(line)
    if modify:
        file_.write_text('\n'.join(outlines))
    return count


def get_cylc_files(base: Path) -> Generator:
    """Given a directory yield paths to check.
    """
    excludes = [Path('log')]

    for rglob in FILEGLOBS:
        for path in base.rglob(rglob):
            # Exclude log directory:
            if path.relative_to(base).parents[0] not in excludes:
                yield path


def get_reference(checks):
    output = ''
    for check, meta in checks.items():
        template = (
            '{index:003d} {checkset} ``{title}``:\n    {summary}\n'
            '    see - {url}\n'
        )
        if meta['url'].startswith('http'):
            url = meta['url']
        else:
            url = URL_STUB + meta['url']
        msg = template.format(
            title=check.pattern.replace('\\', ''),
            checkset=meta['purpose'],
            summary=meta['short'],
            url=url,
            index=meta['index'],
        )
        output += msg
    print(output)


def get_option_parser() -> COP:
    parser = COP(
        __doc__,
        argdoc=[('[targets ...]', 'Directories to lint')],
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
    parser.add_option(
        '--reference', '--ref', '-r',
        help=(
            'generate a reference of errors'
        ),
        action='store_true',
        default=False,
        dest="ref"
    )
    parser.add_option(
        '--linter',
        default='8',
        choices=('8', 'lint', 'all'),
        dest='linter'
    )

    return parser


@cli_function(get_option_parser)
def main(parser: COP, options: 'Values', *targets) -> None:
    checks = parse_checks(options.linter)

    if options.ref:
        get_reference(checks)

    count = 0
    for target in targets:
        target = Path(target)
        if not target.exists():
            LOG.warn(f'Path {target} does not exist.')
        else:
            for file_ in get_cylc_files(target):
                LOG.debug(f'Checking {file_}')
                count += check_cylc_file(file_, checks, options.inplace)
        if count > 0:
            print(Fore.YELLOW + f'Checked {target} and found {count} issues.')
        else:
            print(Fore.GREEN + f'Checked {target} and found {count} issues.')
