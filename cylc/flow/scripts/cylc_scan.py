#!/usr/bin/env python3

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

"""cylc [discovery] scan [OPTIONS] [HOSTS ...]

Print information about running suites.

Use the -o/--suite-owner option to get information of running suites for other
users.

Titles, descriptions, state totals, and cycle point state totals may also be
revealed publicly, depending on global and suite authentication settings.

WARNING: a suite suspended with Ctrl-Z will cause port scans to hang until the
connection times out (see --comms-timeout)."""

import sys
if "--use-ssh" in sys.argv[1:]:
    sys.argv.remove("--use-ssh")
    from cylc.flow.remote import remrun
    if remrun():
        sys.exit(0)

import json

from colorama import Fore, Style, init as color_init

from cylc.flow.exceptions import UserInputError
from cylc.flow.network.scan import (
    get_scan_items_from_fs, re_compile_filters, scan_many)
from cylc.flow.option_parsers import CylcOptionParser as COP
from cylc.flow.suite_status import KEY_META, KEY_NAME, KEY_OWNER, KEY_STATES
from cylc.flow.task_state import TASK_STATUSES_ORDERED
from cylc.flow.task_state_prop import get_status_prop
from cylc.flow import terminal


INDENT = "   "
TITLE_STYLE = Style.BRIGHT
MISSING_STYLE = Fore.MAGENTA
ERROR_STYLE = Fore.RED
META_KEY_ORDER = ['title', 'description', 'group']


def get_option_parser():
    """CLI opts for "cylc scan"."""
    parser = COP(
        __doc__,
        comms=True,
        noforce=True,
        argdoc=[],
    )

    parser.add_option(
        '--ordered',
        help='Display results in order, this may take longer.',
        action='store_true')

    parser.add_option(
        "-n", "--name",
        metavar="PATTERN",
        help="List suites with name matching PATTERN (regular expression). "
             "Defaults to any name. Can be used multiple times.",
        action="append", dest="patterns_name", default=[])

    parser.add_option(
        "-o", "--suite-owner",
        metavar="PATTERN",
        help="List suites with owner matching PATTERN (regular expression). "
             "Defaults to current user. Use '.*' to match all known users. "
             "Can be used multiple times.",
        action="append", dest="patterns_owner", default=[])

    parser.add_option(
        "-d", "--describe",
        help="Print suite metadata if available.",
        action="store_true", default=False, dest="describe")

    parser.add_option(
        "-s", "--state-totals",
        help="Print number of tasks in each state if available "
             "(total, and by cycle point).",
        action="store_true", default=False, dest="state_totals")

    parser.add_option(
        "--publisher",
        help="Append the suite publisher information to output.",
        action="store_true", default=False, dest="publisher")

    parser.add_option(
        "-f", "--full",
        help="Print all available information about each suite.",
        action="store_true", default=False, dest="full")

    parser.add_option(
        '--color', '--colour', action='store', dest='color', default='auto',
        help='Colorize the output, can be "always", "never" or "auto".')

    parser.add_option(
        "--comms-timeout", metavar="SEC",
        help="Set a timeout for network connections "
             "to each running suite. The default is 5 seconds.",
        action="store", default=5.0, dest="comms_timeout")

    parser.add_option(
        '-t',
        '--format',
        default='plain',
        action='store',
        dest='format',
        help=(
             'Set output format:\n'
             ' * plain (default) - text format for interactive use\n'
             ' * raw - parsable format (suite|owner|host|property|value)\n'
             ' * json - JSON format ({suite: {owner: OWNER, host: HOST ...)')
    )

    return parser


@terminal.cli_function(get_option_parser)
def main(parser, options):
    """Implement "cylc scan"."""
    if options.full:
        options.describe = options.state_totals = options.publisher = True
    if options.format in ['raw', 'json']:
        options.color = False

    # color settings
    if options.color in ['auto', 'always'] and terminal.supports_color():
        options.color = True
    else:
        options.color = False
    color_init(autoreset=True, strip=not options.color)

    # name and owner patterns
    if options.patterns_name:
        patterns_name = options.patterns_name
    else:
        patterns_name = ['.*']  # Any suite name.
    patterns_owner = None
    if options.patterns_owner:
        patterns_owner = options.patterns_owner
    try:  # Compile and check "name" and "owner" regular expressions
        cre_owner, cre_name = re_compile_filters(patterns_owner, patterns_name)
    except ValueError as exc:
        parser.error(str(exc))

    # list of endpoints to call
    methods = ['identify']
    if options.describe:
        methods.append('describe')
    if options.state_totals:
        methods.append('state_totals')

    # suite generator
    suites = scan_many(
        get_scan_items_from_fs(cre_owner, cre_name),
        timeout=options.comms_timeout,
        methods=methods,
        ordered=options.ordered
    )

    # determine output format
    if options.format == 'json':
        print(json.dumps(list(suites), indent=4))
    elif options.format == 'raw':
        formatter = format_raw
    elif options.format == 'plain':
        formatter = format_plain
    else:
        raise UserInputError('Unknown format: %s' % options.format)

    # output state legend if necessary
    state_legend = ""
    if options.color and options.state_totals:
        n_states = len(TASK_STATUSES_ORDERED)
        for index, state in enumerate(TASK_STATUSES_ORDERED):
            state_legend += get_status_prop(state, 'ascii_ctrl')
            if index == n_states / 2:
                state_legend += "\n"
        print(state_legend.rstrip() + "\n")

    # work through scan results one by one
    for reg, host, port, pub_port, api, info in suites:
        if isinstance(info, str):
            print(ERROR_STYLE + ' '.join([reg, host, port, info]))
        elif info is None:
            print(ERROR_STYLE +
                  ' '.join([reg, host, port, 'Error Connecting']))
        elif info[KEY_NAME] != reg:
            # TODO - should we do anything here, is this likely?
            print(ERROR_STYLE + 'Warning: suite has changed name %s => %s' % (
                reg, info[KEY_NAME]))
        else:
            formatter(reg, host, port, pub_port, api, info, options)


def sort_meta(item):
    """Sort meta items for the "plain" output format."""
    key = item[0]
    if key in META_KEY_ORDER:
        return str(META_KEY_ORDER.index(key))
    return key


def format_plain(name, host, port, pub_port, api, info, options):
    """Print a scan result, implements --format=plain"""
    owner = info[KEY_OWNER]

    if options.publisher:
        print(Style.BRIGHT + name + Style.NORMAL
              + ' %s@%s:%s' % (owner, host, port)
              + ' %s@%s:%s' % (owner, host, pub_port))
    else:
        print(Style.BRIGHT + name + Style.NORMAL
              + ' %s@%s:%s' % (owner, host, port))

    if options.describe:
        meta_items = info.get(KEY_META)
        meta_items['API'] = api
        if meta_items is None:
            print(INDENT + MISSING_STYLE + "(description withheld)")
            return
        for metaitem, metavalue in sorted(meta_items.items(), key=sort_meta):
            if metaitem in META_KEY_ORDER:
                metaitem = metaitem.capitalize()
            print(INDENT + TITLE_STYLE + metaitem + ":")
            if not metavalue:
                metavalue = MISSING_STYLE + '(no %s)' % metaitem + Fore.RESET
            for line in metavalue.splitlines():
                print(INDENT * 2 + line)

    if options.state_totals:
        totals = info.get(KEY_STATES)
        if totals is None:
            print(INDENT + MISSING_STYLE + "(state totals withheld)")
            return
        print(INDENT + TITLE_STYLE + "Task state totals:")
        for point, state_line in get_point_state_count_lines(
                *totals, use_color=options.color):
            point_prefix = ""
            if point:
                point_prefix = "%s " % point
            print(INDENT * 2 + "%s%s" % (point_prefix, state_line))


def format_raw(name, host, port, pub_port, api, info, options):
    """Print a scan result, implements --format=raw"""
    owner = info[KEY_OWNER]

    if options.publisher:
        print("%s|%s|%s|port|%s|publish-port|%s" % (
            name, owner, host, port, pub_port)
        )
    else:
        print("%s|%s|%s|port|%s" % (name, owner, host, port))

    if options.describe:
        # Extracting required data for these options before processing
        meta_items = info.get(KEY_META)
        meta_items['API'] = api

        # clean_meta_items = {}
        # for key, value in meta_items.items():
        #     if value:
        #         clean_meta_items.update({
        #             key: ' '.join([x.strip() for x in
        #                            str(value).split('\n') if x])})

        # for key, value in meta_items.items():
        #     if value:
        #         clean_meta_items.update({
        #             key: ' '.join([x.strip() for x in
        #                            str(value).split('\n') if x])})

        for key, value in sorted(meta_items.items(), key=sort_meta):
            value = ' '.join(line.strip()
                             for line in str(value).splitlines()
                             if line)
            print("%s|%s|%s|%s|%s" % (name, owner, host, key, value))

    if options.state_totals:
        totals = info.get(KEY_STATES)
        if totals is None:
            return
        for point, state_line in get_point_state_count_lines(*totals):
            key = KEY_STATES
            if point:
                key = "%s:%s" % (KEY_STATES, point)
            print("%s|%s|%s|%s|%s" % (name, owner, host, key, state_line))


def get_point_state_count_lines(state_count_totals, state_count_cycles,
                                use_color=False):
    """Yield (point, state_summary_text) tuples."""
    line = ""
    for state, tot in sorted(state_count_totals.items()):
        if use_color:
            subst = " %d " % tot
            line += get_status_prop(state, 'ascii_ctrl', subst)
        else:
            line += '%s:%d ' % (state, tot)
    yield ("", line.strip())

    for point_string, state_count_cycle in sorted(state_count_cycles.items()):
        line = ""
        for state, tot in sorted(state_count_cycle.items()):
            if use_color:
                subst = " %d " % tot
                line += get_status_prop(state, 'ascii_ctrl', subst)
            else:
                line += '%s:%d ' % (state, tot)
        yield (point_string, line.strip())


if __name__ == "__main__":
    main()
