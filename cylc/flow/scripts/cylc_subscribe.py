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

"""cylc subscribe [OPTIONS] ARGS

(This command is for internal use.)
Invoke suite subscriber to receive published workflow output.
"""

import json
import sys
import time

from google.protobuf.json_format import MessageToDict

from cylc.flow.exceptions import ClientError
from cylc.flow.option_parsers import CylcOptionParser as COP
from cylc.flow.network import get_location
from cylc.flow.network.subscriber import WorkflowSubscriber, process_delta_msg
from cylc.flow.terminal import cli_function
from cylc.flow.data_store_mgr import DELTAS_MAP


def print_message(topic, data, subscriber=None, once=False):
    """Print protobuf message."""
    print(f'Received: {topic}')
    if topic == 'shutdown':
        print(data.decode('utf-8'))
        subscriber.stop()
        return
    sys.stdout.write(
        json.dumps(MessageToDict(data), indent=4) + '\n')
    if once and subscriber is not None:
        subscriber.stop()


def get_option_parser():
    """Augment options parser to current context."""
    parser = COP(
        __doc__,
        argdoc=[
            ('REG', 'Suite name'),
            ('[USER_AT_HOST]', 'user@host:port, shorthand for --user, '
             '--host & --port.')],
        comms=True,
        noforce=True
    )

    delta_keys = list(DELTAS_MAP)
    pb_topics = ("Directly published data-store topics include: '" +
                 ("', '").join(delta_keys[:-1]) +
                 "' and '" + delta_keys[-1] + "'.")
    parser.add_option(
        "-T", "--topics",
        help="Specify a comma delimited list of subscription topics. "
        + pb_topics,
        action="store", dest="topics", default='workflow')

    parser.add_option(
        "-o", "--once",
        help="Show a single publish then exit.",
        action="store_true", default=False, dest="once")

    return parser


@cli_function(get_option_parser)
def main(_, options, *args):
    suite = args[0]

    if len(args) > 1:
        try:
            user_at_host, options.port = args[1].split(':')
            options.owner, options.host = user_at_host.split('@')
        except ValueError:
            print(('USER_AT_HOST must take the form '
                   '"user@host:port"'), file=sys.stderr)
            sys.exit(1)
    elif options.host is None or options.port is None:
        try:
            while True:
                try:
                    options.host, _, options.port = get_location(
                        suite, options.owner, options.host)
                except (ClientError, IOError, TypeError, ValueError):
                    time.sleep(3)
                    continue
                break
        except KeyboardInterrupt:
            exit()

    print(f'Connecting to tcp://{options.host}:{options.port}')
    topic_set = set()
    topic_set.add(b'shutdown')
    for topic in options.topics.split(','):
        topic_set.add(topic.encode('utf-8'))

    subscriber = WorkflowSubscriber(
        suite,
        host=options.host,
        port=options.port,
        topics=topic_set)

    subscriber.loop.create_task(
        subscriber.subscribe(
            process_delta_msg,
            func=print_message,
            subscriber=subscriber,
            once=options.once
        )
    )

    # run Python run
    try:
        subscriber.loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        print('\nDisconnecting')
        subscriber.stop()
        exit()


if __name__ == '__main__':
    main()
