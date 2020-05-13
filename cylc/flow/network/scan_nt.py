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

from contextlib import contextmanager
from pathlib import Path
import re
import time
from queue import Queue

from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler

from cylc.flow.cfgspec.glbl_cfg import glbl_cfg
from cylc.flow.suite_files import SuiteFiles


__all__ = ('scan', 'continuous_scan')


class FlowScanner(RegexMatchingEventHandler):
    """Continuous Cylc flow scanner, use via continuous_scan.

    Uses filesystem events via the watchdog python library.

    Args:
        observer (watchdog.observers.Observer):
            An observer instance to use for scanning.
        queue (queue.Queue):
            Queue for adding events to.
        flow (str):
            For internal use.
        service (bool):
            for internal use.

    """

    def __init__(
            self,
            observer,
            queue,
            flow=None,
            service=False,
            **kwargs
    ):
        self.observer = observer
        self.queue = queue
        self.flow = flow
        self.service = service
        RegexMatchingEventHandler.__init__(self, **kwargs)

    def on_any_event(self, event):
        # TODO: nicify this and build it around this any_event thinggy
        self.queue.put(('event', *event.key))

    def on_created(self, event):
        if not self.flow and event.is_directory:
            flow = Path(event.src_path).name
            self.observer.schedule(
                FlowScanner(
                    self.observer,
                    self.queue,
                    flow=flow,
                    regexes=['.*service']
                ),
                event.src_path
            )
            # self.created.put(flow)
            self.queue.put(('created', flow))
        elif self.flow and not self.service:
            self.observer.schedule(
                FlowScanner(
                    self.observer,
                    self.queue,
                    flow=self.flow,
                    service=True,
                    regexes=['.*contact']
                ),
                event.src_path
            )
            # self.created.put(self.flow)
            self.queue.put('meh')
        elif self.service:
            # self.started.put(self.flow)
            self.queue.put(('started', self.flow))
        else:
            self.queue.put(('error', (self.flow, self.service, event.src_path)))

    def on_deleted(self, event):
        self.queue.put(('on_deleted', event))
        if not self.flow and event.is_directory:
            # self.removed.put(Path(event.src_path).name)
            self.queue.put(('removed', Path(event.src_path).name))
        elif self.flow and not self.service:
            # self.stopped.put(self.flow)
            self.queue.put(('stopped', self.flow))
        elif self.service:
            # self.stopped.put(self.flow)
            self.queue.put(('stopped', self.flow))
        # stop the observer observing?


@contextmanager
def continuous_scan(path=None, patterns=None):
    """
    """
    kwargs = {}
    if patterns:
        kwargs['regexes'] = patterns
    # create the queue
    queue = Queue()
    # perform an initial scan
    for item in scan(path, patterns=patterns, is_active=True):
        # started.put(item)
        queue.put(('started', item))
    for item in scan(path, patterns=patterns, is_active=False):
        # stopped.put(item)
        queue.put(('stopped', item))
    try:
        # boot up the continuous scanner
        observer = Observer()
        event_handler = FlowScanner(observer, queue, **kwargs)
        observer.schedule(event_handler, str(path))
        observer.start()
        print('# started')
        yield queue
    finally:
        # shutdown the continuous scanner
        observer.stop()
        observer.join()
        print('# stopped')


# with continuous_scan('.') as (started, stopped):
#     time.sleep(10)
#     print('started:')
#     while not started.empty():
#         print(started.get())
#     print('stopped:')
#     while not stopped.empty():
#         print(stopped.get())



def regex_combine(patterns):
    """Join regex patterns.

    Examples:
        >>> regex_combine(['a', 'b', 'c'])
        '(a|b|c)'

    """
    return rf'({"|".join(patterns)})'


def scan(path=None, patterns=None, is_active=None):
    path = Path(path)
    if patterns:
        patterns = re.compile(regex_combine(patterns))
    service = Path(SuiteFiles.Service.DIRNAME)
    contact = Path(SuiteFiles.Service.CONTACT)
    run_dir = path or Path(
        glbl_cfg().get_host_item('run directory').replace('$HOME', '~')
    ).expanduser()
    stack = [
        subdir
        for subdir in run_dir.iterdir()
        if subdir.is_dir()
    ]
    for path in stack:
        name = str(path.relative_to(run_dir))
        if (path / service).exists():
            # this is a flow directory

            # check if it's name matches the patterns if provided
            if patterns and not patterns.fullmatch(name):
                continue

            # check if the suite state matches is_active
            if (
                    is_active is not None
                    and (path / service / contact).exists() != is_active
            ):
                continue

            # we have a hit
            yield name
        else:
            # we may have a nested flow, lets see...
            stack.extend([
                subdir
                for subdir in path.iterdir()
                if subdir.is_dir()
            ])



from textwrap import dedent

from cylc.flow.network.client import (
    SuiteRuntimeClient, ClientError, ClientTimeout)


# async def state_info(reg, fields):
#     query = f'query {{ workflows(ids: ["{reg}"]) {{ {"\n".join(fields)} }} }}'
#     client = SuiteRuntimeClient(reg)
#     try:
#         ret = await client(
#             'graphql',
#             {
#                 'request_string': query,
#                 'variables': {}
#             }
#         )
#     except ClientTimeout as exc:
#         LOG.exception(
#             "Timeout: name:%s, host:%s, port:%s", reg, host, port)
#         return {}
#     except ClientError as exc:
#         LOG.exception("ClientError")
#         return {}
#     else:
#         return ret
