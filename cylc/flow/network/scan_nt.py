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


__all__ = ('scan')


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
            # this is a flow directory

            # check if it's name matches the patterns if provided
            if patterns and not patterns.fullmatch(name):
                continue

            # check if the suite state matches is_active
            if (
                    is_active is not None
                    and (path / service / contact).exists() != is_active
            ):
                continue

            # we have a hit
            yield {
                'name': name,
                'path': path,
            }
        else:
            # we may have a nested flow, lets see...
            stack.extend([
                subdir
                for subdir in path.iterdir()
                if subdir.is_dir()
            ])


def pipe(fcn):
    def _pipe(itt, *args):
        nonlocal fcn
        for x in itt:
            ret = fcn(x, *args)
            if ret:
                yield ret
    return _pipe


@pipe
def filter_name(obj, pattern):
    if pattern.match(obj['name']):
        return obj


@pipe
def is_active(obj, is_active):
    obj['contact']


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
