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


class Pipe:
    """An asynchronous pipe implementation in pure Python.

    Example:
        A generator to begin our pipe with:
        >>> @Pipe
        ... async def arange():
        ...    for i in range(10):
        ...        yield i

        A filter which returns a boolean:
        >>> @Pipe
        ... async def even(x):
        ...    return x % 2 == 0

        A transformation returns anything other than a boolean:
        >>> @Pipe
        ... async def mult(x, y):
        ...    return x * y

        Assemble them into a pipe
        >>> mypipe = arange | even | mult(2)
        >>> print(mypipe)
        arange()
        >>> repr(mypipe)
        'arange() | even() | mult(2)'

        Write a function to "consume items":
        >>> async def consumer(pipe):
        ...     async for item in pipe:
        ...         print(item)

        Run pipe run:
        >>> import asyncio
        >>> asyncio.run(consumer(mypipe))
        0
        4
        8
        12
        16

        Real world examples will involve a bit of awaiting.

    """

    def __init__(self, func):
        self.func = func
        self.args = tuple()
        self.kwargs = {}
        self._left = None
        self._right = None

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

    async def __aiter__(self):
        coros = self.__iter__()
        gen = next(coros)
        coros = list(coros)
        async for item in gen.func():
            for coro in coros:
                ret = await coro.func(item, *coro.args, **coro.kwargs)
                if ret is False:
                    break
                if ret is True:
                    pass
                else:
                    item = ret
            else:
                yield item

    def __or__(self, other):
        other._left = self
        self.fastforward()._right = other
        # because we return self we only need __or__ not __ror__
        return self

    def rewind(self):
        """Return the head of the pipe."""
        ptr = self
        while ptr._left:
            ptr = ptr._left
        return ptr

    def fastforward(self):
        """Return the tail of the pipe."""
        ptr = self
        while ptr._right:
            ptr = ptr._right
        return ptr

    def __iter__(self):
        ptr = self.rewind()
        while ptr._right:
            yield ptr
            ptr = ptr._right
        yield ptr

    def __repr__(self):
        return ' | '.join((str(ptr) for ptr in self))

    def __str__(self):
        args = ''
        if self.args:
            args = ', '.join(map(repr, self.args))
        if self.kwargs:
            if args:
                args += ', '
            args += ', '.join(f'{k}={repr(v)}' for k, v in self.kwargs.items())
        return f'{self.func.__name__}({args})'


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


@Pipe
async def filter_name(obj, pattern):
    return pattern.match(obj['name'])


SERVICE = Path(SuiteFiles.Service.DIRNAME)
CONTACT = Path(SuiteFiles.Service.CONTACT)


@Pipe
async def is_active(obj, is_active):
    obj['contact'] = obj['path'] / SERVICE / CONTACT
    return obj['contact'].exists() == is_active


from cylc.flow.suite_files import load_contact_file


@Pipe
async def contact_info(obj):
    obj.update(
        load_contact_file(obj['name'], path=obj['path'])
    )
    return obj


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
