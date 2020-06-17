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

import asyncio
from pathlib import Path

from cylc.flow import LOG
from cylc.flow.async_util import (
    Pipe,
    asyncqgen,
    scandir
)
from cylc.flow.cfgspec.glbl_cfg import glbl_cfg
from cylc.flow.network.client import (
    SuiteRuntimeClient, ClientError, ClientTimeout)
from cylc.flow.suite_files import (
    SuiteFiles,
    load_contact_file
)


SERVICE = Path(SuiteFiles.Service.DIRNAME)
CONTACT = Path(SuiteFiles.Service.CONTACT)


def regex_combine(patterns):
    """Join regex patterns.

    Examples:
        >>> regex_combine(['a', 'b', 'c'])
        '(a|b|c)'

    """
    return rf'({"|".join(patterns)})'


async def dir_is_flow(listing):
    """Return True if a Path contains a flow at the top level."""
    listing = [
        path.name
        for path in listing
    ]
    return (
        SERVICE in listing
        or 'suite.rc' in listing
    )


@Pipe
async def scan(run_dir=None):
    if not run_dir:
        run_dir = Path(
            glbl_cfg().get_host_item('run directory').replace('$HOME', '~')
        ).expanduser()
    stack = asyncio.Queue()
    for subdir in await scandir(run_dir):
        if subdir.is_dir():
            await stack.put(subdir)

    # for path in stack:
    async for path in asyncqgen(stack):
        contents = await scandir(path)
        if await dir_is_flow(contents):
            # this is a flow directory
            yield {
                'name': str(path.relative_to(run_dir)),
                'path': path,
            }
        else:
            # we may have a nested flow, lets see...
            for subdir in contents:
                if subdir.is_dir():
                    await stack.put(subdir)


@Pipe
async def filter_name(flow, pattern):
    """Filter flows by name.

    Args:
        flow (dict):
            Flow information dictionary, provided by scan through the pipe.
        pattern (re.Pattern):
            Compiled regex that flow names must match.

    """
    return bool(pattern.match(flow['name']))


@Pipe
async def is_active(flow, is_active):
    """Filter flows by the presence of a contact file.

    Args:
        flow (dict):
            Flow information dictionary, provided by scan through the pipe.
        is_active (bool):
            True to filter for running flows.
            False to filter for stopped and unregistered flows.

    """
    flow['contact'] = flow['path'] / SERVICE / CONTACT
    return flow['contact'].exists() == is_active


@Pipe
async def contact_info(flow):
    """Read information from the contact file.

    Args:
        flow (dict):
            Flow information dictionary, provided by scan through the pipe.

    """
    flow.update(
        load_contact_file(flow['name'], path=flow['path'])
    )
    return flow


@Pipe
async def query(flow, fields):
    """Obtain information from a GraphQL request to the flow.

    Args:
        flow (dict):
            Flow information dictionary, provided by scan through the pipe.
        fields (list):
            List of GraphQL fields on the `Workflow` object to request from
            the flow.

    """
    field_str = '\n'.join(fields)
    query = f'query {{ workflows(ids: ["{flow["name"]}"]) {{ {field_str} }} }}'
    client = SuiteRuntimeClient(
        flow['name'],
        # use contact_info data if present for efficiency
        host=flow.get('CYLC_SUITE_HOST'),
        port=flow.get('CYLC_SUITE_PORT')
    )
    try:
        ret = await client(
            'graphql',
            {
                'request_string': query,
                'variables': {}
            }
        )
    except ClientTimeout:
        LOG.exception(
            f'Timeout: name: {flow["name"]}, '
            f'host: {client.host}, '
            f'port: {client.port}'
        )
        return False
    except ClientError as exc:
        LOG.exception(exc)
        return False
    else:
        breakpoint()
        return ret
