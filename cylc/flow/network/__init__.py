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
"""Package for network interfaces to cylc suite server objects."""

import asyncio
import getpass
import json
import os
from threading import Thread
from time import sleep

import zmq
import zmq.asyncio

from cylc.flow import LOG
from cylc.flow.exceptions import ClientError, CylcError, SuiteServiceFileError
from cylc.flow.hostuserutil import get_fqdn_by_host
from cylc.flow.suite_files import (
    ContactFileFields,
    create_auth_files
    get_auth_item,
    load_contact_file,
    SuiteFiles,
    UserFiles
)

API = 5  # cylc API version


def encode_(message):
    """Convert the structure holding a message field from JSON to a string."""
    return json.dumps(message)


def decode_(message):
    """Convert an encoded message string to JSON with an added 'user' field."""
    msg = json.loads(message)
    msg['user'] = getpass.getuser()  # assume this is the user
    return msg


def get_location(suite: str, owner: str, host: str):
    """Extract host and port from a suite's contact file.

    NB: if it fails to load the suite contact file, it will exit.

    Args:
        suite (str): suite name
        owner (str): owner of the suite
        host (str): host name
    Returns:
        Tuple[str, int, int]: tuple with the host name and port numbers.
    Raises:
        ClientError: if the suite is not running.
    """
    try:
        contact = load_contact_file(
            suite, owner, host)
    except SuiteServiceFileError:
        raise ClientError(f'Contact info not found for suite '
                          f'"{suite}", suite not running?')

    if not host:
        host = contact[ContactFileFields.HOST]
    host = get_fqdn_by_host(host)

    port = int(contact[ContactFileFields.PORT])
    pub_port = int(contact[ContactFileFields.PUBLISH_PORT])
    return host, port, pub_port


class ZMQSocketBase:
    """Initiate the ZMQ socket bind for specified pattern on new thread.

    NOTE: Security to be provided via zmq.auth (see PR #3359).

    Args:
        pattern (enum): ZeroMQ message pattern (zmq.PATTERN).

        context (object, optional): instantiated ZeroMQ context, defaults
            to zmq.asyncio.Context().

        barrier (object, optional): threading.Barrier object for syncing with
            other threads.

        threaded (bool, optional): Start socket on separate thread.

        daemon (bool, optional): daemonise socket thread.

    This class is designed to be inherited by REP Server (REQ/REP)
    and by PUB Publisher (PUB/SUB), as the start-up logic is similair.


    To tailor this class overwrite it's method on inheritance.

    """

    def __init__(self, pattern, suite=None, bind=False, context=None,
                 barrier=None, threaded=False, daemon=False):
        self.bind = bind
        if context is None:
            self.context = zmq.asyncio.Context()
        else:
            self.context = context
        self.barrier = barrier
        self.pattern = pattern
        self.daemon = daemon
        self.suite = suite
        self.host = None
        self.port = None
        self.socket = None
        self.threaded = threaded
        self.thread = None
        self.loop = None
        self.stopping = False

    def start(self, *args, **kwargs):
        """Start the server/network-component.

        Pass arguments to _start_
        """
        if self.threaded:
            self.thread = Thread(
                target=self._start_sequence,
                args=args,
                kwargs=kwargs,
                daemon=self.daemon
            )
            self.thread.start()
        else:
            self._start_sequence(*args, **kwargs)

    def _start_sequence(self, *args, **kwargs):
        """Create the thread async loop, and bind socket."""
        # set asyncio loop on thread
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        if self.bind:
            self._socket_bind(*args, **kwargs)
        else:
            self._socket_connect(*args, **kwargs)

        # initiate bespoke items
        self._bespoke_start()

    def _socket_bind(self, min_port, max_port, private_key_location=None):
        """Bind socket.

        Will use a port range provided to select random ports.

        """
        if private_key_location is None:
            private_key_location = get_auth_item(
                SuiteFiles.Service.SERVER_PRIVATE_KEY_CERTIFICATE,
                self.suite
            )
        # create socket
        self.socket = self.context.socket(self.pattern)
        self._socket_options()
        server_public_key, server_private_key = zmq.auth.load_certificate(
            private_key_location)
        self.socket.curve_publickey = server_public_key
        self.socket.curve_secretkey = server_private_key
        self.socket.curve_server = True

        try:
            if min_port == max_port:
                self.port = min_port
                self.socket.bind(f'tcp://*:{min_port}')
            else:
                self.port = self.socket.bind_to_random_port(
                    'tcp://*', min_port, max_port)
        except (zmq.error.ZMQError, zmq.error.ZMQBindError) as exc:
            raise CylcError(f'could not start Cylc ZMQ server: {exc}')

        if self.barrier is not None:
            self.barrier.wait()

    def _socket_connect(self, host, port, srv_public_key_loc=None):
        """Connect socket to stub."""
        if srv_public_key_loc is None:
            srv_public_key_loc = get_auth_item(
                SuiteFiles.Service.SERVER_PUBLIC_KEY_CERTIFICATE,
                self.suite,
                content=False)

        self.host = host
        self.port = port
        self.socket = self.context.socket(self.pattern)
        self._socket_options()

        # check for, & create if nonexistent, user keys in the right location
        # if not ensure_user_keys_exist():
        #     raise ClientError("Unable to generate user authentication keys.")

        client_priv_keyfile = os.path.join(
            SuiteFiles.get_user_certificate_full_path(private=True),
            SuiteFiles.Service.CLIENT_PRIVATE_KEY_CERTIFICATE)
        error_msg = "Failed to find user's private key, so cannot connect."
        try:
            client_public_key, client_priv_key = zmq.auth.load_certificate(
                client_priv_keyfile)
        except (OSError, ValueError):
            raise ClientError(error_msg)
        if client_priv_key is None:  # this can't be caught by exception
            raise ClientError(error_msg)
        self.socket.curve_publickey = client_public_key
        self.socket.curve_secretkey = client_priv_key

        # A client can only connect to the server if it knows its public key,
        # so we grab this from the location it was created on the filesystem:
        try:
            # 'load_certificate' will try to load both public & private keys
            # from a provided file but will return None, not throw an error,
            # for the latter item if not there (as for all public key files)
            # so it is OK to use; there is no method to load only the
            # public key.
            server_public_key = zmq.auth.load_certificate(
                srv_public_key_loc)[0]
            self.socket.curve_serverkey = server_public_key
        except (OSError, ValueError):  # ValueError raised w/ no public key
            raise ClientError(
                "Failed to load the suite's public key, so cannot connect.")

        self.socket.connect(f'tcp://{host}:{port}')

    def _socket_options(self):
        """Set socket options.

        i.e. self.socket.sndhwm
        """
        self.socket.sndhwm = 10000

    def _bespoke_start(self):
        """Initiate bespoke items on thread at start."""
        self.stopping = False
        sleep(0)  # yield control to other threads

    def stop(self, stop_loop=True):
        """Stop the server.

        Args:
            stop_loop (Boolean): Stop running IOLoop of current thread.

        """
        self._bespoke_stop()
        if stop_loop and self.loop and self.loop.is_running():
            self.loop.stop()
        if self.thread and self.thread.is_alive():
            self.thread.join()  # Wait for processes to return
        if self.socket and not self.socket.closed:
            self.socket.close()
        LOG.debug('...stopped')

    def _bespoke_stop(self):
        """Bespoke stop items."""
        LOG.debug('stopping zmq socket...')
        self.stopping = True
