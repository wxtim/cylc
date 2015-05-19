#!/usr/bin/env python

# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2015 NIWA
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

try:
    import Pyro.core
except ImportError, x:
    raise SystemExit("ERROR: Pyro is not installed")

import os
import sys
from time import sleep
from uuid import uuid4

import cylc.flags
from cylc.suite_host import get_hostname
from cylc.owner import user, user_at_host
from cylc.port_file import port_retriever
from cylc.network.client_reporter import PyroClientReporter

from Pyro.protocol import DefaultConnValidator
import Pyro.constants
import hmac
try:
	import hashlib
	md5=hashlib.md5
except ImportError:
	import md5
	md5=md5.md5


PASSPHRASES = {
    'free': "364bad4d2149634bc642d75914cd3d2b", # 'suite-identity'
    'info': "30f3c93e46436deb58ba70816a8ec124", # 'the quick brown fox'
    'control': "30f3c93e46436deb58ba70816a8ec124" # 'the quick brown fox'
}

# Example username/password database: (passwords stored in ascii md5 hash)
#EXAMPLE_ALLOWED_USERS = {
#	"irmen": "5ebe2294ecd0e0f08eab7690d2a6ee69",		# 'secret'
#	"guest": "084e0343a0486ff05530df6c705c8bb4",		# 'guest'
#	"root": "bbbb2edef660739a6071ab5a4f8a869f",			# 'change_me'
#}

#	Example login/password validator.
#	Passwords are protected using md5 so they are not stored in plaintext.
#	The actual identification check is done using a hmac-md5 secure hash.

# passphrase(suite,user,get_hostname()).get(suitedir=suitedir)

class ConnValidator(DefaultConnValidator):
    def acceptIdentification(self, daemon, connection, token, challenge):
        # extract tuple (login, processed password) from token as returned by createAuthToken
        # processed password is a hmac hash from the server's challenge string and the password itself.
        login, processedpassword = token.split(':', 1)
        print "LOGIN:", login, processedpassword
        # Check if the username/password is valid.
        # Known passwords are stored as ascii hash, but the auth token contains a binary hash.
        # So we need to convert our ascii hash to binary to be able to validate.
        for priv, pphrase in PASSPHRASES.items():
            if hmac.new(challenge, pphrase.decode("hex")).digest() == processedpassword:
                print "ALLOWED %s (%s)" % (login, priv)
                connection.authenticated = login  # store for later reference by Pyro object
                connection.privelege = priv
                return (1, 0)
        print "DENIED %s" % login
        return (0, Pyro.constants.DENIED_SECURITY)
  
    def createAuthToken(self, authid, challenge, peeraddr, URI, daemon):
        # authid is what mungeIdent returned, a tuple (login, hash-of-password)
        # we return a secure auth token based on the server challenge string.
        return "%s:%s" % (authid[0], hmac.new(challenge,authid[1]).digest() )

    def mungeIdent(self, ident):
        # ident is tuple (login, password), the client sets this.
        # we don't like to store plaintext passwords so store the md5 hash instead.
        return (ident[0], md5(ident[1]).digest())


class PyroServer(Pyro.core.ObjBase):
    """Base class for server-side suite object interfaces."""

    def __init__(self):
        Pyro.core.ObjBase.__init__(self)
        self.client_reporter = PyroClientReporter.get_inst()

    def signout(self, uuid, info):
        self.client_reporter.signout(uuid, info)

    def report(self, command, uuid, info, multi):
        self.client_reporter.report(command, uuid, info, multi)


class PyroClient(object):
    """Base class for client-side suite object interfaces."""

    target_server_object = None

    def __init__(
        self, suite, pphrase, owner=user, host=get_hostname(),
        pyro_timeout=None, port=None, my_uuid=None):

        self.suite = suite
        self.host = host
        self.owner = owner
        if pyro_timeout is not None:
            pyro_timeout = float(pyro_timeout)
        self.pyro_timeout = pyro_timeout
        self.pphrase = pphrase
        self.hard_port = port
        self.pyro_proxy = None
        # Multi-client programs (cylc-gui) can give their own client ID:
        self.my_uuid = my_uuid or uuid4()
        # Possibly non-unique client info:
        self.my_info = {
            'user_at_host': user_at_host,
            'name': os.path.basename(sys.argv[0])
        }
        self.multi = False

    def get_client_uuid(self):
        return self.my_uuid

    def set_multi(self):
        """Declare this to be a multi-connect client (GUI, monitor)."""
        self.multi = True

    def reset(self):
        """Cause _get_proxy() to start from scratch."""
        self.pyro_proxy = None

    def _get_proxy(self):
        """Get the Pyro proxy if we don't already have it."""
        if self.pyro_proxy is None:
            # The following raises a PortFileError if the port file is not found.
            port = (self.hard_port or
                    port_retriever(self.suite, self.host, self.owner).get())
            uri = "PYROLOC://%s:%s/%s" % (
                self.host, str(port), self.__class__.target_server_object)
            # The following only fails for unknown hosts.
            # No connection is made until an RPC call is attempted.
            self.pyro_proxy = Pyro.core.getProxyForURI(uri)
            self.pyro_proxy._setTimeout(self.pyro_timeout)

            #self.pyro_proxy._setIdentification(self.pphrase)
            self.pyro_proxy._setNewConnectionValidator(ConnValidator())
            self.pyro_proxy._setIdentification(("bob", "the quick brown fox"))

    def signout(self):
        """Multi-connect clients should call this on exit."""
        try:
            self._get_proxy()
            try:
                self.pyro_proxy.signout(self.my_uuid, self.my_info)
            except AttributeError:
                # Back compat.
                pass
        except Exception:
            # Suite may have stopped before the client exits.
            pass

    def _report(self, command):
        self._get_proxy()
        try:
            self.pyro_proxy.report(
                command.replace(' ', '_'), self.my_uuid, self.my_info, self.multi)
        except AttributeError:
            # Back compat.
            pass 
