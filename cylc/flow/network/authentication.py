
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
"""Modification for ZMQ Authentication."""


# Required for correct operation where tasks are submitted
# after the suite has been started (cylc submit/insert)
# This will load new keys only on demand i.e. when a new client
# tries to connect.
# This is instead of polling the keys folder.


from zmq.auth.certs import load_certificates
from cylc.flow import LOG


class CredentialsProvider(object):
    """Custom authentication for ZMQ CURVE callback

    Attributes:
        auth:               curve.auth, reference to the
                            Thread Authenticator object
        client_pub_key_dir: client public key directory contained
                            in .service directory
    """

    def __init__(self, authenticator, client_pub_key_dir):
        self.auth = authenticator
        self.client_pub_key_dir = client_pub_key_dir

    def callback(self, domain, key):
        """This is called by ZMQ's _authenticate_curve.
            Loads client public keys on demand.
        """
        if self.auth.certs[domain].get(key):
            return True
        else:
            # Reload keys in public client key folder
            # --------------------------------------------------
            # As suggested in PyZMQ auth/base.py
            # We are not able to call configure_curve to reload key files
            # because of how its front-end API receives the
            # command.
            # In auth/thread.py they only ensure that such commands
            # are processed before then next incoming socket
            # connection, which means we would have to reject the
            # first attempt a new platform client makes to connect
            # and rely on them retrying.
            try:
                # Direct call to PyZMQ load_certificates to ensure
                # encoding is correct.
                # Bypassing configure_curve
                self.auth.certs[domain] = load_certificates(
                    self.client_pub_key_dir)
            except Exception as e:
                LOG.error(
                    f"Failed to load CURVE certs"
                    f" from {self.client_pub_key_dir}: {e}")

            # Check client key again now that we have forced
            # key reload
            if self.auth.certs[domain].get(key):
                return True
            else:
                return False
