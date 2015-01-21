#!/usr/bin/env python

#C: THIS FILE IS PART OF THE CYLC SUITE ENGINE.
#C: Copyright (C) 2008-2014 NIWA
#C:
#C: This program is free software: you can redistribute it and/or modify
#C: it under the terms of the GNU General Public License as published by
#C: the Free Software Foundation, either version 3 of the License, or
#C: (at your option) any later version.
#C:
#C: This program is distributed in the hope that it will be useful,
#C: but WITHOUT ANY WARRANTY; without even the implied warranty of
#C: MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#C: GNU General Public License for more details.
#C:
#C: You should have received a copy of the GNU General Public License
#C: along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os, sys
from suite_host import is_remote_host, get_hostname
from owner import user, is_remote_user
from cylc.cfgspec.globalcfg import GLOBAL_CFG
import flags

"""Processes connecting to a running suite must know which port the
suite server is listening on: at start-up cylc writes the port to
$HOME/.cylc/ports/SUITE.

Task messaging commands know the port number of the target suite from
the task execution environment supplied by the suite: $CYLC_SUITE_PORT,
so they do not need to read the port file (they do not use this class).

Other cylc commands: on the suite host read the port file; on remote
hosts use passwordless ssh to read the port file on the suite host. If
passwordless ssh to the suite host is not configured this will fail and
the user will have to give the port number on the command line."""


class PortFileError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)


class port_file(object):
    """Write or remove a suite port file on localhost."""

    def __init__(self, suite, port):
        self.suite = suite
        # The port file directory is assumed to exist.
        pdir = GLOBAL_CFG.get(['pyro', 'ports directory'])
        self.local_path = os.path.join(pdir, suite)
        self.hostname = get_hostname()
        try:
            self.port = int(port)
        except ValueError:
            raise PortFileError(
                    "ERROR, illegal port number: %s" % str(port))

    def write(self):
        if os.path.exists(self.local_path):
            sys.stderr.write(
                    "ERROR, port file exists:\n  %s" % self.local_path)
            # The hostname here determines where the file is read. The one
            # recorded in the file may be different on a shared filesystem.
            old_port_file = port_retriever(self.suite, self.hostname, user)
            try:
                port, hostname = old_port_file.get()
            except Exception as exc:
                print >> sys.stderr, str(exc)
            else:
                sys.stderr.write(" (%s:%d)\n" % (hostname, port))
            print >> sys.stderr, (
"Use 'cylc scan' on %s to see if %s is still running.\n"
"If it is dead, kill any left-over processes (you can find them with\n"
"\"pgrep -flu $USER %s\", for example) and then delete the port file.\n" % (
    hostname, self.suite, self.suite)
            )
            raise PortFileError( "ERROR, port file already exists")
        if flags.verbose:
            print "Writing port file: %s" % self.local_path
        try:
            f = open(self.local_path, 'w')
        except OSError:
            # Port file exists.

            raise SchedulerError( 'Suite already running? (if not, delete the port file)' )

            raise PortFileError(
                    "ERROR, failed to open port file: %s" % self.port)
        f.write("%d\n%s" % (self.port, self.hostname))
        f.close()

    def unlink(self):
        if flags.verbose:
            print "Removing port file: %s" % self.local_path
        try:
            os.unlink(self.local_path)
        except OSError as exc:
            print >> sys.stderr, str(exc)
            raise PortFileError(
                    "ERROR, cannot remove port file: %s" % self.local_path)

class port_retriever(object):
    """Read a suite port file on a host."""

    def __init__(self, suite, host, owner):
        self.suite = suite
        self.host = host
        self.owner = owner
        self.file_path = None
        self.local_path = os.path.join(
                GLOBAL_CFG.get(['pyro', 'ports directory']), suite)

    def _get_local(self):
        self.file_path = self.local_path
        if not os.path.exists(self.local_path):
            raise PortFileError(
                    "ERROR, port file not found: %s" % self.local_path)
        f = open(self.local_path, 'r')
        str_port = f.readline().rstrip()
        try:
            hostname = f.readline().rstrip()
        except:
            # Back compat (<=cylc-6.1.2 port number only)
            hostname = None
        f.close()
        return (str_port, hostname)

    def _get_remote(self):
        # For remote cylc clients (e.g. tasks?).
        import subprocess
        target = self.owner + '@' + self.host
        remote_path = self.local_path.replace(os.environ['HOME'], '$HOME')
        self.file_path = target + ':' + remote_path
        ssh = subprocess.Popen(
                ['ssh', '-oBatchMode=yes', target, 'cat', remote_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
                )
        str_port = ssh.stdout.readline().rstrip()
        hostname = ssh.stdout.readline().rstrip()
        err = ssh.stderr.readline()
        res = ssh.wait()
        if err:
            print >> sys.stderr, err.rstrip()
        if res != 0:
            raise PortFileError("ERROR, remote port file not found")
        return (str_port, hostname)

    def get(self):
        if flags.verbose:
            print "Reading suite port file..."

        if is_remote_host(self.host) or is_remote_user(self.owner):
            _get = self._get_remote
        else:
            _get = self._get_local
        str_port, hostname = _get()
        try:
            port = int(str_port)
        except ValueError:
            print >> sys.stderr, "ERROR: bad port file %s" % self.file_path
            raise PortFileError(
                    "ERROR, illegal port file content: %s" % str_port)
 
        if flags.verbose:
            sys.stdout.write(" > port %d" % port)
            if hostname is None:
                sys.stdout.write("\n")
            else:
                sys.stdout.write(" on %s\n" % hostname)

        return port, hostname
