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
"""Cylc site and user configuration file spec."""

import os
import re

from cylc.flow import LOG
from cylc.flow import __version__ as CYLC_VERSION
from cylc.flow.hostuserutil import get_user_home, is_remote_user
from cylc.flow.network.authorisation import Priv
from cylc.flow.parsec.config import ParsecConfig
from cylc.flow.parsec.exceptions import ParsecError
from cylc.flow.parsec.upgrade import upgrader
from cylc.flow.parsec.validate import (
    DurationFloat, CylcConfigValidator as VDR, cylc_config_validate)
from cylc.flow.platform_lookup import forward_lookup
from cylc.flow.exceptions import PlatformLookupError

# Nested dict of spec items.
# Spec value is [value_type, default, allowed_2, allowed_3, ...]
# where:
# - value_type: value type (compulsory).
# - default: the default value (optional).
# - allowed_2, ...: the only other allowed values of this setting (optional).
SPEC = {
    # suite
    'process pool size': [VDR.V_INTEGER, 4],
    'process pool timeout': [VDR.V_INTERVAL, DurationFloat(600)],
    # client
    'disable interactive command prompts': [VDR.V_BOOLEAN, True],
    # suite
    'run directory rolling archive length': [VDR.V_INTEGER, -1],
    # suite
    'cylc': {
        'UTC mode': [VDR.V_BOOLEAN],
        'health check interval': [VDR.V_INTERVAL, DurationFloat(600)],
        'task event mail interval': [VDR.V_INTERVAL, DurationFloat(300)],
        'events': {
            'handlers': [VDR.V_STRING_LIST],
            'handler events': [VDR.V_STRING_LIST],
            'mail events': [VDR.V_STRING_LIST],
            'mail from': [VDR.V_STRING],
            'mail smtp': [VDR.V_STRING],
            'mail to': [VDR.V_STRING],
            'mail footer': [VDR.V_STRING],
            'startup handler': [VDR.V_STRING_LIST],
            'timeout handler': [VDR.V_STRING_LIST],
            'inactivity handler': [VDR.V_STRING_LIST],
            'shutdown handler': [VDR.V_STRING_LIST],
            'aborted handler': [VDR.V_STRING_LIST],
            'stalled handler': [VDR.V_STRING_LIST],
            'timeout': [VDR.V_INTERVAL],
            'inactivity': [VDR.V_INTERVAL],
            'abort on timeout': [VDR.V_BOOLEAN],
            'abort on inactivity': [VDR.V_BOOLEAN],
            'abort on stalled': [VDR.V_BOOLEAN],
        },
    },

    # suite
    'suite logging': {
        'rolling archive length': [VDR.V_INTEGER, 5],
        'maximum size in bytes': [VDR.V_INTEGER, 1000000],
    },

    # general
    'documentation': {
        'local': [VDR.V_STRING, ''],
        'online': [VDR.V_STRING,
                   'http://cylc.github.io/doc/built-sphinx/index.html'],
        'cylc homepage': [VDR.V_STRING, 'http://cylc.github.io/'],
    },

    # general
    'document viewers': {
        'html': [VDR.V_STRING, 'firefox'],
    },

    # client
    'editors': {
        'terminal': [VDR.V_STRING, 'vim'],
        'gui': [VDR.V_STRING, 'gvim -f'],
    },

    # client
    'monitor': {
        'sort order': [VDR.V_STRING, 'definition', 'alphanumeric'],
    },

    # job platforms
    'job platforms': {
        '__MANY__': {
            'batch system': [VDR.V_STRING, 'background'],
            'batch submit command template': [VDR.V_STRING],
            'shell': [VDR.V_STRING, '/bin/bash'],
            'run directory': [VDR.V_STRING, '$HOME/cylc-run'],
            'work directory': [VDR.V_STRING, '$HOME/cylc-run'],
            'suite definition directory': [VDR.V_STRING],
            'task communication method': [
                VDR.V_STRING, 'zmq', 'poll'],
            # TODO ensure that it is possible to over-ride the following three
            # settings in suite config.
            'submission polling intervals': [VDR.V_INTERVAL_LIST],
            'submission retry delays': [VDR.V_INTERVAL_LIST, None],
            'execution polling intervals': [VDR.V_INTERVAL_LIST],
            'execution time limit polling intervals': [VDR.V_INTERVAL_LIST],
            'scp command': [
                VDR.V_STRING, 'scp -oBatchMode=yes -oConnectTimeout=10'],
            'ssh command': [
                VDR.V_STRING, 'ssh -oBatchMode=yes -oConnectTimeout=10'],
            'use login shell': [VDR.V_BOOLEAN, True],
            'remote hosts': [VDR.V_STRING_LIST],
            'cylc executable': [VDR.V_STRING, 'cylc'],
            'global init-script': [VDR.V_STRING],
            'copyable environment variables': [VDR.V_STRING_LIST, ''],
            'retrieve job logs': [VDR.V_BOOLEAN],
            'retrieve job logs command': [VDR.V_STRING, 'rsync -a'],
            'retrieve job logs max size': [VDR.V_STRING],
            'retrieve job logs retry delays': [VDR.V_INTERVAL_LIST],
            'task event handler retry delays': [VDR.V_INTERVAL_LIST],
            'tail command template': [
                VDR.V_STRING, 'tail -n +1 -F %(filename)s'],
            'err tailer': [VDR.V_STRING],
            'out tailer': [VDR.V_STRING],
            'err viewer': [VDR.V_STRING],
            'out viewer': [VDR.V_STRING],
            'job name length maximum': [VDR.V_INTEGER],
            'owner': [VDR.V_STRING],
        },
    },

    # Platform Groups
    'platform groups': {
        '__MANY__': {
            'platforms': [VDR.V_STRING_LIST]
        }
    },

    # task
    'task events': {
        'execution timeout': [VDR.V_INTERVAL],
        'handlers': [VDR.V_STRING_LIST],
        'handler events': [VDR.V_STRING_LIST],
        'handler retry delays': [VDR.V_INTERVAL_LIST, None],
        'mail events': [VDR.V_STRING_LIST],
        'mail from': [VDR.V_STRING],
        'mail retry delays': [VDR.V_INTERVAL_LIST],
        'mail smtp': [VDR.V_STRING],
        'mail to': [VDR.V_STRING],
        'submission timeout': [VDR.V_INTERVAL],
    },

    # client
    'test battery': {
        'remote host with shared fs': [VDR.V_STRING],
        'remote host': [VDR.V_STRING],
        'remote owner': [VDR.V_STRING],
        'batch systems': {
            '__MANY__': {
                'host': [VDR.V_STRING],
                'out viewer': [VDR.V_STRING],
                'err viewer': [VDR.V_STRING],
                'directives': {
                    '__MANY__': [VDR.V_STRING],
                },
            },
        },
    },

    # suite
    'suite host self-identification': {
        'method': [VDR.V_STRING, 'name', 'address', 'hardwired'],
        'target': [VDR.V_STRING, 'google.com'],
        'host': [VDR.V_STRING],
    },

    # suite
    'authentication': {
        # Allow owners to grant public shutdown rights at the most, not full
        # control.
        'public': (
            [VDR.V_STRING]
            + [level.name.lower().replace('_', '-') for level in [
                Priv.STATE_TOTALS, Priv.IDENTITY, Priv.DESCRIPTION,
                Priv.STATE_TOTALS, Priv.READ, Priv.SHUTDOWN]])
    },

    # suite
    'suite servers': {
        'run hosts': [VDR.V_SPACELESS_STRING_LIST],
        'run ports': [VDR.V_INTEGER_LIST, list(range(43001, 43101))],
        'condemned hosts': [VDR.V_ABSOLUTE_HOST_LIST],
        'auto restart delay': [VDR.V_INTERVAL],
        'run host select': {
            'rank': [VDR.V_STRING, 'random', 'load:1', 'load:5', 'load:15',
                     'memory', 'disk-space'],
            'thresholds': [VDR.V_STRING],
        },
    },
}


def upg(cfg, descr):
    """Upgrader."""
    u = upgrader(cfg, descr)

    u.obsolete('6.4.1', ['test battery', 'directives'])
    u.obsolete('6.11.0', ['state dump rolling archive length'])
    # Roll over is always done.
    u.obsolete('7.8.0', ['suite logging', 'roll over at start-up'])
    u.obsolete('7.8.1', ['documentation', 'local index'])
    u.obsolete('7.8.1', ['documentation', 'files', 'pdf user guide'])
    u.obsolete('7.8.1', ['documentation', 'files',
                         'single-page html user guide'])
    u.deprecate('7.8.1',
                ['documentation', 'files', 'multi-page html user guide'],
                ['documentation', 'local'])
    u.deprecate('8.0.0',
                ['documentation', 'files', 'html index'],
                ['documentation', 'local'])
    u.deprecate('8.0.0',
                ['documentation', 'urls', 'internet homepage'],
                ['documentation', 'cylc homepage'])
    u.obsolete('8.0.0', ['suite servers', 'scan hosts'])
    u.obsolete('8.0.0', ['suite servers', 'scan ports'])
    u.obsolete('8.0.0', ['communication'])
    u.obsolete('8.0.0', ['temporary directory'])
    u.obsolete('8.0.0', ['task host select command timeout'])
    u.obsolete('8.0.0', ['xtrigger function timeout'])
    u.obsolete('8.0.0', ['enable run directory housekeeping'])
    u.obsolete('8.0.0', ['task messaging'])

    u.upgrade()


class GlobalConfig(ParsecConfig):
    """
    Handle global (all suites) site and user configuration for cylc.
    User file values override site file values.
    """

    _DEFAULT = None
    _HOME = os.getenv('HOME') or get_user_home()
    CONF_BASENAME = "flow.rc"
    SITE_CONF_DIR = os.path.join(os.sep, 'etc', 'cylc', 'flow', CYLC_VERSION)
    USER_CONF_DIR = os.path.join(_HOME, '.cylc', 'flow', CYLC_VERSION)

    @classmethod
    def get_inst(cls, cached=True):
        """Return a GlobalConfig instance.

        Args:
            cached (bool):
                If cached create if necessary and return the singleton
                instance, else return a new instance.
        """
        if not cached:
            # Return an up-to-date global config without affecting the
            # singleton.
            new_instance = cls(SPEC, upg, validator=cylc_config_validate)
            new_instance.load()
            return new_instance
        elif not cls._DEFAULT:
            cls._DEFAULT = cls(SPEC, upg, validator=cylc_config_validate)
            cls._DEFAULT.load()
        return cls._DEFAULT

    def load(self):
        """Load or reload configuration from files."""
        self.sparse.clear()
        self.dense.clear()
        LOG.debug("Loading site/user config files")
        conf_path_str = os.getenv("CYLC_CONF_PATH")
        if conf_path_str:
            # Explicit config file override.
            fname = os.path.join(conf_path_str, self.CONF_BASENAME)
            if os.access(fname, os.F_OK | os.R_OK):
                self.loadcfg(fname, upgrader.USER_CONFIG)
        elif conf_path_str is None:
            # Use default locations.
            for conf_dir, conf_type in [
                    (self.SITE_CONF_DIR, upgrader.SITE_CONFIG),
                    (self.USER_CONF_DIR, upgrader.USER_CONFIG)]:
                fname = os.path.join(conf_dir, self.CONF_BASENAME)
                if not os.access(fname, os.F_OK | os.R_OK):
                    continue
                try:
                    self.loadcfg(fname, conf_type)
                except ParsecError as exc:
                    if conf_type == upgrader.SITE_CONFIG:
                        # Warn on bad site file (users can't fix it).
                        LOG.warning(
                            'ignoring bad %s %s:\n%s', conf_type, fname, exc)
                    else:
                        # Abort on bad user file (users can fix it).
                        LOG.error('bad %s %s', conf_type, fname)
                        raise
        # (OK if no flow.rc is found, just use system defaults).
        # TODO: Consider refactor of self._transform.
        # self._transform()

    @staticmethod
    def get_platform_item_for_job(job_conf, item):
        # Returns platform item job from the job config and item
        return get_platform_item(item, job_conf['platform'])

    def get_platform_item(
        self, item, platform=None, owner=None, replace_home=False,
        owner_home=None
    ):
        """
        Allows us to access platform items. If platform items have not
        been set these are filled in from defaults

        Args:
            item (str):
                The item from the spec we want to get.

        Kwargs:
            platform (str):
                The name of the platform we are interested in getting
                information about. Defaults to None.
            owner (str):
                If set this method will attempt to discover if this owner
                is a remote user. If they are then ...
                Defaults to None.
            replace_home (bool):
                If True this method will attempt to replace explicit
                references to self._HOME with "$HOME" in any config item
                whose key contains the word "directory".
                Defaults to False.
            owner_home (str):
                If set this value will replace self._HOME in any config item
                whose key contains the word "directory".
                Defaults to None.

        Returns:
            A value assigned to an item in the config. Can be almost any
            type.

        Todo:
            Convert all strings representing paths to using pathlib.Path
            objects.

        Chart:

        """
        # Check for the existence of the item we want in the platform specified
        # Or use default values.
        if platform:
            platform, raw_platform = forward_lookup(
                self.get(['job platforms']), platform
            )
            if (item in self.get(['job platforms', raw_platform])):
                value = self.get(['job platforms', raw_platform, item])
        else:
            try:
                value = self.spec['job platforms']['__MANY__'][item][1]
            except IndexError as exc:
                msg = (f"Cannot get platform item because '{item}' has "
                       f"no default value.")
                raise PlatformLookupError(msg)
        # Deal with cases where the setting is a directory.
        if 'directory' in item and value:
            if replace_home:
                # Replace local home dir with $HOME for eval'n on other host.
                value = value.replace(self._HOME, '$HOME')
            elif is_remote_user(owner):
                # Replace with ~owner for direct access via local filesys
                # (works for standard cylc-run directory location).
                if owner_home is None:
                    owner_home = os.path.expanduser('~%s' % owner)
                value = value.replace(self._HOME, owner_home)
                value = value.replace('$HOME', owner_home)
        return value

    def _transform(self):
        """Transform various settings.

        Host item values of None default to modified localhost values.
        Expand environment variables and ~ notations.

        Ensure os.environ['HOME'] is defined with the correct value.
        """
        cfg = self.get()

        for host in cfg['hosts']:
            if host == 'localhost':
                continue
            for item, value in cfg['hosts'][host].items():
                if value is None:
                    newvalue = cfg['hosts']['localhost'][item]
                else:
                    newvalue = value
                if newvalue and 'directory' in item:
                    # replace local home dir with $HOME for evaluation on other
                    # host
                    newvalue = newvalue.replace(self._HOME, '$HOME')
                cfg['hosts'][host][item] = newvalue

        # Expand environment variables and ~user in LOCAL file paths.
        if 'HOME' not in os.environ:
            os.environ['HOME'] = self._HOME
        cfg['documentation']['local'] = os.path.expandvars(
            cfg['documentation']['local'])
        for key, val in cfg['hosts']['localhost'].items():
            if val and 'directory' in key:
                cfg['hosts']['localhost'][key] = os.path.expandvars(val)
