import re
from collections import namedtuple
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main
from unittest.mock import patch

from cylc.flow import flags
from cylc.flow.exceptions import SuiteServiceFileError
from cylc.flow.network.scan import get_scan_items_from_fs


class CaptureStderr(object):
    """Used to mock sys.stderr"""
    lines = []

    def write(self, line):
        """Captures lines written to stderr.

        Args:
            line (str): line written to stderr
        """
        self.lines.append(line)


class TestScan(TestCase):
    """Tests for cylc.flow.network.scan."""

    def setUp(self):
        """Create a simple namedtuple to mock the objects used/returned by the
        pwd module."""
        self.pwentry = namedtuple('PwEntry', ['pw_shell', 'pw_name', 'pw_dir'])
        flags.debug = False

    # --- tests for get_scan_items_from_fs()

    def _get_ignored_shells(self):
        return ["/false", "/bin/false", "/nologin", "/opt/local/bin/nologin"]

    @patch("cylc.flow.network.scan.getpwall")
    def test_get_scan_items_from_fs_with_owner_noshell(self, mocked_getpwall):
        """Test that passwd entries using shells that end with /false or
        /nologin are ignored.

        Args:
            mocked_getpwall (object): mocked pwd.getpwall
        """
        mocked_getpwall.return_value = [
            self.pwentry(shell, 'root', '/root')
            for shell in self._get_ignored_shells()
        ]
        owner_pattern = re.compile(pattern=".*")
        suites = list(get_scan_items_from_fs(owner_pattern=owner_pattern))
        self.assertEqual(0, len(suites))

    @patch("cylc.flow.network.scan.getpwall")
    def test_get_scan_items_from_fs_with_owner_pattern_mismatch(
            self, mocked_getpwall):
        """Test that passwd entries with users that do not match the pattern
        used are ignored.

        Args:
            mocked_getpwall (object): mocked pwd.getpwall
        """
        # mock pwd.getpwall
        mocked_getpwall.return_value = [
            self.pwentry('/bin/bash', 'root', '/root'),
            self.pwentry('/bin/bash', 'proot', '/root'),
            self.pwentry('/bin/bash', 'wheels', '/root'),
            self.pwentry('/bin/bash', 'docker', '/root')
        ]
        owner_pattern = re.compile(pattern=".*__sheep__.*")
        suites = list(get_scan_items_from_fs(owner_pattern=owner_pattern))
        self.assertEqual(0, len(suites))

    @patch("cylc.flow.network.scan.sys")
    def test_get_scan_items_from_fs_with_owner_debug(self, mocked_sys):
        """Test that when debug is enabled.

        Args:
            mocked_sys (object): mocked sys
        """
        # mock sys.stderr.write
        flags.debug = True
        mocked_sys.stderr = CaptureStderr()
        # will include a debug message with the empty list
        owner_pattern = re.compile(pattern=".*__sheep__.*")
        list(get_scan_items_from_fs(owner_pattern=owner_pattern))
        self.assertTrue(len(mocked_sys.stderr.lines) > 0)

    @patch("cylc.flow.network.scan.SuiteSrvFilesManager")
    @patch("cylc.flow.network.scan.getpwall")
    def test_get_scan_items_from_fs_with_owner_do_not_descend(
            self, mocked_getpwall, mocked_srv_files_mgr):
        """Test that it does not descend when it finds a directory called "log".

        Args:
            mocked_getpwall (object): mocked pwd.getpwall
            mocked_srv_files_mgr (object): mocked SuiteSrvFilesManager
        """
        with TemporaryDirectory() as homedir:
            # mock pwd.getpwall
            mocked_getpwall.return_value = [
                self.pwentry('/bin/bash', 'root', homedir),
            ]
            suite_directory = Path(homedir, 'cylc-run', 'blog', 'five', 'log',
                                   'five')
            suite_directory.mkdir(parents=True)
            # mock srv_files_mgr.load_contact_file
            mocked_srv_files_mgr.return_value.get_suite_source_dir \
                .return_value = 'DIR'
            mocked_srv_files_mgr.return_value.get_suite_title \
                .return_value = 'TITLE'
            owner_pattern = re.compile(pattern="^.oo.$")
            reg_pattern = re.compile(pattern="^.*five$")
            suites = list(get_scan_items_from_fs(
                owner_pattern=owner_pattern, reg_pattern=reg_pattern,
                active_only=False))
            # will match blog/five, but will stop once it finds the log dir
            self.assertEqual([('blog/five', 'DIR', 'TITLE')], suites)

    @patch("cylc.flow.network.scan.SuiteSrvFilesManager")
    @patch("cylc.flow.network.scan.getpwall")
    def test_get_scan_items_from_fs_with_owner_active_only(
            self, mocked_getpwall, mocked_srv_files_mgr):
        """Test that only active suites are returned if so requested.

        Args:
            mocked_getpwall (object): mocked pwd.getpwall
            mocked_srv_files_mgr (object): mocked SuiteSrvFilesManager
        """
        # mock sr
        with TemporaryDirectory() as homedir:
            # mock pwd.getpwall
            mocked_getpwall.return_value = [
                self.pwentry('/bin/bash', 'root', homedir),
            ]
            mocked_srv_files_mgr.return_value.KEY_HOST = 'host'
            mocked_srv_files_mgr.return_value.KEY_PORT = 'port'
            # mock srv_files_mgr.load_contact_file
            def mocked_load_contact_file(reg, _):
                if reg == 'good':
                    return {
                        'host': 'localhost',
                        'port': 9999
                    }
                else:
                    raise SuiteServiceFileError(reg)
            mocked_srv_files_mgr.return_value.load_contact_file = \
                mocked_load_contact_file
            for suite_name in ["good", "bad", "ugly"]:
                suite_directory = Path(homedir, 'cylc-run', suite_name)
                suite_directory.mkdir(parents=True)
                # mock srv_files_mgr.load_contact_file
                owner_pattern = re.compile(pattern="^.oo.$")
                suites = list(get_scan_items_from_fs(
                    owner_pattern=owner_pattern, active_only=True))
                # will match blog/five, but will stop once it finds the log dir
                self.assertEqual([('good', 'localhost', 9999)], suites)


if __name__ == '__main__':
    main()
