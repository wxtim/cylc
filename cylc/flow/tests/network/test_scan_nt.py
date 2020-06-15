from pathlib import Path
import re
from shutil import rmtree
from tempfile import TemporaryDirectory

import pytest

from cylc.flow.network.scan_nt import (
    scan,
    filter_name
)
from cylc.flow.suite_files import SuiteFiles


SRV_DIR = Path(SuiteFiles.Service.DIRNAME)


def init_flows(tmp_path, running=None, registered=None, un_registered=None):
    """Create some dummy workflows for scan to discover."""
    for name in (running or []):
        path = Path(tmp_path, name, SRV_DIR)
        path.mkdir(parents=True, exist_ok=True)
        Path(path, SuiteFiles.Service.CONTACT).touch()
    for name in (registered or []):
        Path(tmp_path, name, SRV_DIR).mkdir(parents=True, exist_ok=True)
    for name in (un_registered or []):
        Path(tmp_path, name).mkdir(parents=True, exist_ok=True)
    # chuck a file in there just to be annoying
    # Path(tmp_path, 'this-is-a-random-file').touch()
    # TODO - stick this in a separate test


@pytest.fixture(scope='session')
def sample_run_dir():
    tmp_path = Path(TemporaryDirectory().name)
    tmp_path.mkdir()
    init_flows(
        tmp_path,
        running=('foo', 'bar/pub'),
        registered=('baz',),
        un_registered=('qux',)
    )
    yield tmp_path
    rmtree(tmp_path)


@pytest.fixture(scope='session')
def badly_messed_up_run_dir():
    tmp_path = Path(TemporaryDirectory().name)
    tmp_path.mkdir()
    # one regular workflow
    init_flows(
        tmp_path,
        running=('foo',)
    )
    # and an erroneous service dir at the top level for no reason
    Path(tmp_path, SRV_DIR).mkdir()
    yield tmp_path
    rmtree(tmp_path)


@pytest.fixture(scope='session')
def run_dir_with_symlinks():
    tmp_path = Path(TemporaryDirectory().name)
    tmp_path.mkdir()
    # one regular workflow
    init_flows(
        tmp_path,
        running=('foo',)
    )
    # one symlinked workflow
    tmp_path2 = Path(TemporaryDirectory().name)
    tmp_path2.mkdir()
    init_flows(
        tmp_path2,
        # make it nested to proove that the link is followed
        running=('bar/baz',)
    )
    Path(tmp_path, 'bar').symlink_to(Path(tmp_path2, 'bar'))
    yield tmp_path
    rmtree(tmp_path)


@pytest.fixture(scope='session')
def run_dir_with_nasty_symlinks():
    tmp_path = Path(TemporaryDirectory().name)
    tmp_path.mkdir()
    # one regular workflow
    init_flows(
        tmp_path,
        running=('foo',)
    )
    # and a symlink pointing back at it in the same dir
    Path(tmp_path, 'bar').symlink_to(Path(tmp_path, 'foo'))
    yield tmp_path
    rmtree(tmp_path)


@pytest.fixture(scope='session')
def run_dir_with_really_nasty_symlinks():
    tmp_path = Path(TemporaryDirectory().name)
    tmp_path.mkdir()
    flow = Path(tmp_path, 'foo')
    flow.mkdir(parents=True)
    Path(flow, 'bar').symlink_to(flow)
    yield tmp_path
    rmtree(tmp_path)


# def test_scan(sample_run_dir):
#     """It should list all flows."""
#     assert list(sorted(scan(sample_run_dir))) == [
#         'bar/pub',
#         'baz',
#         'foo'
#     ]


# def test_scan_filter_active(sample_run_dir):
#     """It should filter flows by suite state."""
#     assert list(sorted(
#         scan(sample_run_dir, is_active=True)
#     )) == [
#         'bar/pub',
#         'foo'
#     ]
#     assert list(sorted(
#         scan(sample_run_dir, is_active=False)
#     )) == [
#         'baz'
#     ]


# def test_scan_filter_name(sample_run_dir):
#     """It should filter flows using regex patterns."""
#     assert list(sorted(
#         scan(sample_run_dir, patterns=['.*'])
#     )) == [
#         'bar/pub',
#         'baz',
#         'foo'
#     ]
#     assert list(sorted(
#         scan(sample_run_dir, patterns=['bar.*'])
#     )) == [
#         'bar/pub',
#     ]
#     assert list(sorted(
#         scan(sample_run_dir, patterns=['bar/pub'])
#     )) == [
#         'bar/pub',
#     ]
#     assert list(sorted(scan(
#         sample_run_dir, patterns=['bar/pub'])
#     )) == [
#         'bar/pub',
#     ]
#     assert list(sorted(scan(
#         sample_run_dir, patterns=['bar/pub', 'foo'])
#     )) == [
#         'bar/pub',
#         'foo'
#     ]


# def test_scan_horrible_mess(badly_messed_up_run_dir):
#     """It shouldn't be effected by erroneous cylc files/dirs.

#     How could you end up with a .service dir in cylc-run, well misuse of 
#     Cylc7 can result in this situation so this test ensures Cylc7 suites
#     can't mess up a Cylc8 scan.

#     """
#     assert list(sorted(
#         scan(badly_messed_up_run_dir)
#     )) == [
#         'foo'
#     ]


# def test_scan_symlinks(run_dir_with_symlinks):
#     """It should follow symlinks to flows in other dirs."""
#     assert list(sorted(
#         scan(run_dir_with_symlinks)

#     )) == [
#         'bar/baz',
#         'foo'
#     ]


# def test_scan_nasty_symlinks(run_dir_with_nasty_symlinks):
#     """It should handle strange symlinks because users can be nasty."""
#     assert list(sorted(
#         scan(run_dir_with_nasty_symlinks)

#     )) == [
#         'bar',  # well you got what you asked for
#         'foo'
#     ]


# def test_scan_really_nasty_symlinks(run_dir_with_really_nasty_symlinks):
#     """It should handle infinite symlinks because users can be really nasty."""
#     with pytest.raises(OSError):
#         list(scan(run_dir_with_really_nasty_symlinks))


def test_filter():
    filter_name(
        {'name': 'foo'},
        re.compile('^f')
    )


def test_scan_filter(sample_run_dir):
    assert list(
        filter_name(
            scan(
                sample_run_dir
            ),
            re.compile('^f')
        )
    ) == [{
        'name': 'foo',
        'path': sample_run_dir / 'foo'
    }]
