from pathlib import Path
import re
from shutil import rmtree
from tempfile import TemporaryDirectory
from textwrap import dedent

import pytest

from cylc.flow.network.scan_nt import (
    cylc_version,
    scan,
    filter_name,
    is_active,
    contact_info
)
from cylc.flow.suite_files import (
    ContactFileFields,
    SuiteFiles
)


SRV_DIR = Path(SuiteFiles.Service.DIRNAME)
CONTACT = Path(SuiteFiles.Service.CONTACT)


def init_flows(tmp_path, running=None, registered=None, un_registered=None):
    """Create some dummy workflows for scan to discover."""
    for name in (running or []):
        path = Path(tmp_path, name, SRV_DIR)
        path.mkdir(parents=True, exist_ok=True)
        (path / CONTACT).touch()
    for name in (registered or []):
        Path(tmp_path, name, SRV_DIR).mkdir(parents=True, exist_ok=True)
    for name in (un_registered or []):
        Path(tmp_path, name).mkdir(parents=True, exist_ok=True)
    # chuck a file in there just to be annoying
    # Path(tmp_path, 'this-is-a-random-file').touch()
    # TODO - stick this in a separate test


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
    # one regular workflow
    init_flows(
        tmp_path,
        running=('foo',)
    )
    # and an erroneous service dir at the top level for no reason
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
        # make it nested to proove that the link is followed
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


def test_scan(sample_run_dir):
    """It should list all flows."""
    assert list(sorted(scan(sample_run_dir))) == [
        'bar/pub',
        'baz',
        'foo'
    ]


def test_scan_filter_active(sample_run_dir):
    """It should filter flows by suite state."""
    assert list(sorted(
        scan(sample_run_dir, is_active=True)
    )) == [
        'bar/pub',
        'foo'
    ]
    assert list(sorted(
        scan(sample_run_dir, is_active=False)
    )) == [
        'baz'
    ]


def test_scan_filter_name(sample_run_dir):
    """It should filter flows using regex patterns."""
    assert list(sorted(
        scan(sample_run_dir, patterns=['.*'])
    )) == [
        'bar/pub',
        'baz',
        'foo'
    ]
    assert list(sorted(
        scan(sample_run_dir, patterns=['bar.*'])
    )) == [
        'bar/pub',
    ]
    assert list(sorted(
        scan(sample_run_dir, patterns=['bar/pub'])
    )) == [
        'bar/pub',
    ]
    assert list(sorted(scan(
        sample_run_dir, patterns=['bar/pub'])
    )) == [
        'bar/pub',
    ]
    assert list(sorted(scan(
        sample_run_dir, patterns=['bar/pub', 'foo'])
    )) == [
        'bar/pub',
        'foo'
    ]


def test_scan_horrible_mess(badly_messed_up_run_dir):
    """It shouldn't be effected by erroneous cylc files/dirs.

    How could you end up with a .service dir in cylc-run, well misuse of 
    Cylc7 can result in this situation so this test ensures Cylc7 suites
    can't mess up a Cylc8 scan.

    """
    assert list(sorted(
        scan(badly_messed_up_run_dir)
    )) == [
        'foo'
    ]


def test_scan_symlinks(run_dir_with_symlinks):
    """It should follow symlinks to flows in other dirs."""
    assert list(sorted(
        scan(run_dir_with_symlinks)

    )) == [
        'bar/baz',
        'foo'
    ]


def test_scan_nasty_symlinks(run_dir_with_nasty_symlinks):
    """It should handle strange symlinks because users can be nasty."""
    assert list(sorted(
        scan(run_dir_with_nasty_symlinks)

    )) == [
        'bar',  # well you got what you asked for
        'foo'
    ]


def test_scan_really_nasty_symlinks(run_dir_with_really_nasty_symlinks):
    """It should handle infinite symlinks because users can be really nasty."""
    with pytest.raises(OSError):
        list(scan(run_dir_with_really_nasty_symlinks))


def test_filter_name_preprocess():
    """It should combine provided patterns and compile them."""
    pipe = filter_name('^f', '^c')
    assert pipe.args[0] == re.compile('(^f|^c)')


@pytest.mark.asyncio
async def test_filter_name():
    """It should filter flows by registration name."""
    pipe = filter_name('^f')
    assert await pipe.func(
        {'name': 'foo'},
        *pipe.args
    )
    assert not await pipe.func(
        {'name': 'bar'},
        *pipe.args
    )


@pytest.mark.asyncio
async def test_is_active(sample_run_dir):
    """It should filter flows by presence of a contact file."""
    # running flows
    assert await is_active.func(
        {'path': sample_run_dir / 'foo'},
        True
    )
    assert await is_active.func(
        {'path': sample_run_dir / 'bar/pub'},
        True
    )
    # registered flows
    assert not await is_active.func(
        {'path': sample_run_dir / 'baz'},
        True
    )
    # unregistered flows
    assert not await is_active.func(
        {'path': sample_run_dir / 'qux'},
        True
    )
    # non-existent flows
    assert not await is_active.func(
        {'path': sample_run_dir / 'elephant'},
        True
    )


@pytest.mark.asyncio
async def test_cylc_version():
    version = ContactFileFields.VERSION

    pipe = cylc_version('>= 8.0a1, < 9')
    assert await pipe.func(
        {version: '8.0a1'},
        *pipe.args
    )

    pipe = cylc_version('>= 8.0a1, < 9')
    assert not await pipe.func(
        {version: '7.8.4'},
        *pipe.args
    )


@pytest.mark.asyncio
async def test_contact_info(tmp_path):
    """It should load info from the contact file."""
    # create a dummy flow
    Path(tmp_path, 'foo', SRV_DIR).mkdir(parents=True)
    # write a contact file with some junk in it
    with open(Path(tmp_path, 'foo', SRV_DIR, CONTACT), 'w+') as contact:
        contact.write(dedent('''
            foo=1
            bar=2
            baz=3
        ''').strip())
    # create a flow dict as returned by scan
    flow = {
        'name': 'foo',
        'path': tmp_path / 'foo'
    }
    # ensure the contact fields get added to the flow dict
    assert await contact_info.func(flow) == {
        **flow,
        'foo': '1',
        'bar': '2',
        'baz': '3'
    }
