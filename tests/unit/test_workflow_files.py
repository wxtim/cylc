# THIS FILE IS PART OF THE CYLC WORKFLOW ENGINE.
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

from cylc.flow.option_parsers import Options
import logging
from pathlib import Path
import pytest
import shutil
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type
from unittest import mock

from cylc.flow import CYLC_LOG
from cylc.flow import workflow_files
from cylc.flow.exceptions import (
    CylcError,
    ServiceFileError,
    TaskRemoteMgmtError,
    WorkflowFilesError
)
from cylc.flow.pathutil import parse_dirs
from cylc.flow.scripts.clean import get_option_parser as _clean_GOP
from cylc.flow.workflow_files import (
    WorkflowFiles,
    check_flow_file,
    check_nested_run_dirs,
    get_workflow_source_dir,
    reinstall_workflow, search_install_source_dirs)

from tests.unit.conftest import MonkeyMock


CleanOpts = Options(_clean_GOP())


@pytest.mark.parametrize(
    'path, expected',
    [('a/b/c', '/mock_cylc_dir/a/b/c'),
     ('/a/b/c', '/a/b/c')]
)
def test_get_cylc_run_abs_path(
    path: str, expected: str,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr('cylc.flow.pathutil._CYLC_RUN_DIR', '/mock_cylc_dir')
    assert workflow_files.get_cylc_run_abs_path(path) == expected


@pytest.mark.parametrize('is_abs_path', [False, True])
def test_is_valid_run_dir(is_abs_path: bool, tmp_run_dir: Callable):
    """Test that a directory is correctly identified as a valid run dir when
    it contains a service dir.
    """
    cylc_run_dir: Path = tmp_run_dir()
    prefix = str(cylc_run_dir) if is_abs_path else ''
    # What if no dir there?
    assert workflow_files.is_valid_run_dir(
        Path(prefix, 'nothing/here')) is False
    # What if only flow.cylc exists but no service dir?
    # (Non-run dirs can still contain flow.cylc)
    run_dir = cylc_run_dir.joinpath('foo/bar')
    run_dir.mkdir(parents=True)
    run_dir.joinpath(WorkflowFiles.FLOW_FILE).touch()
    assert workflow_files.is_valid_run_dir(Path(prefix, 'foo/bar')) is False
    # What if service dir exists?
    run_dir.joinpath(WorkflowFiles.Service.DIRNAME).mkdir()
    assert workflow_files.is_valid_run_dir(Path(prefix, 'foo/bar')) is True


def test_check_nested_run_dirs_parents(tmp_run_dir: Callable):
    """Test that check_nested_run_dirs() raises when a parent dir is a
    workflow directory."""
    cylc_run_dir: Path = tmp_run_dir()
    test_dir = cylc_run_dir.joinpath('a/b/c/d/e')
    test_dir.mkdir(parents=True)
    # Parents are not run dirs - ok:
    workflow_files.check_nested_run_dirs(test_dir, 'e')
    # Parent contains a run dir but that run dir is not direct ancestor
    # of our test dir - ok:
    tmp_run_dir('a/Z')
    workflow_files.check_nested_run_dirs(test_dir, 'e')
    # Now make run dir out of parent - not ok:
    tmp_run_dir('a')
    with pytest.raises(WorkflowFilesError) as exc:
        workflow_files.check_nested_run_dirs(test_dir, 'e')
    assert "Nested run directories not allowed" in str(exc.value)


def test_check_nested_run_dirs_children(tmp_run_dir: Callable):
    """Test that check_nested_run_dirs() raises when a child dir is a
    workflow directory."""
    cylc_run_dir: Path = tmp_run_dir()
    cylc_run_dir.joinpath('a/b/c/d/e').mkdir(parents=True)
    test_dir = cylc_run_dir.joinpath('a')
    # No run dir in children - ok:
    workflow_files.check_nested_run_dirs(test_dir, 'a')
    # Run dir in child - not ok:
    d: Path = tmp_run_dir('a/b/c/d/e')
    with pytest.raises(WorkflowFilesError) as exc:
        workflow_files.check_nested_run_dirs(test_dir, 'a')
    assert "Nested run directories not allowed" in str(exc.value)
    shutil.rmtree(d)
    # Run dir in child but below max scan depth - not ideal but passes:
    tmp_run_dir('a/b/c/d/e/f')
    workflow_files.check_nested_run_dirs(test_dir, 'a')


@pytest.mark.parametrize(
    'reg, expected_err, expected_msg',
    [('foo/bar/', None, None),
     ('/foo/bar', WorkflowFilesError, "cannot be an absolute path"),
     ('$HOME/alone', WorkflowFilesError, "invalid workflow name"),
     ('./foo', WorkflowFilesError, "invalid workflow name")]
)
def test_validate_flow_name(reg, expected_err, expected_msg):
    if expected_err:
        with pytest.raises(expected_err) as exc:
            workflow_files.validate_flow_name(reg)
        if expected_msg:
            assert expected_msg in str(exc.value)
    else:
        workflow_files.validate_flow_name(reg)


@pytest.mark.parametrize(
    'reg, stopped, err, err_msg',
    [('foo/..', True, WorkflowFilesError,
      "cannot be a path that points to the cylc-run directory or above"),
     ('foo/../..', True, WorkflowFilesError,
      "cannot be a path that points to the cylc-run directory or above"),
     ('foo', False, ServiceFileError, "Cannot remove running workflow")]
)
def test_clean_check_fail(
    reg: str,
    stopped: bool,
    err: Type[Exception],
    err_msg: str,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that _clean_check() fails appropriately.

    Params:
        reg: Workflow name.
        stopped: Whether the workflow is stopped when _clean_check() is called.
        err: Expected error class.
        err_msg: Message that is expected to be in the exception.
    """
    run_dir = mock.Mock()

    def mocked_detect_old_contact_file(*a, **k):
        if not stopped:
            raise ServiceFileError('Mocked error')

    monkeypatch.setattr('cylc.flow.workflow_files.detect_old_contact_file',
                        mocked_detect_old_contact_file)

    with pytest.raises(err) as exc:
        workflow_files._clean_check(reg, run_dir)
    assert err_msg in str(exc.value)


@pytest.mark.parametrize(
    'db_platforms, opts, clean_called, remote_clean_called',
    [
        pytest.param(
            ['localhost', 'localhost'], {}, True, False,
            id="Only platform in DB is localhost"
        ),
        pytest.param(
            ['horse'], {}, True, True,
            id="Remote platform in DB"
        ),
        pytest.param(
            ['horse'], {'local_only': True}, True, False,
            id="Local clean only"
        ),
        pytest.param(
            ['horse'], {'remote_only': True}, False, True,
            id="Remote clean only"
        )
    ]
)
def test_init_clean(
    db_platforms: List[str],
    opts: Dict[str, Any],
    clean_called: bool,
    remote_clean_called: bool,
    monkeypatch: pytest.MonkeyPatch, monkeymock: MonkeyMock,
    tmp_run_dir: Callable
) -> None:
    """Test the init_clean() function logic.

    Params:
        db_platforms: Platform names that would be loaded from the database.
        opts: Any options passed to the cylc clean CLI.
        clean_called: If a local clean is expected to go ahead.
        remote_clean_called: If a remote clean is expected to go ahead.
    """
    reg = 'foo/bar/'
    tmp_run_dir(reg)
    mock_clean = monkeymock('cylc.flow.workflow_files.clean')
    mock_remote_clean = monkeymock('cylc.flow.workflow_files.remote_clean')
    monkeypatch.setattr('cylc.flow.workflow_files.get_platforms_from_db',
                        lambda x: set(db_platforms))

    workflow_files.init_clean(reg, opts=CleanOpts(**opts))
    assert mock_clean.called is clean_called
    assert mock_remote_clean.called is remote_clean_called


def test_init_clean_no_dir(
    monkeymock: MonkeyMock, tmp_run_dir: Callable,
    caplog: pytest.LogCaptureFixture
) -> None:
    """Test init_clean() when the run dir doesn't exist"""
    caplog.set_level(logging.INFO, CYLC_LOG)
    tmp_run_dir()
    mock_clean = monkeymock('cylc.flow.workflow_files.clean')
    mock_remote_clean = monkeymock('cylc.flow.workflow_files.remote_clean')

    workflow_files.init_clean('foo/bar', opts=CleanOpts())
    assert "No directory to clean" in caplog.text
    assert mock_clean.called is False
    assert mock_remote_clean.called is False


def test_init_clean_no_db(
    monkeymock: MonkeyMock, tmp_run_dir: Callable,
    caplog: pytest.LogCaptureFixture
) -> None:
    """Test init_clean() when the workflow database doesn't exist"""
    caplog.set_level(logging.INFO, CYLC_LOG)
    tmp_run_dir('bespin')
    mock_clean = monkeymock('cylc.flow.workflow_files.clean')
    mock_remote_clean = monkeymock('cylc.flow.workflow_files.remote_clean')

    workflow_files.init_clean('bespin', opts=CleanOpts())
    assert "No workflow database - will only clean locally" in caplog.text
    assert mock_clean.called is True
    assert mock_remote_clean.called is False


def test_init_clean_remote_only_no_db(
    monkeymock: MonkeyMock, tmp_run_dir: Callable
) -> None:
    """Test remote-only init_clean() when the workflow DB doesn't exist"""
    tmp_run_dir('hoth')
    mock_clean = monkeymock('cylc.flow.workflow_files.clean')
    mock_remote_clean = monkeymock('cylc.flow.workflow_files.remote_clean')

    with pytest.raises(ServiceFileError) as exc:
        workflow_files.init_clean('hoth', opts=CleanOpts(remote_only=True))
    assert ("No workflow database - cannot perform remote clean"
            in str(exc.value))
    assert mock_clean.called is False
    assert mock_remote_clean.called is False


def test_init_clean_running_workflow(
    monkeypatch: pytest.MonkeyPatch, tmp_run_dir: Callable
) -> None:
    """Test init_clean() fails when workflow is still running"""
    def mock_err(*args, **kwargs):
        raise ServiceFileError("Mocked error")
    monkeypatch.setattr('cylc.flow.workflow_files.detect_old_contact_file',
                        mock_err)
    tmp_run_dir('yavin')

    with pytest.raises(ServiceFileError) as exc:
        workflow_files.init_clean('yavin', opts=mock.Mock())
    assert "Cannot remove running workflow" in str(exc.value)


@pytest.mark.parametrize(
    'rm_dirs, expected_clean, expected_remote_clean',
    [(None, None, []),
     (["r2d2:c3po"], {"r2d2", "c3po"}, ["r2d2:c3po"])]
)
def test_init_clean_rm_dirs(
    rm_dirs: Optional[List[str]],
    expected_clean: Set[str],
    expected_remote_clean: List[str],
    monkeymock: MonkeyMock, monkeypatch: pytest.MonkeyPatch,
    tmp_run_dir: Callable
) -> None:
    """Test init_clean() with the --rm option.

    Params:
        rm_dirs: Dirs given by --rm option.
        expected_clean: The dirs that are expected to be passed to clean().
        expected_remote_clean: The dirs that are expected to be passed to
            remote_clean().
    """
    reg = 'dagobah'
    run_dir: Path = tmp_run_dir(reg)
    mock_clean = monkeymock('cylc.flow.workflow_files.clean')
    mock_remote_clean = monkeymock('cylc.flow.workflow_files.remote_clean')
    platforms = {'platform_one'}
    monkeypatch.setattr('cylc.flow.workflow_files.get_platforms_from_db',
                        lambda x: platforms)
    opts = CleanOpts(rm_dirs=rm_dirs) if rm_dirs else CleanOpts()

    workflow_files.init_clean(reg, opts=opts)
    mock_clean.assert_called_with(reg, run_dir, expected_clean)
    mock_remote_clean.assert_called_with(
        reg, platforms, expected_remote_clean, opts.remote_timeout)


@pytest.mark.parametrize(
    'reg, symlink_dirs, rm_dirs, expected_deleted, expected_remaining',
    [
        pytest.param(
            'foo/bar',
            {},
            None,
            ['cylc-run/foo'],
            ['cylc-run'],
            id="Basic clean"
        ),
        pytest.param(
            'foo/bar/baz',
            {
                'log': 'sym-log',
                'share': 'sym-share',
                'share/cycle': 'sym-cycle',
                'work': 'sym-work'
            },
            None,
            ['cylc-run/foo', 'sym-log/cylc-run/foo', 'sym-share/cylc-run/foo',
             'sym-cycle/cylc-run/foo', 'sym-work/cylc-run/foo'],
            ['cylc-run', 'sym-log/cylc-run', 'sym-share/cylc-run',
             'sym-cycle/cylc-run', 'sym-work/cylc-run'],
            id="Symlink dirs"
        ),
        pytest.param(
            'foo',
            {
                'run': 'sym-run',
                'log': 'sym-log',
                'share': 'sym-share',
                'share/cycle': 'sym-cycle',
                'work': 'sym-work'
            },
            None,
            ['cylc-run/foo', 'sym-run/cylc-run/foo', 'sym-log/cylc-run/foo',
             'sym-share/cylc-run/foo', 'sym-cycle/cylc-run/foo',
             'sym-work/cylc-run/foo'],
            ['cylc-run', 'sym-run/cylc-run', 'sym-log/cylc-run',
             'sym-share/cylc-run', 'sym-cycle/cylc-run',
             'sym-work'],
            id="Symlink dirs including run dir"
        ),
        pytest.param(
            'foo',
            {},
            {'log', 'share'},
            ['cylc-run/foo/log', 'cylc-run/foo/share'],
            ['cylc-run/foo/work'],
            id="Targeted clean"
        ),
        pytest.param(
            'foo',
            {'log': 'sym-log'},
            {'log'},
            ['cylc-run/foo/log', 'sym-log/cylc-run/foo'],
            ['cylc-run/foo/work', 'cylc-run/foo/share/cycle',
             'sym-log/cylc-run'],
            id="Targeted clean with symlink dirs"
        ),
        pytest.param(
            'foo',
            {},
            {'share/cy*'},
            ['cylc-run/foo/share/cycle'],
            ['cylc-run/foo/log', 'cylc-run/foo/work', 'cylc-run/foo/share'],
            id="Targeted clean with glob"
        ),
    ]
)
def test_clean(
    reg: str,
    symlink_dirs: Dict[str, str],
    rm_dirs: Optional[Set[str]],
    expected_deleted: List[str],
    expected_remaining: List[str],
    tmp_path: Path, tmp_run_dir: Callable
) -> None:
    """Test the clean() function.

    Params:
        reg: Workflow name.
        symlink_dirs: As you would find in the global config
            under [symlink dirs][platform].
        rm_dirs: As passed to clean().
        expected_deleted: Dirs (relative paths under tmp_path) that are
            expected to be cleaned.
        expected_remaining: Any dirs (relative paths under tmp_path) that are
            not expected to be cleaned.
    """
    # --- Setup ---
    run_dir: Path = tmp_run_dir(reg)

    if 'run' in symlink_dirs:
        dst = tmp_path.joinpath(symlink_dirs['run'], 'cylc-run', reg)
        dst.mkdir(parents=True)
        shutil.rmtree(run_dir)
        run_dir.symlink_to(dst)
        symlink_dirs.pop('run')
    for src_name, dst_name in symlink_dirs.items():
        dst = tmp_path.joinpath(dst_name, 'cylc-run', reg, src_name)
        dst.mkdir(parents=True)
        src = run_dir.joinpath(src_name)
        src.symlink_to(dst)
    for d_name in ('log', 'share', 'share/cycle', 'work'):
        path = run_dir.joinpath(d_name)
        if d_name not in symlink_dirs:
            path.mkdir()

    for rel_path in [*expected_deleted, *expected_remaining]:
        assert tmp_path.joinpath(rel_path).exists()

    # --- The actual test ---
    workflow_files.clean(reg, run_dir, rm_dirs)
    for rel_path in expected_deleted:
        assert tmp_path.joinpath(rel_path).exists() is False
        assert tmp_path.joinpath(rel_path).is_symlink() is False
    for rel_path in expected_remaining:
        assert tmp_path.joinpath(rel_path).exists()


def test_clean_broken_symlink_run_dir(
    tmp_path: Path, tmp_run_dir: Callable
) -> None:
    """Test clean() successfully remove a run dir that is a broken symlink."""
    reg = 'foo/bar'
    run_dir: Path = tmp_run_dir(reg)
    target = tmp_path.joinpath('rabbow/cylc-run', reg)
    target.mkdir(parents=True)
    shutil.rmtree(run_dir)
    run_dir.symlink_to(target)
    target.rmdir()

    assert run_dir.parent.exists() is True
    workflow_files.clean(reg, run_dir)
    assert run_dir.parent.exists() is False


def test_clean_bad_symlink_dir_wrong_type(
    tmp_path: Path, tmp_run_dir: Callable
) -> None:
    """Test clean() raises error when a symlink dir actually points to a file
    instead of a dir"""
    reg = 'foo'
    run_dir: Path = tmp_run_dir(reg)
    src = run_dir.joinpath('log')
    dst = tmp_path.joinpath('sym-log', 'cylc-run', reg, 'meow.txt')
    dst.parent.mkdir(parents=True)
    dst.touch()
    src.symlink_to(dst)

    with pytest.raises(WorkflowFilesError) as exc:
        workflow_files.clean(reg, run_dir)
    assert "Target is not a directory" in str(exc.value)
    assert src.exists() is True


def test_clean_bad_symlink_dir_wrong_form(
    tmp_path: Path, tmp_run_dir: Callable
) -> None:
    """Test clean() raises error when a symlink dir points to an
    unexpected dir"""
    run_dir: Path = tmp_run_dir('foo')
    src = run_dir.joinpath('log')
    dst = tmp_path.joinpath('sym-log', 'oops', 'log')
    dst.mkdir(parents=True)
    src.symlink_to(dst)

    with pytest.raises(WorkflowFilesError) as exc:
        workflow_files.clean('foo', run_dir)
    assert 'Expected target to end with "cylc-run/foo/log"' in str(exc.value)
    assert src.exists() is True


@pytest.mark.parametrize('pattern', ['thing/', 'thing/*'])
def test_clean_rm_dir_not_file(pattern: str, tmp_run_dir: Callable):
    """Test clean() does not remove a file when the rm_dir glob pattern would
    match a dir only."""
    reg = 'foo'
    run_dir: Path = tmp_run_dir(reg)
    a_file = run_dir.joinpath('thing')
    a_file.touch()
    rm_dirs = parse_dirs([pattern])

    workflow_files.clean(reg, run_dir, rm_dirs)
    assert a_file.exists()


PLATFORMS = {
    'enterprise': {
        'hosts': ['kirk', 'picard'],
        'install target': 'picard',
        'name': 'enterprise'
    },
    'voyager': {
        'hosts': ['janeway'],
        'install target': 'janeway',
        'name': 'voyager'
    },
    'stargazer': {
        'hosts': ['picard'],
        'install target': 'picard',
        'name': 'stargazer'
    },
    'exeter': {
        'hosts': ['localhost'],
        'install target': 'localhost',
        'name': 'exeter'
    }
}


@pytest.mark.parametrize(
    'install_targets_map, failed_platforms, expected_platforms, expected_err',
    [
        (
            {'localhost': [PLATFORMS['exeter']]}, None, None, None
        ),
        (
            {
                'localhost': [PLATFORMS['exeter']],
                'picard': [PLATFORMS['enterprise']]
            },
            None,
            ['enterprise'],
            None
        ),
        (
            {
                'picard': [PLATFORMS['enterprise'], PLATFORMS['stargazer']],
                'janeway': [PLATFORMS['voyager']]
            },
            None,
            ['enterprise', 'voyager'],
            None
        ),
        (
            {
                'picard': [PLATFORMS['enterprise'], PLATFORMS['stargazer']],
                'janeway': [PLATFORMS['voyager']]
            },
            ['enterprise'],
            ['enterprise', 'stargazer', 'voyager'],
            None
        ),
        (
            {
                'picard': [PLATFORMS['enterprise'], PLATFORMS['stargazer']],
                'janeway': [PLATFORMS['voyager']]
            },
            ['enterprise', 'stargazer'],
            ['enterprise', 'stargazer', 'voyager'],
            (CylcError, "Could not clean on install targets: picard")
        ),
        (
            {
                'picard': [PLATFORMS['enterprise']],
                'janeway': [PLATFORMS['voyager']]
            },
            ['enterprise', 'voyager'],
            ['enterprise', 'voyager'],
            (CylcError, "Could not clean on install targets: picard, janeway")
        )
    ]
)
def test_remote_clean(
    install_targets_map: Dict[str, Any],
    failed_platforms: Optional[List[str]],
    expected_platforms: Optional[List[str]],
    expected_err: Optional[Tuple[Type[Exception], str]],
    monkeymock: MonkeyMock, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    """Test remote_clean() logic.

    Params:
        install_targets_map The map that would be returned by
            platforms.get_install_target_to_platforms_map()
        failed_platforms: If specified, any platforms that clean will
            artificially fail on in this test case.
        expected_platforms: If specified, all the platforms that the
            remote clean cmd is expected to run on.
        expected_err: If specified, a tuple of the form
            (Exception, str) giving an exception that is expected to be raised.
    """
    # ----- Setup -----
    caplog.set_level(logging.DEBUG, CYLC_LOG)
    monkeypatch.setattr(
        'cylc.flow.workflow_files.get_install_target_to_platforms_map',
        lambda x: install_targets_map)
    # Remove randomness:
    monkeymock('cylc.flow.workflow_files.shuffle')

    def mocked_remote_clean_cmd_side_effect(reg, platform, rm_dirs, timeout):
        proc_ret_code = 0
        if failed_platforms and platform['name'] in failed_platforms:
            proc_ret_code = 1
        return mock.Mock(
            poll=lambda: proc_ret_code,
            communicate=lambda: ("", ""),
            args=[])

    mocked_remote_clean_cmd = monkeymock(
        'cylc.flow.workflow_files._remote_clean_cmd',
        side_effect=mocked_remote_clean_cmd_side_effect)
    rm_dirs = ["whatever"]
    # ----- Test -----
    reg = 'foo'
    platform_names = (
        "This arg bypassed as we provide the install targets map in the test")
    if expected_err:
        err, msg = expected_err
        with pytest.raises(err) as exc:
            workflow_files.remote_clean(
                reg, platform_names, rm_dirs, timeout='irrelevant')
        assert msg in str(exc.value)
    else:
        workflow_files.remote_clean(
            reg, platform_names, rm_dirs, timeout='irrelevant')
    if expected_platforms:
        for p_name in expected_platforms:
            mocked_remote_clean_cmd.assert_any_call(
                reg, PLATFORMS[p_name], rm_dirs, 'irrelevant')
    else:
        mocked_remote_clean_cmd.assert_not_called()
    if failed_platforms:
        for p_name in failed_platforms:
            assert f"{p_name}: {TaskRemoteMgmtError.MSG_TIDY}" in caplog.text


@pytest.mark.parametrize(
    'rm_dirs, expected_args',
    [
        (None, []),
        (['holodeck', 'ten_forward'],
         ['--rm', 'holodeck', '--rm', 'ten_forward'])
    ]
)
def test_remote_clean_cmd(
    rm_dirs: Optional[List[str]],
    expected_args: List[str],
    monkeymock: MonkeyMock
) -> None:
    """Test _remote_clean_cmd()

    Params:
        rm_dirs: Argument passed to _remote_clean_cmd().
        expected_args: Expected CLI arguments of the cylc clean command that
            gets constructed.
    """
    reg = 'jean/luc/picard'
    platform = {'name': 'enterprise', 'install target': 'mars'}
    mock_construct_ssh_cmd = monkeymock(
        'cylc.flow.workflow_files.construct_ssh_cmd', return_value=['blah'])
    monkeymock('cylc.flow.workflow_files.Popen')

    workflow_files._remote_clean_cmd(reg, platform, rm_dirs, timeout='dunno')
    args, kwargs = mock_construct_ssh_cmd.call_args
    constructed_cmd = args[0]
    assert constructed_cmd == ['clean', '--local-only', reg, *expected_args]


def test_remove_empty_reg_parents(tmp_path):
    """Test that _remove_empty_parents() doesn't remove parents containing a
    sibling."""
    reg = 'foo/bar/baz/qux'
    path = tmp_path.joinpath(reg)
    tmp_path.joinpath('foo/bar/baz').mkdir(parents=True)
    sibling_reg = 'foo/darmok'
    sibling_path = tmp_path.joinpath(sibling_reg)
    sibling_path.mkdir()
    workflow_files._remove_empty_reg_parents(reg, path)
    assert tmp_path.joinpath('foo/bar').exists() is False
    assert tmp_path.joinpath('foo').exists() is True
    # Also path must be absolute
    with pytest.raises(ValueError) as exc:
        workflow_files._remove_empty_reg_parents(
            'foo/darmok', 'meow/foo/darmok')
    assert 'Path must be absolute' in str(exc.value)
    # Check it skips non-existent dirs, and stops at the right place too
    tmp_path.joinpath('foo/bar').mkdir()
    sibling_path.rmdir()
    workflow_files._remove_empty_reg_parents(reg, path)
    assert tmp_path.joinpath('foo').exists() is False
    assert tmp_path.exists() is True


@pytest.mark.parametrize(
    'run_dir, srv_dir',
    [
        ('a', 'a/R/.service'),
        ('d/a', 'd/a/a/R/.service'),
        ('z/d/a/a', 'z/d/a/a/R/.service')
    ]
)
def test_symlinkrundir_children_that_contain_workflows_raise_error(
        run_dir, srv_dir, monkeypatch):
    """Test that a workflow cannot be contained in a subdir of another
    workflow."""
    monkeypatch.setattr('cylc.flow.workflow_files.os.path.isdir',
                        lambda x: False if (
                            x.find('.service') > 0 and x != srv_dir)
                        else True)
    monkeypatch.setattr(
        'cylc.flow.workflow_files.get_cylc_run_abs_path',
        lambda x: x)
    monkeypatch.setattr('cylc.flow.workflow_files.os.scandir',
                        lambda x: [
                            mock.Mock(path=srv_dir[0:len(x) + 2],
                                      is_symlink=lambda: True)])

    try:
        check_nested_run_dirs(run_dir, 'placeholder_flow')
    except ServiceFileError:
        pytest.fail(
            "Unexpected ServiceFileError, Check symlink logic.")


def test_get_workflow_source_dir_numbered_run(tmp_path):
    """Test get_workflow_source_dir returns correct source for numbered run"""
    cylc_install_dir = (
        tmp_path /
        "cylc-run" /
        "flow-name" /
        "_cylc-install")
    cylc_install_dir.mkdir(parents=True)
    run_dir = (tmp_path / "cylc-run" / "flow-name" / "run1")
    run_dir.mkdir()
    source_dir = (tmp_path / "cylc-source" / "flow-name")
    source_dir.mkdir(parents=True)
    assert get_workflow_source_dir(run_dir) == (None, None)
    (cylc_install_dir / "source").symlink_to(source_dir)
    assert get_workflow_source_dir(run_dir) == (
        str(source_dir), cylc_install_dir / "source")


def test_get_workflow_source_dir_named_run(tmp_path):
    """Test get_workflow_source_dir returns correct source for named run"""
    cylc_install_dir = (
        tmp_path /
        "cylc-run" /
        "flow-name" /
        "_cylc-install")
    cylc_install_dir.mkdir(parents=True)
    source_dir = (tmp_path / "cylc-source" / "flow-name")
    source_dir.mkdir(parents=True)
    (cylc_install_dir / "source").symlink_to(source_dir)
    assert get_workflow_source_dir(
        cylc_install_dir.parent) == (
        str(source_dir),
        cylc_install_dir / "source")


def test_reinstall_workflow(tmp_path, capsys):

    cylc_install_dir = (
        tmp_path /
        "cylc-run" /
        "flow-name" /
        "_cylc-install")
    cylc_install_dir.mkdir(parents=True)
    source_dir = (tmp_path / "cylc-source" / "flow-name")
    source_dir.mkdir(parents=True)
    (source_dir / "flow.cylc").touch()

    (cylc_install_dir / "source").symlink_to(source_dir)
    run_dir = cylc_install_dir.parent
    reinstall_workflow("flow-name", run_dir, source_dir)
    assert capsys.readouterr().out == (
        f"REINSTALLED flow-name from {source_dir}\n")


@pytest.mark.parametrize(
    'filename, expected_err',
    [('flow.cylc', None),
     ('suite.rc', None),
     ('fluff.txt', (WorkflowFilesError, "Could not find workflow 'baa/baa'"))]
)
def test_search_install_source_dirs(
        filename: str, expected_err: Optional[Tuple[Type[Exception], str]],
        tmp_path: Path, mock_glbl_cfg: Callable):
    """Test search_install_source_dirs().

    Params:
        filename: A file to insert into one of the source dirs.
        expected_err: Exception and message expected to be raised.
    """
    horse_dir = Path(tmp_path, 'horse')
    horse_dir.mkdir()
    sheep_dir = Path(tmp_path, 'sheep')
    source_dir = sheep_dir.joinpath('baa', 'baa')
    source_dir.mkdir(parents=True)
    source_dir_file = source_dir.joinpath(filename)
    source_dir_file.touch()
    mock_glbl_cfg(
        'cylc.flow.workflow_files.glbl_cfg',
        f'''
        [install]
            source dirs = {horse_dir}, {sheep_dir}
        '''
    )
    if expected_err:
        err, msg = expected_err
        with pytest.raises(err) as exc:
            search_install_source_dirs('baa/baa')
        assert msg in str(exc.value)
    else:
        flow_file = search_install_source_dirs('baa/baa')
        assert flow_file == source_dir


def test_search_install_source_dirs_empty(mock_glbl_cfg: Callable):
    """Test search_install_source_dirs() when no source dirs configured."""
    mock_glbl_cfg(
        'cylc.flow.workflow_files.glbl_cfg',
        '''
        [install]
            source dirs =
        '''
    )
    with pytest.raises(WorkflowFilesError) as exc:
        search_install_source_dirs('foo')
    assert str(exc.value) == (
        "Cannot find workflow as 'global.cylc[install]source dirs' "
        "does not contain any paths")


@pytest.mark.parametrize(
    'flow_file_exists, suiterc_exists, expected_file',
    [(True, False, WorkflowFiles.FLOW_FILE),
     (True, True, WorkflowFiles.FLOW_FILE),
     (False, True, WorkflowFiles.SUITE_RC)]
)
def test_check_flow_file(
    flow_file_exists: bool, suiterc_exists: bool, expected_file: str,
    tmp_path: Path
) -> None:
    """Test check_flow_file() returns the expected path.

    Params:
        flow_file_exists: Whether a flow.cylc file is found in the dir.
        suiterc_exists: Whether a suite.rc file is found in the dir.
        expected_file: Which file's path should get returned.
    """
    if flow_file_exists:
        tmp_path.joinpath(WorkflowFiles.FLOW_FILE).touch()
    if suiterc_exists:
        tmp_path.joinpath(WorkflowFiles.SUITE_RC).touch()

    assert check_flow_file(tmp_path) == tmp_path.joinpath(expected_file)


@pytest.mark.parametrize(
    'flow_file_target, suiterc_exists, err, expected_file',
    [
        pytest.param(
            WorkflowFiles.SUITE_RC, True, None, WorkflowFiles.FLOW_FILE,
            id="flow.cylc symlinked to suite.rc"
        ),
        pytest.param(
            WorkflowFiles.SUITE_RC, False, WorkflowFilesError, None,
            id="flow.cylc symlinked to non-existent suite.rc"
        ),
        pytest.param(
            'other-path', True, None, WorkflowFiles.SUITE_RC,
            id="flow.cylc symlinked to other file, suite.rc exists"
        ),
        pytest.param(
            'other-path', False, WorkflowFilesError, None,
            id="flow.cylc symlinked to other file, no suite.rc"
        ),
        pytest.param(
            None, True, None, WorkflowFiles.SUITE_RC,
            id="no flow.cylc, suite.rc exists"
        ),
        pytest.param(
            None, False, WorkflowFilesError, None,
            id="no flow.cylc, no suite.rc"
        ),
    ]
)
@pytest.mark.parametrize(
    'symlink_suiterc_arg',
    [pytest.param(False, id="symlink_suiterc=False "),
     pytest.param(True, id="symlink_suiterc=True ")]
)
def test_check_flow_file_symlink(
    flow_file_target: Optional[str],
    suiterc_exists: bool,
    err: Optional[Type[Exception]],
    expected_file: Optional[str],
    symlink_suiterc_arg: bool,
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Test check_flow_file() when flow.cylc is a symlink or doesn't exist.

    Params:
        flow_file_target: Relative path of the flow.cylc symlink's target, or
            None if the symlink doesn't exist.
        suiterc_exists: Whether there is a suite.rc file in the dir.
        err: Type of exception if expected to get raised.
        expected_file: Which file's path should get returned, when
            symlink_suiterc_arg is FALSE (otherwise it will always be
            flow.cylc, assuming no exception occurred).
        symlink_suiterc_arg: Value of the symlink_suiterc arg passed to
            check_flow_file().
    """
    flow_file = tmp_path.joinpath(WorkflowFiles.FLOW_FILE)
    suiterc = tmp_path.joinpath(WorkflowFiles.SUITE_RC)
    tmp_path.joinpath('other-path').touch()
    if suiterc_exists:
        suiterc.touch()
    if flow_file_target:
        flow_file.symlink_to(flow_file_target)
    log_msg = (
        f'The filename "{WorkflowFiles.SUITE_RC}" is deprecated '
        f'in favour of "{WorkflowFiles.FLOW_FILE}"')
    caplog.set_level(logging.WARNING, CYLC_LOG)

    if err:
        with pytest.raises(err):
            check_flow_file(tmp_path, symlink_suiterc_arg)
    else:
        assert expected_file is not None  # otherwise test is wrong
        result = check_flow_file(tmp_path, symlink_suiterc_arg)
        if symlink_suiterc_arg is True:
            assert flow_file.samefile(suiterc)
            expected_file = WorkflowFiles.FLOW_FILE
            if flow_file_target != WorkflowFiles.SUITE_RC:
                log_msg = f'{log_msg}. Symlink created.'
        assert result == tmp_path.joinpath(expected_file)
        assert caplog.messages == [log_msg]
