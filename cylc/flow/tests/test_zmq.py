"""Test abstract ZMQ interface."""

from pathlib import Path
import pytest
from tempfile import TemporaryDirectory

from cylc.flow.cfgspec.glbl_cfg import glbl_cfg
from cylc.flow.exceptions import CylcError
from cylc.flow.network.authentication import (
    encode_, decode_, generate_key_store)
from cylc.flow.network.server import ZMQServer
from cylc.flow.suite_srv_files_mgr import SuiteSrvFilesManager


def get_port_range():
    ports = glbl_cfg().get(['suite servers', 'run ports'])
    return min(ports), max(ports)


PORT_RANGE = get_port_range()


def test_single_port():
    """Test server on a single port and port in use exception."""
    # Create a mock '.cylc' directory to allow the sever keys to be stored in
    # the usual location, under '~/.cylc/.curve'.
    with TemporaryDirectory() as homedir:
        curve_directory = Path(homedir, SuiteSrvFilesManager.DIR_BASE_ETC,
                               SuiteSrvFilesManager.DIR_BASE_AUTH_KEYS)
        curve_directory.mkdir(parents=True)

        serv1 = ZMQServer(encode_, decode_)
        serv2 = ZMQServer(encode_, decode_)

        serv1.start(*PORT_RANGE)
        port = serv1.port

        with pytest.raises(CylcError, match=r"Address already in use") as exc:
            serv2.start(port, port)

        serv1.stop()
