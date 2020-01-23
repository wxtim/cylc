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
#
# This file contains tests for code in cylc/flow/cfgspec/globalconfig.py

import os
import pytest

from cylc.flow.cfgspec.glbl_cfg import glbl_cfg
from cylc.flow.tests.util import set_up_globalrc
from cylc.flow.tests.test_config_upgrader import GLOBALRC
from cylc.flow.exceptions import PlatformLookupError


@pytest.mark.parametrize(
    'inputs, outputs',
    [
        # If `platfrom` is not set default value for item is returned.
        (
            ('batch system',),
            "background"
        ),
        # If `platform` is set item returned is sensible.
        (
            ('batch system', 'hpc'),
            "pbs"
        ),
        # Get a users local directory.
        (
            ('run directory',),
            f"$HOME/cylc-run"
        ),
        # Run directory for any other platform returns the value with $HOME
        # in place.
        (
            ('run directory', 'desktop99'),
            f"$HOME/cylc-run"
        ),
        # An alternative owner is supplied - commented out because
        # this test is not easily portable
        # (
        #     ('run directory', None, "username"),
        #     f"{os.path.dirname(os.getenv('HOME'))}/username/cylc-run"
        # ),
        # `replace_home` is supplied - forces the function to replace
        # and expanded home directories with $HOME
        (
            ('run directory', None, None, True),
            "$HOME/cylc-run"
        ),
        # owner_home explicitly set
        (
            (
                'run directory', None, 'vroomfrondel', False,
                '/home/users/vroofrondel'
            ),
            "/home/users/vroofrondel/cylc-run"
        ),
    ]
)
def test_get_platform_item(set_up_globalrc, inputs, outputs):
    """
    Ensure that get_platform item works in the desired way.
    """
    set_up_globalrc(GLOBALRC)
    conf = glbl_cfg(cached=False)
    assert conf.get_platform_item(*inputs) == outputs


def test_no_default_platform_item(set_up_globalrc):
    set_up_globalrc(GLOBALRC)
    conf = glbl_cfg(cached=False)
    with pytest.raises(PlatformLookupError):
        conf.get_platform_item('submission polling intervals',)
