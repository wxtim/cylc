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

from contextlib import redirect_stdout
import io
import sys
from types import SimpleNamespace
from typing import List

import pytest
from pytest import param

import cylc.flow.flags
from cylc.flow.option_parsers import (
    CylcOptionParser as COP, Options, combine_options, combine_options_pair,
    ARGS, HELP, KWARGS, SOURCES, cleanup_sysargv
)


USAGE_WITH_COMMENT = "usage \n # comment"


@pytest.fixture(scope='module')
def parser():
    return COP(
        USAGE_WITH_COMMENT,
        argdoc=[('SOME_ARG', "Description of SOME_ARG")]
    )


@pytest.mark.parametrize(
    'args,verbosity',
    [
        ([], 0),
        (['-v'], 1),
        (['-v', '-v', '-v'], 3),
        (['-q'], -1),
        (['-q', '-q', '-q'], -3),
        (['-q', '-v', '-q'], -1),
        (['--debug'], 2),
        (['--debug', '-q'], 1),
        (['--debug', '-v'], 3),
    ]
)
def test_verbosity(
    args: List[str],
    verbosity: int,
    parser: COP, monkeypatch: pytest.MonkeyPatch
) -> None:
    """-v, -q, --debug should be additive."""
    # patch the cylc.flow.flags value so that it gets reset after the test
    monkeypatch.setattr('cylc.flow.flags.verbosity', None)
    opts, args = parser.parse_args(['default-arg'] + args)
    assert opts.verbosity == verbosity
    # test side-effect, the verbosity flag should be set
    assert cylc.flow.flags.verbosity == verbosity


def test_help_color(monkeypatch: pytest.MonkeyPatch, parser: COP):
    """Test for colorized comments in 'cylc cmd --help --color=always'."""
    # This colorization is done on the fly when help is printed.
    monkeypatch.setattr("sys.argv", ['cmd', 'foo', '--color=always'])
    parser.parse_args(None)
    assert parser.values.color == "always"
    f = io.StringIO()
    with redirect_stdout(f):
        parser.print_help()
    assert not (f.getvalue()).startswith("Usage: " + USAGE_WITH_COMMENT)


def test_help_nocolor(monkeypatch: pytest.MonkeyPatch, parser: COP):
    """Test for no colorization in 'cylc cmd --help --color=never'."""
    # This colorization is done on the fly when help is printed.
    monkeypatch.setattr(sys, "argv", ['cmd', 'foo', '--color=never'])
    parser.parse_args(None)
    assert parser.values.color == "never"
    f = io.StringIO()
    with redirect_stdout(f):
        parser.print_help()
    assert (f.getvalue()).startswith("Usage: " + USAGE_WITH_COMMENT)


def test_Options_std_opts():
    """Test Python Options API with standard options."""
    parser = COP(USAGE_WITH_COMMENT, auto_add=True)
    MyOptions = Options(parser)
    MyValues = MyOptions(verbosity=1)
    assert MyValues.verbosity == 1


# Add overlapping args tomorrow
@pytest.mark.parametrize(
    'first, second, expect',
    [
        param(
            [{ARGS: ['-f', '--foo'], KWARGS: {}, SOURCES: {'do'}}],
            [{ARGS: ['-f', '--foo'], KWARGS: {}, SOURCES: {'dont'}}],
            (
                [{ARGS: ['-f', '--foo'], KWARGS: {}, SOURCES: {'do', 'dont'}}]
            ),
            id='identical arg lists unchanged'
        ),
        param(
            [{ARGS: ['-f', '--foo'], KWARGS: {}, SOURCES: {'fall'}}],
            [{
                ARGS: ['-f', '--foolish'],
                KWARGS: {'help': 'not identical'}, 
                SOURCES: {'fold'}}],
            (
                [
                    {ARGS: ['--foo'], KWARGS: {}, SOURCES: {'fall'}},
                    {
                        ARGS: ['--foolish'],
                        KWARGS: {'help': 'not identical'},
                        SOURCES: {'fold'}
                    }
                ]
            ),
            id='different arg lists lose shared names'
        ),
        param(
            [{ARGS: ['-f', '--foo'], KWARGS: {}, SOURCES: {'cook'}}],
            [{
                ARGS: ['-f', '--foo'], 
                KWARGS: {'help': 'not identical'}, 
                SOURCES: {'bake'}
            }],
            None,
            id='different args identical arg list cause exception'
        ),
        param(
            [{ARGS: ['-g', '--goo'], KWARGS: {}, SOURCES: {'knit'}}],
            [{ARGS: ['-f', '--foo'], KWARGS: {}, SOURCES: {'feed'}}],
            [
                {ARGS: ['-g', '--goo'], KWARGS: {}, SOURCES: {'knit'}},
                {ARGS: ['-f', '--foo'], KWARGS: {}, SOURCES: {'feed'}},
            ],
            id='all unrelated args added'
        ),
        param(
            [
                {ARGS: ['-f', '--foo'], KWARGS: {}, SOURCES: {'work'}},
                {ARGS: ['-r', '--redesdale'], KWARGS: {}, SOURCES: {'work'}}
            ],
            [
                {ARGS: ['-f', '--foo'], KWARGS: {}, SOURCES: {'sink'}},
                {
                    ARGS: ['-b', '--buttered-peas'], 
                    KWARGS: {}, SOURCES: {'sink'}
                }
            ],
            [
                    {
                        ARGS: ['-f', '--foo'],
                        KWARGS: {},
                        SOURCES: {'work', 'sink'}
                    },
                    {
                        ARGS: ['-b', '--buttered-peas'],
                        KWARGS: {},
                        SOURCES: {'sink'}
                    },
                    {
                        ARGS: ['-r', '--redesdale'],
                        KWARGS: {},
                        SOURCES: {'work'}
                    },
            ],
            id='do not repeat args'
        ),
        param(
            [
                {
                    ARGS: ['-f', '--foo'],
                    KWARGS: {},
                    SOURCES: {'push'}
                },
            ],
            [],
            [
                {
                    ARGS: ['-f', '--foo'],
                    KWARGS: {},
                    SOURCES: {'push'}
                },
            ],
            id='one empty list is fine'
        )
    ]
)
def test_combine_options_pair(first, second, expect):
    """It combines sets of options"""
    if expect is not None:
        result = combine_options_pair(first, second)

        assert result == expect
    else:
        with pytest.raises(Exception, match='Clashing Options'):
            combine_options_pair(first, second)


@pytest.mark.parametrize(
    'inputs, expect',
    [
        param(
            [
                ([{
                    ARGS: ['-i', '--inflammable'],
                    KWARGS: {HELP: ''}, SOURCES: {'wish'}
                }]),
                ([{
                    ARGS: ['-f', '--flammable'], KWARGS: {HELP: ''}, SOURCES: {'rest'}
                }]),
                ([{
                    ARGS: ['-n', '--non-flammable'], KWARGS: {HELP: ''}, SOURCES: {'swim'}
                }]),
            ],
            [
                {ARGS: ['-i', '--inflammable']},
                {ARGS: ['-f', '--flammable']},
                {ARGS: ['-n', '--non-flammable']}
            ],
            id='merge three argsets no overlap'
        ),
        param(
            [
                    [
                        {ARGS: ['-m', '--morpeth'], KWARGS: {HELP: ''}, SOURCES: {'stop'}},
                        {ARGS: ['-r', '--redesdale'], KWARGS: {HELP: ''}, SOURCES: {'stop'}}
                    ],
                    [
                        {ARGS: ['-b', '--byker'], KWARGS: {HELP: ''}, SOURCES: {'walk'}},
                        {ARGS: ['-r', '--roxborough'], KWARGS: {HELP: ''}, SOURCES: {'walk'}}
                    ],
                    [{ARGS: ['-b', '--bellingham'], KWARGS: {HELP: ''}, SOURCES: {'leap'}}]
            ],
            [
                {ARGS: ['--bellingham']},
                {ARGS: ['--roxborough']},
                {ARGS: ['--redesdale']},
                {ARGS: ['--byker']},
                {ARGS: ['-m', '--morpeth']}
            ],
            id='merge three overlapping argsets'
        ),
        param(
            [
                ([]),
                (
                    [{
                        ARGS: ['-c', '--campden'],
                        KWARGS: {HELP: 'x'},
                        SOURCES: {'foo'}
                    }])
            ],
            [
                {ARGS: ['-c', '--campden']}
            ],
            id="empty list doesn't clear result"
        ),
    ]
)
def test_combine_options(inputs, expect):
    """It combines multiple input sets"""
    result = combine_options(*inputs)
    result_args = [i[ARGS] for i in result]

    # Order of args irrelevent to test
    for option in expect:
        assert option[ARGS] in result_args


@pytest.mark.parametrize(
    'argv_before, kwargs, expect',
    [
        param(
            'vip myworkflow --foo something'.split(),
            {
                'script_name': 'play',
                'workflow_id': 'myworkflow',
                'compound_script_opts': [
                    {ARGS: ['--foo', '-f'], KWARGS: {}},
                ],
                'script_opts': [{
                    ARGS: ['--foo', '-f'],
                    KWARGS: {}
                }]
            },
            'play myworkflow --foo something'.split(),
            id='no opts to remove'
        ),
        param(
            'vip myworkflow -f something -b something_else --baz'.split(),
            {
                'script_name': 'play',
                'workflow_id': 'myworkflow',
                'compound_script_opts': [
                    {ARGS: ['--foo', '-f'], KWARGS: {}},
                    {ARGS: ['--bar', '-b'], KWARGS: {'action': 'store'}},
                    {ARGS: ['--baz'], KWARGS: {'action': 'store_true'}},
                ],
                'script_opts': [{
                    ARGS: ['--foo', '-f'],
                    KWARGS: {}
                }]
            },
            'play myworkflow -f something'.split(),
            id='remove some opts'
        ),
        param(
            'vip myworkflow'.split(),
            {
                'script_name': 'play',
                'workflow_id': 'myworkflow',
                'compound_script_opts': [
                    {ARGS: ['--foo', '-f'], KWARGS: {}},
                    {ARGS: ['--bar', '-b'], KWARGS: {}},
                    {ARGS: ['--baz'], KWARGS: {}},
                ],
                'script_opts': []
            },
            'play myworkflow'.split(),
            id='no opts to keep'
        ),
    ]
)
def test_cleanup_sysargv(monkeypatch, argv_before, kwargs, expect):
    """It replaces the contents of sysargv with Cylc Play argv items.
    """
    # Fake up sys.argv: for this test.
    dummy_cylc_path = ['/pathto/my/cylc/bin/cylc']
    monkeypatch.setattr(sys, 'argv', dummy_cylc_path + argv_before)
    # Fake options too:
    opts = SimpleNamespace(**{
        i[ARGS][0].replace('--', ''): i for i in kwargs['compound_script_opts']
    })

    kwargs.update({'options': opts})

    # Test the script:
    cleanup_sysargv(**kwargs)
    assert sys.argv == dummy_cylc_path + expect
