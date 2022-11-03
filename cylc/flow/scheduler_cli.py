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
"""Common logic for "cylc play" CLI."""

from ansimarkup import parse as cparse
import asyncio
from functools import lru_cache
from shlex import quote
import sys
from typing import TYPE_CHECKING

from cylc.flow import LOG
from cylc.flow.exceptions import ServiceFileError
import cylc.flow.flags
from cylc.flow.id import upgrade_legacy_ids
from cylc.flow.host_select import select_workflow_host
from cylc.flow.hostuserutil import is_remote_host
from cylc.flow.id_cli import parse_ids
from cylc.flow.loggingutil import (
    close_log,
    RotatingLogFileHandler,
)
from cylc.flow.network.client import WorkflowRuntimeClient
from cylc.flow.option_parsers import (
    WORKFLOW_ID_ARG_DOC,
    CylcOptionParser as COP,
    Options,
    ICP_OPTION,
    ARGS, KWARGS, HELP, ACTION, DEFAULT, DEST, METAVAR, CHOICES
)
from cylc.flow.pathutil import get_workflow_run_scheduler_log_path
from cylc.flow.remote import cylc_server_cmd
from cylc.flow.scheduler import Scheduler, SchedulerError
from cylc.flow.scripts.common import cylc_header
from cylc.flow.workflow_files import (
    detect_old_contact_file,
    SUITERC_DEPR_MSG
)
from cylc.flow.terminal import cli_function

if TYPE_CHECKING:
    from optparse import Values


PLAY_DOC = r"""cylc play [OPTIONS] ARGS

Start, resume, or restart a workflow.

The scheduler will run as a daemon unless you specify --no-detach.

To avoid overwriting existing run directories, workflows that already ran can
only be restarted from prior state. To start again, "cylc install" a new copy
or "cylc clean" the existing run directory.

By default, new runs begin at the start of the graph, determined by the initial
cycle point. You can also begin at a later cycle point (--start-cycle-point),
or at specified tasks (--start-task) within the graph.

For convenience, any dependence on tasks prior to the start cycle point (or to
the cycle point of the earliest task specified by --start-task) will be taken
as satisfied.

Examples:
    # Start (at the initial cycle point), restart, or resume workflow WORKFLOW
    $ cylc play WORKFLOW

    # Start a new run from a cycle point after the initial cycle point
    # (integer cycling)
    $ cylc play --start-cycle-point=3 WORKFLOW
    # (datetime cycling):
    $ cylc play --start-cycle-point=20250101T0000Z WORKFLOW

    # Start a new run from specified tasks in the graph
    $ cylc play --start-task=3/foo WORKFLOW
    $ cylc play -t 3/foo -t 3/bar WORKFLOW

    # Start, restart or resume the second installed run of the workflow
    # "dogs/fido"
    $ cylc play dogs/fido/run2

At restart, tasks recorded as submitted or running are polled to determine what
happened to them while the workflow was down.
"""


RESUME_MUTATION = '''
mutation (
  $wFlows: [WorkflowID]!
) {
  resume (
    workflows: $wFlows
  ) {
    result
  }
}
'''

RUN_MODE = {
    ARGS: ["-m", "--mode"],
    KWARGS: {
        HELP: "Run mode: live, dummy, simulation (default live).",
        METAVAR: "STRING",
        ACTION: "store",
        DEST: "run_mode",
        DEFAULT: 'live',
        CHOICES: ['live', 'dummy', 'simulation'],
    }
}

PLAY_OPTIONS = [
    {
        ARGS: ["-n", "--no-detach", "--non-daemon"],
        KWARGS: {
            HELP: "Do not daemonize the scheduler (infers --format=plain)",
            ACTION: "store_true",
            DEST: "no_detach",
        }
    },
    {
        ARGS: ["--profile"],
        KWARGS: {
            HELP: "Output profiling (performance) information",
            ACTION: "store_true",
            DEFAULT: False,
            DEST: "profile_mode"
        }
    },
    {
        ARGS: ["--start-cycle-point", "--startcp"],
        KWARGS: {
            HELP:
                "Set the start cycle point, which may be after"
                " the initial cycle point. If the specified start point is"
                " not in the sequence, the next on-sequence point will"
                " be used. (Not to be confused with the initial cycle point)",
            METAVAR: "CYCLE_POINT",
            ACTION: "store",
            DEST: "startcp",
        }
    },
    {
        ARGS: ["--final-cycle-point", "--fcp"],
        KWARGS: {
            HELP:
                "Set the final cycle point. This command line option overrides"
                " the workflow config option"
                " '[scheduling]final cycle point'. ",
            METAVAR: "CYCLE_POINT",
            ACTION: "store",
            DEST: "fcp",
        }
    },
    {
        ARGS: ["--stop-cycle-point", "--stopcp"],
        KWARGS: {
            HELP:
                "Set the stop cycle point. Shut down after all"
                " have PASSED this cycle point. (Not to be confused"
                " the final cycle point.) This command line option overrides"
                " the workflow config option"
                " '[scheduling]stop after cycle point'.",
            METAVAR: "CYCLE_POINT",
            ACTION: "store",
            DEST: "stopcp",
        }
    },
    {
        ARGS: ["--start-task", "--starttask", "-t"],
        KWARGS: {
            HELP:
                "Start from this task instance, given by '<cycle>/<name>'."
                " This can be used multiple times to start from multiple"
                " tasks at once. Dependence on tasks with cycle points earlier"
                " than the earliest start-task will be ignored. A"
                " sub-graph of the workflow will run if selected tasks do"
                " not lead on to the full graph.",
            METAVAR: "TASK_ID",
            ACTION: "append",
            DEST: "starttask",
        }
    },
    {
        ARGS: ["--pause"],
        KWARGS: {
            HELP: "Pause the workflow immediately on start up.",
            ACTION: "store_true",
            DEST: "paused_start",
        }
    },
    {
        ARGS: ["--hold-after", "--hold-cycle-point", "--holdcp"],
        KWARGS: {
            HELP: "Hold all tasks after this cycle point.",
            METAVAR: "CYCLE_POINT",
            ACTION: "store",
            DEST: "holdcp",
        }
    },
    RUN_MODE,
    {
        ARGS: ["--reference-log"],
        KWARGS: {
            HELP: "Generate a reference log for use in reference ",
            ACTION: "store_true",
            DEFAULT: False,
            DEST: "genref",
        }
    },
    {
        ARGS: ["--reference-test"],
        KWARGS: {
            HELP:
                "Do a test run against a previously generated reference.",
            ACTION: "store_true",
            DEFAULT: False,
            DEST: "reftest",
        }
    },
    {
        ARGS: ["--host"],
        KWARGS: {
            HELP:
                "Specify the host on which to start-up the workflow."
                " If not specified, a host will be selected using"
                " the '[scheduler]run hosts' global config.",
            METAVAR: "HOST",
            ACTION: "store",
            DEST: "host",
        }
    },
    {
        ARGS: ["--format"],
        KWARGS: {
            HELP:
                "The format of the output: 'plain'=human readable, 'json",
            CHOICES: ('plain', 'json'),
            DEFAULT: "plain",
            DEST: 'format'
        }
    },
    {
        ARGS: ["--main-loop"],
        KWARGS: {
            HELP:
                "Specify an additional plugin to run in the main"
                " These are used in combination with those specified"
                " [scheduler][main loop]plugins. Can be used multiple times.",
            METAVAR: "PLUGIN_NAME",
            ACTION: "append",
            DEST: "main_loop",
        }
    },
    {
        ARGS: ["--abort-if-any-task-fails"],
        KWARGS: {
            HELP:
                "If set workflow will abort with status 1 if any task fails.",
            ACTION: "store_true",
            DEFAULT: False,
            DEST: "abort_if_any_task_fails",
        }
    },
    ICP_OPTION
]


@lru_cache()
def get_option_parser(add_std_opts: bool = False) -> COP:
    """Parse CLI for "cylc play"."""
    parser = COP(
        PLAY_DOC,
        jset=True,
        comms=True,
        argdoc=[WORKFLOW_ID_ARG_DOC]
    )

    options = parser.get_cylc_rose_options() + PLAY_OPTIONS
    for option in options:
        parser.add_option(*option[ARGS], **option[KWARGS])

    if add_std_opts:
        # This is for the API wrapper for integration tests. Otherwise (CLI
        # use) "standard options" are added later in options.parse_args().
        # They should really be added in options.__init__() but that requires a
        # bit of refactoring because option clashes are handled bass-ackwards
        # ("overrides" are added before standard options).
        parser.add_std_options()

    return parser


# options we cannot simply extract from the parser
DEFAULT_OPTS = {
    'debug': False,
    'verbose': False,
    'templatevars': None,
    'templatevars_file': None
}


RunOptions = Options(get_option_parser(add_std_opts=True), DEFAULT_OPTS)


def _open_logs(id_: str, no_detach: bool, restart_num: int) -> None:
    """Open Cylc log handlers for a flow run."""
    if not no_detach:
        while LOG.handlers:
            LOG.handlers[0].close()
            LOG.removeHandler(LOG.handlers[0])
    log_path = get_workflow_run_scheduler_log_path(id_)
    LOG.addHandler(
        RotatingLogFileHandler(
            log_path,
            no_detach,
            restart_num=restart_num
        )
    )


def scheduler_cli(options: 'Values', workflow_id_raw: str) -> None:
    """Run the workflow.

    This function should contain all of the command line facing
    functionality of the Scheduler, exit codes, logging, etc.

    The Scheduler itself should be a Python object you can import and
    run in a regular Python session so cannot contain this kind of
    functionality.

    """
    # Parse workflow name but delay Cylc 7 suite.rc deprecation warning
    # until after the start-up splash is printed.
    # TODO: singleton
    (workflow_id,), _ = parse_ids(
        workflow_id_raw,
        constraint='workflows',
        max_workflows=1,
        # warn_depr=False,  # TODO
    )
    try:
        detect_old_contact_file(workflow_id)
    except ServiceFileError as exc:
        print(f"Resuming already-running workflow\n\n{exc}")
        pclient = WorkflowRuntimeClient(
            workflow_id,
            timeout=options.comms_timeout,
        )
        mutation_kwargs = {
            'request_string': RESUME_MUTATION,
            'variables': {
                'wFlows': [workflow_id]
            }
        }
        pclient('graphql', mutation_kwargs)
        sys.exit(0)

    # re-execute on another host if required
    _distribute(options.host, workflow_id_raw, workflow_id)

    # print the start message
    if (
        cylc.flow.flags.verbosity > -1
        and (options.no_detach or options.format == 'plain')
    ):
        print(
            cparse(
                cylc_header()
            )
        )

    if cylc.flow.flags.cylc7_back_compat:
        LOG.warning(SUITERC_DEPR_MSG)

    # setup the scheduler
    # NOTE: asyncio.run opens an event loop, runs your coro,
    #       then shutdown async generators and closes the event loop
    scheduler = Scheduler(workflow_id, options)
    asyncio.run(
        _setup(scheduler)
    )

    # daemonize if requested
    # NOTE: asyncio event loops cannot persist across daemonization
    #       ensure you have tidied up all threads etc before daemonizing
    if not options.no_detach:
        from cylc.flow.daemonize import daemonize
        daemonize(scheduler)

    # setup loggers
    _open_logs(
        workflow_id,
        options.no_detach,
        restart_num=scheduler.get_restart_num()
    )

    # run the workflow
    ret = asyncio.run(
        _run(scheduler)
    )

    # exit
    # NOTE: we must clean up all asyncio / threading stuff before exiting
    # NOTE: any threads which include sleep statements could cause
    #       sys.exit to hang if not shutdown properly
    LOG.info("DONE")
    close_log(LOG)
    sys.exit(ret)


def _distribute(host, workflow_id_raw, workflow_id):
    """Re-invoke this command on a different host if requested.

    Args:
        host:
            The remote host to re-invoke on.
        workflow_id_raw:
            The workflow ID as it appears in the CLI arguments.
        workflow_id:
            The workflow ID after it has gone through the CLI.
            This may be different (i.e. the run name may have been inferred).

    """
    # Check whether a run host is explicitly specified, else select one.
    if not host:
        host = select_workflow_host()[0]
    if is_remote_host(host):
        # Protect command args from second shell interpretation
        cmd = list(map(quote, sys.argv[1:]))

        # Ensure the whole workflow ID is used
        if workflow_id_raw != workflow_id:
            # The CLI can infer run names but when we re-invoke the command
            # we would prefer it to use the full workflow ID to better
            # support monitoring systems.
            for ind, item in enumerate(cmd):
                if item == workflow_id_raw:
                    cmd[ind] = workflow_id

        # Prevent recursive host selection
        cmd.append("--host=localhost")

        # Re-invoke the command
        cylc_server_cmd(cmd, host=host)
        sys.exit(0)


async def _setup(scheduler: Scheduler) -> None:
    """Initialise the scheduler."""
    try:
        await scheduler.install()
    except ServiceFileError as exc:
        sys.exit(exc)


async def _run(scheduler: Scheduler) -> int:
    """Run the workflow and handle exceptions."""
    # run cylc run
    ret = 0
    try:
        await scheduler.run()

    # stop cylc stop
    except SchedulerError:
        ret = 1
    except (KeyboardInterrupt, asyncio.CancelledError):
        ret = 2
    except Exception:
        ret = 3

    # kthxbye
    return ret


@cli_function(get_option_parser)
def play(parser: COP, options: 'Values', id_: str):
    """Implement cylc play."""
    return _play(parser, options, id_)


def _play(parser: COP, options: 'Values', id_: str):
    """Allows compound scripts to import play, but supply their own COP."""
    if options.starttask:
        options.starttask = upgrade_legacy_ids(
            *options.starttask,
            relative=True,
        )
    return scheduler_cli(options, id_)
