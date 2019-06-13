#!/usr/bin/env python3

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

import json
import re
from copy import deepcopy
from time import time
from typing import List, Tuple, Union

from cylc.flow import LOG
import cylc.flow.flags
from cylc.flow.hostuserutil import get_user
from cylc.flow.xtriggers.wall_clock import wall_clock

from cylc.flow.subprocctx import SubFuncContext
from cylc.flow.broadcast_mgr import BroadcastMgr
from cylc.flow.subprocpool import SubProcPool
from cylc.flow.task_proxy import TaskProxy

# Templates for string replacement in function arg values.
TMPL_USER_NAME = 'user_name'
TMPL_SUITE_NAME = 'suite_name'
TMPL_TASK_CYCLE_POINT = 'point'
TMPL_TASK_IDENT = 'id'
TMPL_TASK_NAME = 'name'
TMPL_SUITE_RUN_DIR = 'suite_run_dir'
TMPL_SUITE_SHARE_DIR = 'suite_share_dir'
TMPL_DEBUG_MODE = 'debug'
ARG_VAL_TEMPLATES = [
    TMPL_TASK_CYCLE_POINT, TMPL_TASK_IDENT, TMPL_TASK_NAME, TMPL_SUITE_RUN_DIR,
    TMPL_SUITE_SHARE_DIR, TMPL_USER_NAME, TMPL_SUITE_NAME, TMPL_DEBUG_MODE]

# Extract all 'foo' from string templates '%(foo)s'.
RE_STR_TMPL = re.compile(r'%\(([\w]+)\)s')


class XtriggerManager(object):
    """Manage clock triggers and xtrigger functions.

    # Example:
    [scheduling]
        [[xtriggers]]
            clock_0 = wall_clock()  # offset PT0H
            clock_1 = wall_clock(offset=PT1H)
                 # or wall_clock(PT1H)
            suite_x = suite_state(suite=other,
                                  point=%(task_cycle_point)s):PT30S
        [[dependencies]]
            [[[PT1H]]]
                graph = '''
                    @clock_1 & @suite_x => foo & bar
                    @wall_clock = baz  # pre-defined zero-offset clock
                        '''

    Task proxies only store xtriggers labels: clock_0, suite_x, etc. above.
    These are mapped to the defined function calls. Dependence on xtriggers
    is satisfied by calling these functions asynchronously in the task pool
    (except clock triggers which are called synchronously as they're quick).

    A unique call is defined by a unique function call signature, i.e. the
    function name and all arguments. So suite_x above defines a different
    xtrigger for each cycle point. A new call will not be made before the
    previous one has returned via the xtrigger callback. The interval (in
    "name(args):INTVL") determines frequency of calls (default PT10S).

    Once a trigger is satisfied, remember it until the cleanup cutoff point.

    Clock triggers are treated separately and called synchronously in the main
    process, because they are guaranteed to be quick (but they are still
    managed uniquely - i.e. many tasks depending on the same clock trigger
    (with same offset from cycle point) will be satisfied by the same function
    call.

    """

    def __init__(
        self,
        suite: str,
        user: str = None,
        broadcast_mgr: BroadcastMgr = None,
        proc_pool: SubProcPool = None,
        suite_run_dir: str = None,
        suite_share_dir: str = None,
        suite_work_dir: str = None,
        suite_source_dir: str = None,
    ):
        """Initialize the xtrigger manager.

        Args:
            suite (str): suite name
            user (str): suite owner
            broadcast_mgr (BroadcastMgr): the Broadcast Manager
            proc_pool (SubProcPool): pool of Subprocesses
            suite_run_dir (str): suite run directory
            suite_share_dir (str): suite share directory
            suite_source_dir (str): suite source directory
        """
        # Suite function and clock triggers by label.
        self.functx_map = {}
        self.clockx_map = {}
        # When next to call a function, by signature.
        self.t_next_call = {}
        # Satisfied triggers and their function results, by signature.
        self.sat_xtrig = {}
        # Signatures of satisfied clock triggers.
        self.sat_xclock = []
        # Signatures of active functions (waiting on callback).
        self.active = []
        # All trigger and clock signatures in the current task pool.
        self.all_xtrig = []
        self.all_xclock = []

        self.pflag = False

        # For function arg templating.
        if not user:
            user = get_user()
        self.farg_templ = {
            TMPL_SUITE_NAME: suite,
            TMPL_USER_NAME: user,
            TMPL_SUITE_RUN_DIR: suite_run_dir,
            TMPL_SUITE_SHARE_DIR: suite_share_dir,
            TMPL_DEBUG_MODE: cylc.flow.flags.debug
        }
        self.proc_pool = proc_pool
        self.broadcast_mgr = broadcast_mgr
        self.suite_source_dir = suite_source_dir

    def add_clock(self, label: str, fctx: SubFuncContext):
        """Add a new clock xtrigger.

        Args:
            label (str): xtrigger label
            fctx (SubFuncContext): function context
        """
        self.clockx_map[label] = fctx

    def add_trig(self, label: str, fctx: SubFuncContext):
        """Add a new xtrigger.

        Args:
            label (str): xtrigger label
            fctx (SubFuncContext): function context
        Raises:
            ValueError: if any string template in the function context
                arguments are not present in the expected template values.
        """
        self.functx_map[label] = fctx
        # Check any string templates in the function arg values (note this
        # won't catch bad task-specific values - which are added dynamically).
        for argv in fctx.func_args + list(fctx.func_kwargs.values()):
            try:
                for match in RE_STR_TMPL.findall(argv):
                    if match not in ARG_VAL_TEMPLATES:
                        raise ValueError(
                            "Illegal template in xtrigger %s: %s" % (
                                label, match))
            except TypeError:
                # Not a string arg.
                pass

    def load_xtrigger_for_restart(self, row_idx: int, row: Tuple[str, str]):
        """Load satisfied xtrigger results from suite DB.

        Args:
            row_idx (int): row index (used for logging)
            row (Tuple[str, str]): tuple with the signature and results (json)
        Raises:
            ValueError: if the row cannot be parsed as JSON
        """
        if row_idx == 0:
            LOG.info("LOADING satisfied xtriggers")
        sig, results = row
        self.sat_xtrig[sig] = json.loads(results)

    def housekeep(self):
        """Delete satisfied xtriggers and xclocks no longer needed."""
        for sig in list(self.sat_xtrig):
            if sig not in self.all_xtrig:
                del self.sat_xtrig[sig]
        self.sat_xclock = [
            sig for sig in self.sat_xclock if sig in self.all_xclock]

    def satisfy_xclock(self, itask: TaskProxy):
        """Attempt to satisfy itask's clock trigger, if it has one.

        Args:
            itask (TaskProxy): TaskProxy
        """
        label, sig, ctx, satisfied = self._get_xclock(itask)
        if satisfied:
            return
        if wall_clock(*ctx.func_args, **ctx.func_kwargs):
            itask.state.xclock = (label, True)
            self.sat_xclock.append(sig)
            LOG.info('clock xtrigger satisfied: %s = %s' % (label, str(ctx)))

    def _get_xclock(self, itask: TaskProxy, sig_only: bool = False) ->\
            Union[str, Tuple[str, str, SubFuncContext, bool]]:
        """(Internal helper method.)

        Args:
            itask (TaskProxy): TaskProxy
            sig_only (bool): whether to return the signature only or not
        Returns:
            Union[str, Tuple[str, str, SubFuncContext, bool]]: the signature
                of the function (if sigs_only True) or a tuple with
                label, signature, function context, and flag for satisfied.
        """
        label, satisfied = itask.state.xclock
        ctx = deepcopy(self.clockx_map[label])
        ctx.func_kwargs.update(
            {
                'point_as_seconds': itask.get_point_as_seconds(),
            }
        )
        sig = ctx.get_signature()
        if sig_only:
            return sig
        else:
            return label, sig, ctx, satisfied

    def _get_xtrig(self, itask: TaskProxy, unsat_only: bool = False,
                   sigs_only: bool = False):
        """(Internal helper method.)

        Args:
            itask (TaskProxy): TaskProxy
            unsat_only (bool): whether to retrieve only unsatisfied xtriggers
                or not
            sigs_only (bool): whether to append only the function signature
                or not
        Returns:
            List[Union[str, Tuple[str, str, SubFuncContext, bool]]]: a list
                with either signature (if sigs_only True) or with tuples of
                label, signature, function context, and flag for satisfied.
        """
        res = []
        farg_templ = {
            TMPL_TASK_CYCLE_POINT: str(itask.point),
            TMPL_TASK_NAME: str(itask.tdef.name),
            TMPL_TASK_IDENT: str(itask.identity)
        }
        farg_templ.update(self.farg_templ)
        for label, satisfied in itask.state.xtriggers.items():
            if unsat_only and satisfied:
                continue
            ctx = deepcopy(self.functx_map[label])
            ctx.point = itask.point
            kwargs = {}
            args = []
            # Replace legal string templates in function arg values.
            for val in ctx.func_args:
                try:
                    val = val % farg_templ
                except TypeError:
                    pass
                args.append(val)
            for key, val in ctx.func_kwargs.items():
                try:
                    val = val % farg_templ
                except TypeError:
                    pass
                kwargs[key] = val
            ctx.func_args = args
            ctx.func_kwargs = kwargs
            ctx.update_command(self.suite_source_dir)
            sig = ctx.get_signature()
            if sigs_only:
                res.append(sig)
            else:
                res.append((label, sig, ctx, satisfied))
        return res

    def satisfy_xtriggers(self, itask: TaskProxy):
        """Attempt to satisfy itask's xtriggers.

        Args:
            itask (TaskProxy): TaskProxy
        """
        for label, sig, ctx, _ in self._get_xtrig(itask, unsat_only=True):
            if sig in self.sat_xtrig:
                if not itask.state.xtriggers[label]:
                    itask.state.xtriggers[label] = True
                    res = {}
                    for key, val in self.sat_xtrig[sig].items():
                        res["%s_%s" % (label, key)] = val
                    if res:
                        self.broadcast_mgr.put_broadcast(
                            [str(ctx.point)],
                            [itask.tdef.name],
                            [{'environment': res}],
                        )
                continue
            if sig in self.active:
                # Already waiting on this result.
                continue
            now = time()
            if sig in self.t_next_call and now < self.t_next_call[sig]:
                # Too soon to call this one again.
                continue
            self.t_next_call[sig] = now + ctx.intvl
            # Queue to the process pool, and record as active.
            self.active.append(sig)
            self.proc_pool.put_command(ctx, self.callback)

    def collate(self, itasks: List[TaskProxy]):
        """Get list of all current xtrigger signatures.

        Args:
            itasks (List[TaskProxy]): list of TaskProxy's
        """
        self.all_xtrig = []
        self.all_xclock = []
        for itask in itasks:
            self.all_xtrig += self._get_xtrig(itask, sigs_only=True)
            if itask.state.xclock is not None:
                self.all_xclock.append(self._get_xclock(itask, sig_only=True))

    def callback(self, ctx: SubFuncContext):
        """Callback for asynchronous xtrigger functions.

        Record satisfaction status and function results dict.

        Args:
            ctx (SubFuncContext): function context
        Raises:
            ValueError: if the context given is not active
        """
        LOG.debug(ctx)
        sig = ctx.get_signature()
        self.active.remove(sig)
        try:
            satisfied, results = json.loads(ctx.out)
        except (ValueError, TypeError):
            return
        LOG.debug('%s: returned %s' % (sig, results))
        if satisfied:
            self.pflag = True
            self.sat_xtrig[sig] = results

    def check_xtriggers(self, itasks: List[TaskProxy]):
        """See if any xtriggers are satisfied.

        Args:
            itasks (List[TaskProxy]): list of TaskProxy's
        """
        self.collate(itasks)
        for itask in itasks:
            if itask.state.xclock is not None:
                self.satisfy_xclock(itask)
            if itask.state.xtriggers:
                self.satisfy_xtriggers(itask)
