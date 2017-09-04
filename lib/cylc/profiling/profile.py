# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2017 NIWA
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
"""Performs profiling of cylc.

Initiates and runs the "main-suite" then performs analysis on its outputs.

"""

import os
from subprocess import (Popen, PIPE, check_call, CalledProcessError)
import sys
import traceback

from cylc.profiling import (PROFILE_MODES, safe_name, ProfilingException,
                            AnalysisException)
from cylc.profiling.analysis import extract_results
from cylc.profiling.profiling_suite_writer import write_profiling_suite


TASK_NAME_TEMPLATE = ('prof__exp_{experiment_name}__ver_{version_name}__run_'
                      '{run_name}__repeat_{repeat_no}')

# TODO: Unregister cylc 6.x suites?


def run_cmd(cmd, background=False, verbose=True):
    """Simple wrapper for running cylc commands.

    Args:
        cmd (list): The command to run e.g. ['sleep' ,'1'].
        background (bool): If True then the command is run the background.
        verbose (bool): If True then the command will be written to stdout.

    Returns:
        bool: False if the command failed, else True.

    """
    if verbose:
        print '$ ' + ' '.join(cmd)
    ret = True
    if background:
        try:
            Popen(cmd, stdout=PIPE)
        except OSError:
            ret = False
    else:
        try:
            check_call(cmd, stdout=PIPE)
        except CalledProcessError:
            ret = False
    if not ret:
        print >> sys.stderr, '\tCommand Failed'
    return ret


def profile(schedule, install_dir, reg_base):
    """Run profiling.

    Note:
        This only supports profiling experiment on one platform. Due to the CLI
        API schedule can only contain keys for one platform meaning this can be
        negated.

    Args:
        schedule (iterable): Collection of (platform, version, experiment,
            run_name) tuples to profile.
        install_dir (str): The directory that the required cylc suites /
            versions etc are installed in.

    Return:
            dict: Dictionary of results if successfull else False.
            {'version-id': {'experiment-id': {'run-name': {'code': result}}

    """
    # Registration for profile suites.
    #reg_base = 'profile-' + str(time.time()).replace('.', '')
    reg = os.path.join(reg_base, 'main-suite')

    # Create directory to install the 'main-suite' in.
    suite_dir = os.path.join(install_dir, 'main-suite')
    os.mkdir(suite_dir)

    # Write out the 'main-suite' suite.rc file.
    suite_handle = open(os.path.join(suite_dir, 'suite.rc'), 'w+')
    write_profiling_suite(schedule, suite_handle.write, install_dir, reg_base)
    suite_handle.close()

    # Register the 'main-suite'.
    if not run_cmd(['cylc', 'reg', reg, suite_dir]):
        return False

    # Open a GUI for the 'main-suite'.
    run_cmd(['cylc', 'gui', reg], background=True)

    # Run the 'main-suite'.
    if not run_cmd(['cylc', 'run', reg, '--debug']):
        # Error in the profiling suite - keep the suite directory.
        print >> sys.stderr, (
            'ERROR: See suite directory for details "%s".' % reg)
        return False

    # Retrieve results.
    failures = []
    successes = False
    repeat_pad = max(len(str(x['repeats'])) for _, _, e, _ in schedule for x in
                     e['config']['runs'])  # Get padding (e.. 001).
    results = []
    for platform, version, experiment, run_name in schedule:
        for run in experiment['config']['runs']:
            if run['name'] != run_name:
                continue
            try:
                # TODO: Standardise results / prof_results convention.
                results.append((platform,
                                version['id'],
                                experiment['id'],
                                run['name'],
                               retrieve_results(reg, version, experiment,
                                                run, repeat_pad)))
            except ProfilingException:
                # Run un-successfull - no results to process.
                failures.append((version['name'], experiment['name'],
                                run['name']))
            except AnalysisException:
                # Something went wrong during analysis.
                failures.append((version['name'], experiment['name'],
                                run['name']))
                traceback.print_exc()
            else:
                successes = True

    for failure in failures:
        # Report any run failures.
        print >> sys.stderr, (
            'Experiment "{1}:{2}" failed at version "{0}".'.format(*failure))
    if failures:
        # Profiling un-successfull - keep the suite directory for inspection.
        print >> sys.stderr, ('Results will be incomplete.')
        print >> sys.stderr, (
            'ERROR: See suite directory for details "%s".' % reg_base)

    if not successes:
        # No experiments successfully ran.
        return False
    else:
        # Return fresh results.
        return results


def retrieve_results(reg, version, experiment, run, repeat_pad):
    """Return a dictionary of results for a run.

    Extracts and processes results from the suites run directory.

    Args:
        reg (str): The registration of the suite in which the profile files are
            located (i.e. the 'main-suite').
        version (dict): Dictionary representing a cylc version.
        experiment (dict): Dictionary representing a profiling experiment.
        run (dict): Dictionary representing a profiling "run".
            A value contained within experiment['runs'].
        repeat_pad (int): The number of digits in the repeat part of the task
            name (i.e. 3 for 001, ...).

    Return:
        dict: Dictionary of (averaged) profiling results for this run.

    """
    run_files = []
    for repeat in range(run['repeats'] + 1):
        run_files.append(
            retrieve_result_files(reg, version, experiment, run, repeat,
                                  repeat_pad))
    profile_modes = [PROFILE_MODES[mode] for mode in
                     experiment['config']['profile modes']]
    validate_mode = experiment['config'].get('validate_mode', False)
    return extract_results(run_files, profile_modes, validate_mode)


def retrieve_result_files(reg, version, experiment, run, repeat, repeat_pad):
    """Return a dictionary of the file paths for the specified profile run.

    Args:
        reg (str): The registration of the suite in which the profile files are
            located (i.e. the 'main-suite').
        version (dict): Dictionary representing a cylc version.
        experiment (dict): Dictionary representing a profiling experiment.
        run (dict): Dictionary representing a profiling "run".
            A value contained within experiment['runs'].
        repeat (int): The repeat number of the profile run.
        repeat_pad (int): The number of digits in the repeat part of the task
            name (i.e. 3 for 001, ...).

    Return:
        dict: Dictionary of file paths.

    """
    repeat_no = '0' * (repeat_pad - len(str(repeat))) + str(repeat)
    task_name = TASK_NAME_TEMPLATE.format(
        experiment_name=safe_name(experiment['name']),
        version_name=safe_name(version['name']),
        run_name=safe_name(run['name']),
        repeat_no=repeat_no)
    suite_dir = os.path.join(os.path.expanduser('~'), 'cylc-run', reg)
    work_dir = os.path.join(suite_dir, 'work', '1', task_name)
    log_dir = os.path.join(suite_dir, 'log', 'job', '1', task_name, 'NN')

    if not os.path.exists(os.path.join(work_dir, 'success')):
        raise ProfilingException('Run not successfull.')

    return {
        'time': os.path.join(work_dir, 'time-err'),
        'startup': os.path.join(work_dir, 'startup'),
        'out': os.path.join(log_dir, 'job.out'),
        'err': os.path.join(log_dir, 'job.err')
    }
