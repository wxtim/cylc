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
"""Module used for profiling different cylc versions."""

import os
import re
from subprocess import Popen, PIPE
import sys

from .git import is_git_repo


def get_cylc_directory():
    """Returns the location of cylc's working copy."""
    ver_str, _ = Popen(['cylc', 'version', '--long'], stdout=PIPE
                       ).communicate()
    try:
        return os.path.realpath(re.search('\((.*)\)', ver_str).groups()[0])
    except IndexError:
        sys.exit('Could not locate local git repository for cylc.')


# Ensure that the cylc directory is a git repository.
CYLC_DIR = get_cylc_directory()
os.chdir(CYLC_DIR)
IS_GIT_REPO = is_git_repo()

# Files and directories
PROFILE_DIR_NAME = '.profiling'  # Path to profiling directory.
PROFILE_FILE_NAME = 'results.json'  # Path to profiling results file
PROFILE_DB = 'results.db'
PROFILE_PLOT_DIR_NAME = 'plots'  # Path to default plotting directory.
USER_EXPERIMENT_DIR_NAME = 'experiments'  # Path to user defined experiments.
EXPERIMENTS_PATH = os.path.join('dev', 'profile-experiments'
                                )  # Path to built-in experiments.

# Ancestor commit for cylc profile-battery
PROFILE_COMMIT = '0f5a7999ba9c93174d846a6679db4ce413388df7'

# Ancestor commit for analysis-compatible cylc (run|validate) --profile
CYLC_PROFILING_COMMIT = '016e6a97be16eaf1a33ea19398a1ade09f86719e'

# Profiling config.
PROFILE_MODE_TIME = 'PROFILE_MODE_TIME'
PROFILE_MODE_CYLC = 'PROFILE_MODE_CYLC'
PROFILE_MODES = {'time': PROFILE_MODE_TIME,
                 'cylc': PROFILE_MODE_CYLC}

# Profile file suffixes.
PROFILE_FILES = {
    'cmd-out': '',
    'cmd-err': '-cmd.err',
    'time-err': '-time.err',
    'startup': '-startup'
}


# ------------- REGEXES ---------------
# Matches the summary line from the cylc <cmd> --profile output.
SUMMARY_LINE_REGEX = re.compile('([\d]+) function calls \(([\d]+) primitive'
                                ' calls\) in ([\d.]+)(?: CPU)? seconds')
# Matches the memory checkpoints in the cylc <cmd> --profile output
MEMORY_LINE_REGEX = re.compile('PROFILE: Memory: ([\d]+) KiB: ([\w.]+): (.*)')
# Matches main-loop memory checkpoints in cylc <cmd> --profile output.
LOOP_MEMORY_LINE_REGEX = re.compile('(?:loop #|end main loop \(total loops )'
                                    '([\d]+)(?:: |\): )(.*)')
# Matches the sleep function line in cylc <cmd> --profile output.
SLEEP_FUNCTION_REGEX = re.compile('([\d.]+)[\s]+[\d.]+[\s]+\{time.sleep\}')
# The string prefixing the suite-startup timestamp (unix time).
SUITE_STARTUP_STRING = 'SUITE STARTUP: '


# -------------- METRICS ---------------
METRIC_TITLE = 0  # For display purposes.
METRIC_UNIT = 1  # For display purposes.
METRIC_FILENAME = 2  # For output plots (no extension).
METRIC_FIELDS = 3  # Fields metrics can be derived from in order of preference.
# WARNING: Changing metrics affects db table structure!
METRICS = {  # Dict of all metrics measured by profile-battery.
    'elapsed_time': ('Elapsed Time', 's', 'elapsed-time', [
        'real', 'Elapsed (wall clock) time (h:mm:ss or m:ss)', 'cpu time'],),
    'cpu_time': ('CPU Time - Total', 's', 'cpu-time', ['total cpu time'],),
    'user_time': ('CPU Time - User', 's', 'user-time', [
        'user', 'User time (seconds)'],),
    'system_time': ('CPU Time - System', 's', 'system-time', [
        'sys', 'System time (seconds)'],),
    'rss_memory': ('Max Memory', 'kb', 'memory', [
        'maximum resident set size', 'Maximum resident set size (kbytes)',
        'mxmem'],),
    'file_ins': ('File System - Inputs', None, 'file-ins', [
        'block input operations', 'File system inputs'],),
    'file_outs': ('File System - Outputs', None, 'file-outs', [
        'block output operations', 'File system outputs'],),
    'startup_time': ('Startup Time', 's', 'startup-time', ['startup time'],),
    'main_loop_ittrs': (
        'Number Of Main Loop Iterations', None, 'loop-count', ['loop count'],),
    'main_loop_ittr_time': (
        'Average Main Loop Iteration Time', 's', 'loop-time',
         ['avg loop time'],),
    'elapsed_non_sleep_time': (
        'Elapsed Time - time.sleep()', 's', 'awake-time', ['awake cpu time'],)
}
# Metrics used if --full is not set.
#QUICK_ANALYSIS_METRICS = set(['001', '002', '005'])
QUICK_ANALYSIS_METRICS = set(('elapsed_time', 'cpu_time', 'rss_memory'))
# Reverse lookup of METRICS, dict of fields stored with their metric codes.
METRICS_BY_FIELD = {}
for metric in METRICS:
    for field in METRICS[metric][METRIC_FIELDS]:
        METRICS_BY_FIELD[field] = metric


# The profile mode(s) to use if un-specified.
DEFAULT_PROFILE_MODES = ['time']

def safe_name(name):
    """Returns a safe string for experiment, run and version names."""
    for char, repl in [('.', '_'), ('-', '_'), (' ', '_')]:
        name = name.replace(char, repl)
    return name


class ProfilingException(Exception):
    """Exception raised when an error has occured during profiling."""
    pass


class AnalysisException(Exception):
    """Exception to be raised in the event of fatal error during analysis."""
    pass
