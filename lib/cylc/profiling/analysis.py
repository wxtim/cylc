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
"""Module for performing analysis on profiling results and generating plots."""

import os
import re
import sys

# Import modules required for plotting if avaliable.
try:
    import matplotlib.cm as colour_map
    import matplotlib.pyplot as plt
    import numpy
    import warnings
    CAN_PLOT = True
    # Filter numpy polyfit warnings.
    warnings.simplefilter('ignore', numpy.RankWarning)
    # Filter gtk depreciations originating from matplotlib.
    warnings.simplefilter('ignore', DeprecationWarning)
except ImportError:
    CAN_PLOT = False

import cylc.profiling as prof
import cylc.profiling.results as results
from cylc.profiling.git import (order_versions_by_date, describe)
from cylc.wallclock import get_unix_time_from_time_string


def _mean(data):
    """Return the mean average of a list of numbers."""
    return sum(data) / float(len(data))


def _get_startup_time(file_name):
    """Return the value of the "SUITE STARTUP" entry as a string."""
    with open(file_name, 'r') as startup_file:
        return re.search('SUITE STARTUP: (.*)',
                         startup_file.read()).groups()[0]


def _process_time_file(file_name):
    """Extracts results from a result file generated using the /usr/bin/time
    profiler."""
    with open(file_name, 'r') as time_file:
        ret = {}
        lines = time_file.readlines()
        for line in lines:
            try:
                field, value = line.strip().rsplit(': ', 1)
            except ValueError:
                print >> sys.stderr, (
                    'WARNING: Could not parse line "%s"' % line.strip())
                continue
            try:  # Try to cast as integer.
                ret[field] = int(value)
            except ValueError:
                try:  # Try to cast as float.
                    ret[field] = float(value)
                except ValueError:
                    if value.endswith('%'):  # Remove trailing % symbol
                        try:  # Try to cast as integer.
                            ret[field] = int(value[:-1])
                        except ValueError:  # Try to cast as float.
                            ret[field] = float(value[:-1])
                    elif ':' in value:  # Is a time of form h:m:s or m:s
                        seconds = 0.
                        increment = 1.
                        for time_field in reversed(value.split(':')):
                            seconds += float(time_field) * increment
                            increment *= 60
                        ret[field] = seconds
                    else:  # Cannot parse.
                        if 'Command being timed' not in line:
                            print >> sys.stderr, (
                                'WARNING: Could not parse value "%s"' % line)
                            ret[field] = value
        if sys.platform == 'darwin':  # MacOS
            ret['total cpu time'] = (ret['user'] + ret['sys'])
        else:  # Assume Linux
            ret['total cpu time'] = (ret['User time (seconds)'] +
                                     ret['System time (seconds)'])
        return ret


def _process_out_file(file_name, suite_start_time, validate=False):
    """Extract data from the out log file."""
    if not os.path.exists(file_name):
        sys.exit('No file with path {0}'.format(file_name))
    with open(file_name, 'r') as out_file:
        ret = {}
        lines = out_file.readlines()

        # Get start time.
        if lines[0].startswith(prof.SUITE_STARTUP_STRING):
            ret['suite start time'] = float(
                lines[0][len(prof.SUITE_STARTUP_STRING):])

        # Scan through log entries.
        ret['memory'] = []
        loop_mem_entries = []
        for line in lines:
            # Profile summary.
            match = prof.SUMMARY_LINE_REGEX.search(line)
            if match:
                ret['function calls'] = int(match.groups()[0])
                ret['primitive function calls'] = int(match.groups()[1])
                ret['cpu time'] = float(match.groups()[2])
                continue

            # Memory info.
            match = prof.MEMORY_LINE_REGEX.search(line)
            if match:
                memory, module, checkpoint = tuple(match.groups())
                ret['memory'].append((module, checkpoint, int(memory),))

                # Main loop memory info.
                if not validate:
                    match = prof.LOOP_MEMORY_LINE_REGEX.search(checkpoint)
                    if match:
                        loop_no, time_str = match.groups()
                        loop_mem_entries.append((
                            int(loop_no),
                            int(get_unix_time_from_time_string(time_str)),
                        ))
                continue

            # Sleep time.
            match = prof.SLEEP_FUNCTION_REGEX.search(line)
            if match:
                ret['sleep time'] = float(match.groups()[0])
                continue

        # Number of loops.
        if not validate:
            ret['loop count'] = loop_mem_entries[-1][0]
            ret['avg loop time'] = (float(loop_mem_entries[-1][1] -
                                    loop_mem_entries[0][1]) /
                                    loop_mem_entries[-1][0])

        # Maximum memory usage.
        ret['mxmem'] = max([entry[2] for entry in ret['memory']])

        # Startup time (time from running cmd to reaching the end of the first
        # loop).
        if not validate:
            ret['startup time'] = (loop_mem_entries[0][1] -
                                   round(float(suite_start_time), 1))

        # Awake CPU time.
        if not validate:
            ret['awake cpu time'] = (ret['cpu time'] - ret['sleep time'])

    return ret


def _plot_single(prof_results, run_names, versions, metric, _, axis, c_map):
    """Create a bar chart comparing the results of all runs."""
    # Bar chart parameters.
    n_groups = len(versions)
    n_bars = len(run_names)
    ind = numpy.arange(n_groups)
    spacing = 0.1
    width = (1. - spacing) / n_bars

    # Colour map.
    colours = [c_map(x / (n_bars - 0.99)) for x in range(n_bars)]

    # Iterate over runs.
    for run_no, run_name in enumerate(run_names):
        # Get data from results. NOTE: All results are pre-sorted.
        y_data = [prof_result[metric] for key, prof_result in prof_results if
                  key[3] == str(run_name)]

        axis.bar(ind + (run_no * width), y_data, width,
                 label=str(run_name), color=colours[run_no])

    # Plot labels.
    axis.set_xticks(ind + ((width * n_bars) / 2.))
    axis.set_xticklabels([version['name'] for version in versions])
    axis.set_xlabel('Cylc Version')
    axis.set_xlim([0, (1. * n_groups) - spacing])
    if len(run_names) > 1:
        axis.legend(loc='upper left', prop={'size': 9})


def _plot_scale(prof_results, run_names, versions, metric, experiment_options,
               axis, c_map, lobf_order=2):
    """Create a scatter plot with line of best fit interpreting float(run_name)
    as the x-axis value."""
    # Colour map.
    colours = [c_map(x / (len(versions) - 0.99)) for x in range(len(versions))]
    # Plot labels.
    x_data = [int(run_name) for run_name in run_names]

    # Iterate over versions.
    for ver_no, version in enumerate(reversed(versions)):
        # Get data from results. NOTE: All versions & results are pre-sorted.
        y_data = [prof_result[metric] for key, prof_result in prof_results if
                  key[1] == version['id']]

        # Plot data point.
        if lobf_order >= 1:
            axis.plot(x_data, y_data, 'x', color=colours[ver_no])
        else:
            axis.plot(x_data, y_data, 'x', color=colours[ver_no],
                      label=version['name'])

        # Compute and plot line of best fit.
        if lobf_order >= 1:
            if lobf_order > 8:
                reset = 3
                print('WARNING: Line of best fit order too high (%s). Order '
                      'has been set to %s.' % (lobf_order, reset))
                lobf_order = reset
            lobf = numpy.polyfit(x_data, y_data, lobf_order)
            line = numpy.linspace(x_data[0], x_data[-1], 100)
            points = numpy.poly1d(lobf)(line)
            axis.plot(line, points, '-', color=colours[ver_no],
                      label=version['name'])

        # Plot labels.
        axis.set_xlabel(experiment_options.get('x-axis', 'Tasks'))
        axis.legend(loc='upper left', prop={'size': 9})


def get_consistent_metrics(prof_results, quick_analysis=False):
    metrics = None
    for prof_result in prof_results:
        result_metrics = set(prof_result[1])
        if metrics:
            metrics = metrics & result_metrics
        else:
            metrics = result_metrics
    if quick_analysis:
        return sorted(metrics & prof.QUICK_ANALYSIS_METRICS)
    return sorted(metrics)


def extract_results(result_files, profile_modes, validate_mode=False):
    """Return a dictionaty of (averaged) results extracted from result_files.

    Args:
        result_files (list): A list of result files to process.
        profile_modes (list): A list of profiling modes (e.g. 'time').
        validate_mode (bool): If True results are processed as for cylc
            validate.

    Return:
        dict: Dictionary of the form {'key': value} where key is a numerical
        index (see cylc.profiling for details).

    """
    processed_results = []
    for result_file in result_files:
        if prof.PROFILE_MODE_TIME in profile_modes:
            try:
                processed_results.append(_process_time_file(result_file['time']))
            except Exception:
                raise prof.AnalysisException(
                    'Analysis failed for method "%s" in file "%s".' % (
                        prof.PROFILE_MODE_TIME, result_file['time']))
        if prof.PROFILE_MODE_CYLC in profile_modes:
            suite_start_time = None
            try:
                if not validate_mode:
                    suite_start_time = _get_startup_time(result_file['startup'])
                processed_results.append(_process_out_file(
                    result_file['cmd-out'], suite_start_time, validate_mode))
            except Exception:
                raise prof.AnalysisException(
                    'Analysis failed for method "%s" in file "%s".' % (
                        prof.PROFILE_MODE_CYLC, result_file['out']))

    ret = {}
    for metric in processed_results[0]:
        # Get key for metric.
        for key, metrics in prof.METRICS.items():
            if metric in metrics[3]:  # Field name.
                break
        else:
            # Metric is not required - skip.
            continue
        # Compute average over repeats.
        ret[key] = _mean([processed_results[i][metric] for
                         i in range(len(processed_results))])

    return ret


# TODO Move?
def get_metric_title(metric):
    """Return a user-presentable title for a given metric key."""
    metric_title = prof.METRICS[metric][prof.METRIC_TITLE]
    metric_unit = prof.METRICS[metric][prof.METRIC_UNIT]
    if metric_unit:
        metric_title += ' (' + metric_unit + ')'
    return metric_title


def plot(conn, platform, versions, experiment, plot_dir, quick_analysis=True,
         lobf_order=2):
    """Plot results.

    Args:
        conn (sqlite3.Connection): Results database connection.
        platform (str): Select results for the given platform.
        versions (list): List of version dictionaries. Select results for given
            versions.
        experiment (dict): Experiment dictionay. Select results for given
            experiment.
        plot_dir (str/bool): Directory to safe plot files to. If False plots
            will be displayed interractively.
        quick_analysis (bool): If True only plot a short-list of metrics.

    """
    if not CAN_PLOT:
        sys.exit('\nERROR: Plotting requires numpy and maplotlib so cannot be '
                 'run.')

    # Obtain relevant results.
    prof_results = results.get_dict(
        conn,
        platform,
        [version['id'] for version in versions],
        experiment['id'],
        sort=True)  # This sorts all results keys appropriately.

    # Obtain list of relevant metrics.
    metrics = get_consistent_metrics(prof_results, quick_analysis)

    # Obtain experiment configuration.
    experiment_options = results.get_experiment_options_from_db(
        conn, experiment['id'])

    # Obtain sorted list of run names.
    run_names = sorted(set(result[0][3] for result in prof_results))
    try:
        # If all run names are integers cast them else leave them as strings.
        run_names = map(int, run_names)
    except ValueError:
        pass

    # Plot parameters.
    c_map = colour_map.Set1
    plot_type = experiment_options.get('analysis', 'single')

    # Create one plot per metric.
    for metric in metrics:
        # Set up plotting.
        fig = plt.figure(111)
        axis = fig.add_subplot(111)

        plot_args = (prof_results, run_names, versions, metric,
                     experiment_options, axis, c_map)
        if plot_type == 'single':
            _plot_single(*plot_args)
        elif plot_type == 'scale':
            _plot_scale(*plot_args, lobf_order=lobf_order)

        # Common config.
        axis.grid(True)
        axis.set_ylabel(get_metric_title(metric))

        # Output graph.
        if not plot_dir:
            # Output directory not specified, use interractive mode.
            plt.show()
        else:
            # Output directory specified, save figure as a pdf.
            fig.savefig(os.path.join(
                plot_dir,
                prof.METRICS[metric][prof.METRIC_FILENAME] +
                '.pdf'))

            fig.clear()


def tabulate(conn, platform, versions, experiment, quick_analysis,
             markdown=False):
    """Print a table of results.

    Args:
        conn (sqlite3.Connection): Results database connection.
        platform (str): Platform to display results for.
        versions (list): List of version dictionaries representing cylc versions
            to display results for.
        experiment (dict): Experiment dictionary representing the profiling
            experiment to display results for.
        quick_analysis (bool): If True only display a subset of results.
        markdown (bool): If True output the table in markdown format.

    """
    prof_results = results.get_dict(
        conn,
        platform,
        [version['id'] for version in versions],
        experiment['id'],
        sort=True)  # This sorts all results keys appropriately.

    # TODO!?
    metrics = get_consistent_metrics(prof_results, quick_analysis)

    # Make header rows.
    table = [['Version', 'Run'] + [get_metric_title(metric) for
                                   metric in sorted(metrics)]]

    for (_, version_id, _, run_name), result_fields in prof_results:
        row = [version_id, run_name]
        for metric in metrics:
            try:
                row.append(result_fields[metric])
            except KeyError:
                    raise prof.AnalysisException(  # TODO: Remove?
                    'Could not make results table as results are incomplete. '
                    'Metric "%s" missing from %s:%s at version %s' % (
                        metric, experiment['name'], run_name, version_id
                    ))
        table.append(row)

    kwargs = {'transpose': not quick_analysis, 'headers': True}
    if markdown:  # Move into print_table in the long run?
        kwargs.update({'seperator': ' | ', 'border': '|', 'headers': True})
    prof._write_table(table, **kwargs)
