"""Functionalty for generating / writing the "main-suite" which orchestrates
profiling."""

import os
import socket
import sys

from parsec.config import ItemNotFoundError
from cylc.cfgspec.globalcfg import GLOBAL_CFG
import cylc.profiling as prof

LOCALHOST = socket.gethostname()


def get_prof_script(reg, options, profile_modes, mode):
    """Generate the cylc run script for running a profiling experiment.

    Args:
        reg (str): The suite registration.
        options (list): List of Jinja2 variables to provide as 'key=value'
            pairs.
        profile_modes (list): List of profile modes to use (e.g.
            prof.PROFILE_MODE_TIME).
        mode (str): The cylc run mode to profile the suite using (e.g. live).

    Return:
        str: Bash script.

    """
    cmds = []

    # Cylc profiling, echo command start time.
    if prof.PROFILE_MODE_CYLC in profile_modes:
        cmds += ['echo', prof.SUITE_STARTUP_STRING, r'$(date +%s.%N)', '&&']

    # /usr/bin/time profiling.
    if prof.PROFILE_MODE_TIME in profile_modes:
        # TODO: This needs to be applied to the host!
        if sys.platform == 'darwin':  # MacOS
            cmds += ['/usr/bin/time', '-lp']
        else:  # Assume Linux
            cmds += ['/usr/bin/time', '-v']

        # Run using `sh -c` to enable the redirecton of output (darwins
        # /usr/bin/time command does not have a -o option).
        cmds += ['sh', '-c', '"']

    # Cylc run.
    run_cmds = []
    if mode == 'validate':
        run_cmds = ['cylc', 'validate']
    else:
        run_cmds = ['cylc', 'run', '--mode', mode]
    run_cmds += [reg]
    cmds += run_cmds

    # Jinja2 params.
    cmds.extend(['-s {0}'.format(option) for option in options])

    # Cylc profiling.
    if prof.PROFILE_MODE_CYLC in profile_modes:
        if mode == 'validate':
            sys.exit('ERROR: profile_mode "cylc" not possible in validate '
                     'mode')
        else:
            cmds += ['--profile']

    # No-detach mode.
    if mode != 'validate':
        cmds += ['--no-detach']

    # Redirect cylc output to the cylc .out and .err files.
    cmds += ['>', r'\"${CYLC_TASK_LOG_ROOT}.out\"',
             '2>', r'\"${CYLC_TASK_LOG_ROOT}.err\"']
    if prof.PROFILE_MODE_TIME in profile_modes:
        cmds += ['"']  # Close shell.

    # Redirect profiling output.
    cmds += ['>', 'startup', '2>', 'time-err']

    return ' '.join(cmds)


def write_profiling_suite(schedule, writer, install_dir, reg_base=''):
    """Generate a suite.rc configuration file for the "main-suite".

    Args:
        schedule (iterable): Collection of (version, experiment) tuples to
            profile. TODO.
        writer (fcn): Function to write out file using.
        install_dir (str): The directory that the required cylc suites /
            versions etc are installed in.
        reg_base (str): Prefix string for suite registrations.

    """
    graph = []  # Graph lines.
    runtime = {}  # "[runtime]..."

    # List of used parameter values.
    experiment_keys = set([])
    version_keys = set([])
    run_keys = set([])
    repeat_max = max(r['repeats'] for _, _, e, _ in schedule for r in
                     e['config']['runs'])

    for platform, version, experiment, run_name in sorted(schedule):
        # Get safe version name (used in cylc task name).
        ver_name = prof.safe_name(version['name'])
        version_keys.add(ver_name)

        # Get safe experiment name (used in cylc task name).
        exp_name = prof.safe_name(experiment['name'])
        experiment_keys.add(exp_name)

        # Add a family for this experiment (if not already done).
        if exp_name.upper() not in runtime:
            runtime[exp_name.upper()] = {}

        # Loop over this experiment's runs.
        for run in experiment['config']['runs']:
            if run['name'] != run_name:
                continue

            # Get safe run name (used in cylc task name).
            run_name = prof.safe_name(run['name'])
            run_keys.add(run_name)

            # Generate task name.
            task = ('prof'
                    '<experiment=%s, ' % exp_name +
                    'version=%s, ' % ver_name +
                    'run=%s, ' % run_name)

            # Handle repeats.
            for repeat in range(run['repeats'] + 1):
                repeat_no = ('0' * (len(str(repeat_max)) - len(str(repeat)))
                             + str(repeat))
                graph.append(task + 'repeat=%s>' % repeat_no)

            # Generate a registration name for this suite.
            cylc_major_version = version['id'].split('.')[0]
            if int(cylc_major_version) >= 7:
                suite_reg = os.path.join(reg_base, '${CYLC_TASK_NAME}')
            else:
                suite_reg = '%s.%s' % (reg_base, '${CYLC_TASK_NAME}')

            # Generate the script for this task.
            script = get_prof_script(
                suite_reg,
                run['options'] + ['cylc_compat_mode=%s' % cylc_major_version],
                experiment.get('profile modes', ['time']),
                experiment['config'].get('mode', 'live')
            )

            # Add a [runtime] entry for this task.

            if 'globalrc' in run:
                cylc_conf_path = os.path.join(install_dir, run['globalrc'])
            else:
                cylc_conf_path = ''
            runtime[task + 'repeat>'] = {
                'inherit': exp_name.upper(),
                'pre-script': 'cylc reg "%s" "${SUITE_DIR}"' % suite_reg,
                'script': script,
                'environment': {
                    'PATH': '"%s:${PATH}"' % os.path.join(
                        install_dir, prof.PROFILE_CYLC_DIR, version['id'],
                        'bin'),
                    'CYLC_CONF_PATH': cylc_conf_path,
                    'SUITE_DIR': run['suite dir']
                }
            }

            # Add platform configuration to the task.
            if platform != LOCALHOST:
                try:
                    runtime[task + 'repeat>'].update(
                        GLOBAL_CFG.get(['profile battery', platform]))
                except ItemNotFoundError:
                    raise prof.ProfilingException(
                        'WARNING: No configuration for platform "%s" '
                        'found in global configuration,' % platform)

    # Assemble a suite config.
    cfg = {
        'cylc': {
            'parameters': {
                'experiment': ', '.join(experiment_keys),
                'version': ', '.join(version_keys),
                'run': ', '.join(run_keys),
                'repeat': '0..%d' % repeat_max
            },
            'parameter templates': {
                'experiment': '__exp_%(experiment)s',
                'version': '__ver_%(version)s',
                'run': '__run_%(run)s',
                'repeat': '__repeat_%(repeat)s'
            }
        },
        'scheduling': {
            'dependencies': {
                'graph': '"""\n{0}{1}\n{2}"""'.format(
                    ' ' * 12, ('\n' + ' ' * 12).join(graph), ' ' * 8)
            },
            'queues': {
                'default': {
                    'limit': 1
                }
            }
        },
        'runtime': {
            'root': {
                'post-script': 'touch success',
                'job': {
                    'execution time limit': 'PT6H'  # Default.
                }
            }
        }
    }

    # Add the task's runtime sections to the config.
    cfg['runtime'].update(runtime)

    # Write out the suite.rc file.
    write_suiterc(cfg, writer)


def write_suiterc(cfg, writer, level=0):
    """Write a suite.rc file from the provided config.

    Args:
        cfg (dict): Nested dictionary containing configuration items.
        writer (fcn): Function to write out file using.
        level (int): The indentation level to write this configuration section
            at.

    """
    stack = []
    for key, value in cfg.iteritems():
        if isinstance(value, dict):
            stack.append((key, value))
        else:
            writer(' ' * (level * 4) +
                   '%s = %s' % (key, value) +
                   '\n')
    for key, value in stack:
        writer(' ' * (level * 4) +
               '[' * (level + 1) + key + ']' * (level + 1) +
               '\n')
        write_suiterc(value, writer, level + 1)
