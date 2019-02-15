"""Standin for the text functions of the old cylc graph utility."""
import sys

from cylc.config import SuiteConfig
from cylc.cycling.loader import get_point, get_point_relative
from cylc.suite_srv_files_mgr import SuiteSrvFilesManager, SuiteServiceFileError


def sort_key(item):
    """Sort cycle points."""
    name, point = item.split('.')
    try:
        # integer cycling - cast cycle point as integer
        return (name, int(point))
    except ValueError:
        # datetime cycling - ISO8601 cycle points as lexicographically sortable
        return item


def get_cycling_bounds(config, start_point=None, stop_point=None):
    """Determine the start and stop points for graphing a suite."""
    # default start and stop points to values in the visualization section
    if not start_point:
        start_point = config.cfg['visualization']['initial cycle point']
    if not stop_point:
        viz_stop_point = config.cfg['visualization']['final cycle point']
        if viz_stop_point:
            stop_point = viz_stop_point

    # don't allow stop_point before start_point
    if stop_point is not None:
        if stop_point < start_point:
            # Avoid a null graph.
            stop_point = start_point
        else:
            stop_point = stop_point
    else:
        stop_point = None

    return start_point, stop_point


def sort_datetime_edge(item):
    return (item[0], item[1] or '')


def sort_integer_node(item):
    name, point = item.split('.')
    return (name, int(point))


def sort_integer_edge(item):
    return (
        sort_integer_node(item[0]),
        sort_integer_node(item[1]) if item[1] else ('', 0)
    )


def main(suite, start_point=None, stop_point=None):
    """Implement ``cylc-graph -O <FILE>.graph.plain``."""
    # load config
    try:
        suiterc = SuiteSrvFilesManager().get_suite_rc(suite)
    except SuiteServiceFileError:
        # could not find suite, assume we have been given a path instead
        suiterc = suite
        suite = 'test'
    config = SuiteConfig(suite, suiterc)

    # determine sorting
    if config.cfg['scheduling']['cycling mode'] == 'integer':
        # integer sorting
        node_sort = sort_integer_node
        edge_sort = sort_integer_edge
    else:
        # datetime sorting
        node_sort = None  # lexicographically sortable
        edge_sort = sort_datetime_edge

    # get graph
    start_point, stop_point = get_cycling_bounds(config, start_point, stop_point)
    graph = config.get_graph_raw(start_point, stop_point)
    if not graph:
        return

    # for line in graph: print(line)

    # print edges
    for left, right, *_ in sorted(graph, key=edge_sort):
        if right:
            print('edge "%s" "%s"' % (left, right))

    print('graph')
    
    for node in sorted(set(x for y in graph for x in y[0:2] if x),
                       key=node_sort):
        print('node "%s" "%s"' % (node, node.replace('.', r'\n')))

    print('stop')


if __name__ == '__main__':
    main(*sys.argv[1:])
