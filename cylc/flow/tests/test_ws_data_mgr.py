from unittest import main

from cylc.flow.tests.util import CylcWorkflowTestCase, create_task_proxy
from cylc.flow.ws_data_mgr import WsDataMgr


class TestWsDataMgr(CylcWorkflowTestCase):

    suite_name = "five"
    suiterc = """
[meta]
    title = "Inter-cycle dependence + a cold-start task"
[cylc]
    UTC mode = True
[scheduling]
    #runahead limit = 120
    initial cycle point = 20130808T00
    final cycle point = 20130812T00
    [[dependencies]]
        [[[R1]]]
            graph = "prep => foo"
        [[[PT12H]]]
            graph = "foo[-PT12H] => foo => bar"
[visualization]
    initial cycle point = 20130808T00
    final cycle point = 20130808T12
    [[node attributes]]
        foo = "color=red"
        bar = "color=blue"

    """

    def setUp(self) -> None:
        super(TestWsDataMgr, self).setUp()
        self.ws_data_mgr = WsDataMgr(self.scheduler)

    def test_constructor(self):
        self.assertEqual(
            f"{self.owner}/{self.suite_name}",self.ws_data_mgr.workflow_id)
        self.assertIsNone(self.ws_data_mgr.pool_points)

    def test_generate_definition_elements(self):
        self.ws_data_mgr.generate_definition_elements()
        tasks_generated = self.ws_data_mgr.tasks
        self.assertEqual(3, len(tasks_generated))

    def test_increment_graph_elements(self):
        self.assertFalse(self.ws_data_mgr.pool_points)
        self.ws_data_mgr.increment_graph_elements()
        self.assertTrue(self.ws_data_mgr.pool_points)

    def test_generate_graph_elements(self):
        self.ws_data_mgr.generate_graph_elements()
        tasks_proxies_generated = self.ws_data_mgr.task_proxies
        self.assertEqual(3, len(tasks_proxies_generated))

    def test_update_task_proxies(self):
        """Test update_task_proxies. This method will iterate each of the
        task IDs given, and update the data in WsDataMgr.task_proxies."""

        # we need to populate the TaskPool
        for task_name in ["prep", "foo", "bar"]:
            task_proxy = create_task_proxy(
                task_name=task_name,
                suite_config=self.suite_config,
                is_startup=True
            )
            warnings = self.task_pool.insert_tasks(
                items=[task_proxy.identity],
                stopcp=None,
                no_check=False
            )
            assert 0 == warnings

        self.task_pool.release_runahead_tasks()
        # FIXME: expected PbTaskProxy got TaskProxy
        # needs a TaskProxy?
        self.ws_data_mgr.task_proxies = list(
            self.task_pool.pool.values())[0].copy()

        task_identity = 'prep.20130808T0000Z'
        self.ws_data_mgr.update_task_proxies(task_ids=[task_identity])


if __name__ == '__main__':
    main()
