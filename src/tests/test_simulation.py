import unittest
from typing import Optional, Tuple

from pygissim.engine import *
from pygissim import *

from tests.test_design import *

class TestSimulation(unittest.TestCase):
    def test_create(self):
        sim: Simulator = TestSimulation.sample_simulator()
        self.assertEqual('Test Simulator', sim.name)
        self.assertFalse(sim.is_generating_new_requests)

    def test_advancing_time(self):
        sim: Simulator = TestSimulation.sample_simulator()
        sim.design = TestDesign.sample_design()
        self.assertTrue(sim.design.is_valid())
        self.assertIsNone(sim.next_event_time())

        sim.start()
        self.assertTrue(sim.is_generating_new_requests)
        next_time: Optional[int] = sim.next_event_time()
        self.assertIsNotNone(next_time)
        if next_time is None: return

        # print(f'clock is {sim.clock}. Advancing to {next_time}')
        sim.advance_time_to(next_time)
        self.assertTrue(sim.clock > 0)
        non_empty_queues: list[MultiQueue] = list(filter(lambda q: (q.request_count() > 0), sim.queues))
        self.assertTrue(len(non_empty_queues) > 0)

        # Turning off for testing purposes
        sim.is_generating_new_requests = False
        next_time = sim.next_event_time()
        self.assertIsNotNone(next_time)
        if next_time is None: return
        self.assertTrue(sim.clock < next_time)

        while next_time is not None:
            # print(next_time)
            sim.advance_time_to(next_time)
            next_time = sim.next_event_time()
        
        self.assertTrue(len(sim.finished_requests) > 0)
        # for fr in sim.finished_requests:
        #     print(f'{fr.name}')
        non_empty_queues = list(filter(lambda q: (q.request_count() > 0), sim.queues))
        self.assertTrue(len(non_empty_queues) == 0)
        # print(len(sim.request_metrics))
        # for rm in sim.request_metrics:
        #     print(rm)

    def test_queue_metrics(self):
        sim: Simulator = TestSimulation.sample_simulator()
        sim.design = TestDesign.sample_design()
        sim.start()

        for i in range(0,10):
            sim.advance_time_by(500)
            sim.gather_queue_metrics()

        sim.stop()

        non_empty_queues = list(filter(lambda q: (q.request_count() > 0), sim.queues))
        self.assertTrue(len(non_empty_queues) > 0)
        self.assertEqual(10 * len(sim.queues), len(sim.queue_metrics))
        # print(len(sim.queue_metrics))
        # for qm in sim.queue_metrics:
        #     print(qm)

        

    @classmethod
    def sample_simulator(cls) -> Simulator:
        return Simulator(name='Test Simulator', desc='Sim for unit testing')