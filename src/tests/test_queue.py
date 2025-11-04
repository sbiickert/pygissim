import unittest
from typing import Optional, Tuple

from pygissim.engine import *
from tests.test_network import *
from tests.test_compute import *

class TestQueue(unittest.TestCase):
    def test_create(self):
        conn_q: MultiQueue = TestQueue.sample_connection_queue()
        self.assertTrue(isinstance(conn_q.service_time_calculator, Connection))
        self.assertEqual(TestConnection.sample_conn_to_internet(), conn_q.service_time_calculator)

        vm_q: MultiQueue = TestQueue.sample_compute_queue()
        self.assertTrue(isinstance(vm_q.service_time_calculator, ComputeNode))
        self.assertEqual(TestComputeNode.sample_vhost(), vm_q.service_time_calculator)
    
    def test_network_enqueue(self):
        # A single channel. All requests will queue up in order of arrival
        # The connection has non-zero latency, so requests end up in latency_holding until time is up
        conn_q: MultiQueue = TestQueue.sample_connection_queue()
        conn_q.enqueue(TestQueue.sample_connection_cr(), clock=13)
        self.assertEqual(1, len(conn_q.latency_holding))
        self.assertEqual(1, conn_q.request_count())
        self.assertEqual(1, conn_q.available_channel_count())
        # Need to wait until latency is accounted for
        # 13 ms + 100 ms latency
        # 13 ms + 160 ms ST + 100 ms latency
        self.assertIsNotNone(conn_q.next_event_time())
        self.assertEqual(13 + 100, conn_q.next_event_time())

        conn_q.enqueue(TestQueue.sample_connection_cr(), clock=15)
        conn_q.enqueue(TestQueue.sample_connection_cr(), clock=16)
        self.assertEqual(3, conn_q.request_count())
        self.assertEqual(13 + 100, conn_q.next_event_time())

        # Give enough time for the first request's latency to be processed
        finished: list[Tuple[(ClientRequest, RequestMetric)]] = conn_q.remove_finished_requests(13 + 100)
        self.assertEqual(0, len(finished))
        self.assertEqual(3, conn_q.request_count())
        self.assertEqual(2, len(conn_q.latency_holding))
        self.assertEqual(0, conn_q.available_channel_count())

        # Give enough time for the second and third requests' latency to be processed
        finished = conn_q.remove_finished_requests(15 + 100)
        self.assertEqual(0, len(finished))
        finished = conn_q.remove_finished_requests(16 + 100)
        self.assertEqual(0, len(finished))
        self.assertEqual(3, conn_q.request_count())
        self.assertEqual(0, len(conn_q.latency_holding))
        self.assertEqual(0, conn_q.available_channel_count())
        self.assertEqual(2, len(conn_q.main_queue))

        # The next time should be when the first request has been processed
        # 13 ms + 160 ms ST + 100 ms latency
        self.assertEqual(13 + 160 + 100, conn_q.next_event_time())
        finished = conn_q.remove_finished_requests(13 + 160 + 100)
        self.assertEqual(1, len(finished))
        self.assertEqual(0, finished[0][1].queue_time)
        self.assertEqual(0, finished[0][1].service_time)
        self.assertEqual(160, finished[0][1].network_time)
        self.assertEqual(100, finished[0][1].latency_time)
        self.assertEqual(2, conn_q.request_count())
        self.assertEqual(0, len(conn_q.latency_holding))
        self.assertEqual(0, conn_q.available_channel_count())
        self.assertEqual(1, len(conn_q.main_queue))

        # Give enough time for the second request to be processed
        next_clock: Optional[int] = conn_q.next_event_time()
        self.assertIsNotNone(next_clock)
        if next_clock is not None:
            finished = conn_q.remove_finished_requests(next_clock)
            self.assertEqual(1, len(finished))
            self.assertEqual(1, conn_q.request_count())
            self.assertEqual(0, conn_q.available_channel_count())
            self.assertEqual(160 - (15-13), finished[0][1].queue_time) # Arrived 2 ms after the first request
            self.assertEqual(160, finished[0][1].network_time)
            self.assertEqual(100, finished[0][1].latency_time)

    def test_compute_enqueue(self):
        comp_q: MultiQueue = TestQueue.sample_compute_queue()
        comp_q.enqueue(TestQueue.sample_compute_cr(), clock=13)
        self.assertEqual(1, comp_q.request_count())
        self.assertEqual(3, comp_q.available_channel_count()) # 4 vCores, one busy
        st: int = 505 # 141 ms then adjusted for HT and slow hardware
        self.assertEqual(st + 13, comp_q.next_event_time())

        comp_q.enqueue(TestQueue.sample_compute_cr(), clock=23)
        comp_q.enqueue(TestQueue.sample_compute_cr(), clock=33)
        self.assertEqual(3, comp_q.request_count())
        self.assertEqual(1, comp_q.available_channel_count()) # 4 vCores, three busy

        comp_q.enqueue(TestQueue.sample_compute_cr(), clock=43)
        comp_q.enqueue(TestQueue.sample_compute_cr(), clock=53)
        self.assertEqual(5, comp_q.request_count())
        self.assertEqual(0, comp_q.available_channel_count()) # 4 vCores, 4 busy, 1 queued

        finished: list[Tuple[(ClientRequest, RequestMetric)]] = comp_q.remove_finished_requests(13 + st)
        self.assertEqual(1, len(finished))
        self.assertEqual(4, comp_q.request_count())
        self.assertEqual(0, comp_q.available_channel_count()) # 4 vCores, 4 busy, 0 queued
        self.assertEqual(0, finished[0][1].queue_time)
        self.assertEqual(st, finished[0][1].service_time)
        self.assertEqual(0, finished[0][1].latency_time) # No latency time for compute

        finished = comp_q.remove_finished_requests(23 + st)
        self.assertEqual(1, len(finished))
        self.assertEqual(3, comp_q.request_count())
        self.assertEqual(1, comp_q.available_channel_count()) # 4 vCores, 3 busy, 0 queued
        self.assertEqual(0, finished[0][1].queue_time)
        self.assertEqual(st, finished[0][1].service_time)

        finished = comp_q.remove_finished_requests(43 + st)
        self.assertEqual(2, len(finished))
        self.assertEqual(1, comp_q.request_count())
        self.assertEqual(3, comp_q.available_channel_count()) # 4 vCores, 1 busy, 0 queued

        # The last request was queueing until the first request finished
        self.assertEqual(13 + st + st, comp_q.next_event_time())
        finished = comp_q.remove_finished_requests(13 + st + st)
        self.assertEqual(1, len(finished))
        self.assertEqual(0, comp_q.request_count())
        self.assertEqual(4, comp_q.available_channel_count()) # 4 vCores, 0 busy, 0 queued
        self.assertEqual(st - (53-13), finished[0][1].queue_time)
        self.assertEqual(st, finished[0][1].service_time)

    @classmethod
    def sample_connection_queue(cls) -> MultiQueue:
        conn: Connection = TestConnection.sample_conn_to_internet()
        return MultiQueue(st_calculator=conn, wait_mode=WaitMode.TRANSMITTING, channel_count=1)
    
    @classmethod
    def sample_compute_queue(cls) -> MultiQueue:
        vm: ComputeNode = TestComputeNode.sample_vhost()
        return MultiQueue(st_calculator=vm, wait_mode=WaitMode.PROCESSING, channel_count=vm.vcore_count())
    
    @classmethod
    def sample_connection_cr(cls) -> ClientRequest:
        step: ClientRequestSolutionStep = ClientRequestSolutionStep(st_calculator=cls.sample_connection_queue().service_time_calculator, 
                                                                    is_response=True, data_size=2000, chatter=10, service_time=0)
        sln: ClientRequestSolution = ClientRequestSolution(steps=[step])
        return ClientRequest(name=ClientRequest.next_name(), desc='', wf_name='', request_clock=10, solution=sln, tx_id=Transaction.next_id())
    
    @classmethod
    def sample_compute_cr(cls) -> ClientRequest:
        step: ClientRequestSolutionStep = ClientRequestSolutionStep(st_calculator=cls.sample_compute_queue().service_time_calculator,
                                                                    is_response=True, data_size=2000, chatter=0, service_time=141)
        sln: ClientRequestSolution = ClientRequestSolution(steps=[step])
        return ClientRequest(name=ClientRequest.next_name(), desc='', wf_name='', request_clock=10, solution=sln, tx_id=Transaction.next_id())