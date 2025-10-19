"""
engine.py

These classes and functions are the building blocks for simulating a compute system.
They are grouped in this file by function: (general, network, compute, queue, work)

"""
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Set, Tuple
from functools import reduce
from numpy import random
from abc import ABC, abstractmethod
import uuid

#  d888b  d88888b d8b   db d88888b d8888b.  .d8b.  db     
# 88' Y8b 88'     888o  88 88'     88  `8D d8' `8b 88     
# 88      88ooooo 88V8o 88 88ooooo 88oobY' 88ooo88 88     
# 88  ooo 88~~~~~ 88 V8o88 88~~~~~ 88`8b   88~~~88 88     
# 88. ~8~ 88.     88  V888 88.     88 `88. 88   88 88booo.
#  Y888P  Y88888P VP   V8P Y88888P 88   YD YP   YP Y88888P

@dataclass(frozen=True)
class ValidationMessage:
    """ Returned by many classes in their 'validate' method. """
    message: str
    source: str

    def __str__(self):
        return f'VM {self.source}: "{self.message}"'
    
@dataclass(frozen=True)
class QueueMetric:
    """ DataClass for tracking the utilization rate of all queues (compute and network)

    :param source: The name of the MultiQueue that was measured.
    :type source: str
    :param stc_type: The type of the MultiQueue's :class:`ServiceTimeCalculator`'.
    :type stc_type: str
    :param clock: The simulation time (in ms) when the measurement was taken.
    :type clock: int
    :param channel_count: The number of parallel channels in the MultiQueue.
    :type channel_count: int
    :param request_count: The number of requests being processed and queued.
    """
    source: str
    stc_type: str
    clock: int
    channel_count: int
    request_count: int

    def __str__(self):
        return f'QM@{self.clock} ({self.source} channels:{self.channel_count} requests:{self.request_count})'
    
    def utilization(self) -> float:
        """
        Utility function to return the utilization of a network connection or a compute node.

        * Channels represents the number of parallel channels to do work. If they are all full, then 1.0 is returned (100%)
        * If all channels are full and more requests were waiting, then the result will be more than 1.0 (> 100%)
        """
        if self.channel_count == 0 or self.request_count == 0: return 0.0
        return self.request_count / self.channel_count

@dataclass(frozen=True)
class RequestMetric:
    """ DataClass for tracking the utilization rate of all queues (compute and network).
    A RequestMetric is reported every time a :class:`ClientRequest` leaves a :class:`MultiQueue`.
    The time values represent the service time, queue time and latency time the :class:`ClientRequest` experienced
    between being enqueued and completed.

    :param source: The name of the :class:`MultiQueue` that was measured.
    :type source: str
    :param clock: The simulation time (in ms) when the measurement was taken.
    :type clock: int
    :param request_name: The name of the :class:`ClientRequest`.
    :type request_name: str
    :param workflow_name: The name of the :class:`Workflow`.
    :type workflow_name: str
    :param service_time: The time spent processing the :class:`ClientRequest` (in ms).
    :type service_time: int
    :param queue_time: The time spent in queue (in ms).
    :type queue_time: int
    :param latency_time: The time spent in latency (in ms).
    :type latency_time: int
    """
    source: str
    clock: int
    request_name: str
    workflow_name: str
    service_time: int
    queue_time: int
    latency_time: int

    def __str__(self):
        return f'RM@{self.clock} ({self.request_name} ({self.workflow_name}) in {self.source} st:{self.service_time} qt:{self.queue_time} lt:{self.latency_time})'

class ServiceTimeCalculator(ABC):
    @abstractmethod
    def calculate_service_time(self, request: 'ClientRequest') -> int: pass
    @abstractmethod
    def calculate_latency(self, request: 'ClientRequest') -> int: pass
    @abstractmethod
    def provide_queue(self) -> 'MultiQueue': pass


# d8b   db d88888b d888888b db   d8b   db  .d88b.  d8888b. db   dD
# 888o  88 88'     `~~88~~' 88   I8I   88 .8P  Y8. 88  `8D 88 ,8P'
# 88V8o 88 88ooooo    88    88   I8I   88 88    88 88oobY' 88,8P  
# 88 V8o88 88~~~~~    88    Y8   I8I   88 88    88 88`8b   88`8b  
# 88  V888 88.        88    `8b d8'8b d8' `8b  d8' 88 `88. 88 `88.
# VP   V8P Y88888P    YP     `8b8' `8d8'   `Y88P'  88   YD YP   YD

# ------------------------------------------------------------
class ZoneType(Enum):
    LOCAL = 'local'
    EDGE = 'edge'
    INTERNET = 'internet'

# ------------------------------------------------------------
class Connection(ServiceTimeCalculator):
    def __init__(self, source:'Zone', dest:'Zone', bw:int = 1000, lat:int = 0):
        self.source: Zone = source
        self.destination: Zone = dest
        self.bandwidth: int = bw
        self.latency_ms: int = lat
        self.name: str = f'{source.name} to {dest.name}'

    def __eq__(self, other):
        if not isinstance(other, Connection):
            return NotImplemented
        return self.source == other.source and \
            self.destination == other.destination and \
            self.bandwidth == other.bandwidth and \
            self.latency_ms == other.latency_ms
    
    def __str__(self):
        return f'Connection {self.name}'
    
    def description(self) -> str:
        return f'{self.source.description} to {self.destination.description}'
    
    def is_local(self) -> bool:
        return self.source == self.destination
    
    def inverted(self) -> 'Connection':
        return Connection(self.destination, self.source, self.bandwidth, self.latency_ms)

    def calculate_service_time(self, request: 'ClientRequest') -> Optional[int]:
        step: Optional[ClientRequestSolutionStep] = request.solution.current_step()
        if step is None: return None
        data_kb: int = step.data_size * 8
        # Mbps -> kbps -> kb per millisecond (which is the time scale of the simulation)
        bw_kbpms: int = int(self.bandwidth * 1000 / 1000)
        return int(data_kb / bw_kbpms)

    def calculate_latency(self, request) -> Optional[int]:
        step: Optional[ClientRequestSolutionStep] = request.solution.current_step()
        if step is None: return None
        return self.latency_ms * step.chatter
    
    def provide_queue(self) -> 'MultiQueue':
        return MultiQueue(self, WaitMode.TRANSMITTING, channel_count=2)


# ------------------------------------------------------------
class Zone:
    def __init__(self, name: str, desc: str, z_type: ZoneType):
        self.id = str(uuid.uuid1())
        self.name = name
        self.description = desc
        self.type = z_type
    
    def __str__(self):
        return f'Zone {self.name}'

    def connect(self, other: 'Zone', bw: int, lat: int) -> Connection:
        return Connection(self, other, bw, lat)

    def connect_both_ways(self, other: 'Zone', bw: int, lat: int) -> Tuple[Connection, Connection]:
        c1: Connection = Connection(self, other, bw, lat)
        c2 = c1.inverted()
        return (c1,c2)
    
    def self_connect(self, bw: int, lat: int) -> Connection:
        return Connection(self, self, bw, lat)
    
    def local_connection(self, in_network: list[Connection]) -> Optional[Connection]:
        conns = list(filter(lambda conn: (self == conn.source and self == conn.destination), in_network))
        if len(conns) >= 0:
            return conns[0]
        return None
    
    def connections(self, in_network: list[Connection]) -> list[Connection]:
        return list(filter(lambda conn: (self == conn.source or self == conn.destination), in_network))
    
    def entry_connections(self, in_network: list[Connection]) -> list[Connection]:
        return list(filter(lambda conn: (conn.is_local() == False and self == conn.destination), in_network))
    
    def exit_connections(self, in_network: list[Connection]) -> list[Connection]:
        return list(filter(lambda conn: (conn.is_local() == False and self == conn.source), in_network))
    
    def other_connections(self, in_network: list[Connection]) -> list[Connection]:
        return list(filter(lambda conn: (conn.source != self and conn.destination != self), in_network))

    def is_a_source(self, in_network: list[Connection]) -> bool:
        return len(list(filter(lambda conn: (self == conn.source), in_network))) > 0
    
    def is_a_destination(self, in_network: list[Connection]) -> bool:
        return len(list(filter(lambda conn: (self == conn.destination), in_network))) > 0
    
    def is_fully_connected(self, in_network: list[Connection]) -> bool:
        return self.local_connection(in_network) is not None and \
            len(self.entry_connections(in_network)) > 0 and \
            len(self.exit_connections(in_network)) > 0
    
    @classmethod
    def all_zones(cls, in_network: list[Connection]) -> Set['Zone']:
        result: Set[Zone] = set()

        for conn in in_network:
            result.add(conn.source)
            result.add(conn.destination)

        return result

# ------------------------------------------------------------
@dataclass(frozen=True)
class Route:
    connections: list[Connection]

    def __str__(self):
        result: str = f"Route ({self.count()}):\n"
        for c in self.connections:
            result = result + f"\t{c}\n"
        return result

    def count(self) -> int:
        return len(self.connections)

        

#  .o88b.  .d88b.  .88b  d88. d8888b. db    db d888888b d88888b
# d8P  Y8 .8P  Y8. 88'YbdP`88 88  `8D 88    88 `~~88~~' 88'    
# 8P      88    88 88  88  88 88oodD' 88    88    88    88ooooo
# 8b      88    88 88  88  88 88~~~   88    88    88    88~~~~~
# Y8b  d8 `8b  d8' 88  88  88 88      88b  d88    88    88.    
#  `Y88P'  `Y88P'  YP  YP  YP 88      ~Y8888P'    YP    Y88888P

# ------------------------------------------------------------
class ThreadingModel(Enum):
    PHYSICAL = 'physical'
    HYPERTHREADED = 'ht'

    def factor(self) -> float:
        if self is ThreadingModel.PHYSICAL:
            return 1.0
        else:
            return 0.5
        
# ------------------------------------------------------------
class BalancingModel(Enum):
    SINGLE = '1'
    ROUND_ROBIN = 'roundrobin'
    FAILOVER = 'failover'
    CONTAINERIZED = 'containerized'
    OTHER = 'other'

    @classmethod
    def from_str(cls, value: str) -> 'BalancingModel':
        match value.upper():
            case '1': return BalancingModel.SINGLE
            case 'ROUNDROBIN' | 'armv7': return BalancingModel.ROUND_ROBIN
            case 'FAILOVER': return BalancingModel.FAILOVER
            case 'CONTAINER': return BalancingModel.CONTAINERIZED
            case _: return BalancingModel.OTHER

# ------------------------------------------------------------
class ComputeArchitecture(Enum):
    INTEL = 'intel'
    ARM64 = 'arm64'
    RISCV = 'riscv'
    OTHER = 'other'

    @classmethod
    def from_str(cls, value: str) -> 'ComputeArchitecture':
        match value.lower():
            case 'intel': return ComputeArchitecture.INTEL
            case 'aarm64' | 'armv7': return ComputeArchitecture.ARM64
            case 'riscv': return ComputeArchitecture.RISCV
            case _: return ComputeArchitecture.OTHER

# ------------------------------------------------------------
class ComputeNodeType(Enum):
    CLIENT = 'client'
    P_SERVER = 'physical'
    V_SERVER = 'virtual'

# ------------------------------------------------------------
@dataclass(frozen=True)
class ServiceDef:
    name: str
    description: str
    service_type: str
    balancing_model: BalancingModel

# ------------------------------------------------------------
@dataclass(frozen=True)
class HardwareDef:
    processor: str
    cores: int
    specint_rate2017: float
    architecture: ComputeArchitecture
    threading: ThreadingModel

    def specint_rate2017_per_core(self) -> float:
        return self.specint_rate2017 / float(self.cores) * self.threading.factor()

    def __str__(self) -> str:
        return f'HW {self.processor} cores: {self.cores} spec: {self.specint_rate2017} ({self.architecture})'
    baseline_per_core: float = 10.0

# ------------------------------------------------------------
class ComputeNode(ServiceTimeCalculator):
    def __init__(self, name: str, desc: str, 
                 hw_def: HardwareDef, memory_GB: int,
                 zone: Zone, type: ComputeNodeType):
        self.id: str = str(uuid.uuid1())
        self.name: str = name
        self.description: str = desc
        self.hw_def: HardwareDef = hw_def
        self.memory_GB: int = memory_GB
        self.zone: Zone = zone
        self.type: ComputeNodeType = type

        self._v_cores: int = 0                   # if ComputeNodeType.V_SERVER
        self._v_hosts: list['ComputeNode'] = []    # if ComputeNodeType.P_SERVER

    def __str__(self) -> str:
        return f'CNode {self.name} {self.type} in {self.zone.name}'
    
    def vcore_count(self) -> int:
        return self._v_cores
    
    def set_vcore_count(self, count: int):
        if self.type == ComputeNodeType.V_SERVER:
            self._v_cores = count
        else:
            self._v_cores = 0

    def adjusted_service_time(self, st: int) -> int:
        relative = HardwareDef.baseline_per_core / self.hw_def.specint_rate2017_per_core()
        return int(float(st) * relative)

    def calculate_service_time(self, request: 'ClientRequest') -> Optional[int]:
        step: Optional[ClientRequestSolutionStep] = request.solution.current_step()
        if step is None: return None
        return self.adjusted_service_time(step.service_time)

    def calculate_latency(self, request) -> Optional[int]:
        return None # No latency on a compute calculation
    
    def provide_queue(self) -> 'MultiQueue':
        channel_count: int = 1
        match self.type:
            case ComputeNodeType.CLIENT:
                channel_count = 1000 # Arbitrary large number. Clients represent a group not a PC.
            case ComputeNodeType.P_SERVER:
                channel_count = self.hw_def.cores
            case ComputeNodeType.V_SERVER:
                channel_count = self.vcore_count()
        return MultiQueue(self, WaitMode.PROCESSING, channel_count)

    def add_virtual_host(self, name: str, v_cores: int, memory_GB: int):
        if self.type != ComputeNodeType.P_SERVER:
            raise TypeError("Can only add virtual hosts to physical servers.")
        
        if len(name) == 0:
            name = f'VH {self.name}: {len(self._v_hosts)}'
        v_host: ComputeNode = ComputeNode(name, '', self.hw_def, memory_GB, self.zone, ComputeNodeType.V_SERVER)
        v_host.set_vcore_count(v_cores)
        self._v_hosts.append(v_host)

    def remove_virtual_host(self, v_host:'ComputeNode'):
        self._v_hosts.remove(v_host)
    
    def virtual_host_count(self) -> int:
        return len(self._v_hosts)
    
    def virtual_host(self, index: int) -> Optional['ComputeNode']:
        if index >= 0 and index < len(self._v_hosts):
            return self._v_hosts[index]
        return None

    def total_vcpu_allocation(self) -> int:
        total: int = 0
        for v_host in self._v_hosts:
            total = total + v_host.vcore_count()
        return total
    
    def total_cpu_allocation(self) -> int:
        return int(float(self.total_vcpu_allocation()) * self.hw_def.threading.factor())
    
    def total_memory_allocation(self) -> int:
        total: int = 0
        for v_host in self._v_hosts:
            total = total + v_host.memory_GB
        return total

        

# ------------------------------------------------------------
class ServiceProvider:
    def __init__(self, name:str, desc: str, service: ServiceDef, nodes: list[ComputeNode]):
        self.name: str = name
        self.description: str = desc
        self.service: ServiceDef = service
        self.nodes: list[ComputeNode] = nodes
        self._primary: int = 0

    def __eq__(self, other):
        if not isinstance(other, ServiceProvider):
            return NotImplemented
        return self.name == other.name and self.service == other.service
    
    def __hash__(self):
        return hash((self.name, self.service))

    def primary(self) -> int:
        match self.service.balancing_model:
            case BalancingModel.SINGLE:
                return 0
            case _:
                return self._primary
    
    def set_primary(self, value: int):
        if value < 0 or value >= len(self.nodes): return
        self._primary = value

    def rotate_primary(self) -> int:
        self._primary = (self._primary + 1) % len(self.nodes)
        return self._primary
    
    def handler_node(self) -> Optional[ComputeNode]:
        if len(self.nodes) == 0: return None
        result: ComputeNode = self.nodes[self._primary]
        if self.service.balancing_model == BalancingModel.ROUND_ROBIN:
            self.rotate_primary()
        return result
    
    def add_node(self, node: ComputeNode):
        if self.service.balancing_model == BalancingModel.SINGLE and len(self.nodes) > 0: return
        if self.service.balancing_model == BalancingModel.FAILOVER and len(self.nodes) > 1: return
        self.nodes.append(node)

    def remove_node(self, node: ComputeNode):
        self.nodes.remove(node)
        self._primary = 0

    def is_valid(self) -> bool:
        return len(self.validate()) == 0
    
    def validate(self) -> list[ValidationMessage]:
        messages: list[ValidationMessage] = []
        if len(self.nodes) == 0:
            messages.append(ValidationMessage('Service Provider must have at least one node', source=self.name))
        if self.handler_node() is None:
            messages.append(ValidationMessage('Service Provider handler node is None', source=self.name))
        return messages


#  .d88b.  db    db d88888b db    db d88888b
# .8P  Y8. 88    88 88'     88    88 88'    
# 88    88 88    88 88ooooo 88    88 88ooooo
# 88    88 88    88 88~~~~~ 88    88 88~~~~~
# `8P  d8' 88b  d88 88.     88b  d88 88.    
#  `Y88'Y8 ~Y8888P' Y88888P ~Y8888P' Y88888P


# ------------------------------------------------------------
class WaitMode(Enum):
    TRANSMITTING = 'transmitting'
    PROCESSING = 'processing'
    QUEUEING = 'queueing'


# ------------------------------------------------------------
class WaitingRequest:
    def __init__(self, request: 'ClientRequest', wait_start: int, 
                 service_time: Optional[int], latency: Optional[int], 
                 wait_mode: WaitMode, queue_time: int = 0):
        self.request: ClientRequest = request
        self.wait_start: int = wait_start
        self.service_time: Optional[int] = service_time
        self.latency: Optional[int] = latency
        self.wait_mode: WaitMode = wait_mode
        self.queue_time: int = queue_time

    def queue_ended(self, clock: int, wait_mode: WaitMode):
        self.wait_mode = wait_mode
        self.queue_time = clock - self.wait_start

    def wait_end(self) -> Optional[int]:
        if self.wait_mode == WaitMode.QUEUEING or self.service_time is None:
            return None
        lat: int = 0 if self.latency is None else self.latency
        return self.wait_start + self.service_time + lat + self.queue_time


# ------------------------------------------------------------
class MultiQueue:
    def __init__(self, st_calculator: ServiceTimeCalculator, wait_mode: WaitMode, channel_count: int):
        self.service_time_calculator: ServiceTimeCalculator = st_calculator
        self.wait_mode: WaitMode = wait_mode
        self.channels: list[Optional[WaitingRequest]] = [None] * channel_count # Parallel, concurrent requests being worked on
        self.main_queue: list[WaitingRequest] = [] # Serial, requests waiting for a channel

    def name(self) -> str:
        return self.service_time_calculator.name # type: ignore
    
    def available_channel_count(self) -> int:
        count: int = 0
        for channel in self.channels:
            if channel is None:
                count = count + 1
        return count

    def first_available_channel(self) -> Optional[int]:
        for i in range(0, len(self.channels)):
            if self.channels[i] is None: return i
        return None
    
    def channels_with_requests(self) -> list[int]:
        result: list[int] = []
        for i in range(0, len(self.channels)):
            if self.channels[i] is not None: result.append(i)
        return result
       
    def channels_with_finished_requests(self, clock: int) -> list[int]:
        result: list[int] = []
        for i in self.channels_with_requests():
            wr: Optional[WaitingRequest] = self.channels[i]
            if wr is not None:
                wait_end: Optional[int] = wr.wait_end()
                if wait_end is not None and wait_end <= clock:
                    result.append(i)
        return result
    
    def request_count(self) -> int:
        return len(self.main_queue) + len(self.channels) - self.available_channel_count()
    
    def next_event_time(self) -> Optional[int]:
        result: Optional[int] = None
        for i in self.channels_with_requests():
            wr: Optional[WaitingRequest] = self.channels[i]
            if wr is not None:
                wait_end: Optional[int] = wr.wait_end()
                if wait_end is not None:
                    if result is None or wait_end < result: 
                        result = wait_end
        return result
    
    def remove_finished_requests(self, clock: int) -> list[Tuple['ClientRequest', RequestMetric]]:
        finished_channels: list[int] = self.channels_with_finished_requests(clock)
        result: list[Tuple['ClientRequest', RequestMetric]] = []

        for i in finished_channels:
            wr: Optional[WaitingRequest] = self.channels[i]
            if wr is None: continue
            st: int = 0 if wr.service_time is None else wr.service_time
            lt: int = 0 if wr.latency is None else wr.latency
            metric = RequestMetric(source=self.name(), 
                                    clock=clock, 
                                    request_name=wr.request.name, 
                                    workflow_name=wr.request.workflow_name,
                                    service_time=st, 
                                    queue_time=wr.queue_time, 
                                    latency_time=lt)
            result.append((wr.request, metric))
            wr.request.accumulating_metrics.append(metric)
            
            # Move a waiting request into a channel
            if len(self.main_queue) > 0:
                queued_req: WaitingRequest = self.main_queue.pop(0)
                queued_req.queue_ended(clock, wait_mode=self.wait_mode)
                self.channels[i] = queued_req
            else:
                self.channels[i] = None

        return result
    
    def enqueue(self, request: 'ClientRequest', clock: int):
        current_step: Optional[ClientRequestSolutionStep] = request.solution.current_step()
        if current_step is None: return

        st: Optional[int] = self.service_time_calculator.calculate_service_time(request)
        lt: Optional[int] = self.service_time_calculator.calculate_latency(request)

        index: Optional[int] = self.first_available_channel()
        if index is None:
            self.main_queue.append(WaitingRequest(request, wait_start=clock, service_time=st, latency=lt, wait_mode=WaitMode.QUEUEING))
        else:
            self.channels[index] = WaitingRequest(request, wait_start=clock, service_time=st, latency=lt, wait_mode=self.wait_mode)

    def all_waiting_requests(self) -> list[WaitingRequest]:
        full_channels: list[WaitingRequest] = [item for item in self.channels if item is not None]
        return full_channels + self.main_queue
    
    def get_performance_metric(self, clock: int) -> QueueMetric:
        if isinstance(self.service_time_calculator, ComputeNode):
            node: ComputeNode = self.service_time_calculator
            match node.type:
                case ComputeNodeType.CLIENT: stc = "CLIENT"
                case ComputeNodeType.P_SERVER: stc = "P_SERVER"
                case ComputeNodeType.V_SERVER: stc = "V_SERVER"
        elif isinstance(self.service_time_calculator, Connection):
            stc = "CONNECTION"
        else: stc = "UNKNOWN"
        return QueueMetric(source=self.name(), stc_type=stc,
                           clock=clock, channel_count=len(self.channels), request_count=len(self.all_waiting_requests()))
    

# db   d8b   db  .d88b.  d8888b. db   dD
# 88   I8I   88 .8P  Y8. 88  `8D 88 ,8P'
# 88   I8I   88 88    88 88oobY' 88,8P  
# Y8   I8I   88 88    88 88`8b   88`8b  
# `8b d8'8b d8' `8b  d8' 88 `88. 88 `88.
#  `8b8' `8d8'   `Y88P'  88   YD YP   YD

from enum import Enum
from dataclasses import dataclass
from typing import Optional, Set, Tuple

# ------------------------------------------------------------
class DataSourceType(Enum):
    RELATIONAL = 'relational'
    OBJECT = 'object'
    FILE = 'file'
    DBMS = 'dbms'
    BIG = 'big'
    OTHER = 'other'
    NONE = 'none'

    @classmethod
    def from_str(cls, value: str) -> 'DataSourceType':
        match value.lower():
            case 'relational': return DataSourceType.RELATIONAL
            case 'object': return DataSourceType.OBJECT
            case 'file': return DataSourceType.FILE
            case 'dbms': return DataSourceType.DBMS
            case 'big': return DataSourceType.BIG
            case 'none': return DataSourceType.NONE
            case _: return DataSourceType.OTHER

# ------------------------------------------------------------
class WorkflowType(Enum):
    USER = 'user'
    TRANSACTIONAL = 'transactional'

# ------------------------------------------------------------
@dataclass(frozen=True)
class WorkflowDefStep:
    name:str
    description: str
    service_type: str
    service_time: int
    chatter: int
    request_size_kb: int
    response_size_kb: int
    data_source_type: DataSourceType
    cache_pct: int

# ------------------------------------------------------------
@dataclass(frozen=True)
class ClientRequestSolutionStep:
    st_calculator: 'ServiceTimeCalculator'
    is_response: bool
    data_size: int
    chatter: int
    service_time: int

# ------------------------------------------------------------
class WorkflowChain:
    def __init__(self, name: str, desc: str, 
                 steps: list[WorkflowDefStep], 
                 service_providers: dict[str,ServiceProvider],
                 additional_client_step: Optional[WorkflowDefStep] = None):
        self.name: str = name
        self.description: str = desc
        self.steps: list[WorkflowDefStep] = steps
        self.service_providers: dict[str,ServiceProvider] = service_providers
        if additional_client_step is not None:
            self.steps.insert(0, additional_client_step)

    def is_valid(self) -> bool:
        return len(self.validate()) == 0
    
    def validate(self) -> list[ValidationMessage]:
        result: list[ValidationMessage] = []
       
        msp: list[str] = self.missing_service_providers()
        if len(msp) > 0:
            for missing in msp:
                result.append(ValidationMessage(message=f'Missing service provider for {missing}', source=self.name))

        return result

    def update_client_step(self, client_step: WorkflowDefStep):
        self.steps.pop(0)
        self.steps.insert(0, client_step)

    def all_required_service_types(self) -> Set[str]:
        types: list[str] = list(map(lambda step: (step.service_type), self.steps))
        return set(types)
    
    def configured_service_types(self) -> Set[str]:
        return set(self.service_providers.keys())
    
    def missing_service_providers(self) -> list[str]:
        all_req: Set[str] = self.all_required_service_types()
        configured: Set[str] = self.configured_service_types()
        return list(all_req.difference(configured))
    
    def service_provider_for_step(self, step: WorkflowDefStep) -> Optional[ServiceProvider]:
        if step.service_type in self.service_providers.keys():
            return self.service_providers[step.service_type]
        return None
    
    def service_provider_for_step_at_index(self, index: int) -> Optional[ServiceProvider]:
        if index < 0 or index >= len(self.steps): return None
        return self.service_provider_for_step(self.steps[index])


# ------------------------------------------------------------
class WorkflowDef:
    def __init__(self, name: str, desc: str, think_time_s: int, chains: list[WorkflowChain]):
        self.name: str = name
        self.description: str = desc
        self.think_time_s: int = think_time_s
        self.chains: list[WorkflowChain] = chains

    def all_required_service_types(self) -> Set[str]:
        result: Set[str] = set()
        for chain in self.chains:
            result = result.union(chain.all_required_service_types())
        return result
    
    def assign_service_provider(self, service_provider: ServiceProvider):
        for chain in self.chains:
            chain.service_providers[service_provider.service.service_type] = service_provider
    
    def missing_service_providers(self) -> list[str]:
        result: Set[str] = set()
        for chain in self.chains:
            # m = chain.missing_service_providers()
            # print(chain.name + ' is missing ' + str(m))
            result = result.union(chain.missing_service_providers())
        return list(result)
    
    def clear_service_providers(self):
        for chain in self.chains:
            chain.service_providers.clear()

# ------------------------------------------------------------
class ClientRequestGroup:
    _next_id: int = 0

    @classmethod
    def next_id(cls) -> int:
        cls._next_id = cls._next_id + 1
        return cls._next_id

    def __init__(self, clock: int, workflow: 'Workflow'):
        self.id: int = ClientRequestGroup.next_id()
        self.request_clock: int = clock
        self.workflow: Workflow = workflow


# ------------------------------------------------------------
class ClientRequestSolution:
    def __init__(self, steps: list[ClientRequestSolutionStep] = []):
        self.steps: list[ClientRequestSolutionStep] = steps

    def is_finished(self) -> bool:
        return len(self.steps) == 0
    
    def current_step(self) -> Optional[ClientRequestSolutionStep]:
        if len(self.steps) == 0: return None
        return self.steps[0]
    
    def goto_next_step(self):
        if len(self.steps) > 0:
            self.steps.pop(0) # Drop first item. 
    

# ------------------------------------------------------------
class ClientRequest:
    _next_id: int = 0

    @classmethod
    def next_id(cls) -> int:
        cls._next_id = cls._next_id + 1
        return cls._next_id
    
    @classmethod
    def next_name(cls) -> str:
        return f'CR-{cls.next_id()}'
    
    def __init__(self, name: str, desc: str, wf_name: str,
                 request_clock: int, solution: ClientRequestSolution, group_id: int):
        self.name: str = name
        self.description: str = desc
        self.workflow_name: str = wf_name
        self.request_clock: int = request_clock
        self.solution: ClientRequestSolution = solution
        self.group_id: int = group_id
        self.accumulating_metrics: list[RequestMetric] = []

    def __eq__(self, other):
        return self.name == other.name
    
    def is_finished(self) -> bool:
        return self.solution.is_finished()
    
    def summary_metric(self) -> RequestMetric:
        clock: int = 0
        st: int = 0
        qt: int = 0
        lt: int = 0
        if len(self.accumulating_metrics) > 0:
            clock = self.accumulating_metrics[0].clock
            for m in self.accumulating_metrics:
                st = st + m.service_time
                qt = qt + m.queue_time
                lt = lt + m.latency_time
        return RequestMetric(source='Summary', clock=clock, request_name=self.name, workflow_name=self.workflow_name, 
                             service_time=st, queue_time=qt, latency_time=lt)
    

# ------------------------------------------------------------
class Workflow:
    def __init__(self, name: str, desc: str, 
                 type: WorkflowType, definition: WorkflowDef, 
                 user_count: int = 0, productivity: int = 0, tph: int = 0):
        self.name: str = name
        self.description: str = desc
        self.definition: WorkflowDef = definition
        self.type: WorkflowType = type  # USER or TRANSACTIONAL
        self.user_count: int = user_count
        self.productivity: int = productivity
        self.tph: int = tph

    def __str__(self):
        return f'{self.type} {self.definition.name} workflow with tx rate {self.transaction_rate()}'

    def transaction_rate(self) -> int:
        if self.type == WorkflowType.USER:
            return self.user_count * self.productivity * 60
        else:
            return self.tph
    
    def create_client_requests(self, network: list[Connection], clock: int) -> Tuple[ClientRequestGroup, list[ClientRequest]]:
        group: ClientRequestGroup = ClientRequestGroup(clock, self)
        requests: list[ClientRequest] = []
        for chain in self.definition.chains:
            # for step in chain.steps:
            #     sp = chain.service_providers[step.service_type]
            #     print(f'{step.name} {sp.name} {sp.nodes[0].name}')
            solution: ClientRequestSolution = create_solution(chain, network)
            # for step in solution.steps:
            #     print(step.st_calculator.name) # type: ignore

            request: ClientRequest = ClientRequest(ClientRequest.next_name(), desc='', wf_name=self.name,
                                                   request_clock=clock, solution=solution, group_id=group.id)
            requests.append(request)
        return (group,requests)
    
    def calculate_next_event_time(self, clock: int) -> int:
        # transaction_rate is in transactions per hour
        # time between events is in ms
        ms_per_event: float = 3600000.0 / float(self.transaction_rate())
        r_val: int = int(random.normal(loc=ms_per_event, scale=ms_per_event*0.25, size=(1,1))[0][0])
        return clock + r_val
    
    def is_valid(self) -> bool:
        return len(self.validate()) == 0

    def validate(self) -> list[ValidationMessage]:
        result: list[ValidationMessage] = []

        if len(self.definition.chains) == 0:
            result.append(ValidationMessage(message='Need at least one configured Workflow Chain', source=self.name))
        
        invalid_chains: list[WorkflowChain] = list(filter(lambda chain: (chain.is_valid() == False), self.definition.chains))
        for chain in invalid_chains:
            result.append(ValidationMessage(message=f'Workflow Chain {chain.name} is invalid', source=chain.name))

        if self.transaction_rate() < 0:
            result.append(ValidationMessage(message='Transaction rate must be greater than or equal to zero', source=self.name))

        return result


# d88888b db    db d8b   db  .o88b. d888888b d888888b  .d88b.  d8b   db .d8888.
# 88'     88    88 888o  88 d8P  Y8 `~~88~~'   `88'   .8P  Y8. 888o  88 88'  YP
# 88ooo   88    88 88V8o 88 8P         88       88    88    88 88V8o 88 `8bo.  
# 88~~~   88    88 88 V8o88 8b         88       88    88    88 88 V8o88   `Y8b.
# 88      88b  d88 88  V888 Y8b  d8    88      .88.   `8b  d8' 88  V888 db   8D
# YP      ~Y8888P' VP   V8P  `Y88P'    YP    Y888888P  `Y88P'  VP   V8P `8888Y'

# ------------------------------------------------------------
def create_solution(chain: WorkflowChain, in_network: list[Connection]) -> ClientRequestSolution:
    if chain.is_valid() == False:
        raise ValueError(f'Workflow Chain {chain.name} passed to create_solution must be valid')
    
    # Starting at the head of the chain (client), stop at each
    # service provider, traversing the network between each
    step: WorkflowDefStep = chain.steps[0]
    source_sp: Optional[ServiceProvider] = chain.service_provider_for_step(step)
    if source_sp is None:
        raise ValueError(f'ServiceProvider for {step} was None')
    source_node: Optional[ComputeNode] = source_sp.handler_node()
    if source_node is None:
        raise ValueError(f'handler node for ServiceProvider {source_sp.name} was None')
    
    steps: list[ClientRequestSolutionStep] = []
    steps.append(ClientRequestSolutionStep(st_calculator=source_node, 
                                           is_response=False, 
                                           data_size=step.request_size_kb, 
                                           chatter=0, # No chatter for compute step
                                           service_time=step.service_time))
    
    for i in range(1, len(chain.steps)):
        step = chain.steps[i]
        dest_sp: Optional[ServiceProvider] = chain.service_provider_for_step_at_index(i)
        if dest_sp is None:
            raise ValueError(f'ServiceProvider for {step} was None')
        dest_node: Optional[ComputeNode] = dest_sp.handler_node()
        if dest_node is None: 
            raise ValueError(f'handler node for ServiceProvider {dest_sp.name} was None')
        
        if source_node != dest_node:
            # print(f'Routing from {source_node.name} to {dest_node.name}')
            route: Optional[Route] = find_route(source_node.zone, dest_node.zone, in_network)
            if route is None:
                raise ValueError(f'Could not find route from zone {source_node.zone.name} to zone {dest_node.zone.name}')
            
            # Add the network steps
            for conn in route.connections:
                steps.append(ClientRequestSolutionStep(st_calculator=conn, 
                                                    is_response=False, 
                                                    data_size=step.request_size_kb, 
                                                    chatter=step.chatter, 
                                                    service_time=0)) # Service time is based on data size
        
        # Add the next compute step
        steps.append(ClientRequestSolutionStep(st_calculator=dest_node, 
                                               is_response=False, 
                                               data_size=step.request_size_kb, 
                                               chatter=0,  # No chatter for compute step
                                               service_time=step.service_time))
        source_sp = dest_sp
        source_node = dest_node
    
    # Now retrace back to client
    for i in range(len(chain.steps)-2, -1, -1):
        step = chain.steps[i]
        dest_sp = chain.service_provider_for_step_at_index(i)
        if dest_sp is None:
            raise ValueError(f'ServiceProvider for {step} was None')
        dest_node: Optional[ComputeNode] = dest_sp.handler_node()
        if dest_node is None: 
            raise ValueError(f'handler node for ServiceProvider {dest_sp.name} was None')
        
        if source_node != dest_node:
            route: Optional[Route] = find_route(source_node.zone, dest_node.zone, in_network)
            if route is None:
                raise ValueError(f'Could not find route from zone {source_node.zone.name} to zone {dest_node.zone.name}')
        
            
            # Add the network steps
            for conn in route.connections:
                steps.append(ClientRequestSolutionStep(st_calculator=conn, 
                                                    is_response=True, 
                                                    data_size=step.response_size_kb, 
                                                    chatter=step.chatter, 
                                                    service_time=0)) # Service time is based on data size
        
        # Add the next compute step
        steps.append(ClientRequestSolutionStep(st_calculator=dest_node, 
                                               is_response=True, 
                                               data_size=step.response_size_kb, 
                                               chatter=0,  # No chatter for compute step
                                               service_time=step.service_time))
        source_sp = dest_sp
        source_node = dest_node

    return ClientRequestSolution(steps)


# ------------------------------------------------------------
def find_route(start: Zone, end: Zone, in_network: list[Connection]) -> Optional[Route]:
    if not start.is_a_source(in_network) or not end.is_a_destination(in_network):
        print('Start zone is not a source in network')
        return None
    elif start.local_connection(in_network) is None:
        print('End zone is not a destination in network')
        return None
    
    visited: Set[Zone] = set([start])
    working_path: list[Connection] = []
    local: Optional[Connection] = start.local_connection(in_network)
    if local is not None:
        working_path = [local]

    path: list[Connection] = _find_route_dfs(start, end, in_network, visited, working_path)
    if len(path) == 0:
        # print('Path length was zero')
        return None
    return Route(path)

# ------------------------------------------------------------
def _find_route_dfs(start: Zone, end: Zone, 
                in_network: list[Connection], 
                visited: Set[Zone], 
                path: list[Connection]) -> list[Connection]:
    if start == end:
        return path
    
    exits: list[Connection] = start.exit_connections(in_network)
    exits_to_unvisited: list[Connection] = list(filter(lambda conn: (conn.destination not in visited), exits))
    results: list[list[Connection]] = []
    for exit in exits_to_unvisited:
        mod_path = path.copy()
        mod_path.append(exit)
        mod_visited = visited.copy()
        mod_visited.add(exit.destination)
        p = _find_route_dfs(exit.destination, end, in_network, mod_visited, mod_path)
        if p is not None:
            results.append(p)
    
    results = list(filter(lambda path: (len(path) > 0 and path[-1].destination == end), results))
    results.sort(key=lambda path: len(path))
    if len(results) == 0:
        return []
    return results[0]