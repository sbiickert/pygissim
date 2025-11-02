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
        return f'{self.source}: "{self.message}"'
    
@dataclass(frozen=True)
class QueueMetric:
    """ Read-only data class for tracking the utilization rate of all queues (compute and network)

    :param source: The name of the MultiQueue that was measured.
    :type source: str
    :param stc_type: The type of the MultiQueue's :class:`ServiceTimeCalculator`'.
    :type stc_type: str
    :param clock: The simulation time (in ms) when the measurement was taken.
    :type clock: int
    :param channel_count: The number of parallel channels in the MultiQueue.
    :type channel_count: int
    :param request_count: The number of requests being processed and queued.
    :param utilization: The utilization (1.0 = 100%) of this queue.
    """
    source: str
    stc_type: str
    clock: int
    channel_count: int
    request_count: int
    utilization: float

    def __str__(self):
        return f'QM@{self.clock} ({self.source} channels:{self.channel_count} requests:{self.request_count})'
    
    # Old definition of utilization based on numbers of requests at a point in time
    # See MultiQueue get_performance_metric for details
    # def utilization(self) -> float:
    #     """
    #     Utility function to return the utilization of a network connection or a compute node.

    #     * Channels represents the number of parallel channels to do work. If they are all full, then 1.0 is returned (100%)
    #     * If all channels are full and more requests were waiting, then the result will be more than 1.0 (> 100%)
    #     """
    #     if self.channel_count == 0 or self.request_count == 0: return 0.0
    #     return self.request_count / self.channel_count

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
    """ Abstract base class for classes that will be delegates for a MultiQueue to calculate service time and latency time. 
    The two subclasses are :class:`Connection` and :class:`ComputeNode`.
    """
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
class Connection(ServiceTimeCalculator):
    """ Represents a one-way link between Zones. The source and destination :class:`Zone` may be the same. 
    To connect Zones for two-way communication, there needs to be two Connections, upstream and downstream.

    :param source: The upstream Zone. Information flows from the source.
    :type source: Zone
    :param dest: The downstream Zone. Information flows to the destination.
    :type dest: Zone
    :param bw: The stated bandwidth of the connection in megabits per second (Mbps).
    :type bw: int
    :param lat: The average latency of the connection in milliseconds (ms).
    :type lat: int
    """
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
        """ Describes the Connection using the namesx of the source and destination Zones.
         
        Example: 'Zone A to Zone B' """
        return f'{self.source.name} to {self.destination.name}'
    
    def is_local(self) -> bool:
        """ Indication that the Connection's source and destination are the same Zone. 
        
        :returns: True if the source and destination Zone are equal.
        :rtype: bool
        """
        return self.source == self.destination
    
    def inverted(self) -> 'Connection':
        """ Inverts the Connection and returns it.  
        
        :returns: A copy of the Connection with the source and destination swapped.
        :rtype: Connection
        """
        return Connection(self.destination, self.source, self.bandwidth, self.latency_ms)

    def calculate_service_time(self, request: 'ClientRequest') -> Optional[int]:
        """ Calculates the service time for the ClientRequest's current step in this Connection.
        This takes the data size and determines how many milliseconds it would take to transmit. 
        
        :param request: The ClientRequest being transmitted over this Connection.
        :type request: ClientRequest
        :returns: The number of milliseconds to transmit the current step. Is None if the current step is None.
        :rtype: Optional[int]
        """
        step: Optional[ClientRequestSolutionStep] = request.solution.current_step()
        if step is None: return None
        data_kb: int = step.data_size * 8
        # Mbps -> kbps -> kb per millisecond (which is the time scale of the simulation)
        bw_kbpms: int = int(self.bandwidth * 1000 / 1000)
        return int(data_kb / bw_kbpms)

    def calculate_latency(self, request) -> Optional[int]:
        """ Calculates the latency time for the ClientRequest's current step in this Connection. 
        This is the chatter of the current step times the latency of this Connection.
        
        :param request: The ClientRequest being transmitted over this Connection.
        :type request: ClientRequest
        :returns: The number of milliseconds of latency for the current step. Is None if the current step is None.
        :rtype: Optional[int]
        """
        step: Optional[ClientRequestSolutionStep] = request.solution.current_step()
        if step is None: return None
        return self.latency_ms * step.chatter
    
    def provide_queue(self) -> 'MultiQueue':
        """ A ServiceTimeCalculator provides a MultiQueue to the :class:`pygissim.pygissim.Simulator`.
        
        :returns: A queue for handling ClientRequests.
        :rtype: MultiQueue
        """
        return MultiQueue(self, WaitMode.TRANSMITTING, channel_count=2)


# ------------------------------------------------------------
class Zone:
    """ Represents a network environment that can host ComputeNodes. A network is formed by 
    Zones being connected by :class:`Connection` s. Many of the functions below accept an 'in_network'
    parameter, which is the list of Connections, usually maintained by the :class:`pygissim.pygissim.Design`
    in its 'network' property.
    
    :param name: A descriptive name. Should be unique in a :class:`pygissim.pygissim.Design`.
    :param desc: Text that describes the Zone in more detail.
    """
    def __init__(self, name: str, desc: str):
        self.id = str(uuid.uuid1())
        self.name = name
        self.description = desc
    
    def __str__(self):
        return f'Zone {self.name}'

    def connect(self, other: 'Zone', bw: int, lat: int) -> Connection:
        """ Creates a Connection to a Zone. 
        
        :param bw: The bandwidth of the Connection, in megabits per second (Mbps).
        :param lat: the latency of the Connection, in millseconds (ms).
        :returns: The new Connection between Zones.
        :rtype: Connection
        """
        return Connection(self, other, bw, lat)

    def connect_both_ways(self, other: 'Zone', bw: int, lat: int) -> Tuple[Connection, Connection]:
        """ Creates a pair of Connections (forwards and backwards) between Zones.
        The returned Connections are identical except for their direction.
        
        :param bw: The bandwidth of the Connection, in megabits per second (Mbps).
        :param lat: the latency of the Connection, in millseconds (ms).
        :returns: The new Connections between Zones.
        :rtype: Tuple[Connection, Connection]
        """
        c1: Connection = Connection(self, other, bw, lat)
        c2 = c1.inverted()
        return (c1,c2)
    
    def self_connect(self, bw: int, lat: int) -> Connection:
        """ Convenience method to create a local connection for the Zone.
        The source and destination will be this Zone.

        :param bw: The bandwidth of the Connection, in megabits per second (Mbps).
        :param lat: the latency of the Connection, in millseconds (ms).
        :returns: A new local Connection for this Zone.
        :rtype: Connection
        """
        return Connection(self, self, bw, lat)
    
    def local_connection(self, in_network: list[Connection]) -> Optional[Connection]:
        """ Finds a Connection in a list of Connections where the source and destination are this Zone.
        
        :param in_network: The list of Connections representing a network.
        :returns: The first Connection with source and destination equal to this Zone. Returns None if not found.
        :rtype: Optional[Connection]
        """
        conns = list(filter(lambda conn: (self == conn.source and self == conn.destination), in_network))
        if len(conns) >= 0:
            return conns[0]
        return None
    
    def connections(self, in_network: list[Connection]) -> list[Connection]:
        """ All Connections in the list where either the source or destination is this Zone.

        :param in_network: The list of Connections representing a network.
        :returns: All Connections with source or destination equal to this Zone. Returns an empty list if none found.
        :rtype: list[Connection]
        """
        return list(filter(lambda conn: (self == conn.source or self == conn.destination), in_network))
    
    def entry_connections(self, in_network: list[Connection]) -> list[Connection]:
        """ All Connections in the list where the destination is this Zone and the source is a different Zone.

        :param in_network: The list of Connections representing a network.
        :returns: All Connections with destination equal and source not equal to this Zone. Returns an empty list if none found.
        :rtype: list[Connection]
        """
        return list(filter(lambda conn: (conn.is_local() == False and self == conn.destination), in_network))
    
    def exit_connections(self, in_network: list[Connection]) -> list[Connection]:
        """ All Connections in the list where the source is this Zone and the destination is a different Zone.

        :param in_network: The list of Connections representing a network.
        :returns: All Connections with source equal and destination not equal to this Zone. Returns an empty list if none found.
        :rtype: list[Connection]
        """
        return list(filter(lambda conn: (conn.is_local() == False and self == conn.source), in_network))
    
    def other_connections(self, in_network: list[Connection]) -> list[Connection]:
        """ All Connections in the list where neither the source nor the destination is this Zone.

        :param in_network: The list of Connections representing a network.
        :returns: All Connections with neither source nor destination equal to this Zone. Returns an empty list if none found.
        :rtype: list[Connection]
        """
        return list(filter(lambda conn: (conn.source != self and conn.destination != self), in_network))

    def is_a_source(self, in_network: list[Connection]) -> bool:
        """
        :param in_network: The list of Connections representing a network.
        :returns: True if any Connections have this Zone as a source. 
        :rtype: bool
        """
        return len(list(filter(lambda conn: (self == conn.source), in_network))) > 0
    
    def is_a_destination(self, in_network: list[Connection]) -> bool:
        """
        :param in_network: The list of Connections representing a network.
        :returns: True if any Connections have this Zone as a destination. 
        :rtype: bool
        """
        return len(list(filter(lambda conn: (self == conn.destination), in_network))) > 0
    
    def is_fully_connected(self, in_network: list[Connection]) -> bool:
        """
        :param in_network: The list of Connections representing a network.
        :returns: True if this Zone has a local connection and at least one entry and exit connection. 
        :rtype: bool
        """
        return self.local_connection(in_network) is not None and \
            len(self.entry_connections(in_network)) > 0 and \
            len(self.exit_connections(in_network)) > 0
    
    @classmethod
    def all_zones(cls, in_network: list[Connection]) -> Set['Zone']:
        """ The Set of Zones in a list of Connections. 
        
        :param in_network: The list of Connections representing a network.
        :returns: All unique Zones that the Connections reference. 
        :rtype: Set[Zone]
        """
        result: Set[Zone] = set()

        for conn in in_network:
            result.add(conn.source)
            result.add(conn.destination)

        return result

# ------------------------------------------------------------
@dataclass(frozen=True)
class Route:
    """ Immutable data class. A list of Connections that constitute a path through
    a network. Usually created by :func:`find_route`."""
    connections: list[Connection]

    def __str__(self):
        result: str = f"Route ({self.count()}):\n"
        for c in self.connections:
            result = result + f"\t{c}\n"
        return result

    def count(self) -> int:
        """ Convenience method to return the number of Connections in the Route. 

        :returns: The length of the Route in steps.
        """
        return len(self.connections)

        

#  .o88b.  .d88b.  .88b  d88. d8888b. db    db d888888b d88888b
# d8P  Y8 .8P  Y8. 88'YbdP`88 88  `8D 88    88 `~~88~~' 88'    
# 8P      88    88 88  88  88 88oodD' 88    88    88    88ooooo
# 8b      88    88 88  88  88 88~~~   88    88    88    88~~~~~
# Y8b  d8 `8b  d8' 88  88  88 88      88b  d88    88    88.    
#  `Y88P'  `Y88P'  YP  YP  YP 88      ~Y8888P'    YP    Y88888P

# ------------------------------------------------------------
class ThreadingModel(Enum):
    """ Enumeration of threading models. Physical represents 1:1 threads to cores.
    Hyperthreaded represents 2:1 threads to cores. Used for :class:`ComputeNode` service time calculation.
    """
    PHYSICAL = 'physical'
    HYPERTHREADED = 'ht'

    def factor(self) -> float:
        """ The factor applied to the per-core performace for ComputeNodes. """
        if self is ThreadingModel.PHYSICAL:
            return 1.0
        else:
            return 0.5
        
# ------------------------------------------------------------
class BalancingModel(Enum):
    """ Enumeration of ways requests can be balanced across multiple ComputeNodes in a :class:`ServiceProvider`. """
    SINGLE = '1'
    ROUND_ROBIN = 'roundrobin'
    FAILOVER = 'failover'
    CONTAINERIZED = 'containerized'
    OTHER = 'other'

    @classmethod
    def from_str(cls, value: str) -> 'BalancingModel':
        """ Convenience method for constructing BalancingModels from a string representation. 
        
        :param value: The string representation.
        :returns: The BalancingModel. Returns OTHER if the string is not a recognized type.
        :rtype: BalancingModel
        """
        match value.upper():
            case '1': return BalancingModel.SINGLE
            case 'ROUNDROBIN' | 'armv7': return BalancingModel.ROUND_ROBIN
            case 'FAILOVER': return BalancingModel.FAILOVER
            case 'CONTAINER': return BalancingModel.CONTAINERIZED
            case _: return BalancingModel.OTHER


# ------------------------------------------------------------
class ComputeNodeType(Enum):
    """ Enumeration of the functional classes of ComputeNode. Earlier iterations of this
    framework used subclasses, but an enumeration works more clearly. """
    CLIENT = 'client'
    P_SERVER = 'physical'
    V_SERVER = 'virtual'

# ------------------------------------------------------------
@dataclass(frozen=True)
class ServiceDef:
    """ Read-only data class. A tag for :class:`ServiceProvider` to identify the service it
    provides and how requests are balanced. """
    name: str
    description: str
    service_type: str
    balancing_model: BalancingModel

# ------------------------------------------------------------
@dataclass(frozen=True)
class HardwareDef:
    """ Read-only data class representing a hardware platform.
    
    :param processor: A name for the hardware. It must be a unique string in a :class:`pygissim.pygissim.Design`.
    :param cores: The number of physical cores in the hardware platform.
    :param specint_rate2017: The performance of the hardware from https://www.spec.org/cpu2017/results/rint2017/
    """
    processor: str
    cores: int
    specint_rate2017: float

    def specint_rate2017_per_core(self) -> float:
        """ Per-core performance of the hardware. """
        return self.specint_rate2017 / float(self.cores)

    def __str__(self) -> str:
        return f'HW {self.processor} cores: {self.cores} spec: {self.specint_rate2017}'
    
    baseline_per_core: float = 10.0

# ------------------------------------------------------------
class ComputeNode(ServiceTimeCalculator):
    """ Represents a computing resource that has a :class:`HardwareDef` and exists in a network :class:`Zone`.
    
    :param name: A descriptive name. Should be unique in a :class:`pygissim.pygissim.Design`.
    :param desc: Additional descriptive text.
    :param hw_def: The hardware definition of the ComputeNode. Determines its performance.
    :param memory_GB: Configured memory in gigabytes (GB). Is mostly used for determining overallocation of physical host memory on virtual hosts.
    :param zone: The network Zone that the ComputeNode is connected to.
    :param type: The type of ComputeNode.
    """
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
        self.threading: ThreadingModel = ThreadingModel.PHYSICAL if type == ComputeNodeType.CLIENT else ThreadingModel.HYPERTHREADED

        self._v_cores: int = 0                   # if ComputeNodeType.V_SERVER
        self._v_hosts: list['ComputeNode'] = []    # if ComputeNodeType.P_SERVER

    def __str__(self) -> str:
        return f'CNode {self.name} {self.type} in {self.zone.name}'
    
    def vcore_count(self) -> int:
        """        
        :returns: The number of virtual cores assigned to a virtual machine. Will be zero if this ComputeNode is not a virtual host. 
        """
        return self._v_cores
    
    def set_vcore_count(self, count: int):
        """
        :param count: Sets the number of virtual cores assigned to this virtual machine. Will be ignored if not a virtual host.
        """
        if self.type == ComputeNodeType.V_SERVER:
            self._v_cores = count
        else:
            self._v_cores = 0

    def specint_rate2017_per_core(self) -> float:
        """ Per-core performance of the hardware. Including threading factor. """
        return self.hw_def.specint_rate2017_per_core() * self.threading.factor()

    def adjusted_service_time(self, st: int) -> int:
        """ The relative performance of this ComputeNode will affect the service times. All service times in
        :class:`WorkflowDefStep` are based on the baseline per core performance defined in :class:`HardwareDef`.
        The hardware definition of this node may be faster or slower than that baseline and will make the 
        calculated service times shorter or longer, respectively.
        
        :param st: The input baseline service time in milliseconds (ms).
        :returns: The adjusted service time in milliseconds (ms).
        """
        relative = HardwareDef.baseline_per_core / self.specint_rate2017_per_core()
        return int(float(st) * relative)

    def calculate_service_time(self, request: 'ClientRequest') -> Optional[int]:
        """
        :param request: The ClientRequest whose current step needs a service time calculation.
        :returns: The service time in milliseconds (ms). Returns None if there is no current step.
        """
        step: Optional[ClientRequestSolutionStep] = request.solution.current_step()
        if step is None: return None
        return self.adjusted_service_time(step.service_time)

    def calculate_latency(self, request) -> Optional[int]:
        """
        :param request: The ClientRequest whose current step needs a latency time calculation.
        :returns: None. Only Connections have latency.
        """
        return None
    
    def provide_queue(self) -> 'MultiQueue':
        """ Creates and returns a MultiQueue that represent's this ComputeNode's ability to do work.
        This includes a number of 'channels' (slots for processing requests in parallel) and a main
        queue where requests will wait until there is an available channel.
        
        For a ComputeNode, the channel count will generally be the number of cores (virtual or physical).
        Client nodes are the exception, because they represent the compute for a group of users. 
        The channel count for them is an arbitrarily large number. 
        """
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
        """ Creates and adds a virtual ComputeNode to a physical server.

        :raises TypeError: if this is not a physical server.
        :param name: A descriptive string. Should be unique in a Design.
        :param v_cores: The number of virtual cores assigned to this virtual machine.
        :param memory_GB: The amount of memory in gigabytes (GB) assigned to this virtual machine. 
        """
        if self.type != ComputeNodeType.P_SERVER:
            raise TypeError("Can only add virtual hosts to physical servers.")
        
        if len(name) == 0:
            name = f'VH {self.name}: {len(self._v_hosts)}'
        v_host: ComputeNode = ComputeNode(name, '', self.hw_def, memory_GB, self.zone, ComputeNodeType.V_SERVER)
        v_host.set_vcore_count(v_cores)
        self._v_hosts.append(v_host)

    def remove_virtual_host(self, v_host:'ComputeNode'):
        """ Removes a virtual machine from the list on this physical server. 
        
        :param v_host: The virtual machine to remove.
        """
        self._v_hosts.remove(v_host)
    
    def virtual_host_count(self) -> int:
        """
        :returns: The count of virtual hosts on this ComputeNode.
        """
        return len(self._v_hosts)
    
    def virtual_host(self, index: int) -> Optional['ComputeNode']:
        """
        :param index: The ordered index of the virtual host to return.
        :returns: The virtual host at the index. Returns None if the index is invalid.
        :rtype: Optional[ComputeNode]
        """
        if index >= 0 and index < len(self._v_hosts):
            return self._v_hosts[index]
        return None

    def total_vcpu_allocation(self) -> int:
        """
        :returns: The sum of all virtual host virtual core allocations.
        """
        total: int = 0
        for v_host in self._v_hosts:
            total = total + v_host.vcore_count()
        return total
    
    def total_cpu_allocation(self) -> int:
        """
        :returns: The sum of all virtual host virtual core allocations accounting for hyperthreading.
        """
        return int(float(self.total_vcpu_allocation()) * self.threading.factor())
    
    def total_memory_allocation(self) -> int:
        """
        :returns: The sum of all virtual host memory allocations.
        """
        total: int = 0
        for v_host in self._v_hosts:
            total = total + v_host.memory_GB
        return total

        

# ------------------------------------------------------------
class ServiceProvider:
    """ Represents one or more ComputeNodes that are assigned to handle a particular service type.
    
    :param name: A descriptive name. Should be unique in a Design.
    :param desc: Additional descriptive text.
    :param service: The definition of the service that this provides.
    :param nodes: The list of ComputeNodes that will handle the service requests.
    :param tags: A set of tags for grouping service providers if needed.
    """
    def __init__(self, name:str, desc: str, service: ServiceDef, nodes: list[ComputeNode], tags: Optional[Set[str]] = None):
        self.name: str = name
        self.description: str = desc
        self.service: ServiceDef = service
        self.nodes: list[ComputeNode] = nodes
        self._primary: int = 0
        if tags is None:
            self.tags: Set[str] = set()
        else:
            self.tags = tags

    def __eq__(self, other):
        if not isinstance(other, ServiceProvider):
            return NotImplemented
        return self.name == other.name and self.service == other.service
    
    def __hash__(self):
        return hash((self.name, self.service))

    def primary(self) -> int:
        """ :returns: The index of the ComputeNode that will handle the next request. """
        match self.service.balancing_model:
            case BalancingModel.SINGLE:
                return 0
            case _:
                return self._primary
    
    def set_primary(self, value: int):
        """ Changes the index of the primary node in the ServiceProvider. 
        
        :returns: The new primary index.
        """
        if value < 0 or value >= len(self.nodes): return
        self._primary = value

    def rotate_primary(self) -> int:
        """ Changes the index of the primary to the next node.
        
        :returns: The new primary index.
        """
        self._primary = (self._primary + 1) % len(self.nodes)
        return self._primary
    
    def handler_node(self) -> Optional[ComputeNode]:
        """ For a ServiceProvider with a service :class:`BalancingModel` of ROUND_ROBIN,
        the primary will rotate every time this is called.
        
        :returns: The ComputeNode that is currently the primary. Returns None if there are no nodes assigned.
        :rtype: Optional[ComputeNode]
        """
        if len(self.nodes) == 0: return None
        result: ComputeNode = self.nodes[self._primary]
        if self.service.balancing_model == BalancingModel.ROUND_ROBIN:
            self.rotate_primary()
        return result
    
    def add_node(self, node: ComputeNode):
        """ Adds a ComputeNode to the list that handle service requests.
        Will be ignored if the :class:`BalancingModel` does not support additional nodes.

        - Example: BalancingModel SINGLE and there already is a node.
        - Example: BalancingModel FAILOVER and there already are two nodes.
        
        :param node: The ComputeNode to add.
        """
        if self.service.balancing_model == BalancingModel.SINGLE and len(self.nodes) > 0: return
        if self.service.balancing_model == BalancingModel.FAILOVER and len(self.nodes) > 1: return
        self.nodes.append(node)

    def remove_node(self, node: ComputeNode):
        """ Removes a node from the list. Has no effect if node does not exist in [nodes].
        
        :param node: The ComputeNode to remove.
        """
        self.nodes.remove(node)
        self._primary = 0

    def is_valid(self) -> bool:
        """ 
        :returns: True if validate returns no messages.
        :rtype: bool
        """
        return len(self.validate()) == 0
    
    def validate(self) -> list[ValidationMessage]:
        """ Method that evaluates the validity of the configuration of this ServiceProvider.
        
        :returns: Messages indicating invalid configuration.
        :rtype: list[ValidationMessage]
        """
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
    """ Enumeration of the potential states of a :class:`WaitingRequest`. """
    TRANSMITTING = 'transmitting'
    PROCESSING = 'processing'
    QUEUEING = 'queueing'


# ------------------------------------------------------------
class WaitingRequest:
    """ Wrapper class for handling a :class:`ClientRequest` in a :class:`MultiQueue`
    in preparation for processing a step of the request.

    The wait_mode is used for later summarization:
    - TRANSMITTING: a Connection is processing this request.
    - PROCESSSING: a ComputeNode is processing this request.
    - QUEUEING: the request is in a queue, waiting for a channel.
    
    :param request: The request to be wrapped.
    :param wait_start: The simulation clock when the request was wrapped.
    :param service_time: The service time for this step of the request. None is to support cases where the current step might be None.
    :param latency: The latency time for this step of the request. None is supported if latency time is None or there is no current step.
    :param wait_mode: The state of this WaitingRequest.
    """
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
        """ The request is moving out of the queue and into a channel.
        The time spent in the queue is recorded, and the wait mode is updated to reflect
        the type of processor, either a Connection or a ComputeNode.
        
        :param clock: The current simulation time.
        :param wait_mode: The wait mode for this queue's :class:`ServiceTimeCalculator`
        """
        self.wait_mode = wait_mode
        self.queue_time = clock - self.wait_start

    # TODO: Work out if there is potential for requests to get "stuck" in a MultiQueue.
    def wait_end(self) -> Optional[int]:
        """
        :returns: The simulation time when this request will be finished processing. None if queueing or service time is None.
        """
        if self.wait_mode == WaitMode.QUEUEING or self.service_time is None:
            return None
        lat: int = 0 if self.latency is None else self.latency
        return self.wait_start + self.service_time + lat + self.queue_time


# ------------------------------------------------------------
class MultiQueue:
    """ Represents a queue with one or more channels of parallel processing and a single
    queue used when waiting for a turn in a channel.
    
    Analogy: Multiple bank tellers (channels) and a queue waiting to see a teller.

    The number of channels will be determined by the service time calculator.

    - Connection: 2 channels
    - ComputeNode: the number of cores (physical host or client) or virtual cores (virtual host)
    
    :param st_calculator: The :class:`Connection` or :class:`ComputeNode` that is designated to calculate all service times for this MultiQueue.
    :param wait_mode: Will be TRANSMITTING for Connections and PROCESSING for ComputeNodes
    :param channel_count: The number of channels that this MultiQueue will have.
    """
    def __init__(self, st_calculator: ServiceTimeCalculator, wait_mode: WaitMode, channel_count: int):
        self.service_time_calculator: ServiceTimeCalculator = st_calculator
        self.wait_mode: WaitMode = wait_mode
        self.channels: list[Optional[WaitingRequest]] = [None] * channel_count # Parallel, concurrent requests being worked on
        self.main_queue: list[WaitingRequest] = [] # Serial, requests waiting for a channel
        self.last_metric_clock: int = 0 # Update every time get_performance_metric is called
        self.work_done: int = 0         # For utilization

    def name(self) -> str:
        """ :returns: The name of the service time calculator (i.e. the Connection or ComputeNode)"""
        return self.service_time_calculator.name # type: ignore
    
    def available_channel_count(self) -> int:
        """ :returns: The number of channels without any active requests in them. """
        count: int = 0
        for channel in self.channels:
            if channel is None:
                count = count + 1
        return count

    def first_available_channel(self) -> Optional[int]:
        """
        :returns: The index of the first available channel. Returns None if all channels are occupied.
        :rtype: Optional[int]
        """
        for i in range(0, len(self.channels)):
            if self.channels[i] is None: return i
        return None
    
    def channels_with_requests(self) -> list[int]:
        """ :returns: A list of channel indexes with active requests in them. """
        result: list[int] = []
        for i in range(0, len(self.channels)):
            if self.channels[i] is not None: result.append(i)
        return result
       
    def channels_with_finished_requests(self, clock: int) -> list[int]:
        """
        :param clock: The current simulation time.
        :returns: A list of channel indexes with requests in them that have finished processing.
        """
        result: list[int] = []
        for i in self.channels_with_requests():
            wr: Optional[WaitingRequest] = self.channels[i]
            if wr is not None:
                wait_end: Optional[int] = wr.wait_end()
                if wait_end is not None and wait_end <= clock:
                    result.append(i)
        return result
    
    def request_count(self) -> int:
        """ :returns: The total count of requests in channels and in the main queue. """
        return len(self.main_queue) + len(self.channels) - self.available_channel_count()
    
    def next_event_time(self) -> Optional[int]:
        """
        :returns: The simulation clock when the next request in a channel will be finished. Returns None if no requests being processed.
        """
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
        """ The simulation clock has moved forward and any waiting requests whose wait_end has arrived need to move on.
        
        ClientRequests are unwrapped and returned, along with metrics about how long they waited.

        Any open channels are filled with requests from the main queue, if any are waiting.

        :param clock: The current simulation time.
        :returns: A list of all requests that have finished this step and need to move on and their metrics.
        :rtype: Tuple[ClientRequest, RequestMetric]
        """
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
            self._log_work_done(wr, clock)
            
            # Move a waiting request into a channel
            if len(self.main_queue) > 0:
                queued_req: WaitingRequest = self.main_queue.pop(0)
                queued_req.queue_ended(clock, wait_mode=self.wait_mode)
                self.channels[i] = queued_req
            else:
                self.channels[i] = None

        return result
    
    def enqueue(self, request: 'ClientRequest', clock: int):
        """ Wraps the request in a WaitingRequest. If there are available channels, the request will be put in it
        and immediately start processing. If there are no channels available, the request is added to the end of the main queue.
        
        :param request: The request that needs to be processed by this queue's service time calculator.
        :param clock: The current simulation time.
        """
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
        """
        :returns: A list of all waiting requests in channels and in the main queue.
        """
        full_channels: list[WaitingRequest] = [item for item in self.channels if item is not None]
        return full_channels + self.main_queue
    
    def get_performance_metric(self, clock: int) -> QueueMetric:
        """ :returns: A performance metric for the queue indicating how busy it is. """
        if isinstance(self.service_time_calculator, ComputeNode):
            node: ComputeNode = self.service_time_calculator
            match node.type:
                case ComputeNodeType.CLIENT: stc = "CLIENT"
                case ComputeNodeType.P_SERVER: stc = "P_SERVER"
                case ComputeNodeType.V_SERVER: stc = "V_SERVER"
        elif isinstance(self.service_time_calculator, Connection):
            stc = "CONNECTION"
        else: stc = "UNKNOWN"
        waiting: list[WaitingRequest] = self.all_waiting_requests()
        for wr in waiting:
            self._log_work_done(wr, clock)

        qm = QueueMetric(source=self.name(), stc_type=stc,
                           clock=clock, channel_count=len(self.channels), request_count=len(waiting),
                           utilization=self._calc_utilization(clock))
        self.work_done = 0
        self.last_metric_clock = clock
        return qm
    
    def _log_work_done(self, request: WaitingRequest, clock: int):
        wait_end = request.wait_end()
        if request.service_time is None or wait_end is None: return
        # Total work for request is the service time, but if the work started before the last 
        # time it was logged, ignore that part. Also, if the work isn't done yet, then ignore that too.
        total_work: int = request.service_time
        if request.wait_start < self.last_metric_clock:
            total_work = total_work - (self.last_metric_clock - request.wait_start)
        if clock < wait_end:
            total_work = total_work - (wait_end - clock)
        self.work_done = self.work_done + total_work

    def _calc_utilization(self, clock: int) -> float:
        # 1.0 is 100% utilization
        time_window = clock - self.last_metric_clock
        max_work: int = time_window * len(self.channels)
        return float(self.work_done) / float(max_work)


# db   d8b   db  .d88b.  d8888b. db   dD
# 88   I8I   88 .8P  Y8. 88  `8D 88 ,8P'
# 88   I8I   88 88    88 88oobY' 88,8P  
# Y8   I8I   88 88    88 88`8b   88`8b  
# `8b d8'8b d8' `8b  d8' 88 `88. 88 `88.
#  `8b8' `8d8'   `Y88P'  88   YD YP   YD


# ------------------------------------------------------------
class DataSourceType(Enum):
    """ Enumeration of different data source types. Not fully integrated (yet). """
    RELATIONAL = 'relational'
    OBJECT = 'object'
    FILE = 'file'
    DBMS = 'dbms'
    BIG = 'big'
    OTHER = 'other'
    NONE = 'none'

    @classmethod
    def from_str(cls, value: str) -> 'DataSourceType':
        """ Convenience method for creating from string representation. """
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
    """ Enumeration of workflow types.
    
    - USER indicates that transaction rates will be calculated from user counts and productivity.
    - TRANSACTIONAL indicates that transaction rates will be entered directly as transactions per hour.
    """
    USER = 'user'
    TRANSACTIONAL = 'transactional'

# ------------------------------------------------------------
@dataclass(frozen=True)
class WorkflowDefStep:
    """ Read-only class capturing a step in a workflow.
    
    Service time is relative to the baseline_per_core in :class:`HardwareDef`.
    """
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
    """ Read-only data class capturing a step needed to finish a ClientRequest. """
    st_calculator: 'ServiceTimeCalculator'
    is_response: bool
    data_size: int
    chatter: int
    service_time: int

# ------------------------------------------------------------
class WorkflowChain:
    """ Represents a series of steps that define the parts of a Workflow.
    Most Workflows will have more than one chain of steps that are independent from each other.
    
    Example: a mobile WorkflowDef might specify (1) fetching features from a hosted service and (2) fetching basemap tiles.

    Example WorkflowChain: ArcGIS Field Maps > Web Adaptor > Portal > AGS > Relational DataStore
    """
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
        """ 
        :returns: True if validate returns no messages.
        :rtype: bool
        """
        return len(self.validate()) == 0
    
    def validate(self) -> list[ValidationMessage]:
        """ Method that evaluates the validity of the configuration of this WorkflowChain.
        
        :returns: Messages indicating invalid configuration.
        :rtype: list[ValidationMessage]
        """
        result: list[ValidationMessage] = []
       
        msp: list[str] = self.missing_service_providers()
        if len(msp) > 0:
            for missing in msp:
                result.append(ValidationMessage(message=f'Missing service provider for {missing}', source=self.name))

        return result

    def update_client_step(self, client_step: WorkflowDefStep):
        """ Convenience method to replace the first step in the chain.
        Useful when multiple WorkflowChains differ only in their client.
        
        :param client_step: The step that will replace the first step in this chain.
        """
        self.steps.pop(0)
        self.steps.insert(0, client_step)

    def all_required_service_types(self) -> Set[str]:
        """ All of the service types specified by steps. """
        types: list[str] = list(map(lambda step: (step.service_type), self.steps))
        return set(types)
    
    def configured_service_types(self) -> Set[str]:
        """ All of the service types specified by the configured dict of :class:`ServiceProvider`."""
        return set(self.service_providers.keys())
    
    def missing_service_providers(self) -> list[str]:
        """ The service types needed by the chain, but not available in the ServiceProviders. """
        all_req: Set[str] = self.all_required_service_types()
        configured: Set[str] = self.configured_service_types()
        return list(all_req.difference(configured))
    
    def service_provider_for_step(self, step: WorkflowDefStep) -> Optional[ServiceProvider]:
        """ 
        :returns: The configured ServiceProvider that matches the service type of a step. Returns None if no ServiceProvider matches the service type of the step. 
        """
        if step.service_type in self.service_providers.keys():
            return self.service_providers[step.service_type]
        return None
    
    def service_provider_for_step_at_index(self, index: int) -> Optional[ServiceProvider]:
        """
        :returns: The configured ServiceProvider that matches the service type of the step at an index. Returns None for an invalid index or no configured ServiceProvider for the step.
        """
        if index < 0 or index >= len(self.steps): return None
        return self.service_provider_for_step(self.steps[index])


# ------------------------------------------------------------
class WorkflowDef:
    """ Represents a collection of parallel WorkflowChains and the expected think time. 
    
    :param name: A descriptive name
    :param desc: Additional descriptive text
    :param think_time_s: The expected amount of time in seconds that a user will be able to think before issuing the next request
    :param chains: A list of WorkflowChains
    """
    def __init__(self, name: str, desc: str, think_time_s: int, chains: list[WorkflowChain]):
        self.name: str = name
        self.description: str = desc
        self.think_time_s: int = think_time_s
        self.chains: list[WorkflowChain] = chains

    def all_required_service_types(self) -> Set[str]:
        """
        :returns: A list of all service types needed by all WorkflowChain steps
        """
        result: Set[str] = set()
        for chain in self.chains:
            result = result.union(chain.all_required_service_types())
        return result
    
    def assign_service_provider(self, service_provider: ServiceProvider):
        """ Assigns a ServiceProvider to all of the chains. """
        for chain in self.chains:
            chain.service_providers[service_provider.service.service_type] = service_provider
    
    def missing_service_providers(self) -> list[str]:
        """
        :returns: A list of the service types needed by one or more chains that are not satisfied by any assigned ServiceProvider.
        """
        result: Set[str] = set()
        for chain in self.chains:
            # m = chain.missing_service_providers()
            # print(chain.name + ' is missing ' + str(m))
            result = result.union(chain.missing_service_providers())
        return list(result)
    
    def clear_service_providers(self):
        """ Removes all ServiceProviders for all workflow chains. """
        for chain in self.chains:
            chain.service_providers.clear()

    def get_chain(self, name:str) -> Optional[WorkflowChain]:
        """ Find a chain by name in the list.
        
        :param name: The name of the chain to find. Case-insensitive.
        :returns: The named chain. Returns None if not found.
        """
        name_uc:str = name.upper()
        for chain in self.chains:
            if chain.name.upper() == name_uc: return chain
        return None

# ------------------------------------------------------------
class Transaction:
    """ A way to identify which group of :class:`ClientRequest` are all satisfying one triggering of a :class:`Workflow`. """
    _next_id: int = 0

    @classmethod
    def next_id(cls) -> int:
        """
        :returns: Next integer id in sequence.
        """
        cls._next_id = cls._next_id + 1
        return cls._next_id

    def __init__(self, clock: int, workflow: 'Workflow'):
        """
        :param clock: The current simulation time.
        :param workflow: The workflow that the Transaction is being generated from.
        """
        self.id: int = Transaction.next_id()
        self.request_clock: int = clock
        self.workflow: Workflow = workflow


# ------------------------------------------------------------
class ClientRequestSolution:
    """ When a Workflow is executed, a ClientRequestSolution is created for each WorkflowChain.
    The solution is then assigned to the :class:`ClientRequest`.

    A ClientRequestSolution is processed one step at a time. As steps are finished, they are popped
    off the front of the list. The solution is finished when there are no more steps to complete.
    """
    def __init__(self, steps: Optional[list[ClientRequestSolutionStep]] = None):
        self.steps: list[ClientRequestSolutionStep] = []            
        if steps is not None:
            self.steps = steps

    def is_finished(self) -> bool:
        """ :returns: True if list of steps is empty. """
        return len(self.steps) == 0
    
    def current_step(self) -> Optional[ClientRequestSolutionStep]:
        """ :returns: The first step in the remainder. Returns None if no steps remain. """
        if len(self.steps) == 0: return None
        return self.steps[0]
    
    def goto_next_step(self):
        """ Pops the first step off the remaining steps. """
        if len(self.steps) > 0:
            self.steps.pop(0) # Drop first item. 
    

# ------------------------------------------------------------
class ClientRequest:
    """ Represents a single request originating from a configured Workflow.
    ClientRequests are created by the simulator, so not usually created by other means.
    
    :param name: Usually auto-generated by next_name
    :param desc: Additional descriptive text. Usually empty.
    :param wf_name: The name of the workflow that caused this request.
    :param request_clock: The simulation time when the request was created.
    :param solution: The ClientRequestSolution that defines the work to be done.
    :param tx_id: A Workflow execution can create a set of ClientRequests. This is the Transaction id.
    """
    _next_id: int = 0

    @classmethod
    def next_id(cls) -> int:
        """ Increments and returns the next in a sequence of numbers. """
        cls._next_id = cls._next_id + 1
        return cls._next_id
    
    @classmethod
    def next_name(cls) -> str:
        """ Creates a new unique name based on next_id """
        return f'CR-{cls.next_id()}'
    
    def __init__(self, name: str, desc: str, wf_name: str,
                 request_clock: int, solution: ClientRequestSolution, tx_id: int):
        self.name: str = name
        self.description: str = desc
        self.workflow_name: str = wf_name
        self.request_clock: int = request_clock
        self.solution: ClientRequestSolution = solution
        self.tx_id: int = tx_id
        self.accumulating_metrics: list[RequestMetric] = []

    def __eq__(self, other):
        return self.name == other.name
    
    def is_finished(self) -> bool:
        """ Convenience function.
        
        :returns: True if the :class:`ClientRequestSolution` solution is finished.
        """
        return self.solution.is_finished()
    
    def summary_metric(self) -> RequestMetric:
        """ A ClientRequest accumulates a metric after every step. 
        
        :returns: A single RequestMetric that sums all of the accumulated metrics to date.
        """
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
    """ Represents a configuration of a :class:`WorkflowDef`, with productivity stats.
    The configuration of the productivity will determine how frequently this Workflow is triggered.
    
    :param name: A descriptive name.
    :param desc: Additional descriptive text
    :param definition: The workflow definition (chains, steps)
    :param type: Whether this workflow's transaction rate is calculated from numbers of users or tph.
    :param user_count: The number of users. Only applies to WorkflowType.USER.
    :param productivity: The number of transactions per minute per user. Only applies to WorkflowType.USER.
    :param tph: The transactions per hour. Only applies to WorkflowType.TRANSACTIONAL.
    """
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
        """
        :returns: The number of transactions per hour. 
        """
        if self.type == WorkflowType.USER:
            return self.user_count * self.productivity * 60
        else:
            return self.tph
    
    def create_client_requests(self, network: list[Connection], clock: int) -> Tuple[Transaction, list[ClientRequest]]:
        """ When a transaction starts, each chain needs to be solved for. This will result in one or more ClientRequests.
        
        :param network: The set of Connections that represent the network.
        :param clock: The current simulation time.
        """
        tx: Transaction = Transaction(clock, self)
        requests: list[ClientRequest] = []
        for chain in self.definition.chains:
            # for step in chain.steps:
            #     sp = chain.service_providers[step.service_type]
            #     print(f'{step.name} {sp.name} {sp.nodes[0].name}')
            solution: ClientRequestSolution = create_solution(chain, network)
            # for step in solution.steps:
            #     print(step.st_calculator.name) # type: ignore

            request: ClientRequest = ClientRequest(ClientRequest.next_name(), desc='', wf_name=self.name,
                                                   request_clock=clock, solution=solution, tx_id=tx.id)
            requests.append(request)
        return (tx,requests)
    
    def calculate_next_event_time(self, clock: int) -> int:
        """ transaction_rate is in transactions per hour, while the time between events is in ms.
        This function calculates the time between transactions and then applies a normalized random factor
        which allows a more "natural" arrival of requests.

        :param clock: The current simulation time.
        :returns: The next time this Workflow will execute.
        """
        ms_per_event: float = 3600000.0 / float(self.transaction_rate())
        r_val: int = int(random.normal(loc=ms_per_event, scale=ms_per_event*0.25, size=(1,1))[0][0])
        return clock + r_val
    
    def is_valid(self) -> bool:
        """ :returns: True if validate returns no messages. """
        return len(self.validate()) == 0

    def validate(self) -> list[ValidationMessage]:
        """ Tests the transaction rate and the validity of the definition. 
        
        :returns: A list of ValidationMessages. An empty list indicates the Workflow is valid.
        """
        result: list[ValidationMessage] = []

        if len(self.definition.chains) == 0:
            result.append(ValidationMessage(message='Need at least one configured Workflow Chain', source=self.name))
        
        invalid_chains: list[WorkflowChain] = list(filter(lambda chain: (chain.is_valid() == False), self.definition.chains))
        for chain in invalid_chains:
            result.append(ValidationMessage(message=f'Workflow Chain {chain.name} is invalid', source=self.name))

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
    """ Creates a ClientRequestSolution for one WorkflowChain. 

    This is a key part of pygissim! The request is solved by hopping from ServiceProvider to ServiceProvider.

    1. The request passes through the WorkflowChain in order
    2. The request passes back through the WorkflowChain in reverse order, back to the caller.

    For every step, the request is processed by the ServiceProvider,
    and then may take zero or more network hops to get to the next ServiceProvider.
    
    :param chain: The WorkflowChain that is being solved for.
    :param in_network: The list of Connections that represent the network.
    """
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
    """ Finds a path through the network from one Zone to another.
    
    :param start: The Zone to start finding from.
    :param end: The Zone to find the shortest path to.
    :in_network: The list of Connections that represents the network.
    :returns: The shortest route from start to end. Returns None if no route could be found.
    """
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
""" Private function called recursively to find the shortest route.

:param start: The Zone to start finding from.
:param end: The Zone to find the shortest path to.
:in_network: The list of Connections that represents the network.
:param visited: The set of Zones that has already been visited by this algorithm.
:param path: The list of Connections that represent the partly-formed route.
:returns: The list of Connections representing a route from start to end.
"""
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