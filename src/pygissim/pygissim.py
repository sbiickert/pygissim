#!/usr/bin/env python
from typing import Optional, Set, Tuple

from pygissim.engine import *

class Design:
    def __init__(self, name: str, desc: str, 
                 zones: Optional[list[Zone]] = None, 
                 network: Optional[list[Connection]] = None, 
                 services: Optional[dict[str, ServiceDef]] = None,
                 service_providers: Optional[list[ServiceProvider]] = None,
                 workflow_definitions: Optional[list[WorkflowDef]] = None):
        self.name: str = name
        self.description: str = desc
        self.zones: list[Zone] = [] if zones is None else zones
        self.network: list[Connection] = [] if network is None else network
        self.services: dict[str, ServiceDef] = dict() if services is None else services                    # Just defining "map", "dbms", etc.
        self.service_providers: list[ServiceProvider] = [] if service_providers is None else service_providers  # All service providers
        self.workflow_definitions: list[WorkflowDef] = [] if workflow_definitions is None else workflow_definitions

        self._workflows: list[Workflow] = []
        self._compute_nodes: list[ComputeNode] = []

    def compute_nodes(self) -> list[ComputeNode]:
        result: list[ComputeNode] = []

        for node in self._compute_nodes:
            match node.type:
                case ComputeNodeType.CLIENT:
                    result.append(node)
                case ComputeNodeType.P_SERVER:
                    result.extend([node] + node._v_hosts)
                case ComputeNodeType.V_SERVER:
                    raise TypeError(f'Virtual servers should not be in the _compute_nodes property')
        return result
    
    def is_valid(self) -> bool:
        return len(self.validate()) == 0

    def validate(self) -> list[ValidationMessage]:
        messages: list[ValidationMessage] = []

        all_sps_valid: bool = len(list(filter(lambda sp: (sp.is_valid() == False), self.service_providers))) == 0
        all_zones_connected: bool = len(list(filter(lambda z: (z.is_fully_connected(self.network) == False), self.zones))) == 0
        all_workflows_valid: bool = len(list(filter(lambda w: (w.is_valid() == False), self._workflows))) == 0

        for w in self.all_workflows():
            for chain in w.definition.chains:
                for sp in chain.service_providers.values():
                    for node in sp.nodes:
                        in_net: bool = node.zone in Zone.all_zones(self.network)
                        if not in_net: messages.append(ValidationMessage(f'Node {node.name} is in zone {node.zone.name} which is not in network', source=sp.name))
        

        if not all_sps_valid:
            messages.append(ValidationMessage(message='Not all service providers are valid.', source=self.name))
        if not all_zones_connected:
            messages.append(ValidationMessage(message='Not all zones are fully connected.', source=self.name))
        if not all_workflows_valid:
            messages.append(ValidationMessage(message='One or more invalid workflows.', source=self.name))
        if len(self.zones) == 0:
            messages.append(ValidationMessage(message='No zones defined.', source=self.name))
        if len(self.network) == 0:
            messages.append(ValidationMessage(message='No network defined.', source=self.name))
        if len(self.compute_nodes()) == 0:
            messages.append(ValidationMessage(message='No compute nodes configured.', source=self.name))
        if len(self.workflow_definitions) == 0:
            messages.append(ValidationMessage(message='No workflows defined.', source=self.name))
        if len(self._workflows) == 0:
            messages.append(ValidationMessage(message='No workflows configured.', source=self.name))
        if len(self.services) == 0:
            messages.append(ValidationMessage(message='No service types defined.', source=self.name))

        return messages
    

    def add_zone(self, zone: Zone, local_bw_mbps: int, local_latency_ms: int):
        if zone in self.zones: return
        self.zones.append(zone)
        internal_conn: Connection = zone.self_connect(bw=local_bw_mbps, lat=local_latency_ms)
        self.network.append(internal_conn)

    def remove_zone(self, zone: Zone):
        self.zones.remove(zone)
        self.network = zone.other_connections(self.network)
        self._compute_nodes = list(filter(lambda c: (c.zone != zone), self._compute_nodes))
        # May have removed one or more ComputeNodes
        self.update_service_providers()
        self.update_workflow_definitions()

    def get_zone(self, name: str) -> Optional[Zone]:
        for zone in self.zones:
            if zone.name == name: return zone
        return None
    

    def add_connection(self, conn: Connection, add_reciprocal: bool = False):
        self.network.append(conn)
        if add_reciprocal:
            self.network.append(conn.inverted())

    def remove_connection(self, conn: Connection):
        self.network.remove(conn)
    

    def add_compute(self, node: ComputeNode):
        if node.type == ComputeNodeType.V_SERVER:
            raise TypeError('Cannot add a virtual server to design. Add to physical host.')
        self._compute_nodes.append(node)

    def remove_compute(self, node: ComputeNode):
        if node.type == ComputeNodeType.V_SERVER:
            raise TypeError('Cannot remove a virtual server from design. Remove from physical host.')
        self._compute_nodes.remove(node)
        self.update_service_providers()
        self.update_workflow_definitions()

    def get_compute_node(self, name: str) -> Optional[ComputeNode]:
        for node in self.compute_nodes():
            if node.name == name: return node
        return None

    def add_servicedef(self, sd: ServiceDef):
        self.services[sd.service_type] = sd

    def remove_servicedef(self, sd: ServiceDef):
        if sd.service_type in self.services.keys():
            self.services.pop(sd.service_type)
        # Might have removed a whole type of provider
        self.update_service_providers()
        self.update_workflow_definitions()


    def add_service_provider(self, sp: ServiceProvider):
        if sp not in self.service_providers:
            self.service_providers.append(sp)

    def remove_service_provider(self, sp: ServiceProvider):
        self.service_providers.remove(sp)
        self.update_workflow_definitions()


    def add_workflowdef(self, wdef: WorkflowDef):
        self.workflow_definitions.append(wdef)

    def remove_workflowdef(self, wdef: WorkflowDef):
        self.workflow_definitions.remove(wdef)
        self.update_configured_workflows()

    def get_workflowdef(self, name: str) -> Optional[WorkflowDef]:
        for wdef in self.workflow_definitions:
            if wdef.name == name: return wdef
        return None


    def add_client_workflow(self, name:str, desc: str, wdef: WorkflowDef, users: int, productivity: int):
        w: Workflow = Workflow(name=name, desc=desc, 
                               type=WorkflowType.USER, definition=wdef, 
                               user_count=users, productivity=productivity)
        self._workflows.append(w)

    def add_transactional_workflow(self, name:str, desc: str, wdef: WorkflowDef, tph: int):
        w: Workflow = Workflow(name=name, desc=desc, 
                               type=WorkflowType.TRANSACTIONAL, definition=wdef, 
                               tph=tph)
        self._workflows.append(w)

    def remove_workflow(self, w: Workflow):
        self._workflows.remove(w)

    def get_workflow(self, name: str) -> Optional[Workflow]:
        for w in self._workflows:
            if w.name == name: return w
        return None
    
    def all_workflows(self) -> list[Workflow]:
        return self._workflows.copy()

    ### A ServiceDef or ComputeNode has been removed. ###
    def update_service_providers(self):
        remaining: list[ServiceProvider] = []

        for sp in self.service_providers:
            if sp.service in self.services.values(): remaining.append(sp)

        for sp in remaining:
            remaining_nodes: list[ComputeNode] = []
            all_nodes: list[ComputeNode] = self.compute_nodes()
            for node in sp.nodes:
                if node in all_nodes:
                    remaining_nodes.append(node)
            sp.nodes = remaining_nodes

        self.service_providers = remaining

    ### A ServiceProvider has been removed. ###
    def update_workflow_definitions(self):
        for wdef in self.workflow_definitions:
            for chain in wdef.chains:
                remaining: dict[str, ServiceProvider] = dict()
                for sp in chain.service_providers.values():
                    if sp in self.service_providers:
                        remaining[sp.service.service_type] = sp
                chain.service_providers = remaining

    ### WorkflowDef has been removed. ###
    def update_configured_workflows(self):
        remaining: list[Workflow] = []

        for w in self._workflows:
            if w.definition in self.workflow_definitions:
                remaining.append(w)

        self._workflows = remaining

    def provide_queues(self) -> list[MultiQueue]:
        conn_queues: list[MultiQueue] = list(map(lambda c: (c.provide_queue()), self.network))
        comp_queues: list[MultiQueue] = list(map(lambda c: (c.provide_queue()), self.compute_nodes()))
        return conn_queues + comp_queues

    def print_validation_messages(self):
        if not self.is_valid():
            for vm in self.validate():
                print(f'Design {vm}')
            for w in self._workflows:
                for vm in w.validate():
                    print(f'Workflow {vm}')
                    for chain in w.definition.chains:
                        for wc_vm in chain.validate():
                            print(f'Workflow chain {vm}')
            for sp in self.service_providers:
                for vm in sp.validate():
                    print(f'Service provider {vm}')

    _next_id: int = 0
    @classmethod
    def next_id(cls) -> int:
        cls._next_id = cls._next_id + 1
        return cls._next_id

    @classmethod
    def next_name(cls) -> str:
        return f'Design {cls.next_id()}'

class Simulator:
    def __init__(self, name: str, desc: str, design: Optional[Design] = None):
        self.name: str = name
        self.description: str = desc
        self.design: Optional[Design] = design

        self.request_metering_mode: str = 'summary'
        self.clock: int = 0
        self.is_generating_new_requests: bool = False
        self.finished_requests: list[ClientRequest] = list()
        self.queues: list[MultiQueue] = list()
        self._next_event_time_for_workflows: dict[str, int] = dict()
        self.queue_metrics: list[QueueMetric] = list()
        self.request_metrics: list[RequestMetric] = list()

    def start(self):
        if self.design is None:
            raise ValueError('design has not been set')
        if self.design.is_valid() == False:
            self.design.print_validation_messages()
            raise ValueError('design is not valid')
        self.reset()
        self.is_generating_new_requests = True
        for wf in self.design.all_workflows():
            self._next_event_time_for_workflows[wf.name] = wf.calculate_next_event_time(self.clock)
        
    def stop(self):
        self.is_generating_new_requests = False

    def reset(self):
        self.clock = 0
        self.finished_requests.clear()
        self._next_event_time_for_workflows.clear()
        self.queue_metrics.clear()
        self.request_metrics.clear()
        self.queues = self.design.provide_queues() if self.design is not None else []

    def next_event_time(self) -> Optional[int]:
        times: list[int] = []
        wf_t: Optional[Tuple[Workflow, int]] = self._next_workflow()
        q_t: Optional[Tuple[MultiQueue, int]] = self._next_queue()
        if wf_t is not None: times.append(wf_t[1]) 
        if q_t is not None: times.append(q_t[1])
        if len(times) > 0: return min(times)
        return None

    def _next_workflow(self) -> Optional[Tuple[Workflow, int]]:
        result: Optional[Tuple[Workflow,int]] = None
        if self.design is not None and self.is_generating_new_requests:
            for name, time in self._next_event_time_for_workflows.items():
                wf: Optional[Workflow] = self.design.get_workflow(name)
                if wf is None: raise ValueError(f'Could not find workflow named {name} in design workflows.')
                if result is None or time < result[1]:
                    result = (wf, time)
        return result

    def _next_queue(self) -> Optional[Tuple[MultiQueue, int]]:
        result: Optional[Tuple[MultiQueue,int]] = None
        for q in self.queues:
            q_t: Optional[int] = q.next_event_time()
            if q_t is not None:
                if result is None or q_t < result[1]:
                    result = (q, q_t)
        return result
    
    def advance_time_by(self, ms: int) -> int:
        if ms <= 0: raise ValueError('Cannot advance time by a negative amount or zero.')
        return self.advance_time_to(self.clock + ms)

    def advance_time_to(self, clock: int) -> int:
        if clock <= self.clock: raise ValueError(f'Cannot set clock to {clock}, which is before or equal to current clock ({self.clock}).')
        
        time: Optional[int] = self.next_event_time()
        while time is not None and time <= clock:
            # print(f'{time} <= {clock}')
            self.do_the_next_task()
            time = self.next_event_time()

        self.clock = clock
        return self.clock

    def do_the_next_task(self):
        if self.design is None: return
        next_work: Optional[Tuple[Workflow, int]] = self._next_workflow()
        next_queue: Optional[Tuple[MultiQueue, int]] = self._next_queue()

        requests: list[ClientRequest] = []

        now: int = 0
        if next_work is not None and (next_queue is None or next_work[1] < next_queue[1]):
            # print(next_work[0].name)
            g_r: Tuple[ClientRequestGroup, list[ClientRequest]] = next_work[0].create_client_requests(self.design.network, next_work[1])
            requests = g_r[1]
            now = next_work[1]
            self._next_event_time_for_workflows[next_work[0].name] = next_work[0].calculate_next_event_time(now)

        elif next_queue is not None and (next_work is None or next_queue[1] <= next_work[1]):
            # print(next_queue[0].name())
            reqs_and_metrics: list[Tuple[ClientRequest, RequestMetric]] = next_queue[0].remove_finished_requests(next_queue[1])
            for rm in reqs_and_metrics:
                # self.request_metrics.append(rm[1]) # Ignoring the returned metric. Will add when request is finished.
                rm[0].solution.goto_next_step()
                requests.append(rm[0])
            now = next_queue[1]

        # print(f'There are {len(requests)} requests')
        assert len(requests) > 0

        for req in requests:
            if req.is_finished():
                self.finished_requests.append(req)
                if self.request_metering_mode == 'DEBUG':
                    self.request_metrics.extend(req.accumulating_metrics)
                else:
                    self.request_metrics.append(req.summary_metric())
            else:
                step: Optional[ClientRequestSolutionStep] = req.solution.current_step()
                if step is None: raise ValueError('No step in solution that is not finished?')
                st_calc = step.st_calculator
                queue: Optional[MultiQueue] = self.find_queue(st_calc)
                if queue is None:
                    raise ValueError(f'Could not find queue corresponding to ST Calculator {st_calc.name}') # type: ignore
                else:
                    # print(f'Putting request {req.name} in queue {queue.name()}')
                    queue.enqueue(req, clock=now)

    def find_queue(self, st_calc: ServiceTimeCalculator) -> Optional[MultiQueue]:
        result: Optional[MultiQueue] = None
        for q in self.queues:
            if q.service_time_calculator == st_calc: return q
        return result

    def active_requests(self) -> list[WaitingRequest]:
        result: list[WaitingRequest] = []

        for q in self.queues:
            result.extend(q.all_waiting_requests())

        return result
    
    def gather_queue_metrics(self):
        for q in self.queues:
            qm: QueueMetric = q.get_performance_metric(self.clock)
            self.queue_metrics.append(qm)

if __name__ == "__main__":
    print("pygissim is a library.")
