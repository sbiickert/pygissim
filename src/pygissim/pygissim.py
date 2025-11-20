#!/usr/bin/env python

"""
pygissim.py

The higher-level classes: Design and Simulator.

Uses the pygissim.engine extensively to build a simulation design and then run it.
"""
from typing import Optional, Set, Tuple
from functools import cmp_to_key

from pygissim.engine import *

class Design:
    """ The Design is the setup of the infrastructure (network, compute) and the workflows that define
    the system that is being simulated.

    :param name: A descriptive name
    :param desc: Additional descriptive text
    :param network: The list of Connections that will represent the network
    :param services: The dictionary of all known service types (keys) and ServiceDefs (values)
    :param workflow_definitions: The workflows (chains, steps) that are defined in the design.
    """
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
        """ The _compute_nodes private variable only holds physical compute nodes.
        
        :returns: All ComputeNodes, including virtual nodes hosted on physical servers.
        """
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
        """ 
        :returns: True if validate returns no messages.
        :rtype: bool
        """
        return len(self.validate()) == 0

    def validate(self) -> list[ValidationMessage]:
        """ Method that evaluates the validity of the configuration of this Design.
        
        :returns: Messages indicating invalid configuration.
        :rtype: list[ValidationMessage]
        """
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
        """ Convenience method to add the Zone and a local Connection at the same time.
        
        :param zone: The Zone to add to the design.
        :param local_bw_mbps: The bandwidth of the local Connection for the Zone in megabits per second (Mbps)
        :param local_latency_ms: The latence of the local Connection for the Zone in milliseconds (ms)
        """
        if zone in self.zones: return
        self.zones.append(zone)
        internal_conn: Connection = zone.self_connect(bw=local_bw_mbps, lat=local_latency_ms)
        self.network.append(internal_conn)

    def remove_zone(self, zone: Zone):
        """ Removes a Zone from the Design. Doing so can have cascading effects, 
        so internally this method removes any Connections that touched this Zone
        and then calls :func:`update_service_providers` and :func:`update_workflow_definitions`
        because removing a Zone could have removed ComputeNodes. 
        
        :param zone: The Zone to remove.
        """
        self.zones.remove(zone)
        self.network = zone.other_connections(self.network)
        self._compute_nodes = list(filter(lambda c: (c.zone != zone), self._compute_nodes))
        # May have removed one or more ComputeNodes
        self.update_service_providers()
        self.update_workflow_definitions()

    def get_zone(self, name: str) -> Zone:
        """ Convenience method to find a Zone in the Design by name. Assumes name is unique.
        
        :param name: The name of the Zone to find.
        :returns: The named Zone.
        :raises: ValueError if no Zone exists with that name.
        """
        for zone in self.zones:
            if zone.name == name: return zone
        raise ValueError(f"No zone named {name} found.")
    

    def add_connection(self, conn: Connection, add_reciprocal: bool = False):
        """ Convenience method to add a Connection to the design with an option to add the return Connection too.
        
        :param conn: The Connection to add to the Design.
        :param add_reciprocal: If True, will add a second Connection to the Design, in the reciprocal direction.
        """
        self.network.append(conn)
        if add_reciprocal:
            self.network.append(conn.inverted())

    def remove_connection(self, conn: Connection):
        """ Convenience method to remove a Connection from the Design. """
        self.network.remove(conn)
    

    def add_compute(self, node: ComputeNode):
        """ Adds a ComputeNode to the Design. Will not allow a node with :class:`ComputeNodeType` V_SERVER to be added, since
        virtual servers are added implicitly when their physical host is added. 
        
        :param node: The ComputeNode to add to the Design.
        :raises: TypeError if node.type is ComputeNodeType.V_SERVER
        """
        if node.type == ComputeNodeType.V_SERVER:
            raise TypeError('Cannot add a virtual server to design. Add to physical host.')
        self._compute_nodes.append(node)

    def remove_compute(self, node: ComputeNode):
        """ Removes a ComputeNode from the Design. May have cascading effects, 
        so internally this method calls :func:`update_service_providers` and :func:`update_workflow_definitions`
        because the removed ComputeNode could have been a node for a ServiceProvider. 
        
        Because of the above logic, this is the best way to remove a V_SERVER.

        :param node: The ComputeNode to add to the Design.
        """
        if node.type == ComputeNodeType.V_SERVER:
            for n in self._compute_nodes:
                if n.type == ComputeNodeType.P_SERVER and node in n._v_hosts:
                    n.remove_virtual_host(node)
        else:
            self._compute_nodes.remove(node)
        self.update_service_providers()
        self.update_workflow_definitions()

    def get_compute_node(self, name: str) -> ComputeNode:
        """ Convenience method to find a ComputeNode by name in the Design.
        
        :param name: The name of the ComputeNode to find.
        :returns: The named ComputeNode.
        :raises: ValueError if no ComputeNode exists with that name.

        """
        for node in self.compute_nodes(): # Includes V_SERVER nodes
            if node.name == name: return node
        raise ValueError(f"No compute node named {name} found.")

    def add_servicedef(self, sd: ServiceDef):
        """ Convenience method to add a service definition. If a ServiceDefinition with the same
        service_type exists, it will be replaced. 
        
        :param sd: The ServiceDef to add.
        """
        self.services[sd.service_type] = sd

    def remove_servicedef(self, sd: ServiceDef):
        """ Removes a ServiceDef from the Design. May have cascading effects, 
        so internally this method calls :func:`update_service_providers` and :func:`update_workflow_definitions`
        because the removed ServiceDef could have been a type for a ServiceProvider. 
        
        :param sd: The ServiceDef to remove.
        """
        if sd.service_type in self.services.keys():
            self.services.pop(sd.service_type)
        # Might have removed a whole type of provider
        self.update_service_providers()
        self.update_workflow_definitions()


    def add_service_provider(self, sp: ServiceProvider):
        """ Adds a ServiceProvider to the Design. Internally checks that this 
        ServiceProvider does not already exist in the Design.
        
        :param sp: The ServiceProvider to add.
        """
        if sp not in self.service_providers:
            self.service_providers.append(sp)

    def remove_service_provider(self, sp: ServiceProvider):
        """ Removes a ServiceProvider from the Design. May have cascading effects, 
        so internally this method calls :func:`update_workflow_definitions`
        because the removed ServiceProvider could have been referenced in a WorkflowDef. 
        
        :param sp: The ServiceProvider to remove.
        """
        self.service_providers.remove(sp)
        self.update_workflow_definitions()

    def get_service_providers_with_tag(self, tag: str) -> list[ServiceProvider]:
        """
        :param tag: The tag to find.
        :returns: A list of service providers that have the given tag.
        """
        return list(filter(lambda sp: (tag in sp.tags), self.service_providers))


    def add_workflowdef(self, wdef: WorkflowDef):
        """ Convenience method to add a WorkflowDef to the Design.
        
        :param wdef: The WorkflowDef to add.
        """
        self.workflow_definitions.append(wdef)

    def remove_workflowdef(self, wdef: WorkflowDef):
        """ Removes a WorkflowDef from the Design. May have cascading effects, 
        so internally this method calls :func:`update_configured_workflows`
        because the removed WorkflowDef could have been referenced in a Workflow. 
        
        :param wdef: The WorkflowDef to remove.
        """
        self.workflow_definitions.remove(wdef)
        self.update_configured_workflows()

    def get_workflowdef(self, name: str) -> WorkflowDef:
        """ Convenience method to find a WorkflowDef by name in the Design.
        
        :param name: The name of the WorkflowDef to find.
        :returns: The named WorkflowDef.
        :raises: ValueError if no workflow definition has that name.
        """
        for wdef in self.workflow_definitions:
            if wdef.name == name: return wdef
        raise ValueError(f"No workflow definition named {name} found.")


    def add_client_workflow(self, name:str, desc: str, wdef_name: str, users: int, productivity: int) -> Workflow:
        """ Convenience method to configure a USER Workflow from a named WorkflowDef in the Design.

        :param name: The name of the new configured Workflow.
        :param desc: Addtional descriptive text.
        :param wdef_name: The name of the WorkflowDef that will define the new Workflow.
        :param users: The number of users.
        :param productivity: The number of transactions per minute per user.
        :raises: ValueError if wdef_name is not a known name of a WorkflowDef in the Design.
        :returns: The created Workflow.
        """
        wdef: WorkflowDef = self.get_workflowdef(wdef_name)
        w: Workflow = Workflow(name=name, desc=desc, 
                               type=WorkflowType.USER, definition=wdef, 
                               user_count=users, productivity=productivity)
        self._workflows.append(w)
        return w

    def add_transactional_workflow(self, name:str, desc: str, wdef_name: str, tph: int) -> Workflow:
        """ Convenience method to configure a TRANSACTIONAL Workflow from a named WorkflowDef in the Design.

        :param name: The name of the new configured Workflow.
        :param desc: Addtional descriptive text.
        :param wdef_name: The name of the WorkflowDef that will define the new Workflow.
        :param tph: The number of transactions per hour.
        :raises: ValueError if wdef_name is not a known name of a WorkflowDef in the Design.
        :returns: The created Workflow.
        """
        wdef: WorkflowDef = self.get_workflowdef(wdef_name)
        w: Workflow = Workflow(name=name, desc=desc, 
                               type=WorkflowType.TRANSACTIONAL, definition=wdef, 
                               tph=tph)
        self._workflows.append(w)
        return w

    def remove_workflow(self, w: Workflow):
        """ Convenience method to remove a Workflow from the Design.
        
        :raises: ValueError if the Workflow does not exist in the Design.
        """
        self._workflows.remove(w)

    def get_workflow(self, name: str) -> Workflow:
        """ Convenience method to find a Workflow by name in the Design.
        
        :param name: The name of the Workflow to find.
        :returns: The named Workflow.
        :raises: ValueError if no workflow has that name.
        """
        for w in self._workflows:
            if w.name == name: return w
        raise ValueError(f"No workflow named {name} found.")
    
    def all_workflows(self) -> list[Workflow]:
        """ Returns a list of all workflows in the Design. """
        return self._workflows.copy()

    def update_service_providers(self):
        """ Function to be called if a change has been made that may invalidate one or more ServiceProviders.
        Examples of changes that might do this:
        
        - A ServiceDef has been removed
        - A ComputeNode has been removed
        """
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

    def update_workflow_definitions(self):
        """ Function to be called if a change has been made that may invalidate one or more WorkflowDefs.
        Examples of changes that might do this:
        
        - A ServiceProvider has been removed.
        """
        for wdef in self.workflow_definitions:
            for chain in wdef.chains:
                remaining: dict[str, ServiceProvider] = dict()
                for sp in chain.service_providers.values():
                    if sp in self.service_providers:
                        remaining[sp.service.service_type] = sp
                chain.service_providers = remaining

    def update_configured_workflows(self):
        """ Function to be called if a change has been made that may invalidate one or more Workflows.
        Examples of changes that might do this:
        
        - A WorkflowDef has been removed.
        """
        remaining: list[Workflow] = []

        for w in self._workflows:
            if w.definition in self.workflow_definitions:
                remaining.append(w)

        self._workflows = remaining

    def provide_queues(self) -> list[MultiQueue]:
        """ Calls :func:`provide_queue` on every ComputeNode and Connection in the Design.
        
        :returns: A list of all queues in the Design.
        """
        conn_queues: list[MultiQueue] = list(map(lambda c: (c.provide_queue()), self.network))
        comp_queues: list[MultiQueue] = list(map(lambda c: (c.provide_queue()), self.compute_nodes()))
        return conn_queues + comp_queues

    def print_validation_messages(self):
        """ Method to print out all validation messages in all components of the Design. 
        Useful for debugging if :func:`Design.is_valid` is returning False.
        """
        if not self.is_valid():
            for vm in self.validate():
                print(f'{vm}')
            for w in self._workflows:
                for vm in w.validate():
                    print(f'{vm}')
                for chain in w.definition.chains:
                    for wc_vm in chain.validate():
                        print(f'{wc_vm}')
            for sp in self.service_providers:
                for vm in sp.validate():
                    print(f'{vm}')

    _next_id: int = 0
    @classmethod
    def next_id(cls) -> int:
        """ :returns: The next number in a sequence of Design ids. """
        cls._next_id = cls._next_id + 1
        return cls._next_id

    @classmethod
    def next_name(cls) -> str:
        """ :returns: The next name in a sequence of Design names. """
        return f'Design {cls.next_id()}'

class Simulator:
    """ The Simulator is the functional part of pygissim.
    The Simulator takes the setup from a Design and runs it.
    
    :param name: A descriptive name
    :param desc: Additional descriptive text
    :param design: A design to simulate. If not specified, an empty Design will be created.
    """
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
        """ Puts the Simulator into a mode where time can be moved forward and requests will be generated.

        Normal process for running the Simulator:

        1. :func:`start`
        2. :func:`advance_time_by` or :func:`advance_time_to`
        3. :func:`stop`

        The simulator time does not advance by itself, code to run the simulator might look like::

            sim.start()
            for i in range(1,500):
                sim.advance_time_by(500)
                sim.gather_queue_metrics()
            sim.stop()
        
        :raises: ValueError if the Design is None or the Design is not valid.
        """
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
        """ Stops the Simulator from generating new requests.
        Time can still be advanced, and existing requests will continue to be processed until 
        they are all finished. 
        """
        self.is_generating_new_requests = False

    def reset(self):
        """ Resets the clock and all artifacts from previous run(s). Is called by :func:`start`. """
        self.clock = 0
        self.finished_requests.clear()
        self._next_event_time_for_workflows.clear()
        self.queue_metrics.clear()
        self.request_metrics.clear()
        self.queues = self.design.provide_queues() if self.design is not None else []

    def next_event_time(self) -> Optional[int]:
        """ If the simulator is started or there are existing requests in the system, then
        there will be a clock value in the future when the next event happens, such as a new
        Transaction starting or a request being finished processing or passing through a Connection.
        
        :returns: The clock time when the next thing happens. Returns None if there are no forecast events.
        """
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
                wf: Workflow = self.design.get_workflow(name)
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
        """ Moves the Simulator clock forward by a number of milliseconds.
        
        :param ms: The number of milliseconds to move the clock forward.
        :returns: The new Simulator clock.
        :raises: ValueError if ms is negative or zero.
        """
        if ms <= 0: raise ValueError('Cannot advance time by a negative amount or zero.')
        return self.advance_time_to(self.clock + ms)

    def advance_time_to(self, clock: int) -> int:
        """ Moves the Simulator clock forward to a specific future time.
        
        :param clock: The future Simulator time to go to.
        :returns: The new Simulator clock (will be the same as the clock parameter passed)
        :raises: ValueError if the specified clock time is not after the current Simulator clock.
        """
        if clock <= self.clock: raise ValueError(f'Cannot set clock to {clock}, which is before or equal to current clock ({self.clock}).')
        
        time: Optional[int] = self.next_event_time()
        while time is not None and time <= clock:
            # print(f'{time} <= {clock}')
            self._do_the_next_task()
            time = self.next_event_time()

        self.clock = clock
        return self.clock

    def _do_the_next_task(self):
        """ Finds the next event that happens chronologically and executes it. """
        if self.design is None: return

        # Next work might be a new Transaction or an existing request has finished waiting.
        next_work: Optional[Tuple[Workflow, int]] = self._next_workflow()
        next_queue: Optional[Tuple[MultiQueue, int]] = self._next_queue()

        requests: list[ClientRequest] = []

        now: int = 0
        if next_work is not None and (next_queue is None or next_work[1] < next_queue[1]):
            # print(next_work[0].name)
            g_r: Tuple[Transaction, list[ClientRequest]] = next_work[0].create_client_requests(self.design.network, next_work[1])
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
        # This assertion is no longer valid: moving from latency to queue to channel will not result in
        # request being returned.
        # assert len(requests) > 0

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
        """ Convenience method to find the MultiQueue that has a particular :class:`pygissim.engine.ComputeNode` 
        or :class:`pygissim.engine.Connection` as its :class:`pygissim.engine.ServiceTimeCalculator`.

        :param st_calc: The ComputeNode or Connection.
        :returns: The MultiQueue that has st_calc as its service time calculator. Returns None if no queue is found. 
        """
        result: Optional[MultiQueue] = None
        for q in self.queues:
            if q.service_time_calculator == st_calc: return q
        return result

    def active_requests(self) -> list[WaitingRequest]:
        """ :returns: A list of all requests in all queues. """
        result: list[WaitingRequest] = []

        for q in self.queues:
            result.extend(q.all_waiting_requests())

        return result

    def gather_queue_metrics(self):
        """ Gathers a :class:`pygissim.engine.QueueMetric` from every queue and stores it in queue_metrics.
        Sorts the queues so that physical servers are last: their VMs will have had a chance to register
        their utilization first.
        """
        sorted_queues: list[MultiQueue] = sorted(self.queues, key=cmp_to_key(_sort_queues))
        for q in sorted_queues:
            qm: QueueMetric = q.get_performance_metric(self.clock)
            self.queue_metrics.append(qm)

            #Apply work to the physical host
            if q.type() == 'V_SERVER': 
                vm: ComputeNode = q.service_time_calculator # type: ignore
                if self.design is not None:
                    for cn in self.design._compute_nodes:
                        if cn.is_physical_host_for(vm):
                            host_q: Optional[MultiQueue] = self.find_queue(cn)
                            if host_q is not None:
                                host_q.work_done = host_q.work_done + qm.work

    
def _sort_queues(q1:MultiQueue, q2:MultiQueue) -> int:
    type1: str = q1.type()
    type2: str = q2.type()
    if type1 == 'P_SERVER' and not type2 == 'P_SERVER':
        return 1
    if type2 == 'P_SERVER' and not type1 == 'P_SERVER':
        return -1
    return 0
    


if __name__ == "__main__":
    print("pygissim is a library.")
